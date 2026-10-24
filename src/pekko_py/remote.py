"""Remoting: actors on other pekko-py nodes, addressed by path, over TCP.

    async with ActorSystem(guardian, "a", address=("10.0.0.1", 2552)) as system:
        greeter = system.resolve("pekko://b@10.0.0.2:2552/user/greeter")
        greeter.tell(Greet("pekko", reply_to))        # Greet is @remotable

Modelled on Apache Pekko's Artery and narrowed: no membership, no remote watch, no quarantine. A node
has a UniqueAddress (host, port, and a uid per incarnation). Frames are length-prefixed JSON. A message
for a path nobody owns is a dead letter on the receiving node.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import secrets
import struct
import weakref
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .ref import ActorRef, LocalRef
from .serialization import Json, Serialization, SerializationError, remotable

_log = logging.getLogger("pekko_py.remote")
MAX_FRAME = 16 * 2**20
QUEUE_BOUND = 1000  # frames held for a peer while its connection is being opened

if TYPE_CHECKING:
    from .cell import ActorCell
    from .system import ActorSystem


@remotable
@dataclass(frozen=True)
class Address:
    """A system's name and listening address. Ordered by (host, port), as Pekko's addressOrdering."""

    name: str
    host: str
    port: int

    @staticmethod
    def parse(text: str) -> Address:
        """``pekko://name@host:port``."""
        if not text.startswith("pekko://") or "@" not in text or ":" not in text.rpartition("@")[2]:
            raise ValueError(f"not an address: {text!r}")
        name, _, hostport = text[len("pekko://") :].rstrip("/").rpartition("@")
        host, _, port = hostport.rpartition(":")
        return Address(name, host, int(port))

    @property
    def root(self) -> str:
        return f"pekko://{self.name}@{self.host}:{self.port}"

    def __lt__(self, other: Address) -> bool:
        return (self.host, self.port) < (other.host, other.port)


@remotable
@dataclass(frozen=True)
class UniqueAddress:
    """Where a system listens, plus a uid that changes every time the process restarts."""

    host: str
    port: int
    uid: int
    name: str

    @property
    def address(self) -> Address:
        return Address(self.name, self.host, self.port)

    @property
    def authority(self) -> str:
        return f"{self.name}@{self.host}:{self.port}"

    @property
    def root(self) -> str:
        return f"pekko://{self.authority}"

    def __lt__(self, other: UniqueAddress) -> bool:
        return (self.host, self.port, self.uid) < (other.host, other.port, other.uid)


class RemoteRef[T](ActorRef[T]):
    """A ref to an actor on another node. tell serializes and sends; offer awaits the connection's backpressure."""

    __slots__ = ("_path", "_remoting")

    def __init__(self, path: str, remoting: Remoting) -> None:
        self._path = path
        self._remoting = remoting

    @property
    def path(self) -> str:
        return self._path

    def tell(self, msg: T) -> None:
        self._remoting.send(self._path, msg)

    async def offer(self, msg: T) -> None:
        await self._remoting.offer(self._path, msg)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RemoteRef) and other.path == self._path

    def __hash__(self) -> int:
        return hash(self._path)


class TempRefs:
    """Paths for refs that have no actor behind them: ask reply refs, adapters, entity refs.
    Weakly held, so an entry lives exactly as long as the ref it stands for."""

    def __init__(self, root: str) -> None:
        self._root = root
        self._by_path: weakref.WeakValueDictionary[str, ActorRef[Any]] = weakref.WeakValueDictionary()
        self._paths: weakref.WeakKeyDictionary[ActorRef[Any], str] = weakref.WeakKeyDictionary()
        self._n = 0

    def register(self, ref: ActorRef[Any]) -> str:
        path = self._paths.get(ref)
        if path is None:
            self._n += 1
            path = f"{self._root}/temp/${self._n}"
            self._by_path[path] = ref
            self._paths[ref] = path
        return path

    def get(self, path: str) -> ActorRef[Any] | None:
        return self._by_path.get(path)


def resolve_local(system: ActorSystem[Any], path: str) -> ActorRef[Any]:
    """The local ref at path, walking the actor tree from the root. LookupError if there is none."""
    root = system.address.root if system.address is not None else f"pekko://{system.name}"
    if not path.startswith(root + "/"):
        raise LookupError(f"{path} is not on this system ({root})")
    segments = path[len(root) + 1 :].split("/")
    cell: ActorCell[Any] | None
    match segments[0]:
        case "user":
            cell = system._root  # pyright: ignore[reportPrivateUsage]
        case "system":
            cell = system._system_root  # pyright: ignore[reportPrivateUsage]
        case _:
            cell = None
    if cell is None:
        raise LookupError(f"{path}: only /user and /system paths resolve")
    for name in segments[1:]:
        child = cell.children.get(name)
        if child is None:
            raise LookupError(f"no actor at {path}")
        cell = child
    return cell.ref


def pack(obj: Json) -> bytes:
    body = json.dumps(obj, separators=(",", ":")).encode()
    return struct.pack(">I", len(body)) + body


async def read_frame(reader: asyncio.StreamReader) -> Json | None:
    """The next frame, or None at a clean end of stream. Raises ValueError for an oversized frame."""
    try:
        (n,) = struct.unpack(">I", await reader.readexactly(4))
        if n > MAX_FRAME:
            raise ValueError(f"frame of {n} bytes exceeds {MAX_FRAME}")
        return json.loads(await reader.readexactly(n))
    except asyncio.IncompleteReadError:
        return None


def _authority_of(path: str) -> tuple[str, int]:
    """(host, port) from pekko://name@host:port/...; LookupError if the path is not shaped like that."""
    parts = path.split("/", 3)
    if len(parts) < 4 or not path.startswith("pekko://") or "@" not in parts[2]:
        raise LookupError(f"{path} is not a remote path (pekko://name@host:port/...)")
    _, _, hostport = parts[2].rpartition("@")
    host, _, port = hostport.rpartition(":")
    if not host or not port.isdigit():
        raise LookupError(f"{path} has no host:port")
    return host, int(port)


class _Association:
    """The outbound connection to one peer address, opened on first use."""

    def __init__(self, remoting: Remoting, host: str, port: int) -> None:
        self.remoting = remoting
        self.host, self.port = host, port
        self.writer: asyncio.StreamWriter | None = None
        self.pending: deque[tuple[str, object, bytes]] = deque()
        self.connected = asyncio.Event()
        self.task = asyncio.get_running_loop().create_task(self._run())

    def send(self, path: str, msg: object, frame: bytes) -> None:
        if self.writer is not None:
            self.writer.write(frame)
        elif len(self.pending) < QUEUE_BOUND:
            self.pending.append((path, msg, frame))
        else:
            self.remoting.system.dead_letter(msg, RemoteRef(path, self.remoting))

    async def drain(self) -> None:
        await self.connected.wait()
        if self.writer is not None:
            with contextlib.suppress(OSError):  # the connection died under us: the frame is a lost tell, as any
                await self.writer.drain()

    async def _run(self) -> None:
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
        except OSError as e:
            _log.info("cannot connect to %s:%s: %s", self.host, self.port, e)
            for path, msg, _ in self.pending:
                self.remoting.system.dead_letter(msg, RemoteRef(path, self.remoting))
            self.pending.clear()
            self.remoting.drop(self)
            self.connected.set()
            return
        writer.write(self.remoting.hello())
        frames = [frame for _path, _msg, frame in self.pending]  # only the bytes: keep no message alive
        self.pending.clear()
        for frame in frames:
            writer.write(frame)
        self.writer = writer
        self.connected.set()
        try:
            await self.remoting.read_loop(reader)
        finally:
            self.remoting.drop(self)
            self.connected.set()  # a cancelled connect must not leave an offer waiting
            for path, msg, _ in self.pending:
                self.remoting.system.dead_letter(msg, RemoteRef(path, self.remoting))
            self.pending.clear()
            await _close(writer)


async def _close(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with contextlib.suppress(OSError, asyncio.CancelledError):
        await writer.wait_closed()


class Remoting:
    """One per system with an address. Owns the temp table, the listener and the connections."""

    def __init__(self, system: ActorSystem[Any], address: UniqueAddress) -> None:
        self.system = system
        self.address = address
        self.temp = TempRefs(address.root)
        self._associations: dict[tuple[str, int], _Association] = {}
        self._peers: dict[tuple[str, int], int] = {}  # last uid seen per peer address
        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._closed = False
        self.listening = asyncio.Event()
        """Set once listen() has run, however it went: the cluster waits for it before joining."""

    def ref_path(self, ref: ActorRef[Any]) -> str:
        """The path another node can send to: a local or remote ref's own path, else a temp path here."""
        if isinstance(ref, LocalRef | RemoteRef) or ref is self.system or ref is self.system.ignore_ref:
            return ref.path  # the system itself is its guardian's path, which resolves on this node
        return self.temp.register(ref)

    def resolve(self, path: str) -> ActorRef[Any]:
        if path.startswith(self.address.root + "/temp/"):
            ref = self.temp.get(path)
            if ref is None:
                raise LookupError(f"{path} has expired")
            return ref
        if path == self.system.ignore_ref.path:
            return self.system.ignore_ref
        if path.startswith(self.address.root + "/"):
            return resolve_local(self.system, path)
        _authority_of(path)  # validates host:port
        return RemoteRef(path, self)

    # --- outbound -------------------------------------------------------------------------------

    def send(self, path: str, msg: object) -> None:
        """Never blocks and never raises for network reasons; raises SerializationError for an unsendable message."""
        frame = self._frame(path, msg)  # before touching the network: an unsendable message opens nothing
        if self._closed:
            self.system.dead_letter(msg, RemoteRef(path, self))
            return
        self._association(path).send(path, msg, frame)

    async def offer(self, path: str, msg: object) -> None:
        frame = self._frame(path, msg)
        if self._closed:
            self.system.dead_letter(msg, RemoteRef(path, self))
            return
        association = self._association(path)
        association.send(path, msg, frame)
        await association.drain()

    def _frame(self, path: str, msg: object) -> bytes:
        frame = pack({"kind": "msg", "to": path, "from": None, "message": Serialization.encode(msg, self.ref_path)})
        if len(frame) - 4 > MAX_FRAME:
            raise SerializationError(f"message of {len(frame) - 4} bytes exceeds the {MAX_FRAME} byte frame limit")
        return frame

    def _association(self, path: str) -> _Association:
        key = _authority_of(path)
        association = self._associations.get(key)
        if association is None:
            association = self._associations[key] = _Association(self, *key)
            self._track(association.task)
        return association

    def drop(self, association: _Association) -> None:
        if self._associations.get((association.host, association.port)) is association:
            del self._associations[(association.host, association.port)]

    def hello(self) -> bytes:
        a = self.address
        return pack({"kind": "hello", "address": {"host": a.host, "port": a.port, "uid": a.uid, "name": a.name}})

    # --- inbound --------------------------------------------------------------------------------

    async def listen(self) -> None:
        try:
            if self._server is None:
                self._server = await asyncio.start_server(self._accept, self.address.host, self.address.port)
        finally:
            self.listening.set()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._track(asyncio.current_task())
        writer.write(self.hello())
        try:
            await self.read_loop(reader)
        finally:
            await _close(writer)

    async def read_loop(self, reader: asyncio.StreamReader) -> None:
        try:
            while (frame := await read_frame(reader)) is not None:
                self._handle(frame)
        except (ValueError, OSError) as e:
            _log.warning("closing connection: %s", e)

    def _handle(self, frame: Json) -> None:
        if not isinstance(frame, dict):
            _log.debug("malformed frame %r", frame)
            return
        match frame.get("kind"):
            case "hello":
                self._hello(frame.get("address"))
            case "msg":
                to, message = frame.get("to"), frame.get("message")
                try:
                    if not isinstance(to, str):
                        raise LookupError("frame has no recipient")
                    target = self.resolve(to)
                    if isinstance(target, RemoteRef):
                        raise LookupError(f"{to} is not on this node")
                    target.tell(Serialization.decode(message, self.resolve))
                except Exception as e:  # one bad message must not take the connection and the frames behind it
                    _log.debug("dead letter: %r", e)
                    self.system.dead_letter(message, RemoteRef(to, self) if isinstance(to, str) else self.system)
            case other:
                _log.debug("unknown frame kind %r", other)

    def _hello(self, address: Json) -> None:
        if not isinstance(address, dict):
            return
        host, port, uid = address.get("host"), address.get("port"), address.get("uid")
        if not (isinstance(host, str) and isinstance(port, int) and isinstance(uid, int)):
            return
        previous = self._peers.get((host, port))
        if previous is not None and previous != uid:
            _log.info("peer %s:%s restarted (uid %s -> %s)", host, port, previous, uid)
        self._peers[(host, port)] = uid

    # --- lifecycle ------------------------------------------------------------------------------

    def _track(self, task: asyncio.Task[Any] | None) -> None:
        if task is not None:
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def close(self) -> None:
        self._closed = True
        if self._server is not None:
            self._server.close()
        for association in list(self._associations.values()):
            if association.writer is not None:
                association.writer.close()
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._server is not None:
            await self._server.wait_closed()
            self._server = None


def unique_address(host: str, port: int, name: str) -> UniqueAddress:
    return UniqueAddress(host, port, secrets.randbits(63), name)
