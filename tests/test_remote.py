"""Remoting: actors on other ohra nodes, by path, over TCP."""

from __future__ import annotations

import asyncio
import gc
from dataclasses import dataclass

import pytest

from ohra import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, RemoteRef
from ohra.ref import PromiseRef

from .util import eventually, free_port

# --- Task 2: addressing and resolve --------------------------------------------------------------


@dataclass(frozen=True)
class SpawnChild:
    name: str
    reply_to: ActorRef[ActorRef[str]]


def parent(ctx: ActorContext[SpawnChild], msg: SpawnChild) -> Behavior[SpawnChild]:
    msg.reply_to.tell(ctx.spawn(Behaviors.ignore, msg.name))
    return Behaviors.same


async def test_system_address_shapes_paths():
    port = free_port()
    async with ActorSystem(Behaviors.receive(parent), "a", address=("127.0.0.1", port)) as system:
        assert system.path == f"ohra://a@127.0.0.1:{port}/user"
        child = await system.ask(lambda r: SpawnChild("child", r), timeout=1)
        assert child.path == f"ohra://a@127.0.0.1:{port}/user/child"
        assert (
            system.address is not None
            and system.address.name == "a"
            and system.address.port == port
        )


async def test_resolve_local_path_returns_the_local_ref():
    async with ActorSystem(
        Behaviors.receive(parent), "a", address=("127.0.0.1", free_port())
    ) as system:
        child = await system.ask(lambda r: SpawnChild("child", r), timeout=1)
        assert system.resolve(child.path) is child
        assert system.resolve(system.path) is system._root.ref  # pyright: ignore[reportPrivateUsage]


async def test_resolve_foreign_path_is_a_remote_ref():
    async with ActorSystem(Behaviors.ignore, "a", address=("127.0.0.1", free_port())) as system:
        ref = system.resolve("ohra://b@127.0.0.1:1/user/greeter")
        assert isinstance(ref, RemoteRef)
        assert ref.path == "ohra://b@127.0.0.1:1/user/greeter"
        assert ref == system.resolve("ohra://b@127.0.0.1:1/user/greeter")
        assert len({ref, system.resolve("ohra://b@127.0.0.1:1/user/greeter")}) == 1


async def test_temp_refs_are_weak():
    async with ActorSystem(Behaviors.ignore, "a", address=("127.0.0.1", free_port())) as system:
        assert system.remoting is not None and system.address is not None
        fut: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        promise: ActorRef[str] | None = PromiseRef(fut, "local-only")
        path = system.remoting.ref_path(promise)
        assert path.startswith(f"{system.address.root}/temp/")
        assert system.resolve(path) is promise
        promise = None
        gc.collect()
        with pytest.raises(LookupError):
            system.resolve(path)


async def test_resolve_unknown_local_path_raises():
    async with ActorSystem(Behaviors.ignore, "a", address=("127.0.0.1", free_port())) as system:
        with pytest.raises(LookupError):
            system.resolve(f"{system.path}/nobody")


async def test_system_without_address_cannot_resolve_remote_paths():
    async with ActorSystem(Behaviors.receive(parent), "a") as system:
        child = await system.ask(lambda r: SpawnChild("child", r), timeout=1)
        assert system.resolve(child.path) is child  # local resolution works without remoting
        with pytest.raises(LookupError):
            system.resolve("ohra://b@127.0.0.1:1/user/greeter")


# --- Task 3: transport ---------------------------------------------------------------------------

import logging  # noqa: E402
import socket  # noqa: E402
import struct  # noqa: E402

from ohra import (  # noqa: E402
    Entity,
    EntityContext,
    EntityTypeKey,
    Failure,
    Sharding,
    ShardingEnvelope,
    Success,
    Try,
    ask,
    remotable,
)


@remotable
@dataclass(frozen=True)
class Greet:
    name: str
    reply_to: ActorRef[Greeted]


@remotable
@dataclass(frozen=True)
class Greeted:
    text: str


@dataclass(frozen=True)
class NotRemotable:
    x: int


def greeter(log: list[str]) -> Behavior[Greet]:
    def on_message(msg: Greet) -> Behavior[Greet]:
        log.append(msg.name)
        msg.reply_to.tell(Greeted(f"hello {msg.name}"))
        return Behaviors.same

    return Behaviors.receive_message(on_message)


def node(behavior: Behavior[Greet], name: str, port: int) -> ActorSystem[Greet]:
    return ActorSystem(behavior, name, address=("127.0.0.1", port))


async def test_tell_across_nodes():
    log: list[str] = []
    port_a = free_port()
    async with node(greeter(log), "a", port_a) as a, node(Behaviors.ignore, "b", free_port()) as b:
        remote = b.resolve(a.path)
        remote.tell(Greet("pekko", b.ignore_ref))
        await eventually(lambda: log == ["pekko"])


async def test_ask_across_nodes():
    log: list[str] = []
    async with (
        node(greeter(log), "a", free_port()) as a,
        node(Behaviors.ignore, "b", free_port()) as b,
    ):
        remote: ActorRef[Greet] = b.resolve(a.path)
        assert await ask(remote, lambda r: Greet("pekko", r), timeout=1) == Greeted("hello pekko")
        assert await ask(remote, lambda r: Greet("again", r), timeout=1) == Greeted("hello again")
    assert log == ["pekko", "again"]


@dataclass(frozen=True)
class Answer:
    result: Try[Greeted]


async def test_actor_asks_remote_with_ctx_ask():
    log: list[str] = []
    seen: list[str] = []

    def asker(target_path: str) -> Behavior[str | Answer]:
        def on_message(
            ctx: ActorContext[str | Answer], msg: str | Answer
        ) -> Behavior[str | Answer]:
            match msg:
                case str(name):
                    ctx.ask(
                        ctx.system.resolve(target_path),
                        lambda r: Greet(name, r),
                        Answer,
                        timeout=1,
                    )
                case Answer(Success(Greeted(text))):
                    seen.append(text)
                case Answer(Failure(e)):
                    seen.append(f"failed {e!r}")
                case Answer():
                    seen.append("unexpected")
            return Behaviors.same

        return Behaviors.receive(on_message)

    async with node(greeter(log), "a", free_port()) as a:
        async with ActorSystem(asker(a.path), "b", address=("127.0.0.1", free_port())) as b:
            b.tell("pekko")
            await eventually(lambda: seen == ["hello pekko"])


@remotable
@dataclass(frozen=True)
class Add:
    n: int


@remotable
@dataclass(frozen=True)
class Total:
    reply_to: ActorRef[int]


type CounterCmd = Add | Total
Counter = EntityTypeKey[CounterCmd]("Counter")


def counter_entity(_entity: EntityContext[CounterCmd]) -> Behavior[CounterCmd]:
    def active(total: int) -> Behavior[CounterCmd]:
        def on_message(msg: CounterCmd) -> Behavior[CounterCmd]:
            match msg:
                case Add(n):
                    return active(total + n)
                case Total(reply_to):
                    reply_to.tell(total)
                    return Behaviors.same

        return Behaviors.receive_message(on_message)

    return active(0)


async def test_entity_ref_crosses_nodes():
    region = Sharding.init(Entity(Counter, counter_entity))
    async with ActorSystem(region, "a", address=("127.0.0.1", free_port())) as a:
        async with node(Behaviors.ignore, "b", free_port()) as b:
            remote_region: ActorRef[ShardingEnvelope[CounterCmd]] = b.resolve(a.path)
            x = Sharding.entity_ref_for(remote_region, "x")
            x.tell(Add(2))
            x.tell(Add(3))
            assert await ask(x, Total, timeout=1) == 5


async def test_unknown_recipient_is_a_dead_letter(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")
    log: list[str] = []
    async with (
        node(greeter(log), "a", free_port()) as a,
        node(Behaviors.ignore, "b", free_port()) as b,
    ):
        b.resolve(f"{a.path}/nobody").tell(Greet("lost", b.ignore_ref))
        await eventually(lambda: any("nobody" in r.getMessage() for r in caplog.records))
        b.resolve(a.path).tell(Greet("found", b.ignore_ref))  # the connection is still usable
        await eventually(lambda: log == ["found"])


async def test_unregistered_message_raises_at_tell():
    async with (
        node(Behaviors.ignore, "a", free_port()) as a,
        node(Behaviors.ignore, "b", free_port()) as b,
    ):
        from ohra import SerializationError

        with pytest.raises(SerializationError):
            b.resolve(a.path).tell(NotRemotable(1))


async def test_unreachable_node_dead_letters_without_blocking(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")
    closed = free_port()
    async with node(Behaviors.ignore, "b", free_port()) as b:
        ref = b.resolve(f"ohra://a@127.0.0.1:{closed}/user")
        ref.tell(Greet("nobody home", b.ignore_ref))  # returns at once
        await eventually(
            lambda: any("nobody home" in r.getMessage() for r in caplog.records), timeout=3
        )


async def test_peer_restart_is_detected(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO, logger="ohra.remote")
    log1: list[str] = []
    log2: list[str] = []
    port_a = free_port()
    async with node(Behaviors.ignore, "b", free_port()) as b:
        async with node(greeter(log1), "a", port_a) as a:
            path = a.path
            b.resolve(path).tell(Greet("one", b.ignore_ref))
            await eventually(lambda: log1 == ["one"])
        remoting = b.remoting
        assert remoting is not None
        # b has seen the old connection close, so "two" cannot go out on it
        await eventually(lambda: not remoting._associations)  # pyright: ignore[reportPrivateUsage]
        async with node(greeter(log2), "a", port_a):  # the same address, a new uid
            await eventually(lambda: _port_open(port_a))
            b.resolve(path).tell(Greet("two", b.ignore_ref))
            await eventually(lambda: log2 == ["two"], timeout=3)
    assert any("restarted" in r.getMessage() for r in caplog.records)


def _port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


async def test_shutdown_closes_everything():
    port_a = free_port()
    log: list[str] = []
    async with node(greeter(log), "a", port_a) as a, node(Behaviors.ignore, "b", free_port()) as b:
        b.resolve(a.path).tell(Greet("x", b.ignore_ref))
        await eventually(lambda: log == ["x"])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # the listener released its port
        # as a new listener would; TIME_WAIT pairs do not count
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", port_a))


# --- review fixes --------------------------------------------------------------------------------

import gc as _gc  # noqa: E402

from ohra import SerializationError  # noqa: E402


async def test_adapter_reply_refs_survive_garbage_collection():
    """Replies to inline ctx.message_adapter refs must arrive for every message, not just the
    first."""
    log: list[str] = []
    seen: list[str] = []

    def asker(target_path: str) -> Behavior[str | Greeted]:
        def on_message(
            ctx: ActorContext[str | Greeted], msg: str | Greeted
        ) -> Behavior[str | Greeted]:
            match msg:
                case str(name):
                    ctx.system.resolve(target_path).tell(
                        Greet(name, ctx.message_adapter(lambda g: g))
                    )
                case Greeted(text):
                    seen.append(text)
            return Behaviors.same

        return Behaviors.receive(on_message)

    async with node(greeter(log), "a", free_port()) as a:
        async with ActorSystem(asker(a.path), "b", address=("127.0.0.1", free_port())) as b:
            b.tell("one")
            await eventually(lambda: seen == ["hello one"])
            _gc.collect()
            b.tell("two")
            _gc.collect()
            await eventually(lambda: seen == ["hello one", "hello two"])


@remotable
@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[Add]


def ponger(msg: Ping) -> Behavior[Ping]:
    msg.reply_to.tell(Add(1))
    return Behaviors.same


async def test_entity_ref_is_rebuilt_on_the_other_node():
    """An EntityRef crosses the wire as region path plus id; the other node sends straight to the
    region."""
    region = Sharding.init(Entity(Counter, counter_entity))
    async with ActorSystem(
        Behaviors.receive_message(ponger), "a", address=("127.0.0.1", free_port())
    ) as a:
        async with ActorSystem(region, "b", address=("127.0.0.1", free_port())) as b:
            remote_ponger: ActorRef[Ping] = b.resolve(a.path)
            for entity_id in ("x", "y", "x"):
                remote_ponger.tell(Ping(Sharding.entity_ref_for(b, entity_id)))
                _gc.collect()

            async def totals() -> tuple[int, int]:
                x = await ask(Sharding.entity_ref_for(b, "x"), Total, timeout=1)
                y = await ask(Sharding.entity_ref_for(b, "y"), Total, timeout=1)
                return x, y

            for _ in range(200):
                if await totals() == (2, 1):
                    break
                await asyncio.sleep(0.005)
            assert await totals() == (2, 1)


@remotable
@dataclass(frozen=True)
class Positive:
    n: int

    def __post_init__(self) -> None:
        if self.n <= 0:
            raise ValueError("not positive")


async def test_bad_inbound_message_does_not_close_the_connection(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")
    log: list[str] = []
    async with (
        node(greeter(log), "a", free_port()) as a,
        node(Behaviors.ignore, "b", free_port()) as b,
    ):
        remote = b.resolve(a.path)
        object.__setattr__(bad := Positive(1), "n", -1)  # encodes fine, fails to decode on a
        remote.tell(bad)
        remote.tell(Greet("after", b.ignore_ref))
        await eventually(lambda: log == ["after"])
        assert any("Positive" in r.getMessage() for r in caplog.records)


async def test_deeply_nested_frame_closes_the_connection(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.WARNING, logger="ohra.remote")
    port = free_port()
    async with node(Behaviors.ignore, "a", port):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(struct.pack(">I", 100_000) + b"[" * 100_000)
        await writer.drain()
        while await reader.read(65536):  # the hello, then EOF once the node closes it
            pass
        writer.close()
        await writer.wait_closed()
        assert any("closing connection" in r.getMessage() for r in caplog.records)


async def test_oversize_message_raises_at_tell():
    async with (
        node(Behaviors.ignore, "a", free_port()) as a,
        node(Behaviors.ignore, "b", free_port()) as b,
    ):
        with pytest.raises(SerializationError):
            b.resolve(a.path).tell(Greet("x" * (17 * 2**20), b.ignore_ref))


async def test_guardian_stopping_closes_remoting():
    port = free_port()

    def quitter(_msg: str) -> Behavior[str]:
        return Behaviors.stopped()

    system = ActorSystem(Behaviors.receive_message(quitter), "a", address=("127.0.0.1", port))
    async with system:
        assert _port_open(port)
        system.tell("quit")
        await system.when_terminated()
        assert not _port_open(port)


async def test_port_in_use_raises_and_leaks_nothing():
    port = free_port()
    async with node(Behaviors.ignore, "a", port):
        with pytest.raises(OSError):
            async with node(Behaviors.ignore, "b", port):
                pass


async def test_path_without_port_is_a_lookup_error():
    async with node(Behaviors.ignore, "a", free_port()) as a:
        with pytest.raises(LookupError):
            a.resolve("ohra://b@127.0.0.1/user")
        with pytest.raises(LookupError):
            a.resolve("ohra://b@127.0.0.1:notaport/user")


async def test_tell_after_close_is_a_dead_letter(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")
    async with node(Behaviors.ignore, "a", free_port()) as a:
        remote = a.resolve("ohra://b@127.0.0.1:1/user")
    remote.tell(Greet("late", a.ignore_ref))  # the system is gone: nothing may start
    assert any("late" in r.getMessage() for r in caplog.records)
