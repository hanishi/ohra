"""Pekko's ``DurableStore`` protocol and a port of ``LmdbDurableStore`` on stdlib ``sqlite3``.

The Replicator creates the store, sends ``LoadAll``, and gets back zero or one ``LoadData`` and
one ``LoadAllCompleted``. ``Store`` writes an entry and answers through its ``StoreReply``;
``Expire`` deletes entries. The database is ``<dir>/ddata.db``, one table ``ddata(key, envelope)``,
where Pekko's LMDB keeps a database named ``ddata``. Each call opens its own connection and runs
in a thread, as ``SqliteStore`` does. A ``dir`` ending with ``ddata`` gets the system name, the
replicator's name and the port appended, as in Pekko.

Narrowing: ``Expire`` also drops the write-behind values pending for its keys, so an expired key
stays gone after the flush (Pekko writes it back).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from ...behavior import Behavior, Behaviors
from ...context import ActorContext
from ...ref import ActorRef
from ...remote import UniqueAddress
from ...remote.serialization import Serialization, SerializationError
from ...signals import PostStop, Signal
from .envelope import DataEnvelope
from .pruning_state import PruningPerformed, PruningState


class DurableDataEnvelope:
    """Wraps a data envelope for storage. Equality and hash look at the data only."""

    def __init__(self, data_envelope: DataEnvelope) -> None:
        self.data_envelope = data_envelope

    @property
    def data(self) -> Any:
        return self.data_envelope.data

    def __repr__(self) -> str:
        return f"DurableDataEnvelope({self.data})"

    def __hash__(self) -> int:
        return hash(self.data)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DurableDataEnvelope) and self.data == other.data


class DurableStore:
    @dataclass(frozen=True)
    class StoreReply:
        success: Any
        failure: Any
        reply_to: ActorRef[Any]

    @dataclass(frozen=True)
    class Store:
        key: str
        data: DurableDataEnvelope
        reply: DurableStore.StoreReply | None

    @dataclass(frozen=True)
    class LoadData:
        data: Mapping[str, DurableDataEnvelope]

    @dataclass(frozen=True)
    class LoadAllCompleted:
        pass

    @dataclass(frozen=True)
    class LoadAll:
        reply_to: ActorRef[DurableStore.LoadData | DurableStore.LoadAllCompleted]

    @dataclass(frozen=True)
    class Expire:
        keys: frozenset[str]

    class LoadFailed(Exception):  # noqa: N818  Pekko's name
        pass

    type Command = DurableStore.Store | DurableStore.LoadAll | DurableStore.Expire


def _no_ref(*_args: Any) -> Any:
    raise SerializationError("durable data holds no refs")


def encode_durable(envelope: DurableDataEnvelope) -> str:
    """Pekko's ``durableDataEnvelopeToProto``: only the ``PruningPerformed`` markers are kept."""
    env = envelope.data_envelope
    kept: dict[UniqueAddress, PruningState] = {
        k: v for k, v in env.pruning.items() if isinstance(v, PruningPerformed)
    }
    return json.dumps(Serialization.encode(DataEnvelope(env.data, kept), _no_ref))


def decode_durable(text: str) -> DurableDataEnvelope:
    return DurableDataEnvelope(cast(DataEnvelope, Serialization.decode(json.loads(text), _no_ref)))


def durable_dir(
    dir: str | os.PathLike[str], system_name: str, replicator_name: str, port: int
) -> Path:
    path = os.fspath(dir)
    if path.endswith("ddata"):
        return Path(f"{path}-{system_name}-{replicator_name}-{port}")
    return Path(path)


@dataclass(frozen=True)
class _WriteBehind:
    pass


def _connect(dir: Path) -> contextlib.closing[sqlite3.Connection]:
    dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(dir / "ddata.db", isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ddata (key TEXT PRIMARY KEY, envelope TEXT NOT NULL)"
        )
    except BaseException:
        conn.close()
        raise
    return contextlib.closing(conn)


def _put(dir: Path, rows: Mapping[str, str]) -> None:
    with _connect(dir) as conn:
        conn.execute("BEGIN")
        conn.executemany(
            "INSERT INTO ddata (key, envelope) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET envelope = excluded.envelope",
            list(rows.items()),
        )
        conn.execute("COMMIT")


def _delete(dir: Path, keys: frozenset[str]) -> None:
    with _connect(dir) as conn:
        conn.execute("BEGIN")
        conn.executemany("DELETE FROM ddata WHERE key = ?", [(k,) for k in keys])
        conn.execute("COMMIT")


def _load(dir: Path) -> dict[str, DurableDataEnvelope]:
    with _connect(dir) as conn:
        return {k: decode_durable(v) for k, v in conn.execute("SELECT key, envelope FROM ddata")}


def sqlite_durable_store(
    dir: str | os.PathLike[str] = "ddata", write_behind_interval: float = 0.0
) -> Behavior[DurableStore.Command]:
    """``LmdbDurableStore`` on SQLite. A ``write_behind_interval`` above zero batches the writes:
    the last value of each key is committed when the interval ends, and on stop."""

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        path = os.fspath(dir)
        if path.endswith("ddata"):
            # imported here: cluster.core imports this package
            from ..core import Cluster

            parent_name = ctx.self.path.rsplit("/", 2)[-2]
            folder = durable_dir(
                path, ctx.system.name, parent_name, Cluster(ctx.system).self_address.port
            )
        else:
            folder = Path(path)
        pending: dict[str, DurableDataEnvelope] = {}

        async def write_behind() -> None:
            if not pending:
                return
            rows = {k: encode_durable(v) for k, v in pending.items()}
            keys = ", ".join(pending)
            pending.clear()
            try:
                await asyncio.to_thread(_put, folder, rows)
            except Exception:
                ctx.log.exception("failed to store [%s]", keys)

        def flush_on_stop() -> None:
            if pending:
                try:
                    _put(folder, {k: encode_durable(v) for k, v in pending.items()})
                except Exception:
                    ctx.log.exception("failed to store [%s]", ", ".join(pending))
                pending.clear()

        def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
            if sig is PostStop:
                flush_on_stop()
            return Behaviors.same

        async def store(msg: DurableStore.Store) -> None:
            try:
                if write_behind_interval == 0:
                    await asyncio.to_thread(_put, folder, {msg.key: encode_durable(msg.data)})
                else:
                    if not pending:
                        ctx.timers.start_single_timer(
                            "write-behind", _WriteBehind(), write_behind_interval
                        )
                    pending[msg.key] = msg.data
                if msg.reply is not None:
                    msg.reply.reply_to.tell(msg.reply.success)
            except Exception:
                ctx.log.exception("failed to store [%s]", msg.key)
                if msg.reply is not None:
                    msg.reply.reply_to.tell(msg.reply.failure)

        async def expire(keys: frozenset[str]) -> None:
            for key in keys:
                pending.pop(key, None)
            try:
                await asyncio.to_thread(_delete, folder, keys)
            except Exception:
                ctx.log.exception("failed to delete [%s]", ", ".join(keys))

        @Behaviors.receive_message
        async def active(msg: Any) -> Behavior[Any]:
            match msg:
                case DurableStore.Store():
                    await store(msg)
                case _WriteBehind():
                    await write_behind()
                case DurableStore.Expire(keys):
                    await expire(keys)
                case _:
                    pass
            return Behaviors.same

        @Behaviors.receive_message
        async def init(msg: Any) -> Behavior[Any]:
            match msg:
                case DurableStore.LoadAll(reply_to):
                    if folder.exists() and any(folder.iterdir()):
                        try:
                            data = await asyncio.to_thread(_load, folder)
                        except Exception as e:
                            raise DurableStore.LoadFailed(
                                "failed to load durable distributed-data"
                            ) from e
                        if data:
                            reply_to.tell(DurableStore.LoadData(data))
                    reply_to.tell(DurableStore.LoadAllCompleted())
                    return active.receive_signal(on_signal)
                case _:
                    pass
            return Behaviors.same

        return init.receive_signal(on_signal)

    return cast(Behavior[DurableStore.Command], setup)
