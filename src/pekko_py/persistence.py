"""Persisted state: an actor whose latest state is written before it replies and recovered in one read.

    @Behaviors.persisted(store)
    def cart(items: dict[str, int]) -> Behavior[CartCmd]:
        ...
        return cart(new).then_reply(reply_to, new)   # written, then replied

    ctx.spawn(cart.recover("cart-42", empty={}), "cart-42")

No events and no replay: a persisting command writes the whole state. Pekko's DurableStateBehavior
shapes its API around a command handler and Effect so it can share them with event sourcing; pekko-py
has no event sourcing, so state stays where every other behavior keeps it, as the factory's parameter.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import pickle
import sqlite3
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol

from .behavior import Behavior, Setup
from .context import ActorContext
from .ref import ActorRef


class RevisionConflictError(Exception):
    """A write carried a revision that is not the stored revision plus one: another incarnation wrote first."""


class PersistenceError(Exception):
    """A persisted behavior was used in an actor that was not started through ``recover``."""


class Store[S](Protocol):
    """Where states live. ``save`` must reject a revision that is not exactly one past the stored one."""

    async def load(self, persistence_id: str) -> tuple[S, int] | None: ...

    async def save(self, persistence_id: str, state: S, revision: int) -> None: ...

    async def delete(self, persistence_id: str) -> None: ...


class InMemoryStore[S]:
    """For tests and examples. ``states`` maps id to (state, revision)."""

    def __init__(self) -> None:
        self.states: dict[str, tuple[S, int]] = {}

    async def load(self, persistence_id: str) -> tuple[S, int] | None:
        return self.states.get(persistence_id)

    async def save(self, persistence_id: str, state: S, revision: int) -> None:
        current = self.states.get(persistence_id)
        expected = 1 if current is None else current[1] + 1
        if revision != expected:
            raise RevisionConflictError(f"{persistence_id}: revision {revision} written, {expected} expected")
        self.states[persistence_id] = (state, revision)

    async def delete(self, persistence_id: str) -> None:
        self.states.pop(persistence_id, None)



class SqliteStore[S]:
    """Durable states in one SQLite table, through the stdlib. Each call runs in a thread so the event loop
    never blocks. The default codec is pickle: trusted local storage only; pass (dumps, loads) for anything else."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        codec: tuple[Callable[[S], bytes], Callable[[bytes], S]] = (pickle.dumps, pickle.loads),
    ) -> None:
        self.path = Path(path)
        if str(path) == ":memory:" or "mode=memory" in str(path):
            raise ValueError("SqliteStore opens a connection per call, so an in-memory database would not persist; use InMemoryStore")
        if not self.path.parent.is_dir():
            raise FileNotFoundError(f"directory for {self.path} does not exist")
        self._dumps, self._loads = codec
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")  # writers do not block readers, and commits are cheaper
            conn.execute(
                "CREATE TABLE IF NOT EXISTS state ("
                "persistence_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, data BLOB NOT NULL)"
            )

    def _connect(self) -> contextlib.closing[sqlite3.Connection]:
        # Autocommit: each statement is its own transaction. closing() so the connection does not wait for GC.
        return contextlib.closing(sqlite3.connect(self.path, isolation_level=None))

    async def load(self, persistence_id: str) -> tuple[S, int] | None:
        def go() -> tuple[S, int] | None:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT data, revision FROM state WHERE persistence_id = ?", (persistence_id,)
                ).fetchone()
            return None if row is None else (self._loads(row[0]), row[1])

        return await asyncio.to_thread(go)

    async def save(self, persistence_id: str, state: S, revision: int) -> None:
        data = self._dumps(state)

        def go() -> None:
            with self._connect() as conn:
                if revision == 1:
                    cur = conn.execute(
                        "INSERT INTO state (persistence_id, revision, data) VALUES (?, 1, ?) "
                        "ON CONFLICT(persistence_id) DO NOTHING",
                        (persistence_id, data),
                    )
                else:
                    cur = conn.execute(
                        "UPDATE state SET revision = ?, data = ? WHERE persistence_id = ? AND revision = ?",
                        (revision, data, persistence_id, revision - 1),
                    )
                if cur.rowcount != 1:
                    raise RevisionConflictError(f"{persistence_id}: revision {revision} is not the next revision")

        await asyncio.to_thread(go)

    async def delete(self, persistence_id: str) -> None:
        def go() -> None:
            with self._connect() as conn:
                conn.execute("DELETE FROM state WHERE persistence_id = ?", (persistence_id,))

        await asyncio.to_thread(go)


# --- the behavior -----------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Incarnation:
    """One recovered actor: its id, store, current revision, and the task it belongs to.

    A ContextVar is copied into every task created from this one, including child actors, so the
    incarnation is only valid in the task that recovered it."""

    persistence_id: str
    owner: Persisted[Any, Any]
    revision: int
    task: asyncio.Task[Any] | None

    @property
    def store(self) -> Store[Any]:
        return self.owner.store


_current: ContextVar[_Incarnation | None] = ContextVar("pekko_py.persistence", default=None)


def _incarnation(owner: Persisted[Any, Any]) -> _Incarnation:
    inc = _current.get()
    if inc is None or inc.task is not asyncio.current_task():
        raise PersistenceError("a persisted behavior can only run in an actor started with recover(...)")
    if inc.owner is not owner:
        raise PersistenceError(
            f"this actor was recovered through a different persisted factory ({inc.persistence_id!r}); "
            "keep one factory per persisted actor and put phases inside the state"
        )
    return inc


@dataclass(frozen=True, eq=False, slots=True)
class Persist[T](Setup[T]):
    """``factory(state)``: writes the state, runs the deferred effects, then becomes the factory's behavior.
    A Setup, so the runtime interprets it before the next message with no special casing."""

    state: Any
    owner: Persisted[Any, T]
    effects: tuple[Callable[[], None], ...]

    def then_reply[R](self, reply_to: ActorRef[R], message: R) -> Persist[T]:
        """Send message to reply_to once the write has completed."""
        return _node(self.owner, self.state, (*self.effects, lambda: reply_to.tell(message)))

    def then_run(self, callback: Callable[[], None]) -> Persist[T]:
        """Run callback once the write has completed; effects run in the order they were added."""
        return _node(self.owner, self.state, (*self.effects, callback))


class Persisted[S, T]:
    """A state-to-behavior factory whose states are written to a store. Made with ``Behaviors.persisted``."""

    def __init__(self, store: Store[S], factory: Callable[[S], Behavior[T]]) -> None:
        self.store = store
        self.factory = factory

    def __call__(self, state: S) -> Persist[T]:
        return _node(self, state, ())

    def recover(self, persistence_id: str, empty: S) -> Behavior[T]:
        """Load the state for persistence_id, or start from empty, then become ``factory(state)``.
        The only way to start a persisted actor."""

        async def setup(_ctx: ActorContext[T]) -> Behavior[T]:
            loaded = await self.store.load(persistence_id)
            state, revision = loaded if loaded is not None else (empty, 0)
            _current.set(_Incarnation(persistence_id, self, revision, asyncio.current_task()))
            return self.factory(state)

        return Setup(setup)


def _node[S, T](owner: Persisted[S, T], state: S, effects: tuple[Callable[[], None], ...]) -> Persist[T]:
    async def write_then_become(ctx: ActorContext[T]) -> Behavior[T]:
        inc = _incarnation(owner)
        await inc.store.save(inc.persistence_id, state, inc.revision + 1)
        _current.set(replace(inc, revision=inc.revision + 1))
        # The state is committed. Build the behavior before the effects so a failing factory sends no reply,
        # and never let a failing effect roll memory back behind the store: log it and carry on.
        next_behavior = owner.factory(state)
        for effect in effects:
            try:
                effect()
            except Exception as e:
                ctx.log.error("effect after a committed write failed: %r", e, exc_info=e)
        return next_behavior

    return Persist(write_then_become, state, owner, effects)


class _Persister[S](Protocol):
    def __call__[T](self, factory: Callable[[S], Behavior[T]]) -> Persisted[S, T]: ...


def persisted[S](store: Store[S]) -> _Persister[S]:
    """``@persisted(store)`` on ``def cart(items) -> Behavior[Cmd]``; also ``Behaviors.persisted(store)``."""

    def wrap[T](factory: Callable[[S], Behavior[T]]) -> Persisted[S, T]:
        return Persisted(store, factory)

    return wrap
