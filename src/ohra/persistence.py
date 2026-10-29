"""Persisted state: an actor whose latest state is written before it replies and recovered in one
read.

    @Behaviors.persisted(store)
    def cart(items: dict[str, int]) -> Behavior[CartCmd]:
        ...
        return cart(new).then_reply(reply_to, new)   # written, then replied

    ctx.spawn(cart.recover("cart-42", empty={}), "cart-42")

No events and no replay: a persisting command writes the whole state. Pekko's DurableStateBehavior
shapes its API around a command handler and Effect so it can share them with event sourcing;
ohra has no event sourcing, so state stays where every other behavior keeps it, as the
factory's parameter.

The actor never waits on its store inside a handler. As Pekko's Recovering and PersistingState,
the load and each save are piped to the actor, which stashes commands until the store answers.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import pickle
import sqlite3
import time
from collections import deque
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol, cast

from .behavior import (
    POISON_PILL,
    SAME,
    UNHANDLED,
    Behavior,
    Held,
    Holding,
    Setup,
    Stopped,
    StoreAnswer,
    Unstash,
)
from .context import ActorContext
from .ref import ActorRef
from .result import Failure, Success, Try
from .signals import Signal


class RevisionConflictError(Exception):
    """A write carried a revision that is not the stored revision plus one: another incarnation
    wrote first."""


class RecoveryTimedOutError(Exception):
    """A persisted actor's recovery didn't get its state within ``recovery_timeout``: Pekko's
    RecoveryTimedOut."""


class PersistenceError(Exception):
    """A persisted behavior was used in an actor that was not started through ``recover``."""


class Store[S](Protocol):
    """Where states live. ``save`` must reject a revision that is not exactly one past the stored
    one."""

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
            raise RevisionConflictError(
                f"{persistence_id}: revision {revision} written, {expected} expected"
            )
        self.states[persistence_id] = (state, revision)

    async def delete(self, persistence_id: str) -> None:
        self.states.pop(persistence_id, None)


class SqliteStore[S]:
    """Durable states in one SQLite table, through the stdlib. Each call runs in a thread so the
    event loop never blocks. The default codec is pickle: trusted local storage only; pass (dumps,
    loads) for anything else."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        codec: tuple[Callable[[S], bytes], Callable[[bytes], S]] = (pickle.dumps, pickle.loads),
    ) -> None:
        self.path = Path(path)
        if str(path) == ":memory:" or "mode=memory" in str(path):
            raise ValueError(
                "SqliteStore opens a connection per call, so an in-memory database would not "
                "persist; use InMemoryStore"
            )
        if not self.path.parent.is_dir():
            raise FileNotFoundError(f"directory for {self.path} does not exist")
        self._dumps, self._loads = codec
        # Processes opening one fresh file together (pods starting at once): switching it to WAL
        # fails at once with "database is locked" for all but one, since SQLite's deadlock
        # avoidance skips the busy timeout. Retry for about the 5 s that timeout would have waited.
        for attempt in range(100):
            try:
                with self._connect() as conn:
                    # writers do not block readers, and commits are cheaper
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute(
                        "CREATE TABLE IF NOT EXISTS state ("
                        "persistence_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, data BLOB "
                        "NOT NULL)"
                    )
                break
            except sqlite3.OperationalError as e:
                if "locked" not in str(e) or attempt == 99:
                    raise
                time.sleep(0.05)

    def _connect(self) -> contextlib.closing[sqlite3.Connection]:
        # Autocommit: each statement is its own transaction. closing() so the connection does not
        # wait for GC.
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
                        "UPDATE state SET revision = ?, data = ? WHERE persistence_id = ? AND "
                        "revision = ?",
                        (revision, data, persistence_id, revision - 1),
                    )
                if cur.rowcount != 1:
                    raise RevisionConflictError(
                        f"{persistence_id}: revision {revision} is not the next revision"
                    )

        await asyncio.to_thread(go)

    async def delete(self, persistence_id: str) -> None:
        def go() -> None:
            with self._connect() as conn:
                conn.execute("DELETE FROM state WHERE persistence_id = ?", (persistence_id,))

        await asyncio.to_thread(go)


# --- the behavior --------------------------------------------------------------------------------


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


_current: ContextVar[_Incarnation | None] = ContextVar("ohra.persistence", default=None)


def _incarnation(owner: Persisted[Any, Any]) -> _Incarnation:
    inc = _current.get()
    if inc is None or inc.task is not asyncio.current_task():
        raise PersistenceError(
            "a persisted behavior can only run in an actor started with recover(...)"
        )
    if inc.owner is not owner:
        raise PersistenceError(
            "this actor was recovered through a different persisted factory "
            f"({inc.persistence_id!r}); "
            "keep one factory per persisted actor and put phases inside the state"
        )
    return inc


@dataclass(frozen=True, eq=False, slots=True)
class Persist[T](Setup[T]):
    """``factory(state)``: writes the state, runs the deferred effects, then becomes the factory's
    behavior. A Setup, so the runtime interprets it before the next message with no special
    casing."""

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
    """A state-to-behavior factory whose states are written to a store. Made with
    ``Behaviors.persisted``."""

    def __init__(
        self, store: Store[S], factory: Callable[[S], Behavior[T]], stash_capacity: int = 4096
    ) -> None:
        self.store = store
        self.factory = factory
        self.stash_capacity = stash_capacity

    def __call__(self, state: S) -> Persist[T]:
        return _node(self, state, ())

    def recover(
        self, persistence_id: str, empty: S, recovery_timeout: float = 30.0
    ) -> Behavior[T]:
        """Load the state for persistence_id, or start from empty, then become ``factory(state)``.
        The only way to start a persisted actor. As Pekko's Recovering: the load is piped to the
        actor, commands wait in the internal stash until it arrives, and a load that takes longer
        than ``recovery_timeout`` fails the actor with RecoveryTimedOutError."""

        def setup(ctx: ActorContext[T]) -> Behavior[T]:
            token = object()
            ctx.pipe_to_self(self.store.load(persistence_id), lambda r: _loaded(token, r))
            ctx.timers.start_single_timer(
                _RECOVERY_TIMER, cast(T, _RecoveryTimeout(token)), recovery_timeout
            )

            def done(msg: object, held: Held) -> Behavior[T] | None:
                match msg:
                    case _GetSuccess(t, loaded) if t is token:
                        ctx.timers.cancel(_RECOVERY_TIMER)
                        if held.poisoned and not held.stash and not held.signals:  # as Pekko
                            return Stopped()
                        state, revision = loaded if loaded is not None else (empty, 0)
                        task = asyncio.current_task()
                        _current.set(_Incarnation(persistence_id, self, revision, task))
                        held.recovery_failed = True  # until the factory has run, as a setup
                        recovered = self.factory(state)
                        held.recovery_failed = False
                        return _hand_over(held, recovered)
                    case _GetFailure(t, error) if t is token:
                        held.recovery_failed = True
                        raise error
                    case _RecoveryTimeout(t) if t is token:
                        held.recovery_failed = True
                        raise RecoveryTimedOutError(
                            f"Recovery timed out, didn't get state within {recovery_timeout:g}s"
                        )
                    case _:
                        return None

            return _holding(ctx, self.stash_capacity, done)

        return Setup(setup)


# --- Pekko's InternalProtocol: what the store calls send back ------------------------------------


@dataclass(frozen=True)
class _GetSuccess(StoreAnswer):
    token: object
    loaded: tuple[Any, int] | None


@dataclass(frozen=True)
class _GetFailure(StoreAnswer):
    token: object
    error: Exception


@dataclass(frozen=True)
class _RecoveryTimeout(StoreAnswer):
    token: object


@dataclass(frozen=True)
class _UpsertSuccess(StoreAnswer):
    token: object


@dataclass(frozen=True)
class _UpsertFailure(StoreAnswer):
    token: object
    error: Exception


_RECOVERY_TIMER = object()  # the recovery timeout's timer key


def _loaded(token: object, result: Try[tuple[Any, int] | None]) -> Any:
    match result:
        case Success(loaded):
            return _GetSuccess(token, loaded)
        case Failure(error):
            return _GetFailure(token, error)


def _saved(token: object, result: Try[None]) -> Any:
    match result:
        case Success():
            return _UpsertSuccess(token)
        case Failure(error):
            return _UpsertFailure(token, error)


def _hand_over[T](held: Held, behavior: Behavior[T]) -> Behavior[T]:
    """Become ``behavior``: the held signals first, then the stash, then the stop if one came."""
    return Unstash(held.stash, behavior, tuple(held.signals), held.poisoned, held)


def _holding[T](
    ctx: ActorContext[T], capacity: int, done: Callable[[object, Held], Behavior[T] | None]
) -> Behavior[T]:
    """A recovering or persisting state: ``done`` handles the store's answer (None: not this
    call's, ignore it); commands are stashed, a full stash drops, and after an ``after_pending``
    stop new commands are unhandled. PostStop and PreRestart never reach it: the runtime sends
    them to the pre-write behavior."""
    held = Held(deque(), [])

    def on_message(_ctx: ActorContext[T], msg: T) -> Behavior[T]:
        if msg is POISON_PILL:
            held.poisoned = True
            return SAME
        if isinstance(msg, StoreAnswer):
            nb = done(msg, held)
            return SAME if nb is None else nb
        if held.poisoned:
            return UNHANDLED
        if len(held.stash) >= capacity:
            ctx.system.dropped(msg, "Stash is full in [DurableState]", ctx.self)
        else:
            held.stash.append(msg)
        return SAME

    def on_signal(_ctx: ActorContext[T], sig: Signal) -> Behavior[T]:
        held.signals.append(sig)
        return SAME

    return Holding(on_message, on_signal, held=held)


def _node[S, T](
    owner: Persisted[S, T], state: S, effects: tuple[Callable[[], None], ...]
) -> Persist[T]:
    def write(ctx: ActorContext[T]) -> Behavior[T]:
        """Pekko's PersistingState: the save is piped to the actor, and commands wait in the
        internal stash until it answers."""
        inc = _incarnation(owner)
        token = object()
        ctx.pipe_to_self(
            inc.store.save(inc.persistence_id, state, inc.revision + 1),
            lambda r: _saved(token, r),
        )

        def done(msg: object, held: Held) -> Behavior[T] | None:
            match msg:
                case _UpsertSuccess(t) if t is token:
                    committed = _incarnation(owner)
                    _current.set(replace(committed, revision=committed.revision + 1))
                    # The state is committed. Build the behavior before the effects so a failing
                    # factory sends no reply, and never let a failing effect roll memory back
                    # behind the store: log it and carry on.
                    next_behavior = owner.factory(state)
                    for effect in effects:
                        try:
                            effect()
                        except Exception as e:
                            ctx.log.error(
                                "effect after a committed write failed: %r", e, exc_info=e
                            )
                    return _hand_over(held, next_behavior)
                case _UpsertFailure(t, error) if t is token:
                    raise error
                case _:
                    return None

        return _holding(ctx, owner.stash_capacity, done)

    return Persist(write, state, owner, effects)


class _Persister[S](Protocol):
    def __call__[T](self, factory: Callable[[S], Behavior[T]]) -> Persisted[S, T]: ...


def persisted[S](store: Store[S], stash_capacity: int = 4096) -> _Persister[S]:
    """``@persisted(store)`` on ``def cart(items) -> Behavior[Cmd]``; also
    ``Behaviors.persisted(store)``."""

    def wrap[T](factory: Callable[[S], Behavior[T]]) -> Persisted[S, T]:
        return Persisted(store, factory, stash_capacity)

    return wrap
