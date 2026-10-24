"""ActorCell: the runtime behind one actor.

One asyncio task per actor runs the message loop. System messages (stop, watch,
death notifications, backoff timers) always go before user messages, as in Pekko.
A handler that awaits keeps the actor busy: messages are processed strictly one
at a time, including across awaits inside the handler.
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import itertools
import logging
import random
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from .behavior import (
    SAME,
    UNHANDLED,
    Behavior,
    MaybeAwaitable,
    Receive,
    Setup,
    Stopped,
    Supervised,
    Unstash,
    WithTimers,
)
from .context import ActorContext
from .mailbox import MailboxOverflowError, MailboxSelector
from .ref import ActorRef, FunctionRef, LocalRef
from .result import Failure, Success, Try
from .routers import PoolRouter
from .signals import ChildFailed, DeathPactError, PostStop, PreRestart, Signal, Terminated
from .supervision import Backoff, Restart, Resume, Stop

if TYPE_CHECKING:
    from .system import ActorSystem


# --- internal messages ----------------------------------------------------------


class _StopMsg:
    __slots__ = ()


_STOP = _StopMsg()


@dataclass(frozen=True, slots=True)
class _Watch:
    watcher: ActorCell[Any]


@dataclass(frozen=True, slots=True)
class _Unwatch:
    watcher: ActorCell[Any]


@dataclass(frozen=True, slots=True)
class _DeathNotify:
    cell: ActorCell[Any]
    failure: BaseException | None


@dataclass(frozen=True, slots=True)
class _Adapted:
    thunk: Callable[[], object]  # returns the message, or _DROP to skip it


_DROP = object()
_POISON = object()  # a user-queue stop: processed in order, after the messages ahead of it


@dataclass(frozen=True, slots=True, eq=False)
class _BackoffRestart(Signal):
    token: object


# --- runtime supervisor -----------------------------------------------------------


@dataclass(eq=False)
class _Supervisor[T](Behavior[T]):
    """Per-incarnation wrapper created when a Supervised template starts."""

    template: Supervised[T]
    inner: Behavior[T] | None = None  # None while waiting for a backoff restart
    restarts: deque[float] = field(default_factory=lambda: deque[float]())
    attempt: int = 0
    last_failure: float = float("-inf")
    stash: deque[Any] | None = None
    token: object = field(default_factory=object)


# --- timers -------------------------------------------------------------------------


class TimerScheduler[T]:
    """Pekko's TimerScheduler, reached as ``ctx.timers`` or through ``Behaviors.with_timers``.

    A timer message is validated when it is dequeued, so a message from a timer that
    was cancelled or restarted while it sat in the mailbox is dropped. Starting a timer
    with a key that is in use cancels the previous timer. All timers are cancelled when
    the actor restarts or stops.
    """

    __slots__ = ("_cell", "_active", "_handles")

    def __init__(self, cell: ActorCell[T]) -> None:
        self._cell = cell
        self._active: dict[object, object] = {}  # key -> generation token
        self._handles: dict[object, asyncio.TimerHandle] = {}

    def start_single_timer(self, key: object, msg: T, delay: float) -> None:
        self._start(key, msg, delay, None)

    def start_timer_with_fixed_delay(
        self, key: object, msg: T, delay: float, initial_delay: float | None = None
    ) -> None:
        self._start(key, msg, delay if initial_delay is None else initial_delay, delay)

    def is_timer_active(self, key: object) -> bool:
        return key in self._active

    def cancel(self, key: object) -> None:
        if self._active.pop(key, None) is not None:
            handle = self._handles.pop(key, None)
            if handle is not None:
                handle.cancel()

    def cancel_all(self) -> None:
        for key in list(self._active):
            self.cancel(key)

    def _start(self, key: object, msg: T, delay: float, interval: float | None) -> None:
        self.cancel(key)
        self._active[key] = token = object()
        loop = asyncio.get_running_loop()

        def deliver() -> object:
            if self._active.get(key) is not token:
                return _DROP
            if interval is None:
                del self._active[key]
            return msg

        def fire() -> None:
            if interval is None:
                del self._handles[key]
            else:
                self._handles[key] = loop.call_later(interval, fire)
            self._cell.send_adapted(deliver)

        self._handles[key] = loop.call_later(delay, fire)


async def _call[X](fn: Callable[..., MaybeAwaitable[X]], *args: Any) -> X:
    r = fn(*args)
    if inspect.isawaitable(r):
        return await cast(Awaitable[X], r)
    return r


_STOPPED: Behavior[Any] = Stopped()


class ActorCell[T]:
    def __init__(
        self,
        system: ActorSystem[Any],
        parent: ActorCell[Any] | None,
        name: str,
        path: str,
        behavior: Behavior[T],
        mailbox: MailboxSelector = MailboxSelector(),
    ) -> None:
        self.system = system
        self.parent = parent
        self.name = name
        self.path = path
        self.mailbox = mailbox
        self.ref: LocalRef[T] = LocalRef(self)
        self.ctx: ActorContext[T] = ActorContext(self)
        self.log = logging.getLogger("pekko_py.actor." + path.split("://", 1)[-1].replace("/", "."))
        loop = asyncio.get_running_loop()
        self.done: asyncio.Future[None] = loop.create_future()
        self.children: dict[str, ActorCell[Any]] = {}
        self.adapters: list[ActorRef[Any]] = []  # message_adapter refs live as long as the actor, as in Pekko
        self.watching: dict[ActorCell[Any], tuple[T] | None] = {}
        self.watched_by: set[ActorCell[Any]] = set()
        self.failure: BaseException | None = None
        self.stopping = False
        self.terminated = False
        self._poisoned = False  # a _POISON sits in _user: it takes no capacity and is never evicted
        self._initial = behavior
        self._behavior: Behavior[T] = behavior
        self._last_live: Behavior[T] | None = None
        self._sys: deque[object] | None = None  # created on the first system message; most actors never get one
        self._user: deque[object] = deque()
        self._waiter: asyncio.Future[None] | None = None  # set while the loop is idle in _run
        self._space: deque[asyncio.Future[None]] | None = None  # senders parked in offer()
        self._tasks: set[asyncio.Task[None]] = set()
        self._timers: dict[object, asyncio.TimerHandle] = {}
        self.timers: TimerScheduler[T] = TimerScheduler(self)
        self._anon = itertools.count()
        self._task = loop.create_task(self._run(), name=path)

    # --- mailbox -----------------------------------------------------------------

    def send_user(self, msg: object) -> None:
        if self.stopping:
            self.system.dead_letter(msg, self.ref)
            return
        cap = self.mailbox.capacity
        if cap is not None and self._occupancy() >= cap:
            match self.mailbox.overflow:
                case "drop_new":
                    self.system.dead_letter(msg, self.ref)
                    return
                case "drop_old":
                    victim = self._user.popleft()
                    if victim is _POISON:  # the oldest real message goes, the pending stop stays in front
                        victim = self._user.popleft()
                        self._user.appendleft(_POISON)
                    self.system.dead_letter(victim, self.ref)
                case "fail":
                    raise MailboxOverflowError(f"mailbox of {self.path} is full ({cap})")
        self._user.append(msg)
        self._wake()

    def _occupancy(self) -> int:
        return len(self._user) - self._poisoned

    def send_adapted(self, thunk: Callable[[], object]) -> None:
        self.send_user(_Adapted(thunk))

    async def offer(self, msg: object) -> None:
        """Enqueue, waiting for room while a bounded mailbox is full. FIFO among waiters."""
        cap = self.mailbox.capacity
        granted = False
        while not self.stopping and cap is not None and self._occupancy() >= cap:
            if self._space is None:
                self._space = deque()
            slot = asyncio.get_running_loop().create_future()
            # A sender that was granted a slot and lost it to a tell keeps its place at the front.
            (self._space.appendleft if granted else self._space.append)(slot)
            try:
                await slot
            except asyncio.CancelledError:
                if slot.done() and not slot.cancelled():
                    self._release_slot()  # the grant must not die with this sender
                else:
                    with contextlib.suppress(ValueError):
                        self._space.remove(slot)
                raise
            granted = True
        if self.stopping:
            self.system.dead_letter(msg, self.ref)
            return
        self._user.append(msg)
        self._wake()

    def _release_slot(self) -> None:
        """A message left the mailbox: let the longest-waiting offer() try again."""
        while self._space:
            slot = self._space.popleft()
            if not slot.done():
                slot.set_result(None)
                return

    def send_sys(self, msg: object) -> None:
        if self.terminated:
            if isinstance(msg, _Watch):  # watching a dead actor: notify right away
                msg.watcher.send_sys(_DeathNotify(self, self.failure))
            return
        if self._sys is None:
            self._sys = deque()
        self._sys.append(msg)
        self._wake()

    def _wake(self) -> None:
        w = self._waiter
        if w is not None and not w.done():
            w.set_result(None)

    # --- children / watch / pipe ---------------------------------------------------

    def spawn_child[U](
        self, behavior: Behavior[U], name: str | None, mailbox: MailboxSelector | None
    ) -> ActorCell[U]:
        if self.stopping:
            raise RuntimeError(f"{self.path} is stopping; cannot spawn children")
        if name is None:
            name = f"${next(self._anon)}"
        elif not name or "/" in name or name.startswith("$"):
            raise ValueError(f"invalid actor name {name!r}")
        if name in self.children:
            raise ValueError(f"{self.path} already has a child named {name!r}")
        child = ActorCell(
            self.system, self, name, f"{self.path}/{name}", behavior, mailbox or MailboxSelector()
        )
        self.children[name] = child
        return child

    def stop_child(self, ref: ActorRef[Any], after_pending: bool = False) -> None:
        cell = _cell_of(ref)
        if cell is self:
            raise ValueError("an actor stops itself by returning Behaviors.stopped")
        if self.children.get(cell.name) is not cell:
            raise ValueError(f"{ref.path} is not a child of {self.path}")
        if after_pending:
            if not cell.stopping and not cell._poisoned:
                cell._poisoned = True
                cell._user.append(_POISON)  # outside the capacity: a stop must not be dropped
                cell._wake()
        else:
            cell.send_sys(_STOP)

    def watch(self, ref: ActorRef[Any], custom: tuple[T] | None) -> None:
        other = _cell_of(ref)
        if other is self:
            return
        self.watching[other] = custom
        other.send_sys(_Watch(self))

    def unwatch(self, ref: ActorRef[Any]) -> None:
        other = _cell_of(ref)
        self.watching.pop(other, None)
        other.send_sys(_Unwatch(self))

    def pipe_to_self[R](self, awaitable: Awaitable[R], adapt: Callable[[Try[R]], T]) -> None:
        async def run() -> None:
            result: Try[R]
            try:
                result = Success(await awaitable)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                result = Failure(e)
            self.send_adapted(lambda: adapt(result))

        task = asyncio.get_running_loop().create_task(run(), name=f"{self.path}/$pipe")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # --- message loop ----------------------------------------------------------------

    async def _run(self) -> None:
        try:
            self._set(await self._start(self._initial))
            while not isinstance(self._behavior, Stopped):
                if self._sys:
                    await self._system_message(self._sys.popleft())
                elif self._user:
                    msg = self._user.popleft()
                    self._release_slot()
                    if isinstance(msg, _Adapted):
                        try:
                            msg = msg.thunk()
                        except Exception:
                            self.log.exception("message adapter failed; message dropped")
                            continue
                        if msg is _DROP:
                            continue
                    if msg is _POISON:
                        self._poisoned = False
                        self._set(_STOPPED)
                        continue
                    self._set(await self._deliver(self._behavior, msg))
                else:
                    self._waiter = asyncio.get_running_loop().create_future()
                    await self._waiter
                    self._waiter = None
        except asyncio.CancelledError:
            await self._finish()
            raise
        except Exception as e:
            self.failure = e
            self.log.error("actor failed and is stopping: %r", e, exc_info=e)
        await self._finish()

    def _set(self, b: Behavior[T]) -> None:
        self._behavior = b
        if not isinstance(b, Stopped):
            self._last_live = b

    async def _system_message(self, m: object) -> None:
        match m:
            case _StopMsg():
                self._behavior = _STOPPED
            case _Watch(watcher):
                self.watched_by.add(watcher)
            case _Unwatch(watcher):
                self.watched_by.discard(watcher)
            case _DeathNotify(cell, failure):
                if self.children.get(cell.name) is cell:
                    del self.children[cell.name]
                if cell in self.watching:
                    custom = self.watching.pop(cell)
                    if custom is not None:
                        self._set(await self._deliver(self._behavior, custom[0]))
                    else:
                        sig = (
                            ChildFailed(cell.ref, failure)
                            if failure is not None and cell.parent is self
                            else Terminated(cell.ref)
                        )
                        self._set(await self._signal(self._behavior, sig))
            case _BackoffRestart():
                self._set(await self._signal(self._behavior, m))
            case _:
                self.log.warning("unknown system message %r", m)

    # --- behavior interpretation ---------------------------------------------------

    async def _start(self, b: Behavior[T]) -> Behavior[T]:
        """Turn a behavior into one that can receive: run setup, wrap supervisors."""
        while True:
            if isinstance(b, Setup):
                b = await _call(b.factory, self.ctx)
            elif isinstance(b, WithTimers):
                b = await _call(b.factory, self.timers)
            elif isinstance(b, PoolRouter):
                b = b.start(self.ctx)
            elif isinstance(b, Unstash):
                b = await self._replay(await self._start(b.behavior), b.messages)
            elif isinstance(b, Supervised):
                return await self._start_supervised(b)
            elif b is SAME or b is UNHANDLED:
                raise ValueError(f"{b!r} is not allowed as an initial behavior")
            else:
                return b

    async def _start_supervised(self, template: Supervised[T]) -> Behavior[T]:
        sup = _Supervisor(template)
        try:
            inner = await self._start(template.initial)
        except template.exc_type as e:
            return await self._on_failure(sup, e, during_start=True)
        if isinstance(inner, Stopped):
            return inner
        sup.inner = inner
        return sup

    async def _canonicalize(self, nb: Behavior[T], current: Behavior[T]) -> Behavior[T]:
        if nb is SAME or nb is UNHANDLED:
            return current
        if isinstance(nb, Unstash):
            return await self._replay(await self._canonicalize(nb.behavior, current), nb.messages)
        return await self._start(nb)

    async def _replay(self, b: Behavior[T], msgs: deque[Any]) -> Behavior[T]:
        """Deliver the messages queued now, in order. Messages added meanwhile stay queued.
        If the actor stops midway, the rest become dead letters."""
        for _ in range(len(msgs)):
            if isinstance(b, Stopped):
                break
            b = await self._deliver(b, msgs.popleft())
        if isinstance(b, Stopped):
            while msgs:
                self.system.dead_letter(msgs.popleft(), self.ref)
        return b

    async def _deliver(self, b: Behavior[T], msg: Any) -> Behavior[T]:
        if isinstance(b, _Supervisor):
            return await self._supervise(b, msg, is_signal=False)
        if isinstance(b, Receive):
            r = b
            nb = await _call(r.on_message, self.ctx, msg)
            if nb is UNHANDLED:
                self.system.unhandled(msg, self.ref)
            return await self._canonicalize(nb, r)
        raise TypeError(f"cannot deliver to {b!r}")

    async def _signal(self, b: Behavior[T], sig: Signal) -> Behavior[T]:
        if isinstance(b, _Supervisor):
            return await self._supervise(b, sig, is_signal=True)
        if isinstance(b, Receive):
            r = b
            if isinstance(sig, _BackoffRestart):
                return r
            nb = UNHANDLED if r.on_signal is None else await _call(r.on_signal, self.ctx, sig)
            if nb is UNHANDLED and isinstance(sig, Terminated):
                raise DeathPactError(sig.ref)
            return await self._canonicalize(nb, r)
        return b

    async def _supervise(self, sup: _Supervisor[T], item: Any, is_signal: bool) -> Behavior[T]:
        if isinstance(item, _BackoffRestart) and item.token is sup.token:
            return await self._backoff_restart(sup)
        if sup.inner is None:  # backing off: stash messages, drop signals
            if not is_signal:
                strategy = cast(Backoff, sup.template.strategy)
                if sup.stash is not None and len(sup.stash) < strategy.stash_capacity:
                    sup.stash.append(item)
                else:
                    self.system.dead_letter(item, self.ref)
            return sup
        try:
            if is_signal:
                nb = await self._signal(sup.inner, item)
            else:
                nb = await self._deliver(sup.inner, item)
        except sup.template.exc_type as e:
            if item is PostStop or item is PreRestart:
                raise
            return await self._on_failure(sup, e)
        if isinstance(nb, Stopped):
            return nb
        sup.inner = nb
        return sup

    async def _on_failure(self, sup: _Supervisor[T], e: Exception, during_start: bool = False) -> Behavior[T]:
        strategy = sup.template.strategy
        now = asyncio.get_running_loop().time()
        match strategy:
            case Resume():
                if during_start:
                    raise e
                self.log.warning("resuming after %r", e)
                return sup
            case Stop():
                raise e
            case Restart(max_restarts=max_restarts, within=within, stop_children=stop_children):
                if during_start:
                    raise e  # plain restart on a failing setup would spin; use backoff for that
                if within is not None:
                    while sup.restarts and now - sup.restarts[0] > within:
                        sup.restarts.popleft()
                if 0 <= max_restarts <= len(sup.restarts):
                    raise e
                sup.restarts.append(now)
                self.log.warning("restarting after %r", e)
                await self._pre_restart(sup)
                if stop_children:
                    await self._stop_children()
                inner = await self._start(sup.template.initial)
                if isinstance(inner, Stopped):
                    return inner
                sup.inner = inner
                return sup
            case Backoff():
                if strategy.reset_after is not None and now - sup.last_failure > strategy.reset_after:
                    sup.attempt = 0
                if 0 <= strategy.max_restarts <= sup.attempt:
                    raise e
                sup.last_failure = now
                await self._pre_restart(sup)
                await self._stop_children()
                sup.inner = None
                if sup.stash is None:
                    sup.stash = deque()
                delay = strategy.delay(sup.attempt, random.random())
                sup.attempt += 1
                self.log.warning("restarting in %.3fs after %r", delay, e)
                self._timers[sup.token] = asyncio.get_running_loop().call_later(
                    delay, self._fire_backoff, sup.token
                )
                return sup
            case _:
                raise e

    def _fire_backoff(self, token: object) -> None:
        self._timers.pop(token, None)
        self.send_sys(_BackoffRestart(token))

    async def _backoff_restart(self, sup: _Supervisor[T]) -> Behavior[T]:
        stash, sup.stash = sup.stash or deque(), None
        try:
            inner = await self._start(sup.template.initial)
        except sup.template.exc_type as e:
            sup.stash = stash
            return await self._on_failure(sup, e, during_start=False)
        if isinstance(inner, Stopped):
            return await self._replay(inner, stash)
        sup.inner = inner
        return await self._replay(sup, stash)

    async def _pre_restart(self, sup: _Supervisor[T]) -> None:
        self.timers.cancel_all()  # as Pekko's RestartSupervisor: timers do not survive a restart
        if sup.inner is None:
            return
        try:
            await self._signal(sup.inner, PreRestart)
        except Exception:
            self.log.exception("PreRestart handler failed")

    async def _stop_children(self) -> None:
        kids = list(self.children.values())
        self.children.clear()
        for k in kids:
            self.watching.pop(k, None)
            k.send_sys(_STOP)
        if kids:
            await asyncio.gather(*(k.done for k in kids))

    async def _finish(self) -> None:
        self.stopping = True
        while self._space:  # parked senders wake, see stopping, and dead-letter their message
            slot = self._space.popleft()
            if not slot.done():
                slot.set_result(None)
        self.timers.cancel_all()
        for h in self._timers.values():
            h.cancel()
        self._timers.clear()
        for t in list(self._tasks):
            t.cancel()
        await self._stop_children()
        if self._last_live is not None:
            try:
                await self._signal(self._last_live, PostStop)
            except Exception:
                self.log.exception("PostStop handler failed")
        if isinstance(self._behavior, Stopped) and self._behavior.post_stop is not None:
            try:
                self._behavior.post_stop()
            except Exception:
                self.log.exception("post_stop callback failed")
        # From here on no awaits: nothing can interleave between these steps.
        self.terminated = True
        for m in self._sys or ():
            if isinstance(m, _Watch):
                self.watched_by.add(m.watcher)
        self._sys = None
        for m in self._user:
            if m is not _POISON:
                self.system.dead_letter(m, self.ref)
        self._user.clear()
        self.adapters.clear()
        note = _DeathNotify(self, self.failure)
        for w in self.watched_by:
            w.send_sys(note)
        if self.parent is not None and self.parent not in self.watched_by:
            self.parent.send_sys(note)
        if not self.done.done():
            self.done.set_result(None)


def _cell_of(ref: ActorRef[Any]) -> ActorCell[Any]:
    if isinstance(ref, LocalRef):
        return ref._cell  # pyright: ignore[reportPrivateUsage]
    if isinstance(ref, FunctionRef) and ref.owner is not None:  # a message adapter: watch its actor
        return ref.owner
    raise TypeError(f"{ref!r} is not a local actor")


def is_local_actor(ref: ActorRef[Any]) -> bool:
    """True for a ref that can be watched: a local actor, or a message adapter of one."""
    return isinstance(ref, LocalRef) or (isinstance(ref, FunctionRef) and ref.owner is not None)
