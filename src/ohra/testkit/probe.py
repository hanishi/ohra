"""Pekko's TestProbe: a real actor whose messages a test awaits and asserts on."""

from __future__ import annotations

import asyncio
import inspect
import itertools
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from ..behavior import Behavior, Behaviors, MaybeAwaitable
from ..context import ActorContext
from ..ref import ActorRef
from ..signals import Signal, Terminated
from .settings import seconds

if TYPE_CHECKING:
    from .actor_testkit import ActorTestKit


# --- FishingOutcome -------------------------------------------------------------------


@dataclass(frozen=True)
class Continue:
    """Keep the message and fish on."""


@dataclass(frozen=True)
class ContinueAndIgnore:
    """Drop the message and fish on."""


@dataclass(frozen=True)
class Complete:
    """Keep the message and stop fishing."""


@dataclass(frozen=True)
class Fail:
    """Fail the test with ``message``."""

    message: str


type FishingOutcome = Continue | ContinueAndIgnore | Complete | Fail


class FishingOutcomes:
    """What a ``fish_for_message`` fisher returns for each message."""

    @staticmethod
    def complete() -> FishingOutcome:
        return Complete()

    @staticmethod
    def continue_() -> FishingOutcome:
        return Continue()

    @staticmethod
    def continue_and_ignore() -> FishingOutcome:
        return ContinueAndIgnore()

    @staticmethod
    def fail(message: str) -> FishingOutcome:
        return Fail(message)


# --- the probe actor --------------------------------------------------------------------


@dataclass(frozen=True)
class _WatchActor:
    ref: ActorRef[Any]


class _Stop:
    __slots__ = ()


_STOP = _Stop()

# Pekko shares the classic TestKit's counter, so every probe name is unique
_test_actor_id = itertools.count(1)


def _test_actor[M](
    queue: asyncio.Queue[M], terminations: asyncio.Queue[Terminated]
) -> Behavior[M]:
    def on_message(ctx: ActorContext[M], msg: M) -> Behavior[M]:
        match msg:
            case _WatchActor(ref):
                ctx.watch(ref)
                return Behaviors.same
            case _Stop():
                return Behaviors.stopped()
            case other:
                queue.put_nowait(other)
                return Behaviors.same

    def on_signal(_ctx: ActorContext[M], sig: Signal) -> Behavior[M]:
        if isinstance(sig, Terminated):
            terminations.put_nowait(sig)
        return Behaviors.same

    return Behaviors.receive(on_message).receive_signal(on_signal)


async def _poll[X](queue: asyncio.Queue[X], max: float) -> X | None:
    """The next item within ``max`` seconds, or None; ``max <= 0`` only looks."""
    if max <= 0:
        try:
            return queue.get_nowait()
        except asyncio.QueueEmpty:
            return None
    try:
        return await asyncio.wait_for(queue.get(), max)
    except TimeoutError:
        return None


class TestProbe[M]:
    """``probe: TestProbe[int] = testkit.create_test_probe()``, then pass ``probe.ref`` where
    the actor under test replies, and await the ``expect_*`` methods.

    Every wait is awaited. A wait without ``max`` waits what is left of the enclosing
    ``within``, else ``single_expect_default``. Durations are seconds; an explicit ``max`` is
    multiplied by the settings' ``timefactor``, as Pekko dilates it.
    """

    __test__ = False  # not a pytest test class

    def __init__(self, testkit: ActorTestKit, name: str | None = None) -> None:
        self.settings = testkit.settings
        self._queue: asyncio.Queue[M] = asyncio.Queue()
        self._terminations: asyncio.Queue[Terminated] = asyncio.Queue()
        self._end: float | None = None  # Pekko's `end`, None for Duration.Undefined
        self._last_was_no_message = False
        actor_name = f"{name or 'testProbe'}-{next(_test_actor_id)}"
        cell = testkit.system._system_root.spawn_child(  # pyright: ignore[reportPrivateUsage]
            _test_actor(self._queue, self._terminations), actor_name, None
        )
        self.ref: ActorRef[M] = cell.ref

    # --- remaining ------------------------------------------------------------------

    @property
    def remaining(self) -> float:
        """The time left in the enclosing ``within``; fails outside one."""
        if self._end is None:
            raise AssertionError("`remaining` may not be called outside of `within`")
        return self._end - time.monotonic()

    @property
    def remaining_or_default(self) -> float:
        return self.remaining_or(self.settings.single_expect_default_dilated)

    def remaining_or(self, d: float) -> float:
        return d if self._end is None else self._end - time.monotonic()

    def _max(self, max: float | None) -> float:
        return self.remaining_or_default if max is None else self.settings.dilated(max)

    @asynccontextmanager
    async def within(self, max: float, min: float = 0.0) -> AsyncGenerator[None]:
        """The block must take between ``min`` and ``max`` seconds; waits inside it without a
        ``max`` wait only what is left. A block ending in ``expect_no_message`` may overrun."""
        max = self.settings.dilated(max)
        start = time.monotonic()
        rem = float("inf") if self._end is None else self._end - start
        assert rem >= min, (
            f"required min time {seconds(min)} not possible, only {seconds(rem)} left"
        )
        self._last_was_no_message = False
        max_diff = max if max < rem else rem
        prev_end = self._end
        self._end = start + max_diff
        try:
            yield
        finally:
            self._end = prev_end
        diff = time.monotonic() - start
        assert min <= diff, f"block took {seconds(diff)}, should at least have been {seconds(min)}"
        if not self._last_was_no_message:
            assert diff <= max_diff, f"block took {seconds(diff)}, exceeding {seconds(max_diff)}"

    # --- receiving ------------------------------------------------------------------

    async def _receive_one(self, max: float) -> M | None:
        message = await _poll(self._queue, max)
        self._last_was_no_message = False
        return message

    async def expect_message(self, obj: M, max: float | None = None, hint: str | None = None) -> M:
        """The next message must equal ``obj``; returns it."""
        max = self._max(max)
        if isinstance(obj, Signal):
            raise ValueError(
                f"{type(obj).__name__} is a signal, expecting signals with a TestProbe is not "
                "possible"
            )
        o = await self._receive_one(max)
        hint_or_empty = "" if hint is None else f": {hint}"
        if o is None:
            raise AssertionError(
                f"timeout ({seconds(max)}) during expect_message while waiting for "
                f"{obj!r}{hint_or_empty}"
            )
        if o != obj:
            raise AssertionError(f"expected {obj!r}, found {o!r}{hint_or_empty}")
        return o

    async def expect_message_type[C](self, cls: type[C], max: float | None = None) -> C:
        """The next message must be an instance of ``cls``; returns it, typed as ``cls``."""
        max = self._max(max)
        if issubclass(cls, Signal):
            raise ValueError(
                f"{cls.__name__} is a signal, expecting signals with a TestProbe is not possible"
            )
        o = await self._receive_one(max)
        if o is None:
            raise AssertionError(
                f"Timeout ({seconds(max)}) during expect_message_type waiting for {cls}"
            )
        if not isinstance(o, cls):
            raise AssertionError(f"Expected {cls}, found {type(o)} ({o!r})")
        return cast(C, o)

    async def receive_message(self, max: float | None = None) -> M:
        max = self._max(max)
        o = await self._receive_one(max)
        if o is None:
            raise AssertionError(
                f"Timeout ({seconds(max)}) during receive_message while waiting for message."
            )
        return o

    async def expect_no_message(self, max: float | None = None) -> None:
        """No message may arrive within ``max``, default ``expect_no_message_default``. As in
        Pekko, an explicit ``max`` is not dilated."""
        if max is None:
            max = self.settings.expect_no_message_default_dilated
        o = await self._receive_one(max)
        if o is not None:
            raise AssertionError(f"Received unexpected message {o!r}")
        self._last_was_no_message = True

    async def receive_messages(self, n: int, max: float | None = None) -> list[M]:
        """The next ``n`` messages, all within ``max``."""
        max = self._max(max)
        stop = time.monotonic() + max
        messages: list[M] = []
        for x in range(1, n + 1):
            o = await self._receive_one(stop - time.monotonic())
            if o is None:
                raise AssertionError(
                    f"timeout ({seconds(max)}) while expecting {n} messages (got {x - 1})"
                )
            messages.append(o)
        return messages

    async def fish_for_message(
        self, max: float, fisher: Callable[[M], FishingOutcome], hint: str = ""
    ) -> list[M]:
        """Hand each message to ``fisher`` until it completes; returns the messages it kept,
        ending with the completing one."""
        max = self.settings.dilated(max)
        timeout = max
        seen: list[M] = []
        while True:
            start = time.monotonic()
            message = await self._receive_one(timeout)
            if message is None:
                raise AssertionError(
                    f"timeout ({seconds(max)}) during fish_for_message, seen messages "
                    f"{seen!r}, hint: {hint}"
                )
            match fisher(message):
                case Complete():
                    return [*seen, message]
                case Fail(error):
                    raise AssertionError(f"{error}, hint: {hint}")
                case Continue():
                    seen.append(message)
                case ContinueAndIgnore():
                    pass
                case _:  # a fisher whose match had no case for it, Pekko's MatchError
                    raise AssertionError(
                        f"Unexpected message {message!r} while fishing for messages, "
                        f"seen messages {seen!r}, hint: {hint}"
                    )
            timeout -= time.monotonic() - start

    async def expect_terminated(self, ref: ActorRef[Any], max: float | None = None) -> None:
        """Watch ``ref`` from the probe actor and wait for its Terminated."""
        max = self._max(max)
        cast(ActorRef[Any], self.ref).tell(_WatchActor(ref))
        message = await _poll(self._terminations, max)
        if message is None:
            raise AssertionError(
                f"timeout ({seconds(max)}) during expect_terminated waiting for actor "
                f"[{ref.path}] to stop"
            )
        if message.ref != ref:
            raise AssertionError(
                f"expected [{ref.path}] to stop, but saw [{message.ref.path}] stop"
            )

    async def await_assert[A](
        self, fn: Callable[[], MaybeAwaitable[A]], max: float | None = None, interval: float = 0.1
    ) -> A:
        """Call ``fn`` (sync or async) until it stops raising; after ``max`` the last error is
        raised. Any Exception counts as a failed try, as Pekko's NonFatal."""
        max = self._max(max)
        stop = time.monotonic() + max
        t = min(max, interval)
        while True:
            try:
                r = fn()
                if inspect.isawaitable(r):
                    r = await cast(Awaitable[A], r)
                return cast(A, r)
            except Exception:
                if time.monotonic() + t >= stop:
                    raise
            await asyncio.sleep(t)
            t = min(stop - time.monotonic(), interval)

    def stop(self) -> None:
        """Stop the probe actor."""
        cast(ActorRef[Any], self.ref).tell(_STOP)
