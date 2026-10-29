"""Behaviors: values describing how an actor reacts to the next message.

Mirrors org.apache.pekko.actor.typed.scaladsl.Behaviors. A handler returns the
next behavior; returning ``Behaviors.same`` keeps the current one.
Handlers may be plain functions or coroutines.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .signals import Signal
from .supervision import SupervisorStrategy

if TYPE_CHECKING:
    from .persistence import Store, _Persister  # pyright: ignore[reportPrivateUsage]
    from .cell import TimerScheduler
    from .context import ActorContext

type MaybeAwaitable[X] = X | Awaitable[X]


class Behavior[T]:
    """Base of all behaviors. T is the message type the actor accepts."""

    __slots__ = ()

    def _accepts(self, msg: T) -> None:  # pins T as contravariant, never called
        raise NotImplementedError


class _Same(Behavior[Any]):
    __slots__ = ()

    def __repr__(self) -> str:
        return "Behaviors.same"


class _Unhandled(Behavior[Any]):
    __slots__ = ()

    def __repr__(self) -> str:
        return "Behaviors.unhandled"


@dataclass(frozen=True, eq=False, slots=True)
class Stopped(Behavior[Any]):
    post_stop: Callable[[], None] | None = None


@dataclass(frozen=True, eq=False, slots=True)
class Setup[T](Behavior[T]):
    factory: Callable[[ActorContext[T]], MaybeAwaitable[Behavior[T]]]


@dataclass(frozen=True, eq=False, slots=True)
class Receive[T](Behavior[T]):
    on_message: Callable[[ActorContext[T], T], MaybeAwaitable[Behavior[T]]]
    on_signal: Callable[[ActorContext[T], Signal], MaybeAwaitable[Behavior[T]]] | None = None

    def receive_signal(
        self, handler: Callable[[ActorContext[T], Signal], MaybeAwaitable[Behavior[T]]]
    ) -> Receive[T]:
        return Receive(self.on_message, handler)


@dataclass(frozen=True, eq=False, slots=True)
class WithTimers[T](Behavior[T]):
    factory: Callable[[TimerScheduler[T]], MaybeAwaitable[Behavior[T]]]


@dataclass(frozen=True, eq=False, slots=True)
class Unstash[T](Behavior[T]):
    """Returned by StashBuffer.unstash_all: the runtime replays ``messages`` into ``behavior``.
    Persistence also uses it to hand over what it held during a store call: ``signals`` are
    delivered first, and ``stop_after`` stops the actor once the replay is done, as an
    ``after_pending`` stop would have."""

    messages: deque[T]
    behavior: Behavior[T]
    signals: tuple[Signal, ...] = ()
    stop_after: bool = False
    held: Held | None = None


class StoreAnswer:
    """Base of the messages a store call sends back to a persisted actor. Only a Holding takes
    them; one that arrives after its Holding is gone, from an earlier incarnation, is ignored."""

    __slots__ = ()


@dataclass(eq=False)
class Held:
    """What a Holding keeps until its store call answers: the stashed commands, the signals,
    whether an ``after_pending`` stop came (Pekko's receivedPoisonPill), whether the recovery
    itself failed, which supervision treats as a failing start, and the behavior it handed over
    to once the store answered, as the runtime started it, which a ``resume`` keeps."""

    stash: deque[Any]
    signals: list[Signal]
    poisoned: bool = False
    recovery_failed: bool = False
    answered: Any = None


@dataclass(frozen=True, eq=False, slots=True)
class Holding[T](Receive[T]):
    """A Receive waiting on a store: persistence's recovering and persisting states. PostStop and
    PreRestart go to ``visible``, the behavior the user's code had before the write (Pekko's
    visibleState), which the runtime fills in; an ``after_pending`` stop is delivered to it as
    ``POISON_PILL`` instead of stopping the actor, as Pekko's DurableStateBehavior handles a
    PoisonPill."""

    visible: Behavior[T] | None = None
    held: Held = field(default_factory=lambda: Held(deque(), []))


class _PoisonPill:
    __slots__ = ()

    def __repr__(self) -> str:
        return "POISON_PILL"


POISON_PILL: Any = _PoisonPill()


class StashOverflowError(Exception):
    """Raised by StashBuffer.stash when the buffer is full."""


class StashBuffer[T]:
    """Pekko's StashBuffer. Create one with ``Behaviors.with_stash``."""

    __slots__ = ("_capacity", "_messages")

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._messages: deque[T] = deque()

    @property
    def size(self) -> int:
        return len(self._messages)

    @property
    def is_empty(self) -> bool:
        return not self._messages

    @property
    def is_full(self) -> bool:
        return len(self._messages) >= self._capacity

    def stash(self, msg: T) -> None:
        if self.is_full:
            raise StashOverflowError(f"stash buffer is full ({self._capacity})")
        self._messages.append(msg)

    def unstash_all(self, behavior: Behavior[T]) -> Behavior[T]:
        """Process the stashed messages, in order, with ``behavior`` before any new message.

        Messages stashed during the replay stay in the buffer. Behaves like
        ``Behaviors.same`` once the buffer is empty."""
        return Unstash(self._messages, behavior)


@dataclass(frozen=True, eq=False, slots=True)
class Supervised[T](Behavior[T]):
    """Template: the runtime wraps it in a supervisor when the actor starts."""

    initial: Behavior[T]
    strategy: SupervisorStrategy
    exc_type: type[Exception] = Exception


class _SuperviseBuilder[T]:
    __slots__ = ("_behavior",)

    def __init__(self, behavior: Behavior[T]) -> None:
        self._behavior = behavior

    def on_failure(
        self, strategy: SupervisorStrategy, exc_type: type[Exception] = Exception
    ) -> Behavior[T]:
        return Supervised(self._behavior, strategy, exc_type)


SAME: Behavior[Any] = _Same()
UNHANDLED: Behavior[Any] = _Unhandled()


def _unhandled_message(_ctx: Any, _msg: Any) -> Behavior[Any]:
    return UNHANDLED


def _ignore_message(_ctx: Any, _msg: Any) -> Behavior[Any]:
    return SAME


class Behaviors:
    """Factory namespace, named after the Scala DSL."""

    same: Behavior[Any] = SAME
    unhandled: Behavior[Any] = UNHANDLED
    empty: Behavior[Any] = Receive(_unhandled_message)
    ignore: Behavior[Any] = Receive(_ignore_message)

    @staticmethod
    def stopped(post_stop: Callable[[], None] | None = None) -> Behavior[Any]:
        return Stopped(post_stop)

    @staticmethod
    def setup[T](factory: Callable[[ActorContext[T]], MaybeAwaitable[Behavior[T]]]) -> Behavior[T]:
        return Setup(factory)

    @staticmethod
    def receive[T](
        handler: Callable[[ActorContext[T], T], MaybeAwaitable[Behavior[T]]],
    ) -> Receive[T]:
        return Receive(handler)

    @staticmethod
    def receive_signal[T](
        handler: Callable[[ActorContext[T], Signal], MaybeAwaitable[Behavior[T]]],
    ) -> Receive[T]:
        """Handle signals only; messages are unhandled."""
        return Receive(_unhandled_message, handler)

    @staticmethod
    def receive_message[T](handler: Callable[[T], MaybeAwaitable[Behavior[T]]]) -> Receive[T]:
        def on_message(_ctx: ActorContext[T], msg: T) -> MaybeAwaitable[Behavior[T]]:
            return handler(msg)

        return Receive(on_message)

    @staticmethod
    def supervise[T](behavior: Behavior[T]) -> _SuperviseBuilder[T]:
        return _SuperviseBuilder(behavior)

    @staticmethod
    def persisted[S](store: Store[S], stash_capacity: int = 4096) -> _Persister[S]:
        """Make a state-to-behavior factory persisted: ``@Behaviors.persisted(store)`` on ``def
        cart(items)``. Calling the factory writes the state before becoming; ``factory.recover(id,
        empty)`` starts it. Commands that arrive while the actor recovers or writes wait in an
        internal stash of ``stash_capacity``."""
        from .persistence import persisted

        return persisted(store, stash_capacity)

    @staticmethod
    def with_timers[T](
        factory: Callable[[TimerScheduler[T]], MaybeAwaitable[Behavior[T]]],
    ) -> Behavior[T]:
        """Give the behavior access to the actor's timers. All timers are cancelled on restart."""
        return WithTimers(factory)

    @staticmethod
    def with_stash[T](
        capacity: int, factory: Callable[[StashBuffer[T]], MaybeAwaitable[Behavior[T]]]
    ) -> Behavior[T]:
        """Give the behavior a fresh StashBuffer per incarnation."""
        return Setup(lambda _ctx: factory(StashBuffer(capacity)))
