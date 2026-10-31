"""Pekko's typed EventStream, reached as ``system.event_stream``, and the dead-letter events.

Every dead letter, unhandled message and dropped message is published on it, as in Pekko. A
subscription to a class also gets its subclasses: subscribe to ``AllDeadLetters`` for all three.
The stream is local to one actor system.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .behavior import Behavior, Behaviors
from .signals import Signal, Terminated

if TYPE_CHECKING:
    from .context import ActorContext
    from .ref import ActorRef


class AllDeadLetters:
    """The base of the three events about a message that was not handled."""

    __slots__ = ()
    message: object
    recipient: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class DeadLetter(AllDeadLetters):
    """A message that could not be delivered: its actor had stopped, or its mailbox was full."""

    message: object
    recipient: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class UnhandledMessage(AllDeadLetters):
    """A message the actor's behavior did not handle."""

    message: object
    recipient: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class Dropped(AllDeadLetters):
    """A message dropped on purpose, with the reason."""

    message: object
    reason: str
    recipient: ActorRef[Any]


class DeadLetterSuppression:
    """Pekko's marker for a message whose dead letter is expected and harmless, such as a cluster
    heartbeat to a node that has crashed. Its dead letter is logged at debug level, not
    published."""

    __slots__ = ()


class EventStream:
    """The commands ``system.event_stream`` accepts."""

    @dataclass(frozen=True, slots=True)
    class Publish:
        event: object

    @dataclass(frozen=True, slots=True)
    class Subscribe[E]:
        """Deliver every published event that is an instance of ``topic`` to ``subscriber``.
        Pekko reads the topic from the type parameter; Python needs the class."""

        topic: type[E]
        subscriber: ActorRef[E]

        def __post_init__(self) -> None:
            # pyright takes a generic alias such as list[int] as a class; isinstance cannot
            if not isinstance(self.topic, type):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise TypeError(f"topic must be a class, not {self.topic!r}")

    @dataclass(frozen=True, slots=True)
    class Unsubscribe:
        """Remove ``subscriber`` from every topic."""

        subscriber: ActorRef[Any]

    type Command = Publish | Subscribe[Any] | Unsubscribe


def is_suppressed(msg: object, dead_letter: bool = False) -> bool:
    """A message that is only logged: one of the stream's own, which would otherwise loop through
    a stopped subscriber or a stopped stream, or, as a dead letter, a ``DeadLetterSuppression``,
    which as in Pekko still publishes when unhandled or dropped."""
    return isinstance(msg, (AllDeadLetters, EventStream.Publish)) or (
        dead_letter and isinstance(msg, DeadLetterSuppression)
    )


def _watched(ref: ActorRef[Any]) -> ActorRef[Any] | None:
    """The actor whose Terminated ends this subscription: the ref's own, or a message adapter's
    owner. None for a ref that is not a local actor, which is never watched."""
    from .cell import _cell_of  # pyright: ignore[reportPrivateUsage]

    try:
        return _cell_of(ref).ref
    except TypeError:
        return None


def event_stream(
    subscribers: dict[ActorRef[Any], tuple[type, ...]],
) -> Behavior[EventStream.Command]:
    """The stream's actor, at ``/system/eventStream``. It watches its subscribers and forgets
    each one that stops, as Pekko's EventStreamUnsubscriber does."""

    def on_message(
        ctx: ActorContext[EventStream.Command], msg: EventStream.Command
    ) -> Behavior[EventStream.Command]:
        match msg:
            case EventStream.Publish(event):
                for ref, topics in subscribers.items():
                    if isinstance(event, topics):
                        try:  # as in Pekko, publishing never fails: a refused event is dead
                            ref.tell(event)
                        except Exception:
                            ctx.system.dead_letter(event, ref)
                return Behaviors.same
            case EventStream.Subscribe(topic, ref):
                if topic in subscribers.get(ref, ()):  # as Pekko's subscribe: already there
                    return Behaviors.same
                watched = _watched(ref)
                if ref not in subscribers and watched is not None:
                    ctx.watch(watched)
                return event_stream({**subscribers, ref: (*subscribers.get(ref, ()), topic)})
            case EventStream.Unsubscribe(ref):
                if ref not in subscribers:
                    return Behaviors.same
                rest = {r: t for r, t in subscribers.items() if r != ref}
                watched = _watched(ref)
                if watched is not None and all(_watched(r) != watched for r in rest):
                    ctx.unwatch(watched)
                return event_stream(rest)

    def on_signal(
        _ctx: ActorContext[EventStream.Command], sig: Signal
    ) -> Behavior[EventStream.Command]:
        if isinstance(sig, Terminated):
            return event_stream({r: t for r, t in subscribers.items() if _watched(r) != sig.ref})
        return Behaviors.same

    return Behaviors.receive(on_message).receive_signal(on_signal)
