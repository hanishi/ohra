"""ActorContext: the actor's handle on itself and the system. Only use it from inside the actor."""
from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from .mailbox import MailboxSelector
from .patterns import Replyable, ask
from .ref import ActorRef, FunctionRef
from .result import Try

if TYPE_CHECKING:
    from .behavior import Behavior
    from .cell import ActorCell, TimerScheduler
    from .system import ActorSystem


class ActorContext[T]:
    __slots__ = ("_cell",)

    def __init__(self, cell: ActorCell[T]) -> None:
        self._cell = cell

    @property
    def self(self) -> ActorRef[T]:
        return self._cell.ref

    @property
    def system(self) -> ActorSystem[Any]:
        return self._cell.system

    @property
    def log(self) -> logging.Logger:
        return self._cell.log

    @property
    def timers(self) -> TimerScheduler[T]:
        """This actor's timers, the same scheduler ``Behaviors.with_timers`` hands out.
        Lets a single ``Behaviors.setup`` start timers without another factory layer.
        Not in Apache Pekko, where timers come only through ``withTimers``."""
        return self._cell.timers

    @property
    def children(self) -> list[ActorRef[Any]]:
        return [c.ref for c in self._cell.children.values()]

    def child(self, name: str) -> ActorRef[Any] | None:
        c = self._cell.children.get(name)
        return None if c is None else c.ref

    # --- creation and termination -------------------------------------------

    def spawn[U](
        self, behavior: Behavior[U], name: str, mailbox: MailboxSelector | None = None
    ) -> ActorRef[U]:
        return self._cell.spawn_child(behavior, name, mailbox).ref

    def spawn_anonymous[U](
        self, behavior: Behavior[U], mailbox: MailboxSelector | None = None
    ) -> ActorRef[U]:
        return self._cell.spawn_child(behavior, None, mailbox).ref

    def stop(self, child: ActorRef[Any], *, after_pending: bool = False) -> None:
        """Stop a direct child after its current message; its pending messages become dead letters.
        With after_pending=True the stop queues behind the messages already in the child's mailbox,
        like classic Pekko's PoisonPill, which Pekko Typed has no public equivalent of."""
        self._cell.stop_child(child, after_pending)

    # --- DeathWatch -----------------------------------------------------------

    def watch(self, ref: ActorRef[Any]) -> None:
        """Receive Terminated (or ChildFailed for a failed child) as a signal when ref stops.
        Leaving that signal unhandled fails this actor with DeathPactError."""
        self._cell.watch(ref, None)

    def watch_with(self, ref: ActorRef[Any], msg: T) -> None:
        """Receive msg as an ordinary message when ref stops."""
        self._cell.watch(ref, (msg,))

    def unwatch(self, ref: ActorRef[Any]) -> None:
        self._cell.unwatch(ref)

    # --- async integration ----------------------------------------------------

    def pipe_to_self[R](self, awaitable: Awaitable[R], adapt: Callable[[Try[R]], T]) -> None:
        """Run awaitable in the background; deliver adapt(Success|Failure) to this actor.

        adapt runs inside the actor, so it may read actor state safely. The background
        work is cancelled if the actor stops first."""
        self._cell.pipe_to_self(awaitable, adapt)

    def message_adapter[U](self, f: Callable[[U], T]) -> ActorRef[U]:
        """A ref that accepts U and delivers f(u) to this actor. f runs inside the actor.
        The actor keeps the adapter for its lifetime, so create adapters in setup rather than per message."""
        ref: FunctionRef[U] = FunctionRef(
            f"{self._cell.path}/$adapter", lambda u: self._cell.send_adapted(lambda: f(u)), owner=self._cell
        )
        self._cell.adapters.append(ref)
        return ref

    def ask[Res](
        self,
        target: ActorRef[Any],
        make: Callable[[ActorRef[Res]], Replyable[Res]],
        adapt: Callable[[Try[Res]], T],
        timeout: float,
    ) -> None:
        """Ask from inside an actor: the reply (or timeout failure) arrives as adapt(result)."""
        reply: Awaitable[Res] = ask(target, make, timeout)
        self.pipe_to_self(reply, adapt)
