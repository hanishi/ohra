"""Patterns built on the core: ask, ask_with_status and StatusReply, batching, SpawnProtocol."""

from __future__ import annotations

import asyncio
import inspect
import itertools
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Final, Never, Protocol, cast, final

from .behavior import Behavior, Behaviors, MaybeAwaitable
from .mailbox import MailboxSelector
from .ref import ActorRef, PromiseRef
from .remote.serialization import remotable, transient
from .result import Failure, Success, Try
from .scheduler import EventLoopScheduler, Scheduler
from .signals import PostStop, Signal

if TYPE_CHECKING:
    from .cell import TimerScheduler
    from .context import ActorContext

_ids = itertools.count()


class AskTimeoutError(TimeoutError):
    pass


class Replyable[Res](Protocol):
    """A request message: anything with a ``reply_to: ActorRef[Res]`` field. ``ask`` reads the
    reply type from it, which is what lets ``lambda r: Query(sql, r)`` type-check."""

    @property
    def reply_to(self) -> ActorRef[Res]: ...


async def ask[Res](
    target: ActorRef[Any],
    make: Callable[[ActorRef[Res]], Replyable[Res]],
    timeout: float,
    *,
    scheduler: Scheduler | None = None,
) -> Res:
    """Send ``make(reply_to)`` to target and await the first reply.

    ``make`` is usually a lambda or the message class itself. The target's accepted message
    type is not checked here (see README); the reply type is.
    A reply arriving after the timeout goes to dead letters; nothing is left pending. The
    timeout runs on ``scheduler``, the system's when asked through it, else the event loop.
    """
    return await start_ask(target, make, timeout, scheduler=scheduler)


def start_ask[Res](
    target: ActorRef[Any],
    make: Callable[[ActorRef[Res]], Replyable[Res]],
    timeout: float,
    *,
    scheduler: Scheduler | None = None,
) -> asyncio.Future[Res]:
    """Start the timeout and send the request now, as Pekko's AskPattern does, and return the
    reply's future: await it or cancel it, which ends the ask. A failing ``make`` or ``tell``
    raises here."""
    fut: asyncio.Future[Res] = asyncio.get_running_loop().create_future()
    reply_to = PromiseRef(fut, f"{target.path}/$ask-{next(_ids)}")

    def expire() -> None:
        if not fut.done():
            fut.set_exception(AskTimeoutError(f"ask to {target.path} timed out after {timeout}s"))

    timer = (scheduler or EventLoopScheduler()).schedule_once(timeout, expire)
    # the callback holds reply_to until the ask ends: remoting keeps only a weak temp path for it
    fut.add_done_callback(lambda _: (timer.cancel(), reply_to))
    try:
        target.tell(make(reply_to))
    except BaseException:
        timer.cancel()  # now: the done callbacks run only on the next loop iteration
        fut.cancel()
        raise
    return fut


@remotable
@final
@dataclass(frozen=True, slots=True)
class Done:
    """Pekko's ``Done``: a completion, or an acknowledgement, that carries no value."""

    def __repr__(self) -> str:
        return "DONE"


DONE: Final = Done()


@remotable
@dataclass(frozen=True)
class StatusReply[T]:
    """Pekko's ``StatusReply``: a reply that is a success value or an error, for a request that
    can be denied. Build it with ``success``, ``error``, ``from_try`` or ``ACK``; ``value`` gives
    the value or raises the error.

    An error made from a text holds ``ErrorMessage(text)``. One made from an exception keeps the
    exception on this node; on the wire only its message goes, so it arrives as an
    ``ErrorMessage``, as Pekko serializes no arbitrary exception."""

    success_value: T | None = None
    error_message: str | None = None
    cause: BaseException | None = transient()

    ACK: ClassVar[StatusReply[Done]]

    class ErrorMessage(Exception):  # noqa: N818 (Pekko's name)
        """Pekko's ``StatusReply.ErrorMessage``: the error of a reply made from a text."""

        def __eq__(self, other: object) -> bool:
            return isinstance(other, StatusReply.ErrorMessage) and other.args == self.args

        def __hash__(self) -> int:
            return hash(self.args)

    @staticmethod
    def success[V](value: V) -> StatusReply[V]:
        return StatusReply(value)

    @staticmethod
    def error(error: str | BaseException) -> StatusReply[Never]:
        if isinstance(error, str):
            return StatusReply(error_message=error)
        return StatusReply(error_message=str(error), cause=error)

    @staticmethod
    def from_try[V](status: Try[V]) -> StatusReply[V]:
        """A success, or an error with the exception's message, or its class name when it has
        none: the exception itself is not kept."""
        match status:
            case Success(value):
                return StatusReply(value)
            case Failure(e):
                return StatusReply(error_message=str(e) or type(e).__name__)

    @staticmethod
    def from_try_keep_exception[V](status: Try[V]) -> StatusReply[V]:
        match status:
            case Success(value):
                return StatusReply(value)
            case Failure(e):
                return StatusReply.error(e)

    @property
    def is_success(self) -> bool:
        return self.error_message is None

    @property
    def is_error(self) -> bool:
        return self.error_message is not None

    @property
    def value(self) -> T:
        """The success value; for an error, raises it."""
        if self.error_message is not None:
            raise self.exception
        return cast(T, self.success_value)

    @property
    def exception(self) -> BaseException:
        """The error: the exception it was made from on this node, else ``ErrorMessage``."""
        if self.error_message is None:
            raise ValueError("Expected reply to be a failure, but was a success")
        return self.cause or StatusReply.ErrorMessage(self.error_message)

    def __str__(self) -> str:
        if self.error_message is None:
            return f"Success({self.success_value})"
        return f"Error({self.error_message})"


StatusReply.ACK = StatusReply.success(DONE)


def flatten_status(reply: object) -> Any:
    """Pekko's ``flattenStatusFuture`` on one reply: the success value, or raises the error."""
    match reply:
        case StatusReply() if reply.is_success:
            return cast(StatusReply[Any], reply).success_value
        case StatusReply():
            raise cast(StatusReply[Any], reply).exception
        case _:
            raise ValueError(f"Unexpected status reply success value: {reply!r}")


async def ask_with_status[Res](
    target: ActorRef[Any],
    make: Callable[[ActorRef[StatusReply[Res]]], Replyable[StatusReply[Res]]],
    timeout: float,
    *,
    scheduler: Scheduler | None = None,
) -> Res:
    """Pekko's ``askWithStatus``: ``ask`` for a ``StatusReply``, returning its value or raising
    its error. A reply that is not a ``StatusReply`` raises ``ValueError``."""
    return cast(Res, flatten_status(await ask(target, make, timeout, scheduler=scheduler)))


class _Flush:
    __slots__ = ()


_FLUSH = _Flush()


def batching[T](
    max_n: int, max_wait: float, handle: Callable[[list[T]], MaybeAwaitable[None]]
) -> Behavior[T]:
    """Hand messages to ``handle`` in batches: when ``max_n`` have arrived, or ``max_wait``
    seconds after the first message of a batch. Messages arriving while an async
    ``handle`` runs form the next batch. An async ``handle`` holds the batcher, stops included,
    until it returns, so one that can stall needs a timeout. A pending batch is flushed on stop.
    Not in Pekko.
    """

    @Behaviors.with_timers
    def with_timers(timers: TimerScheduler[T | _Flush]) -> Behavior[T | _Flush]:
        batch: list[T] = []

        async def flush() -> None:
            timers.cancel(_FLUSH)
            items, batch[:] = list(batch), []
            r = handle(items)
            if inspect.isawaitable(r):
                await r

        @Behaviors.receive_message
        async def on_message(msg: T | _Flush) -> Behavior[T | _Flush]:
            if isinstance(msg, _Flush):
                await flush()
                return Behaviors.same
            if not batch:
                timers.start_single_timer(_FLUSH, _FLUSH, max_wait)
            batch.append(msg)
            if len(batch) >= max_n:
                await flush()
            return Behaviors.same

        async def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[T | _Flush]:
            if sig is PostStop and batch:
                await flush()
            return Behaviors.same

        return on_message.receive_signal(on_signal)

    return with_timers


class SpawnProtocol:
    """A guardian that spawns on request (org.apache.pekko.actor.typed.SpawnProtocol), for
    creating actors from outside the system::

        async with ActorSystem(SpawnProtocol.apply(), "app") as system:
            ref = await system.ask(lambda r: SpawnProtocol.Spawn(worker(), "worker", r), timeout=1)
    """

    @dataclass(frozen=True, slots=True)
    class Spawn[T]:
        behavior: Behavior[T]
        name: str  # "" spawns anonymously; a taken name gets a "-1", "-2", ... suffix
        reply_to: ActorRef[ActorRef[T]]
        mailbox: MailboxSelector | None = None

    type Command = Spawn[Any]

    @staticmethod
    def apply() -> Behavior[Command]:
        @Behaviors.receive
        def on_message(
            ctx: ActorContext[SpawnProtocol.Command], msg: SpawnProtocol.Command
        ) -> Behavior[SpawnProtocol.Command]:
            if not msg.name:
                ref = ctx.spawn_anonymous(msg.behavior, msg.mailbox)
            else:
                name = msg.name
                for n in itertools.count(1):
                    if ctx.child(name) is None:
                        break
                    name = f"{msg.name}-{n}"
                ref = ctx.spawn(msg.behavior, name, msg.mailbox)
            msg.reply_to.tell(ref)
            return Behaviors.same

        return on_message
