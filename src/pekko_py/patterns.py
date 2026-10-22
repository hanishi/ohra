"""Patterns built on the core: ask, batching, SpawnProtocol."""
from __future__ import annotations

import asyncio
import inspect
import itertools
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .behavior import Behavior, Behaviors, MaybeAwaitable
from .mailbox import MailboxSelector
from .ref import ActorRef, PromiseRef
from .signals import PostStop, Signal

if TYPE_CHECKING:
    from .cell import TimerScheduler
    from .context import ActorContext

_ids = itertools.count()


class AskTimeoutError(TimeoutError):
    pass


async def ask[Req, Res](
    target: ActorRef[Req], make: Callable[[ActorRef[Res]], Req], timeout: float
) -> Res:
    """Send ``make(reply_to)`` to target and await the first reply.

    A reply arriving after the timeout goes to dead letters; nothing is left pending.
    """
    fut: asyncio.Future[Res] = asyncio.get_running_loop().create_future()
    target.tell(make(PromiseRef(fut, f"{target.path}/$ask-{next(_ids)}")))
    try:
        return await asyncio.wait_for(fut, timeout)
    except TimeoutError:
        raise AskTimeoutError(f"ask to {target.path} timed out after {timeout}s") from None


class _Flush:
    __slots__ = ()


_FLUSH = _Flush()


def batching[T](
    max_n: int, max_wait: float, handle: Callable[[list[T]], MaybeAwaitable[None]]
) -> Behavior[T]:
    """Hand messages to ``handle`` in batches: when ``max_n`` have arrived, or ``max_wait``
    seconds after the first message of a batch. Messages arriving while an async
    ``handle`` runs form the next batch. A pending batch is flushed on stop. Not in Pekko.
    """

    def with_timers(timers: TimerScheduler[T | _Flush]) -> Behavior[T | _Flush]:
        batch: list[T] = []

        async def flush() -> None:
            timers.cancel(_FLUSH)
            items, batch[:] = list(batch), []
            r = handle(items)
            if inspect.isawaitable(r):
                await r

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

        return Behaviors.receive_message(on_message).receive_signal(on_signal)

    return Behaviors.with_timers(with_timers)


class SpawnProtocol:
    """A guardian that spawns on request (org.apache.pekko.actor.typed.SpawnProtocol), for
    creating actors from outside the system::

        async with ActorSystem(SpawnProtocol.apply(), "app") as system:
            ref = await system.ask(partial(SpawnProtocol.Spawn[WorkerCmd], worker(), "worker"), timeout=1)

    Name the type parameter in the partial: pyright cannot infer it from the behavior alone.
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
        def on_message(ctx: ActorContext[SpawnProtocol.Command], msg: SpawnProtocol.Command) -> Behavior[SpawnProtocol.Command]:
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

        return Behaviors.receive(on_message)
