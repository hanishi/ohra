"""``ActorFlow.ask``, a port of Apache Pekko's stream-typed ``ActorFlow.ask``: Pekko's
``Flow.watch(ref).mapAsync(parallelism)(ask)``."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from typing import TYPE_CHECKING, Any, cast

from ..patterns import Replyable, start_ask
from ..ref import ActorRef
from .dsl import NOT_USED, Flow, NotUsed
from .ops import Stage, closing, map_async_op, watch_ref

if TYPE_CHECKING:
    from .dsl import _Ctx  # pyright: ignore[reportPrivateUsage]


class WatchedActorTerminatedError(RuntimeError):
    """Pekko's ``WatchedActorTerminatedException``: an actor a stage watches has stopped."""

    def __init__(self, watching_stage_name: str, ref: ActorRef[Any]) -> None:
        super().__init__(f"Actor watched by [{watching_stage_name}] has terminated! Was: {ref!r}")
        self.ref = ref


class ActorFlow:
    """Pekko's stream-typed ``ActorFlow``."""

    @staticmethod
    def ask[I, Q, A](
        ref: ActorRef[Q],
        make_message: Callable[[I, ActorRef[A]], Q],
        timeout: float,
        parallelism: int = 2,
    ) -> Flow[I, A, NotUsed]:
        """Asks ``ref`` ``make_message(element, reply_to)`` for each element and emits the
        replies in order, at most ``parallelism`` asks in flight. Fails with ``AskTimeoutError``
        when a reply takes longer than ``timeout`` (on the system scheduler), and with
        ``WatchedActorTerminatedError`` when ``ref`` stops."""
        if parallelism < 1:
            raise ValueError("parallelism must be at least 1")

        def build(upstream: AsyncIterator[I], ctx: _Ctx) -> tuple[AsyncIterator[A], NotUsed]:
            return Stage(_ask(upstream, ctx), upstream), NOT_USED

        async def _ask(upstream: AsyncIterator[I], ctx: _Ctx) -> AsyncGenerator[A, None]:
            stop: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            stop.add_done_callback(lambda f: f.cancelled() or f.exception())  # may go unread

            def terminated() -> None:
                if not stop.done():
                    stop.set_exception(WatchedActorTerminatedError("ask()", ref))

            def ask(element: I) -> asyncio.Future[A]:
                def message(reply_to: ActorRef[A]) -> Replyable[A]:
                    return cast(Replyable[A], make_message(element, reply_to))

                return start_ask(ref, message, timeout, scheduler=ctx.system.scheduler)

            unwatch = watch_ref(ref, terminated)
            try:
                op = map_async_op(parallelism, ask, True, stop)
                async with closing(op(upstream, ctx)) as replies:
                    async for reply in replies:
                        yield reply
            finally:
                unwatch()

        return Flow(build)
