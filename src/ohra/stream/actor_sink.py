"""``ActorSink.actor_ref`` and ``ActorSink.actor_ref_with_backpressure``, ports of Apache Pekko's
stream-typed ``ActorSink``, after its ``ActorRefSinkStage`` and ``ActorRefBackpressureSinkStage``.

Both watch their target: when it stops, the sink completes and cancels upstream. A stream that
ends without the completion or failure message having been sent (the stream was cancelled or
aborted) sends ``on_failure_message(AbruptStageTerminationError(...))``, as Pekko's ``postStop``.
"""

from __future__ import annotations

import asyncio
import itertools
from collections import deque
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import TYPE_CHECKING, Any

from ..ref import ActorRef, FunctionRef
from .dsl import NOT_USED, AbruptStageTerminationError, NotUsed, Sink
from .ops import run_drive, watch_ref

if TYPE_CHECKING:
    from .dsl import _Ctx  # pyright: ignore[reportPrivateUsage]

_ids = itertools.count()

# Pekko's default InputBuffer max, which ActorRefBackpressureSinkStage buffers up to
_MAX_BUFFER = 16


class _Stopped:
    """The target stopped while the sink waited for upstream."""


_STOPPED = _Stopped()


async def _pull_unless[T](up: AsyncIterator[T], stop: asyncio.Future[None]) -> T | _Stopped:
    """The next element, or ``_STOPPED`` if ``stop`` completes first. A pull still running is
    cancelled and awaited, so that upstream can be closed."""
    pull = asyncio.ensure_future(anext(up))
    try:
        await asyncio.wait((pull, stop), return_when=asyncio.FIRST_COMPLETED)
    finally:
        if not pull.done():
            pull.cancel()
            await asyncio.gather(pull, return_exceptions=True)
    return _STOPPED if pull.cancelled() else pull.result()


def _terminated(ref: ActorRef[Any]) -> tuple[asyncio.Future[None], Callable[[], None]]:
    fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def stopped() -> None:
        if not fut.done():
            fut.set_result(None)

    return fut, watch_ref(ref, stopped)


class ActorSink:
    """Pekko's stream-typed ``ActorSink``."""

    @staticmethod
    def actor_ref[M](
        ref: ActorRef[M], on_complete_message: M, on_failure_message: Callable[[BaseException], M]
    ) -> Sink[M, NotUsed]:
        """Tells ``ref`` every element, then ``on_complete_message``, or
        ``on_failure_message(exc)`` when the stream fails. No backpressure: ``ref``'s mailbox
        may fill up."""

        def build(
            upstream: AsyncIterator[M], ctx: _Ctx
        ) -> tuple[Coroutine[Any, Any, None], NotUsed]:
            async def drive() -> None:
                signalled = False
                terminated, unwatch = _terminated(ref)

                async def consume(up: AsyncIterator[M]) -> None:
                    nonlocal signalled
                    while True:
                        try:
                            x = await _pull_unless(up, terminated)
                        except StopAsyncIteration:
                            signalled = True
                            ref.tell(on_complete_message)
                            return
                        except Exception as e:
                            signalled = True
                            ref.tell(on_failure_message(e))
                            return
                        if isinstance(x, _Stopped):  # Terminated(ref): complete the stage
                            return
                        ref.tell(x)

                try:
                    await run_drive(ctx, upstream, consume)
                finally:
                    unwatch()
                    if not signalled:
                        ref.tell(
                            on_failure_message(
                                AbruptStageTerminationError("stream terminated abruptly")
                            )
                        )

            return drive(), NOT_USED

        return Sink(build)

    @staticmethod
    def actor_ref_with_backpressure[T, M, A](
        ref: ActorRef[M],
        message_adapter: Callable[[ActorRef[A], T], M],
        on_init_message: Callable[[ActorRef[A]], M],
        ack_message: A,
        on_complete_message: M,
        on_failure_message: Callable[[BaseException], M],
    ) -> Sink[T, NotUsed]:
        """Tells ``ref`` ``on_init_message(ack_to)`` first, then each element as
        ``message_adapter(ack_to, element)``, one at a time: the next goes only after ``ref`` has
        told ``ack_to`` the ``ack_message``, for the init message as for each element. Then
        ``on_complete_message``, once the last element is acked, or ``on_failure_message(exc)``
        at once when the stream fails. Upstream is pulled ahead up to 16 elements."""

        def build(
            upstream: AsyncIterator[T], ctx: _Ctx
        ) -> tuple[Coroutine[Any, Any, None], NotUsed]:
            async def drive() -> None:
                signalled = False
                terminated, unwatch = _terminated(ref)
                acks = 0
                sig: asyncio.Future[None] | None = None

                def on_ack(msg: A) -> None:
                    nonlocal acks
                    if msg == ack_message:  # other messages are ignored, as in Pekko
                        acks += 1
                        if sig is not None and not sig.done():
                            sig.set_result(None)

                ack_to = FunctionRef[A](f"/temp/$sink-{next(_ids)}", on_ack)

                async def consume(up: AsyncIterator[T]) -> None:
                    nonlocal signalled, acks, sig
                    buffer: deque[T] = deque()
                    ack_received = False
                    upstream_done = False
                    pull: asyncio.Future[T] | None = None

                    def finish() -> None:
                        nonlocal signalled
                        ref.tell(on_complete_message)
                        signalled = True

                    ref.tell(on_init_message(ack_to))
                    try:
                        while True:
                            if pull is None and not upstream_done and len(buffer) < _MAX_BUFFER:
                                pull = asyncio.ensure_future(anext(up))
                            if not acks:
                                sig = asyncio.get_running_loop().create_future()
                                waiting: set[asyncio.Future[Any]] = {sig, terminated}
                                if pull is not None:
                                    waiting.add(pull)
                                await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
                            if terminated.done():  # Terminated(ref): complete the stage
                                return
                            if pull is not None and pull.done():
                                got, pull = pull, None
                                try:
                                    buffer.append(got.result())  # onPush
                                except StopAsyncIteration:  # onUpstreamFinish
                                    upstream_done = True
                                    if not buffer:
                                        finish()
                                        return
                                except Exception as e:  # onUpstreamFailure
                                    ref.tell(on_failure_message(e))
                                    signalled = True
                                    return
                                else:
                                    if ack_received:
                                        ref.tell(message_adapter(ack_to, buffer.popleft()))
                                        ack_received = False
                            while acks:  # the ack, as the stage actor receives it
                                acks -= 1
                                if not buffer:
                                    ack_received = True
                                    if upstream_done:
                                        finish()
                                        return
                                else:
                                    ref.tell(message_adapter(ack_to, buffer.popleft()))
                    finally:
                        # before upstream closes: a pull suspended inside it can't be closed
                        if pull is not None and not pull.done():
                            pull.cancel()
                            await asyncio.gather(pull, return_exceptions=True)

                try:
                    await run_drive(ctx, upstream, consume)
                finally:
                    unwatch()
                    if not signalled:
                        ref.tell(
                            on_failure_message(
                                AbruptStageTerminationError("stream terminated abruptly")
                            )
                        )

            return drive(), NOT_USED

        return Sink(build)
