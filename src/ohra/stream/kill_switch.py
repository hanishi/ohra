"""Apache Pekko's ``KillSwitches.single`` and ``UniqueKillSwitch``: a flow stage that completes
or fails its stream from outside."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator

from .dsl import Flow, _Ctx  # pyright: ignore[reportPrivateUsage]
from .ops import Stage, closing


class UniqueKillSwitch:
    """Controls the one materialization of ``KillSwitches.single`` it came from. The first
    ``shutdown`` or ``abort`` wins; later ones, and any after the stream has ended, are
    ignored."""

    __slots__ = ("_signal",)

    def __init__(self, signal: asyncio.Future[BaseException | None]) -> None:
        self._signal = signal  # the result is None for shutdown, the exception for abort

    def shutdown(self) -> None:
        """Complete downstream and cancel upstream."""
        if not self._signal.done():
            self._signal.set_result(None)

    def abort(self, exc: BaseException) -> None:
        """Fail downstream with ``exc`` and cancel upstream."""
        if not self._signal.done():
            self._signal.set_result(exc)


class KillSwitches:
    @staticmethod
    def single[T]() -> Flow[T, T, UniqueKillSwitch]:
        """A pass-through flow materializing a new ``UniqueKillSwitch`` on each run."""

        def build(
            upstream: AsyncIterator[T], _ctx: _Ctx
        ) -> tuple[AsyncIterator[T], UniqueKillSwitch]:
            signal: asyncio.Future[BaseException | None] = (
                asyncio.get_running_loop().create_future()
            )
            return Stage(_killable(upstream, signal), upstream), UniqueKillSwitch(signal)

        return Flow(build)


async def _killable[T](
    upstream: AsyncIterator[T], signal: asyncio.Future[BaseException | None]
) -> AsyncGenerator[T, None]:
    """After Pekko's ``KillableGraphStageLogic``: each upstream pull, in a task, races the
    switch. A switch already thrown when the stage starts ends it before any pull."""
    async with closing(upstream) as up:
        pull: asyncio.Future[T] | None = None
        try:
            while not signal.done():
                pull = asyncio.ensure_future(anext(up))
                await asyncio.wait((pull, signal), return_when=asyncio.FIRST_COMPLETED)
                if signal.done():
                    break
                got, pull = pull, None
                try:
                    x = got.result()
                except StopAsyncIteration:
                    return
                yield x
        finally:
            # before upstream closes: a pull still suspended inside it would make its aclose()
            # fail with "already running"
            if pull is not None:
                pull.cancel()
                await asyncio.gather(pull, return_exceptions=True)
    exc = signal.result()
    if exc is not None:
        raise exc  # upstream is cancelled first, then downstream fails
