"""throttle (shaping mode), after Apache Pekko's FlowThrottleSpec."""

from __future__ import annotations

import asyncio

import pytest

from ohra.stream import Sink, SinkQueue, Source
from ohra.testkit import ActorTestKit, ManualTime

from ..util import eventually


async def _not_yet(q: SinkQueue[int]) -> None:
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(q.pull(), 0.05)


async def test_throttle_spaces_elements():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        q = Source.from_iterable([1, 2, 3]).throttle(1, 1.0, 1).run_with(Sink.queue(), kit.system)
        assert await asyncio.wait_for(q.pull(), 1) == 1
        await _not_yet(q)
        await time.time_passes(0.5)
        await _not_yet(q)
        await time.time_passes(0.5)
        assert await asyncio.wait_for(q.pull(), 1) == 2
        await _not_yet(q)
        await time.time_passes(1.0)
        assert await asyncio.wait_for(q.pull(), 1) == 3
        assert await asyncio.wait_for(q.pull(), 1) is None


async def test_throttle_allows_burst():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        q = (
            Source.from_iterable([1, 2, 3, 4])
            .throttle(1, 1.0, 3)
            .run_with(Sink.queue(), kit.system)
        )
        for i in (1, 2, 3):
            assert await asyncio.wait_for(q.pull(), 1) == i
        await _not_yet(q)
        await time.time_passes(1.0)
        assert await asyncio.wait_for(q.pull(), 1) == 4


async def test_throttle_default_burst_follows_the_rate():
    # 100 per second: a token every 10 ms, so max(1, int(0.1 / 0.01)) = 10 may burst
    async with ActorTestKit(manual_time=True) as kit:
        q = Source.from_iterable(range(11)).throttle(100, 1.0).run_with(Sink.queue(), kit.system)
        for i in range(10):
            assert await asyncio.wait_for(q.pull(), 1) == i
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(q.pull(), 0.05)


async def test_throttle_works_as_a_flow():
    from ohra.stream import Flow

    async with ActorTestKit(manual_time=True) as kit:
        flow = Flow.of(int).throttle(1, 1.0, 2)
        q = Source.from_iterable([1, 2, 3]).via(flow).run_with(Sink.queue(), kit.system)
        assert await asyncio.wait_for(q.pull(), 1) == 1
        assert await asyncio.wait_for(q.pull(), 1) == 2
        await _not_yet(q)


async def test_throttle_rejects_bad_arguments():
    with pytest.raises(ValueError):
        Source.single(1).throttle(0, 1.0)
    with pytest.raises(ValueError):
        Source.single(1).throttle(1, 0.0)
    with pytest.raises(ValueError):
        Source.single(1).throttle(1, 1.0, -1)


async def test_throttle_cancel_while_waiting_cancels_upstream_and_timer():
    log: list[str] = []
    gate = asyncio.Event()

    async def upstream():
        try:
            yield 1
            yield 2
            await gate.wait()  # never set: really suspends
            yield 3
        finally:
            log.append("upstream closed")

    async with ActorTestKit(manual_time=True) as kit:
        q = (
            Source.from_async_iterable(upstream)
            .throttle(1, 1.0, 1)
            .run_with(Sink.queue(), kit.system)
        )
        assert await asyncio.wait_for(q.pull(), 1) == 1
        await _not_yet(q)  # 2 waits for the timer
        q.cancel()
        await eventually(lambda: log == ["upstream closed"])
        assert not kit.system.scheduler._scheduled  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage, reportUnknownMemberType]
