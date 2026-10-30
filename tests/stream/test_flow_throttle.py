"""throttle (shaping mode), after Apache Pekko's FlowThrottleSpec."""

from __future__ import annotations

import asyncio

import pytest

from ohra.stream import Flow, Sink, Source
from ohra.testkit import ActorTestKit, ManualTime, TestProbe

from ..util import eventually


async def test_throttle_spaces_elements():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        probe: TestProbe[int] = kit.create_test_probe()
        done = (
            Source.from_iterable([1, 2, 3])
            .throttle(1, 1.0, 1)
            .run_foreach(probe.ref.tell, kit.system)
        )
        await probe.expect_message(1)
        await time.expect_no_message_for(0.5, probe)
        await time.expect_no_message_for(0.4, probe)
        await time.time_passes(0.1)
        await probe.expect_message(2)
        await time.expect_no_message_for(0.9, probe)
        await time.time_passes(0.1)
        await probe.expect_message(3)
        await done


async def test_throttle_allows_burst():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        probe: TestProbe[int] = kit.create_test_probe()
        done = (
            Source.from_iterable([1, 2, 3, 4])
            .throttle(1, 1.0, 3)
            .run_foreach(probe.ref.tell, kit.system)
        )
        for i in (1, 2, 3):
            await probe.expect_message(i)
        await time.expect_no_message_for(0.9, probe)
        await time.time_passes(0.1)
        await probe.expect_message(4)
        await done


async def test_throttle_default_burst_follows_the_rate():
    # 100 per second: a token every 10 ms, so max(1, int(0.1 / 0.01)) = 10 may burst
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        probe: TestProbe[int] = kit.create_test_probe()
        done = (
            Source.from_iterable(range(11))
            .throttle(100, 1.0)
            .run_foreach(probe.ref.tell, kit.system)
        )
        for i in range(10):
            await probe.expect_message(i)
        await time.expect_no_message_for(0.009, probe)
        await time.time_passes(0.001)
        await probe.expect_message(10)
        await done


async def test_throttle_works_as_a_flow():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        probe: TestProbe[int] = kit.create_test_probe()
        flow = Flow.of(int).throttle(1, 1.0, 2)
        done = Source.from_iterable([1, 2, 3]).via(flow).run_foreach(probe.ref.tell, kit.system)
        await probe.expect_message(1)
        await probe.expect_message(2)
        await time.expect_no_message_for(0.9, probe)
        await time.time_passes(0.1)
        await probe.expect_message(3)
        await done


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
        time = ManualTime(kit.system)
        assert await q.pull() == 1
        second = asyncio.ensure_future(q.pull())
        await time.time_passes(0.5)
        assert not second.done()  # 2 waits for the timer
        q.cancel()
        assert await asyncio.wait_for(second, 3) is None
        await eventually(lambda: log == ["upstream closed"])
        assert not kit.system.scheduler._scheduled  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage, reportUnknownMemberType]


async def test_throttle_keeps_pace_at_a_rate_that_is_not_decimal():
    # ManualTime rounds its clock to nanoseconds; the bucket counts in whole nanoseconds too,
    # so three per second stepped by a third of a second neither drifts nor loses an element
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        probe: TestProbe[int] = kit.create_test_probe()
        done = (
            Source.from_iterable(range(6))
            .throttle(3, 1.0, 1)
            .run_foreach(probe.ref.tell, kit.system)
        )
        await probe.expect_message(0)
        for i in range(1, 6):
            await time.expect_no_message_for(0, probe)
            await time.time_passes(1 / 3)
            await probe.expect_message(i)
        await done
