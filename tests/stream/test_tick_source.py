"""Source.tick, after Apache Pekko's TickSourceSpec."""

from __future__ import annotations

import asyncio

import pytest

from ohra.stream import Sink, Source
from ohra.testkit import ActorTestKit, ManualTime


async def test_tick_emits_after_initial_delay_then_interval():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        q = Source.tick(1.0, 2.0, "t").to_mat(Sink.queue(), lambda _c, q: q).run(kit.system)
        pull = asyncio.ensure_future(q.pull())
        await asyncio.wait({pull}, timeout=0.05)
        await time.time_passes(0.9)
        await asyncio.wait({pull}, timeout=0.05)
        assert not pull.done()
        await time.time_passes(0.1)
        assert await asyncio.wait_for(pull, 1) == "t"
        pull = asyncio.ensure_future(q.pull())
        await asyncio.wait({pull}, timeout=0.05)
        await time.time_passes(1.9)
        await asyncio.wait({pull}, timeout=0.05)
        assert not pull.done()
        await time.time_passes(0.1)
        assert await asyncio.wait_for(pull, 1) == "t"
        q.cancel()


async def test_tick_drops_ticks_without_demand():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        q = Source.tick(1.0, 1.0, "t").to_mat(Sink.queue(), lambda _c, q: q).run(kit.system)
        first = asyncio.ensure_future(q.pull())
        await asyncio.wait({first}, timeout=0.05)
        await time.time_passes(1.0)
        assert await asyncio.wait_for(first, 1) == "t"
        await time.time_passes(3.0)  # nobody pulled: three ticks fall away
        pull = asyncio.ensure_future(q.pull())
        await asyncio.wait({pull}, timeout=0.05)
        assert not pull.done()
        await time.time_passes(1.0)
        assert await asyncio.wait_for(pull, 1) == "t"
        q.cancel()


async def test_tick_cancellable_completes_stream():
    async with ActorTestKit(manual_time=True) as kit:
        cancellable, done = (
            Source.tick(1.0, 1.0, "t").to_mat(Sink.ignore(), lambda c, d: (c, d)).run(kit.system)
        )
        assert cancellable.cancel() is True
        assert cancellable.is_cancelled
        assert cancellable.cancel() is False
        await asyncio.wait_for(done, 1)


async def test_tick_cancel_after_ticking_completes_stream():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        cancellable, out = (
            Source.tick(1.0, 1.0, "t").to_mat(Sink.seq(), lambda c, o: (c, o)).run(kit.system)
        )
        await asyncio.sleep(0)
        await time.time_passes(1.0)
        cancellable.cancel()
        assert set(await asyncio.wait_for(out, 1)) <= {"t"}


async def test_tick_cancelled_downstream_cancels_the_timer():
    async with ActorTestKit(manual_time=True) as kit:
        out = Source.tick(1.0, 1.0, "t").take(0).run_with(Sink.seq(), kit.system)
        assert await asyncio.wait_for(out, 1) == []
        assert not kit.system.scheduler._scheduled  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage, reportUnknownMemberType]


async def test_tick_rejects_bad_interval():
    with pytest.raises(ValueError):
        Source.tick(0.0, 0.0, 1)
