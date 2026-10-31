"""Source.tick, after Apache Pekko's TickSourceSpec."""

from __future__ import annotations

import asyncio

import pytest

from ohra.scheduler import Cancellable
from ohra.stream import Keep, Sink, SinkQueue, Source
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


async def _pull_into(q: SinkQueue[str], probe: TestProbe[str | None]) -> None:
    probe.ref.tell(await q.pull())


async def test_tick_emits_after_initial_delay_then_interval():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        probe: TestProbe[str] = kit.create_test_probe()
        cancellable = (
            Source.tick(1.0, 2.0, "t")
            .to_mat(Sink.foreach(probe.ref.tell), Keep.left)
            .run(kit.system)
        )
        await time.expect_no_message_for(0.9, probe)
        await time.time_passes(0.1)
        await probe.expect_message("t")
        await time.expect_no_message_for(1.9, probe)
        await time.time_passes(0.1)
        await probe.expect_message("t")
        cancellable.cancel()


async def test_tick_drops_ticks_without_demand():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        probe: TestProbe[str | None] = kit.create_test_probe()
        cancellable, q = Source.tick(1.0, 1.0, "t").to_mat(Sink.queue(), Keep.both).run(kit.system)
        pull = asyncio.ensure_future(_pull_into(q, probe))
        await time.time_passes(1.0)
        await probe.expect_message("t")
        await pull
        await time.time_passes(3.0)  # nobody pulls: three ticks fall away
        pull = asyncio.ensure_future(_pull_into(q, probe))
        await time.expect_no_message_for(0.9, probe)
        await time.time_passes(0.1)
        await probe.expect_message("t")
        await pull
        cancellable.cancel()


async def test_tick_cancellable_completes_stream():
    async with ActorTestKit(manual_time=True) as kit:
        cancellable, done = (
            Source.tick(1.0, 1.0, "t").to_mat(Sink.ignore(), Keep.both).run(kit.system)
        )
        assert cancellable.cancel() is True
        assert cancellable.is_cancelled
        assert cancellable.cancel() is False
        await done


async def test_tick_cancel_after_ticking_completes_stream():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        cancellable, out = Source.tick(1.0, 1.0, "t").to_mat(Sink.seq(), Keep.both).run(kit.system)
        await time.time_passes(1.0)
        cancellable.cancel()
        assert await out == ["t"]


async def test_tick_cancelled_downstream_cancels_the_timer():
    async with ActorTestKit(manual_time=True) as kit:
        out = Source.tick(1.0, 1.0, "t").take(0).run_with(Sink.seq(), kit.system)
        assert await out == []
        assert not kit.system.scheduler._scheduled  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage, reportUnknownMemberType]


async def test_tick_build_that_fails_after_the_tick_cancels_the_timer():
    def boom(_c: Cancellable, _d: object) -> None:
        raise RuntimeError("combine failed")

    async with ActorTestKit(manual_time=True) as kit:
        with pytest.raises(RuntimeError, match="combine failed"):
            Source.tick(1.0, 1.0, "t").to_mat(Sink.ignore(), boom).run(kit.system)
        assert not kit.system.scheduler._scheduled  # pyright: ignore[reportAttributeAccessIssue, reportPrivateUsage, reportUnknownMemberType]


async def test_tick_rejects_bad_interval():
    with pytest.raises(ValueError):
        Source.tick(0.0, 0.0, 1)
