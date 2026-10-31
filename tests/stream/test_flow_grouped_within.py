"""grouped_within, after Apache Pekko's FlowGroupedWithinSpec."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest

from ohra.stream import Sink, Source
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


def _fed() -> tuple[asyncio.Queue[int | None], Source[int, object]]:
    inbox: asyncio.Queue[int | None] = asyncio.Queue()

    async def gen() -> AsyncGenerator[int, None]:
        while (x := await inbox.get()) is not None:
            yield x

    return inbox, Source.from_async_iterable(gen)


async def test_grouped_within_emits_on_size():
    async with ActorTestKit(manual_time=True) as kit:
        out = (
            Source.from_iterable(range(5)).grouped_within(2, 10.0).run_with(Sink.seq(), kit.system)
        )
        assert await out == [[0, 1], [2, 3], [4]]


async def test_grouped_within_emits_on_timeout():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        probe: TestProbe[list[int]] = kit.create_test_probe()
        inbox, src = _fed()
        inbox.put_nowait(1)
        inbox.put_nowait(2)
        done = src.grouped_within(3, 1.0).run_foreach(probe.ref.tell, kit.system)
        await time.expect_no_message_for(0.9, probe)
        await time.time_passes(0.1)
        await probe.expect_message([1, 2])
        inbox.put_nowait(None)
        await done
        await probe.expect_no_message(0)


async def test_grouped_within_not_empty_groups():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        probe: TestProbe[list[int]] = kit.create_test_probe()
        inbox, src = _fed()
        done = src.grouped_within(3, 1.0).run_foreach(probe.ref.tell, kit.system)
        await time.expect_no_message_for(2.0, probe)  # the timer fires twice with nothing buffered
        inbox.put_nowait(7)
        await time.expect_no_message_for(0.9, probe)
        await time.time_passes(0.1)
        await probe.expect_message([7])
        inbox.put_nowait(None)
        await done


async def test_grouped_within_emits_the_rest_on_completion():
    async with ActorTestKit(manual_time=True) as kit:
        out = (
            Source.from_iterable([1, 2, 3])
            .grouped_within(10, 10.0)
            .run_with(Sink.seq(), kit.system)
        )
        assert await out == [[1, 2, 3]]


async def test_grouped_within_fails_with_upstream():
    async def gen() -> AsyncGenerator[int, None]:
        yield 1
        raise ValueError("boom")

    async with ActorTestKit(manual_time=True) as kit:
        out = (
            Source.from_async_iterable(gen).grouped_within(3, 1.0).run_with(Sink.seq(), kit.system)
        )
        with pytest.raises(ValueError, match="boom"):
            await out


async def test_grouped_within_rejects_bad_arguments():
    with pytest.raises(ValueError):
        Source.single(1).grouped_within(0, 1.0)
    with pytest.raises(ValueError):
        Source.single(1).grouped_within(1, 0.0)


async def test_grouped_within_cancel_closes_upstream_suspended_in_pull():
    log: list[str] = []
    gate = asyncio.Event()

    async def upstream() -> AsyncGenerator[int, None]:
        try:
            yield 1
            await gate.wait()  # never set: the pull really suspends
            yield 2
        finally:
            log.append("upstream closed")

    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        q = (
            Source.from_async_iterable(upstream)
            .grouped_within(3, 1.0)
            .run_with(Sink.queue(), kit.system)
        )
        first = asyncio.ensure_future(q.pull())
        await time.time_passes(1.0)
        assert await asyncio.wait_for(first, 3) == [1]
        second = asyncio.ensure_future(q.pull())
        await time.time_passes(0.5)  # settles with the pull suspended in the upstream
        assert not second.done()
        q.cancel()
        assert await asyncio.wait_for(second, 3) is None
    assert log == ["upstream closed"]
