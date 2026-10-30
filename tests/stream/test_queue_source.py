"""Source.queue, after Apache Pekko's QueueSourceSpec."""

from __future__ import annotations

import asyncio

import pytest

from ohra.stream import (
    DONE,
    AbruptStageTerminationError,
    BufferOverflowError,
    Keep,
    OverflowStrategy,
    QueueOfferResult,
    Sink,
    Source,
)
from ohra.testkit import ActorTestKit

from ..util import eventually

Enqueued = QueueOfferResult.Enqueued
Dropped = QueueOfferResult.Dropped
QueueClosed = QueueOfferResult.QueueClosed


def _pair(size: int, strategy: OverflowStrategy, kit: ActorTestKit):
    return Source.queue(size, strategy).to_mat(Sink.queue(), Keep.both).run(kit.system)


def _queue_to_seq(size: int, strategy: OverflowStrategy, kit: ActorTestKit):
    return Source.queue(size, strategy).to_mat(Sink.seq(), Keep.both).run(kit.system)


async def test_emits_offered_elements():
    async with ActorTestKit() as kit:
        q, out = _queue_to_seq(10, OverflowStrategy.fail, kit)
        assert await q.offer(1) == Enqueued
        assert await q.offer(2) == Enqueued
        q.complete()
        assert await out == [1, 2]
        assert await q.watch_completion() is DONE


async def test_is_reusable():
    src = Source.queue(1, OverflowStrategy.fail)
    async with ActorTestKit() as kit:
        q1, f1 = src.to_mat(Sink.seq(), Keep.both).run(kit.system)
        q2, f2 = src.to_mat(Sink.seq(), Keep.both).run(kit.system)
        assert q1 is not q2
        await q1.offer(1)
        await q2.offer(2)
        q1.complete()
        q2.complete()
        assert await f1 == [1]
        assert await f2 == [2]


async def test_offer_backpressures_when_full():
    async with ActorTestKit() as kit:
        q, sq = _pair(1, OverflowStrategy.backpressure, kit)
        assert await q.offer(1) == Enqueued
        second = asyncio.ensure_future(q.offer(2))
        await asyncio.sleep(0)  # let the offer run to its wait
        assert not second.done()
        assert await sq.pull() == 1  # frees a slot
        assert await second == Enqueued
        assert await sq.pull() == 2
        sq.cancel()


async def test_backpressure_buffer_zero_waits_for_demand():
    async with ActorTestKit() as kit:
        q, sq = _pair(0, OverflowStrategy.backpressure, kit)
        first = asyncio.ensure_future(q.offer(1))
        await asyncio.sleep(0)
        assert not first.done()
        assert await sq.pull() == 1
        assert await first == Enqueued
        sq.cancel()


async def test_second_concurrent_offer_in_backpressure_mode_fails():
    async with ActorTestKit() as kit:
        q, sq = _pair(1, OverflowStrategy.backpressure, kit)
        await q.offer(1)
        second = asyncio.ensure_future(q.offer(2))
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="Too many concurrent offers"):
            await q.offer(3)
        second.cancel()
        sq.cancel()
        await asyncio.gather(second, return_exceptions=True)


async def test_backpressure_buffer_zero_second_offer_fails():
    async with ActorTestKit() as kit:
        q, sq = _pair(0, OverflowStrategy.backpressure, kit)
        first = asyncio.ensure_future(q.offer(1))
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="wait for previous offer"):
            await q.offer(2)
        first.cancel()
        sq.cancel()
        await asyncio.gather(first, return_exceptions=True)


async def test_offer_after_complete_is_queue_closed():
    async with ActorTestKit() as kit:
        q, out = _queue_to_seq(10, OverflowStrategy.fail, kit)
        q.complete()
        assert await out == []
        assert await q.offer(1) == QueueClosed


async def test_offer_while_completing_is_dropped():
    async with ActorTestKit() as kit:
        q, sq = _pair(10, OverflowStrategy.fail, kit)
        await q.offer(1)
        q.complete()  # an element is still buffered
        assert await q.offer(2) == Dropped
        assert await sq.pull() == 1
        assert await sq.pull() is None


async def test_buffered_elements_are_delivered_before_completion():
    async with ActorTestKit() as kit:
        q, out = _queue_to_seq(10, OverflowStrategy.fail, kit)
        await q.offer(1)
        await q.offer(2)
        q.complete()
        assert await out == [1, 2]


async def test_drop_head_reports_enqueued_and_drops_oldest():
    async with ActorTestKit() as kit:
        q, sq = _pair(2, OverflowStrategy.drop_head, kit)
        assert [await q.offer(i) for i in (1, 2, 3)] == [Enqueued] * 3
        q.complete()
        assert [x async for x in sq] == [2, 3]


async def test_drop_tail_drops_newest_buffered():
    async with ActorTestKit() as kit:
        q, sq = _pair(2, OverflowStrategy.drop_tail, kit)
        assert [await q.offer(i) for i in (1, 2, 3)] == [Enqueued] * 3
        q.complete()
        assert [x async for x in sq] == [1, 3]


async def test_drop_buffer_empties_the_buffer():
    async with ActorTestKit() as kit:
        q, sq = _pair(2, OverflowStrategy.drop_buffer, kit)
        assert [await q.offer(i) for i in (1, 2, 3)] == [Enqueued] * 3
        q.complete()
        assert [x async for x in sq] == [3]


async def test_buffer_zero_drop_tail_drops_when_nobody_waits():
    async with ActorTestKit() as kit:
        q, sq = _pair(0, OverflowStrategy.drop_tail, kit)
        first = asyncio.ensure_future(q.offer(1))  # pending: no demand yet
        await asyncio.sleep(0)
        assert await q.offer(2) == Dropped
        assert await sq.pull() == 1
        assert await first == Enqueued
        sq.cancel()


async def test_buffer_zero_drop_head_drops_the_pending_offer():
    async with ActorTestKit() as kit:
        q, sq = _pair(0, OverflowStrategy.drop_head, kit)
        first = asyncio.ensure_future(q.offer(1))
        await asyncio.sleep(0)
        second = asyncio.ensure_future(q.offer(2))
        assert await first == Dropped
        assert await sq.pull() == 2
        assert await second == Enqueued
        sq.cancel()


async def test_buffer_zero_delivers_offer_when_demand_is_signalled():
    async with ActorTestKit() as kit:
        q, sq = _pair(0, OverflowStrategy.fail, kit)
        pull = asyncio.ensure_future(sq.pull())
        await asyncio.sleep(0)
        assert await q.offer(1) == Enqueued
        assert await pull == 1
        sq.cancel()


async def test_fail_overflow_fails_stream_and_reports_failure():
    async with ActorTestKit() as kit:
        q, out = _queue_to_seq(1, OverflowStrategy.fail, kit)
        assert await q.offer(1) == Enqueued
        result = await q.offer(2)
        match result:
            case QueueOfferResult.Failure(cause=BufferOverflowError()):
                pass
            case _:
                pytest.fail(f"unexpected {result!r}")
        with pytest.raises(BufferOverflowError):
            await q.watch_completion()
        with pytest.raises(BufferOverflowError):
            await out


async def test_fail_fails_the_stream_and_the_completion():
    async with ActorTestKit() as kit:
        q, out = _queue_to_seq(10, OverflowStrategy.fail, kit)
        q.fail(ValueError("boom"))
        with pytest.raises(ValueError, match="boom"):
            await out
        with pytest.raises(ValueError, match="boom"):
            await q.watch_completion()


async def test_downstream_cancel_closes_the_queue_and_pending_offers():
    async with ActorTestKit() as kit:
        q, sq = _pair(1, OverflowStrategy.backpressure, kit)
        await q.offer(1)
        pending = asyncio.ensure_future(q.offer(2))
        await asyncio.sleep(0)
        sq.cancel()
        assert await pending == QueueClosed
        assert await q.offer(3) == QueueClosed
        assert await q.watch_completion() is DONE


async def test_cancelled_before_the_first_pull_closes_the_queue():
    async with ActorTestKit() as kit:
        q = Source.queue(1, OverflowStrategy.fail).to(Sink.cancelled()).run(kit.system)
        await eventually(q.watch_completion().done)
        assert await q.offer(1) == QueueClosed


async def test_cancelled_offer_leaves_the_queue_usable():
    async with ActorTestKit() as kit:
        q, sq = _pair(1, OverflowStrategy.backpressure, kit)
        await q.offer(1)
        pending = asyncio.ensure_future(q.offer(2))
        await asyncio.sleep(0)
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        third = asyncio.ensure_future(q.offer(3))  # the cancelled offer freed the slot
        assert await sq.pull() == 1
        assert await third == Enqueued
        assert await sq.pull() == 3
        sq.cancel()


async def test_terminate_while_offer_is_backpressured():
    async with ActorTestKit() as kit:
        q, sq = _pair(1, OverflowStrategy.backpressure, kit)
        await q.offer(1)
        pending = asyncio.ensure_future(q.offer(2))
        await asyncio.sleep(0)
        kit.system.terminate()
        await kit.system.when_terminated()
    assert await asyncio.wait_for(pending, 1) == QueueClosed
    assert await q.offer(3) == QueueClosed
    with pytest.raises(AbruptStageTerminationError):
        await asyncio.wait_for(q.watch_completion(), 1)


def test_invalid_buffer_size():
    with pytest.raises(ValueError):
        Source.queue(-1, OverflowStrategy.fail)
