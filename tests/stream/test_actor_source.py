"""ActorSource.actor_ref, after Apache Pekko's ActorSourceSinkSpec and the overflow and completion
cases of ActorRefSourceSpec."""

from __future__ import annotations

import asyncio

import pytest

from ohra import ActorRef
from ohra.stream import (
    ActorSource,
    BufferOverflowError,
    Keep,
    OverflowStrategy,
    Sink,
    SinkQueue,
    Source,
)
from ohra.testkit import ActorTestKit

from ..util import eventually


def source(
    buffer_size: int = 10, overflow_strategy: OverflowStrategy = OverflowStrategy.drop_head
) -> Source[object, ActorRef[object]]:
    return ActorSource.actor_ref(
        completion_matcher=lambda m: m == "complete",
        failure_matcher=lambda m: RuntimeError("boom!") if m == "fail" else None,
        buffer_size=buffer_size,
        overflow_strategy=overflow_strategy,
    )


def run(
    kit: ActorTestKit,
    buffer_size: int = 10,
    overflow_strategy: OverflowStrategy = OverflowStrategy.drop_head,
) -> tuple[ActorRef[object], SinkQueue[object]]:
    return source(buffer_size, overflow_strategy).to_mat(Sink.queue(), Keep.both).run(kit.system)


def waiting(ref: ActorRef[object]) -> bool:
    """The source is suspended in its own wait: downstream pulls and the buffer is empty."""
    return ref._source._waiter is not None  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]


async def test_send_messages_and_complete():
    async with ActorTestKit() as kit:
        ref, q = run(kit)
        ref.tell("one")
        ref.tell("two")
        ref.tell("complete")
        assert [m async for m in q] == ["one", "two"]


async def test_send_messages_and_complete_into_a_seq():
    async with ActorTestKit() as kit:
        ref, out = source().to_mat(Sink.seq(), Keep.both).run(kit.system)
        ref.tell("one")
        ref.tell("two")
        ref.tell("complete")
        assert await out == ["one", "two"]


async def test_fail_the_stream():
    async with ActorTestKit() as kit:
        ref, q = run(kit)
        ref.tell("fail")
        with pytest.raises(RuntimeError, match="boom!"):
            await q.pull()


async def test_failure_overtakes_draining_after_completion():
    async with ActorTestKit() as kit:
        ref, q = run(kit)
        ref.tell("a")
        ref.tell("complete")
        ref.tell("fail")
        with pytest.raises(RuntimeError, match="boom!"):
            await q.pull()


async def test_drop_head_keeps_the_newest():
    async with ActorTestKit() as kit:
        ref, q = run(kit, 2, OverflowStrategy.drop_head)
        for m in (1, 2, 3, "complete"):
            ref.tell(m)
        assert [m async for m in q] == [2, 3]


async def test_drop_tail_replaces_the_newest_buffered():
    async with ActorTestKit() as kit:
        ref, q = run(kit, 2, OverflowStrategy.drop_tail)
        for m in (1, 2, 3, "complete"):
            ref.tell(m)
        assert [m async for m in q] == [1, 3]


async def test_drop_buffer_keeps_only_the_new_one():
    async with ActorTestKit() as kit:
        ref, q = run(kit, 2, OverflowStrategy.drop_buffer)
        for m in (1, 2, 3, "complete"):
            ref.tell(m)
        assert [m async for m in q] == [3]


async def test_fail_fails_the_stream_with_buffer_overflow():
    async with ActorTestKit() as kit:
        ref, q = run(kit, 1, OverflowStrategy.fail)
        ref.tell(1)
        ref.tell(2)
        with pytest.raises(BufferOverflowError, match="max capacity was: 1"):
            await q.pull()


async def test_overflow_fails_downstream_at_once_without_a_pull():
    async with ActorTestKit() as kit:
        (ref, terminated), q = (
            source(1, OverflowStrategy.fail)
            .watch_termination(Keep.both)
            .to_mat(Sink.queue(), Keep.both)
            .run(kit.system)
        )
        ref.tell(1)
        ref.tell(2)  # nothing pulls: the queue's consumer is busy elsewhere
        with pytest.raises(BufferOverflowError):
            await asyncio.wait_for(terminated, 1)
        with pytest.raises(BufferOverflowError):
            await q.pull()


def test_backpressure_is_rejected():
    with pytest.raises(ValueError, match="Backpressure overflowStrategy not supported"):
        source(1, OverflowStrategy.backpressure)


def test_a_negative_buffer_size_is_rejected():
    with pytest.raises(ValueError, match="bufferSize must be greater than or equal to 0"):
        source(-1)


async def test_buffer_size_zero_drops_unless_downstream_pulls():
    async with ActorTestKit() as kit:
        ref, q = run(kit, 0)
        ref.tell(1)
        pending = asyncio.ensure_future(q.pull())
        await eventually(lambda: waiting(ref))
        ref.tell(2)
        assert await pending == 2
        ref.tell("complete")
        assert await q.pull() is None


async def test_elements_after_completion_are_dropped():
    async with ActorTestKit() as kit:
        ref, q = run(kit)
        ref.tell("a")
        ref.tell("complete")
        ref.tell("b")
        assert [m async for m in q] == ["a"]


async def test_tells_after_a_cancel_while_waiting_are_dead_letters(
    caplog: pytest.LogCaptureFixture,
):
    async with ActorTestKit() as kit:
        ref, q = run(kit)
        pending = asyncio.ensure_future(q.pull())
        await eventually(lambda: waiting(ref))
        q.cancel()
        assert await pending is None
        with caplog.at_level("DEBUG", logger="ohra.dead_letters"):
            ref.tell(1)
        assert "dead letter" in caplog.text
        assert ref.path.startswith("/temp/$source-")


async def test_queue_cancel_cancels_actor_source(caplog: pytest.LogCaptureFixture):
    async with ActorTestKit() as kit:
        ref, q = run(kit)
        ref.tell(1)  # buffered, then discarded by the cancel
        q.cancel()
        assert await q.pull() is None
        with caplog.at_level("DEBUG", logger="ohra.dead_letters"):
            ref.tell(3)
        assert "dead letter" in caplog.text


async def test_cancelled_before_the_first_pull_detaches_the_source(
    caplog: pytest.LogCaptureFixture,
):
    async with ActorTestKit() as kit:
        ref, done = source().watch_termination(Keep.both).to(Sink.cancelled()).run(kit.system)
        await done
        with caplog.at_level("DEBUG", logger="ohra.dead_letters"):
            ref.tell(1)
        assert "dead letter" in caplog.text


async def test_terminate_detaches_the_source(caplog: pytest.LogCaptureFixture):
    async with ActorTestKit() as kit:
        ref, _ = run(kit)
        kit.system.terminate()
        await kit.system.when_terminated()
        with caplog.at_level("DEBUG", logger="ohra.dead_letters"):
            ref.tell(1)
        assert "dead letter" in caplog.text


async def test_each_run_materializes_a_new_ref():
    async with ActorTestKit() as kit:
        blueprint = source().to_mat(Sink.queue(), Keep.both)
        ref1, q1 = blueprint.run(kit.system)
        ref2, q2 = blueprint.run(kit.system)
        assert ref1 != ref2
        assert ref1.path != ref2.path
        ref1.tell("one")
        ref2.tell("two")
        ref1.tell("complete")
        ref2.tell("complete")
        assert [m async for m in q1] == ["one"]
        assert [m async for m in q2] == ["two"]


async def test_elements_stay_buffered_between_pulls():
    async with ActorTestKit() as kit:
        ref, q = run(kit)
        for m in (1, 2, "complete"):
            ref.tell(m)
        assert await q.pull() == 1
        assert [m async for m in q] == [2]


async def test_a_second_concurrent_pull_is_refused():
    async with ActorTestKit() as kit:
        ref, q = run(kit)
        first = asyncio.ensure_future(q.pull())
        await eventually(lambda: waiting(ref))
        with pytest.raises(RuntimeError, match="concurrent pulls"):
            await q.pull()
        ref.tell(1)
        assert await first == 1
        q.cancel()


async def test_a_matcher_that_raises_fails_the_source_not_the_sender():
    async with ActorTestKit() as kit:
        ref, q = (
            ActorSource.actor_ref(
                completion_matcher=lambda m: m == 1 // 0,
                failure_matcher=lambda _: None,
                buffer_size=10,
                overflow_strategy=OverflowStrategy.fail,
            )
            .to_mat(Sink.queue(), Keep.both)
            .run(kit.system)
        )
        ref.tell(1)
        with pytest.raises(ZeroDivisionError):
            await q.pull()


async def test_a_failed_source_raises_its_failure_on_every_later_pull():
    # rule 1.7, no signal after a terminal one: a pull after the failure does not complete.
    # No stage pulls after a failure, so this reaches the source's iterator directly.
    from ohra.stream.dsl import _Ctx  # pyright: ignore[reportPrivateUsage]

    async with ActorTestKit() as kit:
        source: Source[object, ActorRef[object]] = ActorSource.actor_ref(
            completion_matcher=lambda _: False,
            failure_matcher=lambda m: m if isinstance(m, ValueError) else None,
            buffer_size=4,
            overflow_strategy=OverflowStrategy.fail,
        )
        it, ref = source._build(_Ctx(kit.system))  # pyright: ignore[reportPrivateUsage]
        ref.tell(ValueError("boom"))
        for _ in range(2):
            with pytest.raises(ValueError, match="boom"):
                await anext(it)
        await it.aclose()  # pyright: ignore[reportAttributeAccessIssue]
