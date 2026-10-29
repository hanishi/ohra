"""ActorSource.actor_ref, after Apache Pekko's ActorSourceSinkSpec and the overflow and completion
cases of ActorRefSourceSpec."""

from __future__ import annotations

import asyncio

import pytest

from ohra.stream import ActorSource, BufferOverflowError, OverflowStrategy


def source(
    buffer_size: int = 10, overflow_strategy: OverflowStrategy = OverflowStrategy.drop_head
) -> ActorSource[object]:
    return ActorSource.actor_ref(
        completion_matcher=lambda m: m == "complete",
        failure_matcher=lambda m: RuntimeError("boom!") if m == "fail" else None,
        buffer_size=buffer_size,
        overflow_strategy=overflow_strategy,
    )


async def test_send_messages_and_complete():
    s = source()
    s.ref.tell("one")
    s.ref.tell("two")
    s.ref.tell("complete")
    assert [m async for m in s] == ["one", "two"]


async def test_fail_the_stream():
    s = source()
    s.ref.tell("fail")
    with pytest.raises(RuntimeError, match="boom!"):
        async for _ in s:
            pass


async def test_failure_overtakes_draining_after_completion():
    s = source()
    s.ref.tell("a")
    s.ref.tell("complete")
    s.ref.tell("fail")
    seen: list[object] = []
    with pytest.raises(RuntimeError, match="boom!"):
        async for m in s:
            seen.append(m)
    assert seen == []


async def test_drop_head_keeps_the_newest():
    s = source(2, OverflowStrategy.drop_head)
    for m in (1, 2, 3, "complete"):
        s.ref.tell(m)
    assert [m async for m in s] == [2, 3]


async def test_drop_tail_replaces_the_newest_buffered():
    s = source(2, OverflowStrategy.drop_tail)
    for m in (1, 2, 3, "complete"):
        s.ref.tell(m)
    assert [m async for m in s] == [1, 3]


async def test_drop_buffer_keeps_only_the_new_one():
    s = source(2, OverflowStrategy.drop_buffer)
    for m in (1, 2, 3, "complete"):
        s.ref.tell(m)
    assert [m async for m in s] == [3]


async def test_fail_raises_buffer_overflow_in_the_loop():
    s = source(1, OverflowStrategy.fail)
    s.ref.tell(1)
    s.ref.tell(2)
    with pytest.raises(BufferOverflowError, match="max capacity was: 1"):
        async for _ in s:
            pass


async def test_backpressure_is_rejected():
    with pytest.raises(ValueError, match="Backpressure overflowStrategy not supported"):
        source(1, OverflowStrategy.backpressure)


async def test_buffer_size_zero_drops_unless_the_loop_waits():
    s = source(0)
    s.ref.tell(1)
    it = aiter(s)
    waiting = asyncio.ensure_future(anext(it))
    await asyncio.sleep(0)
    s.ref.tell(2)
    assert await waiting == 2
    s.ref.tell("complete")
    with pytest.raises(StopAsyncIteration):
        await anext(it)


async def test_elements_after_completion_are_dropped():
    s = source()
    s.ref.tell("a")
    s.ref.tell("complete")
    s.ref.tell("b")
    assert [m async for m in s] == ["a"]


async def test_tells_after_cancel_are_dead_letters_and_nothing_leaks(
    caplog: pytest.LogCaptureFixture,
):
    s = source()

    async def consume() -> None:
        async for _ in s:
            pass

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0)
    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer
    with caplog.at_level("DEBUG", logger="ohra.dead_letters"):
        s.ref.tell(1)
    assert "dead letter" in caplog.text
    assert s.ref.path.startswith("/temp/$source-")


async def test_cancel_makes_later_tells_dead_letters(caplog: pytest.LogCaptureFixture):
    s = source()
    s.ref.tell(1)
    s.cancel()
    assert [m async for m in s] == []
    with caplog.at_level("DEBUG", logger="ohra.dead_letters"):
        s.ref.tell(3)
    assert "dead letter" in caplog.text


async def test_break_leaves_the_rest_for_the_next_loop():
    s = source()
    for m in (1, 2, "complete"):
        s.ref.tell(m)
    async for _ in s:
        break
    for _ in range(3):
        await asyncio.sleep(0)
    assert [m async for m in s] == [2]


async def test_a_second_concurrent_consumer_is_refused():
    s = source()
    first = asyncio.ensure_future(anext(aiter(s)))
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="one consumer"):
        await anext(aiter(s))
    s.ref.tell(1)
    assert await first == 1


async def test_a_matcher_that_raises_fails_the_source_not_the_sender():
    s: ActorSource[object] = ActorSource.actor_ref(
        completion_matcher=lambda m: m == 1 // 0,
        failure_matcher=lambda _: None,
        buffer_size=10,
        overflow_strategy=OverflowStrategy.fail,
    )
    s.ref.tell(1)
    with pytest.raises(ZeroDivisionError):
        async for _ in s:
            pass


async def test_a_negative_buffer_size_is_rejected():
    with pytest.raises(ValueError, match="bufferSize must be greater than or equal to 0"):
        source(-1)
