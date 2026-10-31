"""Linear operators, after Apache Pekko's FlowCollectSpec, FlowMapConcatSpec, FlowTakeSpec,
FlowTakeWhileSpec, FlowDropSpec, FlowDropWhileSpec, FlowGroupedSpec, FlowScanSpec and
FlowFoldSpec."""

from __future__ import annotations

import asyncio
import operator
from collections.abc import AsyncGenerator

import pytest

from ohra.stream import Flow, Keep, KillSwitches, OverflowStrategy, QueueOfferResult, Sink, Source
from ohra.testkit import ActorTestKit


async def run[T](src: Source[T, object]) -> list[T]:
    async with ActorTestKit() as kit:
        return await src.run_with(Sink.seq(), kit.system)


async def test_filter_not():
    assert await run(Source.from_iterable(range(6)).filter_not(lambda x: x % 2 == 0)) == [1, 3, 5]


async def test_collect_skips_none():
    src = Source.from_iterable(range(6)).collect(lambda x: str(x) if x % 2 else None)
    assert await run(src) == ["1", "3", "5"]


async def test_collect_keeps_falsy_values():
    assert await run(Source.from_iterable([0, 1]).collect(lambda x: x)) == [0, 1]


async def test_map_concat():
    assert await run(Source.from_iterable([1, 2, 3]).map_concat(lambda x: [x] * x)) == [
        1,
        2,
        2,
        3,
        3,
        3,
    ]


async def test_map_concat_empty_iterables():
    assert await run(Source.from_iterable([1, 2]).map_concat(lambda _: [])) == []


async def test_take():
    assert await run(Source.from_iterable(range(10)).take(3)) == [0, 1, 2]


async def test_take_more_than_available():
    assert await run(Source.from_iterable([1, 2]).take(5)) == [1, 2]


async def _noisy(log: list[str]) -> AsyncGenerator[int, None]:
    try:
        for i in range(100):
            yield i
    finally:
        log.append("closed")


async def test_take_cancels_upstream_after_n():
    log: list[str] = []
    assert await run(Source.from_async_iterable(lambda: _noisy(log)).take(2)) == [0, 1]
    assert log == ["closed"]


async def test_take_zero_emits_nothing_and_never_pulls_upstream():
    log: list[str] = []
    assert await run(Source.from_async_iterable(lambda: _noisy(log)).take(0)) == []
    assert log == []  # upstream was cancelled before it started: the iterator never existed


async def test_take_cancels_upstream_with_the_nth_element_before_another_pull():
    async with ActorTestKit() as kit:
        src, out = (
            Source.queue(4, OverflowStrategy.backpressure)
            .take(1)
            .to_mat(Sink.queue(), Keep.both)
            .run(kit.system)
        )
        assert await src.offer(1) is QueueOfferResult.Enqueued
        assert await out.pull() == 1
        assert src.watch_completion().done()  # upstream was cancelled before 1 came out
        assert await src.offer(2) is QueueOfferResult.QueueClosed
        assert await out.pull() is None


async def _fails_to_close() -> AsyncGenerator[int, None]:
    try:
        yield 1
        yield 2
    finally:
        raise RuntimeError("close failed")


async def test_take_completes_when_upstream_fails_to_close(caplog: pytest.LogCaptureFixture):
    # Pekko logs a stage that fails while it stops; the stream already completed downstream
    async with ActorTestKit() as kit:
        out = (
            Source.from_async_iterable(_fails_to_close).take(1).run_with(Sink.queue(), kit.system)
        )
        assert await out.pull() == 1
        assert await out.pull() is None
    assert any("close failed" in str(r.exc_info and r.exc_info[1]) for r in caplog.records)


async def test_a_generator_that_ignores_generator_exit_does_not_fail_the_stream(
    caplog: pytest.LogCaptureFixture,
):
    async def stubborn() -> AsyncGenerator[int, None]:
        ignored = False
        while True:
            try:
                yield 1
            except GeneratorExit:
                if ignored:  # the event loop's finalizer, later: let it close
                    raise
                ignored = True  # keeps going: the stream's aclose() raises RuntimeError

    async with ActorTestKit() as kit:
        assert await Source.from_async_iterable(stubborn).take(2).run_with(
            Sink.seq(), kit.system
        ) == [1, 1]
    assert any(
        "ignored GeneratorExit" in str(r.exc_info and r.exc_info[1]) for r in caplog.records
    )


async def test_take_does_not_outlive_terminate_when_upstream_never_finishes_closing():
    closes: list[str] = []

    async def hangs_on_close() -> AsyncGenerator[int, None]:
        try:
            yield 1
            yield 2
        finally:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                closes.append("cancelled")
                raise

    async with ActorTestKit() as kit:
        out = Source.from_async_iterable(hangs_on_close).take(1).run_with(Sink.queue(), kit.system)
        assert await out.pull() == 1
    assert closes == ["cancelled"]  # terminate cancelled the close; no task outlives the test


async def test_a_cleanup_that_raises_under_a_cancelled_pull_is_logged(
    caplog: pytest.LogCaptureFixture,
):
    # a kill switch pulls in a task of its own: its cancel reaches the source inside an await
    pulled = asyncio.Event()

    async def fails_to_clean_up() -> AsyncGenerator[int, None]:
        try:
            pulled.set()
            await asyncio.Event().wait()
            yield 1
        finally:
            raise RuntimeError("cleanup failed")

    async with ActorTestKit() as kit:
        switch, done = (
            Source.from_async_iterable(fails_to_clean_up)
            .via_mat(KillSwitches.single(), Keep.right)
            .to_mat(Sink.ignore(), Keep.both)
            .run(kit.system)
        )
        await pulled.wait()
        switch.shutdown()
        await done
    assert any("cleanup failed" in str(r.exc_info and r.exc_info[1]) for r in caplog.records)


async def test_take_while():
    assert await run(Source.from_iterable([1, 2, 3, 1]).take_while(lambda x: x < 3)) == [1, 2]


async def test_take_while_inclusive():
    src = Source.from_iterable([1, 2, 3, 4]).take_while(lambda x: x < 3, inclusive=True)
    assert await run(src) == [1, 2, 3]


async def test_drop():
    assert await run(Source.from_iterable(range(5)).drop(2)) == [2, 3, 4]


async def test_drop_more_than_available():
    assert await run(Source.from_iterable(range(2)).drop(5)) == []


async def test_drop_while():
    assert await run(Source.from_iterable([1, 2, 3, 1]).drop_while(lambda x: x < 3)) == [3, 1]


async def test_grouped_emits_remainder():
    assert await run(Source.from_iterable(range(7)).grouped(3)) == [[0, 1, 2], [3, 4, 5], [6]]


async def test_grouped_of_empty_stream_emits_nothing():
    assert await run(Source.empty().grouped(3)) == []


def test_grouped_rejects_a_size_below_one():
    with pytest.raises(ValueError):
        Source.single(1).grouped(0)
    with pytest.raises(ValueError):
        Flow.of(int).grouped(-1)


async def test_scan_emits_zero_first():
    assert await run(Source.from_iterable([1, 2, 3]).scan(0, operator.add)) == [0, 1, 3, 6]


async def test_scan_of_empty_stream_emits_zero():
    assert await run(Source.from_iterable([]).scan(7, operator.add)) == [7]


async def test_fold_emits_once():
    assert await run(Source.from_iterable([1, 2, 3]).fold(0, operator.add)) == [6]


async def test_fold_of_empty_stream_emits_zero():
    assert await run(Source.from_iterable([]).fold(5, operator.add)) == [5]


async def test_operators_on_flow_match_source():
    async with ActorTestKit() as kit:
        flow = Flow.of(int).take(2)
        got = await Source.from_iterable([1, 2, 3]).via(flow).run_with(Sink.seq(), kit.system)
        assert got == [1, 2]


async def test_more_operators_on_flow():
    flow = (
        Flow.of(int)
        .drop(1)
        .filter_not(lambda x: x == 3)
        .map_concat(lambda x: [x, x])
        .grouped(3)
        .fold([], lambda acc, g: acc + [g])
    )
    assert await run(Source.from_iterable([0, 1, 2, 3, 4]).via(flow)) == [[[1, 1, 2], [2, 4, 4]]]
