"""Linear operators, after Apache Pekko's FlowCollectSpec, FlowMapConcatSpec, FlowTakeSpec,
FlowTakeWhileSpec, FlowDropSpec, FlowDropWhileSpec, FlowGroupedSpec, FlowScanSpec and
FlowFoldSpec."""

from __future__ import annotations

import operator
from collections.abc import AsyncGenerator

from ohra.stream import Flow, Sink, Source
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
