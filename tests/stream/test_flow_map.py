"""map, filter and composition, after Apache Pekko's FlowMapSpec and FlowFilterSpec."""

from __future__ import annotations

import asyncio

import pytest

from ohra.stream import Flow, Keep, Sink, Source
from ohra.testkit import ActorTestKit


async def test_map():
    async with ActorTestKit() as kit:
        out = Source.from_iterable([1, 2, 3]).map(lambda x: x * 2).run_with(Sink.seq(), kit.system)
        assert await out == [2, 4, 6]


async def test_map_raising_fails_stream():
    def f(x: int) -> int:
        if x == 2:
            raise ValueError("two")
        return x

    async with ActorTestKit() as kit:
        with pytest.raises(ValueError, match="two"):
            await Source.from_iterable([1, 2, 3]).map(f).run_with(Sink.seq(), kit.system)


async def test_filter():
    async with ActorTestKit() as kit:
        src = Source.from_iterable(range(1, 11)).filter(lambda x: x % 2 == 0)
        assert await src.run_with(Sink.seq(), kit.system) == [2, 4, 6, 8, 10]


async def test_via_flow_of():
    async with ActorTestKit() as kit:
        src = Source.from_iterable([1, 2, 3]).via(Flow.of(int).map(str))
        assert await src.run_with(Sink.seq(), kit.system) == ["1", "2", "3"]


async def test_flow_to_mat_sink():
    async with ActorTestKit() as kit:
        sink = Flow.of(int).filter(lambda x: x > 1).to_mat(Sink.seq(), Keep.right)
        assert await Source.from_iterable([1, 2, 3]).run_with(sink, kit.system) == [2, 3]


async def test_flow_to_keeps_the_flow_value():
    async with ActorTestKit() as kit:
        sink = Flow.of(int).map_materialized_value(lambda _: "flow").to(Sink.seq())
        assert Source.single(1).run_with(sink, kit.system) == "flow"


async def test_to_mat_keep_both():
    async with ActorTestKit() as kit:
        graph = (
            Source.single(1).map_materialized_value(lambda _: "src").to_mat(Sink.seq(), Keep.both)
        )
        mat, fut = graph.run(kit.system)
        assert mat == "src"
        assert isinstance(fut, asyncio.Future)
        assert await fut == [1]


async def test_via_mat_and_keep_left():
    async with ActorTestKit() as kit:
        flow = Flow.of(int).map_materialized_value(lambda _: "flow")
        graph = Source.single(1).via_mat(flow, Keep.right).to(Sink.ignore())
        assert graph.run(kit.system) == "flow"
