"""map_async and map_async_unordered, after Apache Pekko's FlowMapAsyncSpec and
FlowMapAsyncUnorderedSpec."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest

from ohra.stream import Sink, Source
from ohra.testkit import ActorTestKit

from ..util import eventually


async def test_map_async_keeps_order():
    gates = {i: asyncio.Event() for i in (1, 2, 3)}

    started: list[int] = []
    finished: list[int] = []

    async def f(x: int) -> int:
        started.append(x)
        await gates[x].wait()
        finished.append(x)
        return x

    async with ActorTestKit() as kit:
        out = Source.from_iterable([1, 2, 3]).map_async(3, f).run_with(Sink.seq(), kit.system)
        await eventually(lambda: len(started) == 3)
        for i in (3, 2, 1):  # later elements finish first
            gates[i].set()
            await eventually(lambda: i in finished)
        assert await out == [1, 2, 3]


async def test_map_async_runs_at_most_parallelism():
    running = peak = 0

    async def f(x: int) -> int:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.001)
        running -= 1
        return x

    async with ActorTestKit() as kit:
        out = Source.from_iterable(range(10)).map_async(2, f).run_with(Sink.seq(), kit.system)
        assert await out == list(range(10))
    assert peak == 2


async def test_map_async_unordered_emits_in_completion_order():
    gates = {i: asyncio.Event() for i in (1, 2, 3)}

    started: list[int] = []
    finished: list[int] = []

    async def f(x: int) -> int:
        started.append(x)
        await gates[x].wait()
        finished.append(x)
        return x

    async with ActorTestKit() as kit:
        src = Source.from_iterable([1, 2, 3]).map_async_unordered(3, f)
        out = src.run_with(Sink.seq(), kit.system)
        await eventually(lambda: len(started) == 3)
        for i in (3, 1, 2):
            gates[i].set()
            await eventually(lambda: i in finished)
        assert await out == [3, 1, 2]


async def test_map_async_failure_cancels_in_flight():
    never = asyncio.Event()
    started: list[int] = []
    cancelled: list[int] = []

    async def f(x: int) -> int:
        started.append(x)
        if x == 3:
            await eventually(lambda: len(started) == 3)
            raise ValueError("three")
        try:
            await never.wait()
        except asyncio.CancelledError:
            cancelled.append(x)
            raise
        return x

    async with ActorTestKit() as kit:
        out = Source.from_iterable([1, 2, 3]).map_async(3, f).run_with(Sink.seq(), kit.system)
        with pytest.raises(ValueError, match="three"):
            await out
    assert sorted(cancelled) == [1, 2]


async def test_map_async_does_not_pull_ahead_of_parallelism():
    yielded = 0
    gate = asyncio.Event()

    async def gen() -> AsyncGenerator[int, None]:
        nonlocal yielded
        for i in range(100):
            yielded += 1
            yield i

    async def f(x: int) -> int:
        await gate.wait()
        return x

    async with ActorTestKit() as kit:
        src = Source.from_async_iterable(gen).map_async(2, f)
        out = src.take(1).run_with(Sink.seq(), kit.system)
        await eventually(lambda: yielded >= 2)
        await asyncio.sleep(0.01)  # give a wrongly eager stage the chance to over-pull
        assert yielded <= 3
        gate.set()
        assert await out == [0]


def test_map_async_rejects_bad_parallelism():
    with pytest.raises(ValueError):
        Source.from_iterable([1]).map_async(0, lambda x: asyncio.sleep(0, x))
    with pytest.raises(ValueError):
        Source.from_iterable([1]).map_async_unordered(0, lambda x: asyncio.sleep(0, x))


def _awaiting_upstream(log: list[str]):
    async def gen() -> AsyncGenerator[int, None]:
        try:
            for i in range(100):
                await asyncio.sleep(0.01)  # awaits before each yield, as ActorSource does
                yield i
        finally:
            log.append("closed")

    return Source.from_async_iterable(gen)


@pytest.mark.parametrize("unordered", [False, True])
async def test_map_async_downstream_cancel_with_awaiting_upstream(unordered: bool):
    log: list[str] = []

    async def f(x: int) -> int:
        await asyncio.sleep(0.001)
        return x

    src = _awaiting_upstream(log)
    mapped = src.map_async_unordered(2, f) if unordered else src.map_async(2, f)
    async with ActorTestKit() as kit:
        assert await mapped.take(1).run_with(Sink.seq(), kit.system) == [0]
        await eventually(lambda: log == ["closed"])


@pytest.mark.parametrize("unordered", [False, True])
async def test_map_async_failure_with_awaiting_upstream(unordered: bool):
    async def f(x: int) -> int:
        if x == 0:
            raise ValueError("boom")
        await asyncio.Event().wait()
        return x

    src = _awaiting_upstream([])
    mapped = src.map_async_unordered(4, f) if unordered else src.map_async(4, f)
    async with ActorTestKit() as kit:
        with pytest.raises(ValueError, match="boom"):
            await mapped.run_with(Sink.seq(), kit.system)
