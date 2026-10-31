"""Source factories, after Apache Pekko's SourceSpec."""

from __future__ import annotations

import asyncio

import pytest

from ohra.stream import Sink, Source
from ohra.testkit import ActorTestKit


async def test_single_emits_one_element():
    async with ActorTestKit() as kit:
        assert await Source.single(1).run_with(Sink.seq(), kit.system) == [1]


async def test_from_iterable_is_reusable():
    async with ActorTestKit() as kit:
        src = Source.from_iterable([1, 2, 3])
        assert await src.run_with(Sink.seq(), kit.system) == [1, 2, 3]
        assert await src.run_with(Sink.seq(), kit.system) == [1, 2, 3]


async def test_from_async_iterable_creates_an_iterator_per_run():
    async def gen():
        for i in range(3):
            yield i

    async with ActorTestKit() as kit:
        src = Source.from_async_iterable(gen)
        assert await src.run_with(Sink.seq(), kit.system) == [0, 1, 2]
        assert await src.run_with(Sink.seq(), kit.system) == [0, 1, 2]


async def test_empty_completes():
    async with ActorTestKit() as kit:
        assert await Source.empty().run_with(Sink.seq(), kit.system) == []


async def test_failed_fails_the_sink():
    async with ActorTestKit() as kit:
        with pytest.raises(RuntimeError, match="boom"):
            await Source.failed(RuntimeError("boom")).run_with(Sink.seq(), kit.system)


async def test_future_emits_its_result():
    async with ActorTestKit() as kit:
        fut: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        out = Source.future(fut).run_with(Sink.seq(), kit.system)
        fut.set_result("hello")
        assert await out == ["hello"]


async def test_future_failure_fails_the_stream():
    async def boom() -> int:
        raise RuntimeError("boom")

    async with ActorTestKit() as kit:
        with pytest.raises(RuntimeError, match="boom"):
            await Source.future(boom()).run_with(Sink.seq(), kit.system)


async def test_repeat_head():
    async with ActorTestKit() as kit:
        assert await Source.repeat(7).run_with(Sink.head(), kit.system) == 7
