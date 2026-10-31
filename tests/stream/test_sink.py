"""Sinks, after Apache Pekko's SinkSpec, HeadSinkSpec and SeqSinkSpec."""

from __future__ import annotations

import asyncio
import gc
import itertools
import logging
import operator
from collections.abc import AsyncIterator

import pytest

from ohra.stream import DONE, NOT_USED, Keep, Sink, Source
from ohra.testkit import ActorTestKit


async def test_head_cancels_upstream():
    closed = asyncio.Event()

    async def gen() -> AsyncIterator[int]:
        try:
            for i in itertools.count():
                yield i
        finally:
            closed.set()

    async with ActorTestKit() as kit:
        assert await Source.from_async_iterable(gen).run_with(Sink.head(), kit.system) == 0
        await asyncio.wait_for(closed.wait(), 1)


async def test_head_of_empty_fails():
    async with ActorTestKit() as kit:
        with pytest.raises(LookupError, match="head of empty stream"):
            await Source.empty().run_with(Sink.head(), kit.system)


async def test_head_yields_the_first_error():
    async with ActorTestKit() as kit:
        with pytest.raises(RuntimeError, match="ex"):
            await Source.failed(RuntimeError("ex")).run_with(Sink.head(), kit.system)


async def test_head_option_of_empty_is_none():
    async with ActorTestKit() as kit:
        assert await Source.empty().run_with(Sink.head_option(), kit.system) is None
        assert await Source.single(1).run_with(Sink.head_option(), kit.system) == 1


async def test_last_and_last_option():
    async with ActorTestKit() as kit:
        src = Source.from_iterable([1, 2, 3])
        assert await src.run_with(Sink.last(), kit.system) == 3
        assert await src.run_with(Sink.last_option(), kit.system) == 3
        assert await Source.empty().run_with(Sink.last_option(), kit.system) is None
        with pytest.raises(LookupError, match="last of empty stream"):
            await Source.empty().run_with(Sink.last(), kit.system)


async def test_fold_and_reduce():
    async with ActorTestKit() as kit:
        src = Source.from_iterable([1, 2, 3, 4])
        assert await src.run_with(Sink.fold(0, operator.add), kit.system) == 10
        assert await src.run_with(Sink.reduce(operator.add), kit.system) == 10
        assert await src.run_fold(0, operator.add, kit.system) == 10
        assert await src.run_reduce(operator.add, kit.system) == 10
        with pytest.raises(LookupError, match="reduce over empty stream"):
            await Source.empty().run_with(Sink.reduce(operator.add), kit.system)


async def test_foreach_completes_with_done():
    seen: list[int] = []
    async with ActorTestKit() as kit:
        src = Source.from_iterable([1, 2, 3])
        assert await src.run_with(Sink.foreach(seen.append), kit.system) == DONE
        assert await src.run_foreach(seen.append, kit.system) == DONE
    assert seen == [1, 2, 3, 1, 2, 3]


async def test_ignore_completes_with_done():
    async with ActorTestKit() as kit:
        assert await Source.from_iterable([1, 2]).run_with(Sink.ignore(), kit.system) == DONE


async def test_seq_fails_when_upstream_fails():
    async def gen() -> AsyncIterator[int]:
        yield 1
        raise RuntimeError("boom")

    async with ActorTestKit() as kit:
        with pytest.raises(RuntimeError, match="boom"):
            await Source.from_async_iterable(gen).run_with(Sink.seq(), kit.system)


class _Resource:
    """An upstream that holds something from materialization on: closing it is the cancel."""

    def __init__(self) -> None:
        self.closed = asyncio.Event()

    def __aiter__(self) -> _Resource:
        return self

    async def __anext__(self) -> int:
        return 1

    async def aclose(self) -> None:
        self.closed.set()


async def test_cancelled_cancels_upstream():
    res = _Resource()
    src = Source[int, object](lambda _ctx: (res, NOT_USED)).map(str)
    async with ActorTestKit() as kit:
        assert src.run_with(Sink.cancelled(), kit.system) is NOT_USED
        await asyncio.wait_for(res.closed.wait(), 1)


async def test_failed_future_dropped_by_keep_is_not_reported(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.ERROR, logger="asyncio")
    async with ActorTestKit() as kit:
        graph = Source.failed(RuntimeError("boom")).to_mat(Sink.seq(), Keep.right)
        fut = graph.run(kit.system)
        await asyncio.wait([fut])  # done, its exception never asked for
        del fut, graph
    gc.collect()
    assert "never retrieved" not in caplog.text
