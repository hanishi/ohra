"""concat, after Apache Pekko's FlowConcatSpec."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest

from ohra.stream import DONE, Done, Flow, Sink, Source
from ohra.testkit import ActorTestKit

from ..util import suspending


async def test_concat_emits_first_then_second():
    async with ActorTestKit() as kit:
        out = Source.from_iterable([1, 2, 3]).concat(Source.from_iterable([4, 5]))
        assert await out.run_with(Sink.seq(), kit.system) == [1, 2, 3, 4, 5]


async def test_concat_pulls_second_after_first():
    log: list[str] = []
    waiting = asyncio.Event()
    release = asyncio.Event()

    async def first() -> AsyncGenerator[int, None]:
        yield 1
        log.append("first waiting")
        waiting.set()
        await release.wait()
        log.append("first completes")

    async def second() -> AsyncGenerator[int, None]:
        log.append("second started")
        yield 2

    async with ActorTestKit() as kit:
        out = (
            Source.from_async_iterable(first)
            .concat(Source.from_async_iterable(second))
            .run_with(Sink.seq(), kit.system)
        )
        await waiting.wait()
        assert log == ["first waiting"]
        release.set()
        assert await out == [1, 2]
        assert log == ["first waiting", "first completes", "second started"]


async def test_concat_closes_the_unpulled_second_when_the_stream_ends_early():
    stash: list[asyncio.Future[Done]] = []
    async with ActorTestKit() as kit:
        second = Source.from_iterable([9]).watch_termination(lambda _, done: stash.append(done))
        out = Source.from_iterable([1, 2, 3]).concat(second).take(1)
        assert await out.run_with(Sink.seq(), kit.system) == [1]
        assert stash[0].result() is DONE  # settled by the time the stream has ended


async def test_concat_downstream_cancel_closes_a_suspended_second():
    log: list[str] = []
    started = asyncio.Event()
    async with ActorTestKit() as kit:
        out = Source.single(1).concat(Source.from_async_iterable(suspending(log, started)))
        q = out.run_with(Sink.queue(), kit.system)
        assert await q.pull() == 1
        pull = asyncio.ensure_future(q.pull())
        await started.wait()  # the second input suspended inside a pull
        q.cancel()
        assert await pull is None
        assert log == ["closed"]


async def test_concat_fails_with_an_immediately_failed_first():
    async with ActorTestKit() as kit:
        out = Source.failed(ValueError("boom")).concat(Source.from_iterable([1, 2]))
        with pytest.raises(ValueError, match="boom"):
            await out.run_with(Sink.seq(), kit.system)


async def test_concat_fails_with_a_failed_second():
    async with ActorTestKit() as kit:
        out = Source.from_iterable([1, 2]).concat(Source.failed(ValueError("boom")))
        with pytest.raises(ValueError, match="boom"):
            await out.run_with(Sink.seq(), kit.system)


async def test_concat_on_flow():
    async with ActorTestKit() as kit:
        flow = Flow.of(int).concat(Source.from_iterable([10, 20]))
        out = Source.from_iterable([1, 2]).via(flow).run_with(Sink.seq(), kit.system)
        assert await out == [1, 2, 10, 20]
