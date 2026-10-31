"""zip and zip_with, after Apache Pekko's FlowZipSpec and FlowZipWithSpec."""

from __future__ import annotations

import asyncio

import pytest

from ohra.stream import DONE, Done, Flow, Keep, Sink, Source
from ohra.testkit import ActorTestKit

from ..util import suspending


async def test_zip_stops_at_shorter():
    async with ActorTestKit() as kit:
        zipped = Source.from_iterable([1, 2, 3, 4]).zip(Source.from_iterable(["a", "b", "c"]))
        assert await zipped.run_with(Sink.seq(), kit.system) == [(1, "a"), (2, "b"), (3, "c")]


async def test_zip_with():
    async with ActorTestKit() as kit:
        zipped = Source.from_iterable([1, 2, 3]).zip_with(
            Source.from_iterable([10, 20, 30, 40]), lambda a, b: a + b
        )
        assert await zipped.run_with(Sink.seq(), kit.system) == [11, 22, 33]


async def test_zip_with_fails_when_the_function_raises():
    async with ActorTestKit() as kit:
        zipped = Source.from_iterable([1, 2, 0]).zip_with(
            Source.from_iterable([1, 1, 1]), lambda a, b: b // a
        )
        with pytest.raises(ZeroDivisionError):
            await zipped.run_with(Sink.seq(), kit.system)


async def test_zip_completes_with_an_immediately_completed_input():
    async with ActorTestKit() as kit:
        zipped = Source.from_iterable([1, 2, 3]).zip(Source.empty())
        assert await zipped.run_with(Sink.seq(), kit.system) == []


async def test_zip_completes_when_one_side_completes_while_the_other_is_suspended():
    log: list[str] = []
    started = asyncio.Event()
    async with ActorTestKit() as kit:
        zipped = Source.from_async_iterable(suspending(log, started, 1)).zip(
            Source.from_iterable(["a"])
        )
        assert await zipped.run_with(Sink.seq(), kit.system) == [(1, "a")]
        assert started.is_set()
        assert log == ["closed"]


async def test_zip_fails_with_an_immediately_failed_input():
    async with ActorTestKit() as kit:
        zipped = Source.from_iterable([1, 2, 3]).zip(Source.failed(ValueError("boom")))
        with pytest.raises(ValueError, match="boom"):
            await zipped.run_with(Sink.seq(), kit.system)


async def test_zip_downstream_cancel_closes_suspended_inputs():
    log: list[str] = []
    started = (asyncio.Event(), asyncio.Event())
    async with ActorTestKit() as kit:
        zipped = Source.from_async_iterable(suspending(log, started[0], 1)).zip(
            Source.from_async_iterable(suspending(log, started[1], 2))
        )
        q = zipped.run_with(Sink.queue(), kit.system)
        assert await q.pull() == (1, 2)
        pull = asyncio.ensure_future(q.pull())
        await started[0].wait()
        await started[1].wait()  # both inputs suspended inside a pull
        q.cancel()
        assert await pull is None
        assert sorted(log) == ["closed", "closed"]


async def test_zip_closes_the_other_when_cancelled_before_the_first_pull():
    stash: list[asyncio.Future[Done]] = []
    async with ActorTestKit() as kit:
        other = Source.from_iterable([2]).watch_termination(lambda _, done: stash.append(done))
        zipped = Source.single(1).zip(other).watch_termination(Keep.right)
        assert await zipped.to_mat(Sink.cancelled(), Keep.left).run(kit.system) is DONE
        assert stash[0].result() is DONE


async def test_zip_on_flow():
    async with ActorTestKit() as kit:
        flow = Flow.of(int).zip(Source.from_iterable("ab"))
        out = Source.from_iterable([1, 2, 3]).via(flow).run_with(Sink.seq(), kit.system)
        assert await out == [(1, "a"), (2, "b")]
