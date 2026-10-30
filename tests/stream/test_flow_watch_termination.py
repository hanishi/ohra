"""watch_termination, after Apache Pekko's FlowWatchTerminationSpec."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest

from ohra.stream import DONE, AbruptStageTerminationError, Flow, Keep, Sink, Source
from ohra.testkit import ActorTestKit


async def test_watch_termination_completes_with_done():
    async with ActorTestKit() as kit:
        watched = Source.from_iterable([1, 2]).watch_termination(Keep.right)
        done, seq = watched.to_mat(Sink.seq(), Keep.both).run(kit.system)
        assert await seq == [1, 2]
        assert await done is DONE


async def test_watch_termination_fails_with_upstream_failure():
    async with ActorTestKit() as kit:
        watched = Source.failed(ValueError("boom")).watch_termination(Keep.right)
        done, seq = watched.to_mat(Sink.seq(), Keep.both).run(kit.system)
        with pytest.raises(ValueError, match="boom"):
            await done
        with pytest.raises(ValueError, match="boom"):
            await seq


async def test_watch_termination_completes_on_downstream_cancel():
    async with ActorTestKit() as kit:
        watched = Source.from_iterable([1, 2, 3]).watch_termination(Keep.right)
        done, head = watched.to_mat(Sink.head(), Keep.both).run(kit.system)
        assert await head == 1
        assert await done is DONE


async def test_watch_termination_completes_when_cancelled_before_the_first_pull():
    async with ActorTestKit() as kit:
        watched = Source.from_iterable([1]).watch_termination(Keep.right)
        done = watched.to_mat(Sink.cancelled(), Keep.left).run(kit.system)
        assert await done is DONE


async def test_watch_termination_fails_when_the_system_terminates():
    kit = ActorTestKit()
    async with kit:
        blocked: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        watched = Source.future(blocked).watch_termination(Keep.right)
        done, seq = watched.to_mat(Sink.seq(), Keep.both).run(kit.system)
        await asyncio.sleep(0)
        kit.system.terminate()
        await kit.system.when_terminated()
    with pytest.raises(AbruptStageTerminationError):
        await done
    with pytest.raises(AbruptStageTerminationError):
        await seq


async def test_watch_termination_on_flow():
    async with ActorTestKit() as kit:
        flow = Flow.of(int).watch_termination(Keep.right)
        done = (
            Source.from_iterable([1, 2])
            .via_mat(flow, Keep.right)
            .to_mat(Sink.ignore(), Keep.left)
            .run(kit.system)
        )
        assert await done is DONE


def _suspended_upstream(suspended: asyncio.Event):
    async def gen() -> AsyncGenerator[int, None]:
        suspended.set()
        await asyncio.Event().wait()  # never set
        yield 1

    return Source.from_async_iterable(gen)


async def test_watch_termination_is_done_when_downstream_cancels_through_an_operator():
    suspended = asyncio.Event()
    async with ActorTestKit() as kit:
        done, sq = (
            _suspended_upstream(suspended)
            .watch_termination(Keep.right)
            .map(lambda x: x)
            .to_mat(Sink.queue(), Keep.both)
            .run(kit.system)
        )
        pending = asyncio.ensure_future(sq.pull())
        await suspended.wait()  # the drive's pull task is suspended inside the upstream
        sq.cancel()
        assert await pending is None
        assert await asyncio.wait_for(done, 1) is DONE


async def test_watch_termination_fails_when_terminated_through_an_operator():
    suspended = asyncio.Event()
    async with ActorTestKit() as kit:
        done, sq = (
            _suspended_upstream(suspended)
            .watch_termination(Keep.right)
            .map(lambda x: x)
            .to_mat(Sink.queue(), Keep.both)
            .run(kit.system)
        )
        pending = asyncio.ensure_future(sq.pull())
        await suspended.wait()
        kit.system.terminate()
        await kit.system.when_terminated()
    with pytest.raises(AbruptStageTerminationError):
        await asyncio.wait_for(done, 1)
    with pytest.raises(AbruptStageTerminationError):
        await asyncio.wait_for(pending, 1)


async def test_watch_termination_fails_when_terminated_through_an_operator_into_a_future_sink():
    suspended = asyncio.Event()
    async with ActorTestKit() as kit:
        done, out = (
            _suspended_upstream(suspended)
            .watch_termination(Keep.right)
            .map(lambda x: x)
            .to_mat(Sink.ignore(), Keep.both)
            .run(kit.system)
        )
        await suspended.wait()
        kit.system.terminate()
        await kit.system.when_terminated()
    with pytest.raises(AbruptStageTerminationError):
        await asyncio.wait_for(done, 1)
    with pytest.raises(AbruptStageTerminationError):
        await asyncio.wait_for(out, 1)
