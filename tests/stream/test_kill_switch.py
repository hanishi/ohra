"""KillSwitches.single, after Apache Pekko's FlowKillSwitchSpec ("A UniqueKillSwitch")."""

from __future__ import annotations

import asyncio

import pytest

from ohra.stream import Keep, KillSwitches, Sink, Source, UniqueKillSwitch
from ohra.testkit import ActorTestKit

from ..util import suspending


async def test_kill_switch_shutdown_completes_downstream_and_cancels_upstream():
    log: list[str] = []
    started = asyncio.Event()
    async with ActorTestKit() as kit:
        switch, out = (
            Source.from_async_iterable(suspending(log, started, 1))
            .via_mat(KillSwitches.single(), Keep.right)
            .to_mat(Sink.seq(), Keep.both)
            .run(kit.system)
        )
        await started.wait()  # upstream suspended inside a pull
        switch.shutdown()
        assert await out == [1]
        assert log == ["closed"]


async def test_kill_switch_abort_fails_downstream():
    log: list[str] = []
    started = asyncio.Event()
    async with ActorTestKit() as kit:
        switch, out = (
            Source.from_async_iterable(suspending(log, started, 1))
            .via_mat(KillSwitches.single(), Keep.right)
            .to_mat(Sink.seq(), Keep.both)
            .run(kit.system)
        )
        await started.wait()
        switch.abort(ValueError("Abort"))
        with pytest.raises(ValueError, match="Abort"):
            await out
        assert log == ["closed"]


async def test_kill_switch_works_if_used_multiple_times_in_a_flow():
    log: list[str] = []
    started = asyncio.Event()
    async with ActorTestKit() as kit:
        (switch1, switch2), q = (
            Source.from_async_iterable(suspending(log, started, 1))
            .via_mat(KillSwitches.single(), Keep.right)
            .recover(lambda _: -1)
            .via_mat(KillSwitches.single(), Keep.both)
            .to_mat(Sink.queue(), Keep.both)
            .run(kit.system)
        )
        assert await q.pull() == 1
        pull = asyncio.ensure_future(q.pull())
        await started.wait()
        switch1.abort(ValueError("Abort"))
        assert await pull == -1
        assert log == ["closed"]
        switch2.shutdown()
        assert await q.pull() is None


async def test_kill_switch_ignores_completion_after_already_completed():
    async with ActorTestKit() as kit:
        switch, out = (
            Source.from_async_iterable(suspending([], asyncio.Event()))
            .via_mat(KillSwitches.single(), Keep.right)
            .to_mat(Sink.seq(), Keep.both)
            .run(kit.system)
        )
        switch.shutdown()
        switch.abort(ValueError("Won't happen"))
        assert await out == []


async def test_kill_switch_shutdown_before_the_first_pull_completes_at_once():
    async with ActorTestKit() as kit:
        switch, q = (
            Source.from_iterable([1, 2, 3])
            .via_mat(KillSwitches.single(), Keep.right)
            .to_mat(Sink.queue(), Keep.both)
            .run(kit.system)
        )
        switch.shutdown()
        assert await q.pull() is None


async def test_each_materialization_has_its_own_switch():
    async with ActorTestKit() as kit:
        graph = (
            Source.from_iterable([1]).via_mat(KillSwitches.single(), Keep.right).to(Sink.ignore())
        )
        first: UniqueKillSwitch = graph.run(kit.system)
        second = graph.run(kit.system)
        assert first is not second
