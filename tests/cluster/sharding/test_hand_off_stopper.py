"""The HandOffStopper of Apache Pekko's ShardRegion.scala (521-597). Pekko has no spec of its own
for it; these cases follow its control flow, on ManualTime.

- replies ShardStopped once every entity has stopped (Terminated, 549-554)
- no stop message: PoisonPill, so each entity handles its pending messages first
- warns after StopTimeoutWarningAfter (5 s) (556-566)
- stops the remaining entities at the hand-off timeout (568-581)
- the warning ends early while CoordinatedShutdown runs (562-565)

Differences: ``stop`` stands for ``ActorRef.tell(PoisonPill)`` and ``context.stop`` of the
entities, which are the shard's children; the message name is the Python class name.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import pytest

from ohra import ActorContext, ActorRef, Behavior, Behaviors, CoordinatedShutdown
from ohra.cluster.sharding.shard import ShardStopped, hand_off_stopper
from ohra.coordinated_shutdown import Reason
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


@dataclass(frozen=True)
class Stop:
    pass


@dataclass(frozen=True)
class Note:
    text: str


@dataclass(frozen=True)
class Start:
    pass


@dataclass(frozen=True)
class TestReason(Reason):
    __test__ = False


def stopping_entity(log: list[str]) -> Behavior[Any]:
    def on_message(msg: Any) -> Behavior[Any]:
        match msg:
            case Stop():
                return Behaviors.stopped()
            case Note(text):
                log.append(text)
        return Behaviors.same

    return Behaviors.receive_message(on_message)


def ignoring_entity() -> Behavior[Any]:
    return Behaviors.receive_message(lambda _msg: Behaviors.same)


def parent(
    entities: list[Behavior[Any]],
    reply_to: ActorRef[ShardStopped],
    stopper_out: ActorRef[ActorRef[Any]],
    stop_message: object | None,
    handoff_timeout: float = 55.0,
    pending: str | None = None,
) -> Behavior[Start]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Start]) -> Behavior[Start]:
        refs = frozenset(ctx.spawn(b, f"e{i}") for i, b in enumerate(entities))
        if pending is not None:
            for r in refs:
                r.tell(Note(pending))
        stopper = ctx.spawn_anonymous(
            hand_off_stopper(
                "Cart",
                "1",
                reply_to,
                refs,
                stop_message,
                handoff_timeout,
                lambda ref, after_pending: ctx.stop(ref, after_pending=after_pending),
            )
        )
        stopper_out.tell(stopper)
        return Behaviors.receive_message(lambda _m: Behaviors.same)

    return setup


async def test_the_stopper_replies_shard_stopped_once_every_entity_stopped():
    async with ActorTestKit(manual_time=True) as kit:
        replies: TestProbe[ShardStopped] = kit.create_test_probe()
        out: TestProbe[ActorRef[Any]] = kit.create_test_probe()
        kit.spawn(parent([stopping_entity([])] * 2, replies.ref, out.ref, Stop()))
        stopper = await out.receive_message()
        await replies.expect_message(ShardStopped("1"))
        await out.expect_terminated(stopper)
        await replies.expect_no_message(0.05)


async def test_no_stop_message_stops_each_entity_after_its_pending_messages():
    async with ActorTestKit(manual_time=True) as kit:
        log: list[str] = []
        replies: TestProbe[ShardStopped] = kit.create_test_probe()
        out: TestProbe[ActorRef[Any]] = kit.create_test_probe()
        kit.spawn(parent([stopping_entity(log)], replies.ref, out.ref, None, pending="pending"))
        await replies.expect_message(ShardStopped("1"))
        assert log == ["pending"]


async def test_the_stopper_warns_after_five_seconds(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.WARNING)
    async with ActorTestKit(manual_time=True) as kit:
        manual = ManualTime(kit.system)
        replies: TestProbe[ShardStopped] = kit.create_test_probe()
        out: TestProbe[ActorRef[Any]] = kit.create_test_probe()
        kit.spawn(parent([ignoring_entity()], replies.ref, out.ref, Note("x")))
        await out.receive_message()
        await manual.time_passes(4.9)
        assert not caplog.records
        await manual.time_passes(0.2)
        assert len(caplog.records) == 1
        text = caplog.records[0].getMessage()
        assert "[1] of the entities in shard [1] not stopped after [5 s]" in text
        assert "handOffStopMessage [Note]" in text
        assert "Waiting additional [55 s]" in text
        await manual.time_passes(60)
        await replies.expect_message(ShardStopped("1"))


async def test_the_stopper_stops_stragglers_at_the_hand_off_timeout(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.WARNING)
    async with ActorTestKit(manual_time=True) as kit:
        manual = ManualTime(kit.system)
        replies: TestProbe[ShardStopped] = kit.create_test_probe()
        out: TestProbe[ActorRef[Any]] = kit.create_test_probe()
        kit.spawn(parent([ignoring_entity()], replies.ref, out.ref, Note("x")))
        await out.receive_message()
        await manual.time_passes(55.0)
        await replies.expect_message(ShardStopped("1"))
        text = caplog.records[-1].getMessage()
        assert "HandOffStopMessage [Note]" in text
        assert "after [55 s]" in text
        assert "remaining [1] entities" in text


async def test_the_warning_is_shorter_while_coordinated_shutdown_runs(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.WARNING)
    async with ActorTestKit(manual_time=True) as kit:
        manual = ManualTime(kit.system)
        # a task that never ends keeps the run, and so the reason, going without terminating
        shutdown = CoordinatedShutdown(kit.system)
        release = asyncio.Event()
        shutdown.add_task("before-service-unbind", "hold", release.wait)
        running = shutdown.run(TestReason())
        replies: TestProbe[ShardStopped] = kit.create_test_probe()
        out: TestProbe[ActorRef[Any]] = kit.create_test_probe()
        kit.spawn(parent([ignoring_entity()], replies.ref, out.ref, Note("x")))
        await out.receive_message()
        await manual.time_passes(5.1)
        warns = [r.getMessage() for r in caplog.records if "not stopped after" in r.getMessage()]
        assert len(warns) == 1
        assert warns[0].endswith("is not handled? ")
        release.set()
        await running
