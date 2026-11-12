"""Pekko's BackoffOnStopSupervisor (pattern/internal/BackoffOnStopSupervisor.scala and
HandleBackoff.scala), after BackoffOnStopSupervisorSpec: a stopped child is started again after
an exponential backoff, the restart count resets ``min_backoff`` after a start, messages without a
child go to dead letters, and the final stop message stops the supervisor. The spec's cases for
``GetRestartCount``, ``GetCurrentChild`` and ``Reset`` are not ported: the supervisor leaves them
out."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ohra import ActorRef, Behaviors, DeadLetter, EventStream
from ohra.behavior import Behavior
from ohra.cluster.sharding.backoff_supervisor import backoff_on_stop_supervisor
from ohra.context import ActorContext
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


@dataclass(frozen=True)
class Started:
    pass


@dataclass(frozen=True)
class Stop:
    pass


@dataclass(frozen=True)
class Final:
    pass


def child(probe: ActorRef[Any]) -> Behavior[Any]:
    """Says ``Started`` when it starts, forwards every message, and stops on ``Stop`` or
    ``Final``."""

    @Behaviors.setup
    def setup(_ctx: ActorContext[Any]) -> Behavior[Any]:
        probe.tell(Started())

        @Behaviors.receive_message
        def forward(msg: Any) -> Behavior[Any]:
            probe.tell(msg)
            if isinstance(msg, Stop | Final):
                return Behaviors.stopped()
            return Behaviors.same

        return forward

    return setup


def supervisor(
    probe: TestProbe[Any], min_backoff: float = 1.0, max_backoff: float = 10.0
) -> Behavior[Any]:
    return backoff_on_stop_supervisor(
        child(probe.ref), "child", min_backoff, max_backoff, 0.0, lambda m: isinstance(m, Final)
    )


async def test_messages_are_forwarded_to_the_child():
    async with ActorTestKit(manual_time=True) as testkit:
        probe: TestProbe[Any] = testkit.create_test_probe()
        sup = testkit.spawn(supervisor(probe))
        await probe.expect_message(Started())
        sup.tell("hello")
        await probe.expect_message("hello")


async def test_a_stopped_child_is_started_again_after_the_backoff():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        probe: TestProbe[Any] = testkit.create_test_probe()
        sup = testkit.spawn(supervisor(probe))
        await probe.expect_message(Started())
        sup.tell(Stop())
        await probe.expect_message(Stop())
        await manual.expect_no_message_for(0.99, probe)
        await manual.time_passes(0.01)
        await probe.expect_message(Started())


async def test_the_backoff_doubles_with_each_stop_up_to_the_max():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        probe: TestProbe[Any] = testkit.create_test_probe()
        sup = testkit.spawn(supervisor(probe, max_backoff=3.0))
        await probe.expect_message(Started())
        for delay in (1.0, 2.0, 3.0, 3.0):
            sup.tell(Stop())
            await probe.expect_message(Stop())
            await manual.expect_no_message_for(delay - 0.01, probe)
            await manual.time_passes(0.01)
            await probe.expect_message(Started())


async def test_the_restart_count_resets_min_backoff_after_a_start():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        probe: TestProbe[Any] = testkit.create_test_probe()
        sup = testkit.spawn(supervisor(probe))
        await probe.expect_message(Started())
        sup.tell(Stop())
        await probe.expect_message(Stop())
        await manual.time_passes(1.0)
        await probe.expect_message(Started())
        await manual.time_passes(1.0)  # _ResetRestartCount
        sup.tell(Stop())
        await probe.expect_message(Stop())
        await manual.expect_no_message_for(0.99, probe)
        await manual.time_passes(0.01)  # min_backoff again, not twice it
        await probe.expect_message(Started())


async def test_messages_without_a_child_go_to_dead_letters():
    async with ActorTestKit(manual_time=True) as testkit:
        probe: TestProbe[Any] = testkit.create_test_probe()
        dead: TestProbe[DeadLetter] = testkit.create_test_probe()
        testkit.system.event_stream.tell(EventStream.Subscribe(DeadLetter, dead.ref))
        sup = testkit.spawn(supervisor(probe))
        await probe.expect_message(Started())
        sup.tell(Stop())
        await probe.expect_message(Stop())
        sup.tell("lost")
        await dead.expect_message(DeadLetter("lost", sup))


async def test_the_final_stop_message_stops_the_supervisor_once_the_child_stops():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        probe: TestProbe[Any] = testkit.create_test_probe()
        sup = testkit.spawn(supervisor(probe))
        await probe.expect_message(Started())
        sup.tell(Final())
        await probe.expect_message(Final())
        await probe.expect_terminated(sup)
        await manual.expect_no_message_for(10.0, probe)


async def test_the_final_stop_message_during_the_backoff_stops_the_supervisor():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        probe: TestProbe[Any] = testkit.create_test_probe()
        sup = testkit.spawn(supervisor(probe))
        await probe.expect_message(Started())
        sup.tell(Stop())
        await probe.expect_message(Stop())
        sup.tell(Final())
        await probe.expect_terminated(sup)
        await manual.expect_no_message_for(10.0, probe)
