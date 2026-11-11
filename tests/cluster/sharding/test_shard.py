"""The Shard of Apache Pekko's Shard.scala (424-1200), without remembered entities: ohra's own
tests of the spec's B1-B5 and of the shard's queries and logging. The Pekko specs that cover the
shard (``ShardSpec``, ``ClusterShardingLeaseSpec``, ...) are ported in Tasks 4-9.

- B1: the default strategy passivates an entity idle for 120 s, checked every 60 s
- B2: ``buffer_size`` caps the shard's buffered messages in total, across entities
- B3: a shard handing off leaves every message unhandled
- B5: a hand-off drops, silently, what was buffered for a passivating entity
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    ClusterShardingSettings,
    CurrentShardRegionState,
    Entity,
    EntityContext,
    EntityTypeKey,
    GetShardRegionState,
    Passivate,
    Sharding,
    ShardingEnvelope,
    Signal,
)
from ohra.cluster.sharding.shard import (
    CurrentShardState,
    GetShardState,
    GetShardStats,
    HandOff,
    ShardInitialized,
    ShardStats,
    ShardStopped,
    shard_behavior,
)
from ohra.eventstream import Dropped, UnhandledMessage
from ohra.signals import PostStop
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


@dataclass(frozen=True)
class Add:
    n: int


@dataclass(frozen=True)
class Get:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Stop:
    """Ask the shard to passivate this entity."""


@dataclass(frozen=True)
class Halt:
    """The stop message."""


type Cmd = Add | Get | Stop | Halt

Counter = EntityTypeKey[Cmd]("Counter")


def counter(
    events: ActorRef[str],
    gate: asyncio.Event | None = None,
    halts: bool = True,
    reports_start: bool = False,
) -> Callable[[EntityContext[Cmd]], Behavior[Cmd]]:
    """Reports ``halt <id>`` on its stop message, which stops it if ``halts``, and
    ``stopping <id>`` on PostStop, which then waits for ``gate``. With ``reports_start`` it
    reports ``started <id>`` each time it starts."""

    def create(entity: EntityContext[Cmd]) -> Behavior[Cmd]:
        if reports_start:
            events.tell(f"started {entity.entity_id}")

        def active(total: int) -> Behavior[Cmd]:
            def on_message(ctx: ActorContext[Cmd], msg: Cmd) -> Behavior[Cmd]:
                match msg:
                    case Add(n):
                        return active(total + n)
                    case Get(reply_to):
                        reply_to.tell(total)
                    case Stop():
                        entity.shard.tell(Passivate(ctx.self))
                    case Halt():
                        events.tell(f"halt {entity.entity_id}")
                        if halts:
                            return Behaviors.stopped()
                return Behaviors.same

            async def on_signal(_ctx: ActorContext[Cmd], sig: Signal) -> Behavior[Cmd]:
                if sig is PostStop:
                    events.tell(f"stopping {entity.entity_id}")
                    if gate is not None:
                        await gate.wait()
                return Behaviors.same

            return Behaviors.receive(on_message).receive_signal(on_signal)

        return active(0)

    return create


async def entity_ids(shard: ActorRef[Any], probe: TestProbe[CurrentShardState]) -> frozenset[str]:
    shard.tell(GetShardState(probe.ref))
    return (await probe.expect_message_type(CurrentShardState)).entity_ids


async def test_a_shard_tells_its_region_it_is_initialized():
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        region: TestProbe[Any] = testkit.create_test_probe()
        testkit.spawn(shard_behavior(Entity(Counter, counter(events.ref)), "7", region.ref))
        await region.expect_message(ShardInitialized("7"))


async def test_an_entity_idle_for_the_default_120_seconds_is_passivated():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        events: TestProbe[str] = testkit.create_test_probe()
        state: TestProbe[CurrentShardRegionState] = testkit.create_test_probe()
        region = testkit.spawn(Sharding.init(Entity(Counter, counter(events.ref))))
        Sharding.entity_ref_for(region, "a").tell(Add(1))
        region.tell(GetShardRegionState(state.ref))
        assert (await state.receive_message()).entity_ids == {"a"}
        await manual.time_passes(119.0)
        region.tell(GetShardRegionState(state.ref))
        assert (await state.receive_message()).entity_ids == {"a"}
        await manual.time_passes(61.0)
        await events.expect_message("stopping a")
        region.tell(GetShardRegionState(state.ref))
        assert (await state.receive_message()).entity_ids == frozenset()


async def test_the_shard_buffer_caps_buffered_messages_across_entities():
    async with ActorTestKit(manual_time=True) as testkit:
        gate = asyncio.Event()
        events: TestProbe[str] = testkit.create_test_probe()
        region: TestProbe[Any] = testkit.create_test_probe()
        dropped = testkit.create_dropped_message_probe()
        replies: TestProbe[int] = testkit.create_test_probe()
        entity = Entity(Counter, counter(events.ref, gate, reports_start=True)).with_buffer_size(2)
        shard = testkit.spawn(shard_behavior(entity, "1", region.ref))
        for entity_id in ("a", "b"):
            shard.tell(ShardingEnvelope(entity_id, Stop()))
            await events.expect_message(f"started {entity_id}")
            await events.expect_message(f"stopping {entity_id}")
        shard.tell(ShardingEnvelope("a", Add(1)))
        shard.tell(ShardingEnvelope("b", Add(1)))
        shard.tell(ShardingEnvelope("b", Add(2)))
        await dropped.expect_message(
            Dropped(ShardingEnvelope("b", Add(2)), "Buffer for [b] is full", shard)
        )
        gate.set()
        # each restart replays and clears its buffer first, so the Gets are not dropped
        assert {await events.receive_message(), await events.receive_message()} == {
            "started a",
            "started b",
        }
        for entity_id in ("a", "b"):
            shard.tell(ShardingEnvelope(entity_id, Get(replies.ref)))
            await replies.expect_message(1)


async def test_messages_to_a_shard_handing_off_are_unhandled():
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        region: TestProbe[Any] = testkit.create_test_probe()
        stopped: TestProbe[ShardStopped] = testkit.create_test_probe()
        state: TestProbe[CurrentShardState] = testkit.create_test_probe()
        unhandled = testkit.create_unhandled_message_probe()
        entity = Entity(Counter, counter(events.ref, halts=False)).with_stop_message(Halt())
        shard = testkit.spawn(shard_behavior(entity, "1", region.ref))
        shard.tell(ShardingEnvelope("a", Add(1)))
        shard.tell(HandOff("1", stopped.ref))
        await events.expect_message("halt a")
        envelope = ShardingEnvelope("a", Add(2))
        shard.tell(envelope)
        shard.tell(GetShardState(state.ref))
        await unhandled.expect_message(UnhandledMessage(envelope, shard))
        await unhandled.expect_message(UnhandledMessage(GetShardState(state.ref), shard))
        await state.expect_no_message(0.05)


async def test_a_hand_off_drops_what_was_buffered_for_a_passivating_entity():
    async with ActorTestKit(manual_time=True) as testkit:
        gate = asyncio.Event()
        events: TestProbe[str] = testkit.create_test_probe()
        region: TestProbe[Any] = testkit.create_test_probe()
        stopped: TestProbe[ShardStopped] = testkit.create_test_probe()
        state: TestProbe[CurrentShardState] = testkit.create_test_probe()
        dead_letters = testkit.create_dead_letter_probe()
        shard = testkit.spawn(
            shard_behavior(Entity(Counter, counter(events.ref, gate)), "1", region.ref)
        )
        shard.tell(ShardingEnvelope("b", Add(1)))
        shard.tell(ShardingEnvelope("a", Stop()))
        await events.expect_message("stopping a")
        shard.tell(ShardingEnvelope("a", Add(1)))
        assert await entity_ids(shard, state) == {"a", "b"}
        shard.tell(HandOff("1", stopped.ref))
        await events.expect_message("stopping b")
        gate.set()
        await stopped.expect_message(ShardStopped("1"))
        await dead_letters.expect_no_message(0.05)


async def test_a_hand_off_with_no_entities_replies_at_once():
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        region: TestProbe[Any] = testkit.create_test_probe()
        stopped: TestProbe[ShardStopped] = testkit.create_test_probe()
        shard = testkit.spawn(
            shard_behavior(Entity(Counter, counter(events.ref)), "1", region.ref)
        )
        shard.tell(HandOff("1", stopped.ref))
        await stopped.expect_message(ShardStopped("1"))
        await stopped.expect_terminated(shard)


async def test_a_hand_off_for_another_shard_is_refused_with_a_warning(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.WARNING)
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        region: TestProbe[Any] = testkit.create_test_probe()
        stopped: TestProbe[ShardStopped] = testkit.create_test_probe()
        state: TestProbe[CurrentShardState] = testkit.create_test_probe()
        shard = testkit.spawn(
            shard_behavior(Entity(Counter, counter(events.ref)), "1", region.ref)
        )
        shard.tell(HandOff("2", stopped.ref))
        assert await entity_ids(shard, state) == frozenset()
        assert any(
            r.getMessage() == "Counter: Shard [1] can not hand off for another Shard [2]"
            for r in caplog.records
        )
        await stopped.expect_no_message(0.05)


async def test_get_shard_stats_counts_active_and_passivating_entities():
    async with ActorTestKit(manual_time=True) as testkit:
        gate = asyncio.Event()
        events: TestProbe[str] = testkit.create_test_probe()
        region: TestProbe[Any] = testkit.create_test_probe()
        stats: TestProbe[ShardStats] = testkit.create_test_probe()
        state: TestProbe[CurrentShardState] = testkit.create_test_probe()
        shard = testkit.spawn(
            shard_behavior(Entity(Counter, counter(events.ref, gate)), "1", region.ref)
        )
        shard.tell(ShardingEnvelope("a", Add(1)))
        shard.tell(ShardingEnvelope("b", Add(1)))
        shard.tell(ShardingEnvelope("c", Stop()))
        await events.expect_message("stopping c")
        shard.tell(GetShardStats(stats.ref))
        await stats.expect_message(ShardStats("1", 3))
        assert await entity_ids(shard, state) == {"a", "b", "c"}
        gate.set()


async def test_passivation_already_in_progress_sends_no_second_stop_message():
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        region: TestProbe[Any] = testkit.create_test_probe()
        state: TestProbe[CurrentShardState] = testkit.create_test_probe()
        entity = Entity(Counter, counter(events.ref, halts=False)).with_stop_message(Halt())
        shard = testkit.spawn(shard_behavior(entity, "1", region.ref))
        shard.tell(ShardingEnvelope("a", Stop()))
        shard.tell(ShardingEnvelope("a", Stop()))
        await events.expect_message("halt a")
        # the second Passivate is in the shard's mailbox by now
        assert await entity_ids(shard, state) == {"a"}
        await events.expect_no_message(0.05)


@pytest.mark.parametrize("verbose", [True, False])
async def test_verbose_debug_logging_logs_each_delivery(
    verbose: bool, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        region: TestProbe[Any] = testkit.create_test_probe()
        state: TestProbe[CurrentShardState] = testkit.create_test_probe()
        entity = Entity(Counter, counter(events.ref)).with_settings(
            ClusterShardingSettings(verbose_debug_logging=verbose)
        )
        shard = testkit.spawn(shard_behavior(entity, "1", region.ref))
        shard.tell(ShardingEnvelope("a", Add(1)))  # creates a: not logged per delivery
        shard.tell(ShardingEnvelope("a", Add(1)))
        assert await entity_ids(shard, state) == {"a"}
    logged = any(
        r.getMessage() == "Counter: Delivering message of type [Add] to [a]"
        for r in caplog.records
    )
    assert logged == verbose


async def test_a_shard_with_no_passivation_strategy_schedules_no_interval():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        events: TestProbe[str] = testkit.create_test_probe()
        region: TestProbe[Any] = testkit.create_test_probe()
        state: TestProbe[CurrentShardState] = testkit.create_test_probe()
        entity = Entity(Counter, counter(events.ref)).with_settings(
            ClusterShardingSettings().with_no_passivation_strategy()
        )
        shard = testkit.spawn(shard_behavior(entity, "1", region.ref))
        shard.tell(ShardingEnvelope("a", Add(1)))
        assert await entity_ids(shard, state) == {"a"}
        await manual.time_passes(3600)
        assert await entity_ids(shard, state) == {"a"}
        await events.expect_no_message(0.05)
