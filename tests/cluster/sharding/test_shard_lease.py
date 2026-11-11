"""The shard's lease (Shard.scala 480-560, 772-785, 1194-1198) and the region's buffer until the
shard is initialized (ShardRegion.scala 1210-1214, 1281-1370): ohra's own tests of B8 and of the
review focus. The Pekko specs are ported in test_shard_with_lease_spec.py and
test_cluster_sharding_lease_spec.py.

- the lease's name and owner are Pekko's;
- a shard sends ``ShardInitialized`` only once it holds its lease, and releases the lease when
  it stops;
- a shard stopped while its acquire is pending leaves nothing behind (review focus 1);
- the region buffers up to ``buffer_size`` messages for a shard waiting on its lease, and drops
  the rest with one warning (review focus 2);
- a lease lost during a hand-off is unhandled, so the hand-off still ends with ``ShardStopped``
  (review focus 3);
- a failed acquire is retried after ``lease_retry_interval``.

The shard-level tests spawn ``shard_behavior`` under a probe standing in for the region, on an
``ActorTestKit`` system, which has no cluster, so the lease's owner is the system's name.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace
from typing import Any

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterShardingSettings,
    Entity,
    EntityContext,
    EntityTypeKey,
    ShardingEnvelope,
)
from ohra.cluster.sharding import shard_id_of
from ohra.cluster.sharding.shard import (
    HandOff,
    ShardInitialized,
    ShardStopped,
    shard_behavior,
)
from ohra.coordination import LeaseUsageSettings
from ohra.testkit import ActorTestKit, ManualTime, TestProbe
from ohra.testkit.lease import TestLease, TestLeaseExt

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, Count, node, ref, start
from .test_cluster_sharding_leaving_spec import Host
from .test_ddata_shard_coordinator import manual_node

LEASE = LeaseUsageSettings("test-lease")


@dataclass(frozen=True)
class Add:
    n: int


@dataclass(frozen=True)
class Get:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Ignored:
    """A stop message the entity does not act on."""


type Cmd = Add | Get | Ignored

Counter = EntityTypeKey[Cmd]("Counter")


def counter(_entity: EntityContext[Cmd]) -> Behavior[Cmd]:
    def active(total: int) -> Behavior[Cmd]:
        @Behaviors.receive
        def on_message(_ctx: ActorContext[Cmd], msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Add(n):
                    return active(total + n)
                case Get(reply_to):
                    reply_to.tell(total)
                case Ignored():
                    pass
            return Behaviors.same

        return on_message

    return active(0)


def leased(settings: ClusterShardingSettings = ClusterShardingSettings()) -> Entity[Cmd]:
    return Entity(Counter, counter).with_settings(settings.with_lease_settings(LEASE))


async def lease_of(system: ActorSystem[Any], probe: TestProbe[Any], name: str) -> TestLease:
    return await probe.await_assert(lambda: TestLeaseExt(system).get_test_lease(name))


class LostLeaseError(RuntimeError):
    pass


async def test_the_lease_name_and_owner_are_pekkos():
    port = free_port()
    async with node(port, [port]) as system:
        TestLease.register(system)
        await eventually(lambda: all_up(system, 1), timeout=5)
        start(system, settings=FAST_SHARDING.with_lease_settings(LEASE))
        probe = TestProbe[int](Host(system))
        ref(system, "a").tell(Count(1, probe.ref))
        shard = shard_id_of("a", FAST_SHARDING.number_of_shards)
        lease = await lease_of(system, probe, f"{system.name}-shard-Counter-{shard}")
        assert lease.settings.lease_name == f"{system.name}-shard-Counter-{shard}"
        assert lease.settings.owner_name == f"{system.name}@127.0.0.1:{port}"
        lease.initial_promise.set_result(True)  # so the shard hands off at shutdown
        await probe.expect_message(1)


async def test_no_shard_initialized_until_the_lease_is_acquired():
    async with ActorTestKit() as testkit:
        TestLease.register(testkit.system)
        region: TestProbe[Any] = testkit.create_test_probe()
        testkit.spawn(shard_behavior(leased(), "7", region.ref))
        name = f"{testkit.system.name}-shard-Counter-7"
        lease = await lease_of(testkit.system, region, name)
        assert lease.settings.owner_name == testkit.system.name
        await lease.probe.expect_message(TestLease.AcquireReq(testkit.system.name))
        await region.expect_no_message(0.1)
        lease.initial_promise.set_result(True)
        await region.expect_message(ShardInitialized("7"))


async def test_a_shard_waiting_for_its_lease_stashes_beyond_the_buffer_size():
    """Pekko's Shard stashes with the classic ``Stash``, unbounded by default
    (``stash-capacity = -1``), so messages sent to it directly while the lease is pending are
    all delivered once it is acquired."""
    async with ActorTestKit() as testkit:
        TestLease.register(testkit.system)
        region: TestProbe[Any] = testkit.create_test_probe()
        replies: TestProbe[int] = testkit.create_test_probe()
        settings = replace(ClusterShardingSettings(), buffer_size=2)
        shard = testkit.spawn(shard_behavior(leased(settings), "7", region.ref))
        lease = await lease_of(testkit.system, region, f"{testkit.system.name}-shard-Counter-7")
        await lease.probe.expect_message(TestLease.AcquireReq(testkit.system.name))
        for _ in range(5):
            shard.tell(ShardingEnvelope("a", Add(1)))
        shard.tell(ShardingEnvelope("a", Get(replies.ref)))
        lease.initial_promise.set_result(True)
        await region.expect_message(ShardInitialized("7"))
        await replies.expect_message(5)


async def test_the_lease_is_released_when_the_shard_stops():
    async with ActorTestKit() as testkit:
        TestLease.register(testkit.system)
        region: TestProbe[Any] = testkit.create_test_probe()
        shard = testkit.spawn(shard_behavior(leased(), "7", region.ref))
        lease = await lease_of(testkit.system, region, f"{testkit.system.name}-shard-Counter-7")
        owner = lease.settings.owner_name
        await lease.probe.expect_message(TestLease.AcquireReq(owner))
        lease.initial_promise.set_result(True)
        await region.expect_message(ShardInitialized("7"))
        stopped: TestProbe[ShardStopped] = testkit.create_test_probe()
        shard.tell(HandOff("7", stopped.ref))
        await stopped.expect_message(ShardStopped("7"))
        await lease.probe.expect_message(TestLease.ReleaseReq(owner))


async def test_a_shard_stopped_while_acquiring_leaves_nothing_behind():
    """Review focus 1: the system terminates before the acquire completes."""
    async with ActorTestKit() as testkit:
        TestLease.register(testkit.system)
        region: TestProbe[Any] = testkit.create_test_probe()
        testkit.spawn(shard_behavior(leased(), "7", region.ref))
        lease = await lease_of(testkit.system, region, f"{testkit.system.name}-shard-Counter-7")
        owner = lease.settings.owner_name
        await lease.probe.expect_message(TestLease.AcquireReq(owner))
        testkit.system.terminate()
        await asyncio.wait_for(
            testkit.system.when_terminated(),
            lease.settings.timeout_settings.operation_timeout + 1,
        )
        await lease.probe.expect_message(TestLease.ReleaseReq(owner))
        lease.initial_promise.set_result(True)


async def test_messages_beyond_the_buffer_wait_for_the_lease_and_are_dropped(
    caplog: pytest.LogCaptureFixture,
):
    """Review focus 2: with ``buffer_size=3``, the region keeps the first three of five
    messages for a shard waiting on its lease, and delivers them in order once it has it."""
    caplog.set_level(logging.WARNING)
    port = free_port()
    async with node(port, [port]) as system:
        TestLease.register(system)
        await eventually(lambda: all_up(system, 1), timeout=5)
        settings = replace(FAST_SHARDING, buffer_size=3).with_lease_settings(LEASE)
        start(system, settings=settings)
        probe = TestProbe[int](Host(system))
        for n in range(1, 6):
            ref(system, "a").tell(Count(n, probe.ref))
        shard = shard_id_of("a", settings.number_of_shards)
        lease = await lease_of(system, probe, f"{system.name}-shard-Counter-{shard}")
        await lease.probe.expect_message(TestLease.AcquireReq(lease.settings.owner_name))
        full = [
            r
            for r in caplog.records
            if "buffer is full, dropping message for shard" in r.getMessage()
        ]
        assert len(full) == 1
        assert full[0].levelno == logging.WARNING
        lease.initial_promise.set_result(True)
        # running totals: 1, then 1 + 2, then 1 + 2 + 3, in the order sent
        assert await probe.receive_messages(3) == [1, 3, 6]
        await probe.expect_no_message(0.1)


async def test_a_lease_lost_during_a_hand_off_does_not_stop_the_hand_off():
    """Review focus 3: ``LeaseLost`` is unhandled while the shard hands off, as in Pekko, so the
    shard waits for its entity, which ignores its stop message, until the entity hand-off
    timeout of 1 s (``handoff_timeout - 5``, at least 1 s)."""
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        TestLease.register(testkit.system)
        region: TestProbe[Any] = testkit.create_test_probe()
        entity = leased(ClusterShardingSettings(handoff_timeout=2.0)).with_stop_message(Ignored())
        shard = testkit.spawn(shard_behavior(entity, "7", region.ref))
        lease = await lease_of(testkit.system, region, f"{testkit.system.name}-shard-Counter-7")
        await lease.probe.expect_message(TestLease.AcquireReq(lease.settings.owner_name))
        lease.initial_promise.set_result(True)
        await region.expect_message(ShardInitialized("7"))
        shard.tell(ShardingEnvelope("a", Add(1)))
        stopped: TestProbe[ShardStopped] = testkit.create_test_probe()
        shard.tell(HandOff("7", stopped.ref))
        await manual.time_passes(0)
        lease.current_callback()(LostLeaseError("lost during hand-off"))
        await manual.expect_no_message_for(0.9, stopped)
        await manual.time_passes(0.1)
        await stopped.expect_message(ShardStopped("7"))


async def test_a_failed_acquire_is_retried_after_the_retry_interval():
    async with manual_node() as n:
        assert n.manual is not None
        TestLease.register(n.system)
        region = n.probe()
        n.system._system_root.spawn_child(  # pyright: ignore[reportPrivateUsage]
            shard_behavior(leased(), "7", region.ref), "shard", None
        )
        lease = await lease_of(n.system, region, f"{n.system.name}-shard-Counter-7")
        acquire = TestLease.AcquireReq(lease.settings.owner_name)
        await lease.probe.expect_message(acquire)
        lease.initial_promise.set_result(False)
        await n.manual.expect_no_message_for(4.9, lease.probe)
        await n.manual.time_passes(0.2)
        await lease.probe.expect_message(acquire)
