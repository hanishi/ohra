"""The stats queries: Apache Pekko's ``GetShardRegionStats`` and ``GetClusterShardingStats``
(ShardRegion.scala 281-321, 966-985, 1066-1101; ShardCoordinator.scala 917-940;
ShardingQueries.scala 37-57). ohra's own tests of B9; the Pekko specs are ported in
test_cluster_sharding_stats_spec.py and test_cluster_sharding_stats_multi_node_spec.py.

- a region counts the entities of each of its shards;
- a region with no shards answers at once;
- a shard still waiting for its lease is reported failed after ``shard_region_query_timeout``,
  with Pekko's warning, and the other shards' counts are returned (review focus 4);
- the coordinator answers ``ClusterShardingStats({})`` when a region does not answer in time;
- the stats cross the wire.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from ohra import (
    ClusterShardingSettings,
    ClusterShardingStats,
    Entity,
    GetClusterShardingStats,
    GetShardRegionStats,
    Sharding,
    ShardingEnvelope,
    ShardRegionStats,
)
from ohra.cluster.sharding.shard import shard_id_of
from ohra.remote import Address
from ohra.remote.serialization import Serialization
from ohra.testkit import ActorTestKit, FishingOutcome, FishingOutcomes, ManualTime, TestProbe
from ohra.testkit.lease import TestLease

from .test_active_entity_limit import ids_in_shard
from .test_ddata_shard_coordinator import active, manual_node, registered
from .test_shard import Add, Counter, Get, counter
from .test_shard_lease import LEASE, lease_of

N = 10
X, Y = "0", "1"


async def test_region_stats_count_entities_per_shard():
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        settings = ClusterShardingSettings(number_of_shards=N)
        region = testkit.spawn(
            Sharding.init(Entity(Counter, counter(events.ref)).with_settings(settings))
        )
        for entity_id in ids_in_shard(X, 2) + ids_in_shard(Y, 1):
            region.tell(ShardingEnvelope(entity_id, Add(1)))
        stats: TestProbe[ShardRegionStats] = testkit.create_test_probe()
        region.tell(GetShardRegionStats(stats.ref))
        await stats.expect_message(ShardRegionStats({X: 2, Y: 1}, frozenset()))


async def test_region_stats_with_no_shards_are_empty():
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        region = testkit.spawn(Sharding.init(Entity(Counter, counter(events.ref))))
        stats: TestProbe[ShardRegionStats] = testkit.create_test_probe()
        region.tell(GetShardRegionStats(stats.ref))
        await stats.expect_message(ShardRegionStats({}, frozenset()))


async def test_a_shard_waiting_for_its_lease_is_reported_failed(
    caplog: pytest.LogCaptureFixture,
):
    """Review focus 4: shard A holds its lease and one entity, shard B still waits for its
    lease, so it stashes the ``GetShardStats``."""
    caplog.set_level(logging.WARNING)
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        TestLease.register(testkit.system)
        events: TestProbe[str] = testkit.create_test_probe()
        settings = ClusterShardingSettings(number_of_shards=N).with_lease_settings(LEASE)
        region = testkit.spawn(
            Sharding.init(Entity(Counter, counter(events.ref)).with_settings(settings))
        )
        name = f"{testkit.system.name}-shard-Counter-"
        [a], [b] = ids_in_shard(X, 1), ids_in_shard(Y, 1)

        region.tell(ShardingEnvelope(a, Add(1)))
        lease_a = await lease_of(testkit.system, events, name + X)
        await lease_a.probe.expect_message(TestLease.AcquireReq(lease_a.settings.owner_name))
        lease_a.initial_promise.set_result(True)
        replies: TestProbe[int] = testkit.create_test_probe()
        region.tell(ShardingEnvelope(a, Get(replies.ref)))
        await replies.expect_message(1)

        region.tell(ShardingEnvelope(b, Add(1)))
        lease_b = await lease_of(testkit.system, events, name + Y)
        await lease_b.probe.expect_message(TestLease.AcquireReq(lease_b.settings.owner_name))

        stats: TestProbe[ShardRegionStats] = testkit.create_test_probe()
        region.tell(GetShardRegionStats(stats.ref))
        await manual.expect_no_message_for(settings.shard_region_query_timeout - 0.1, stats)
        await manual.time_passes(0.1)
        await stats.expect_message(ShardRegionStats({X: 1}, frozenset({Y})))
        warnings = [r for r in caplog.records if "[1] failed after" in r.getMessage()]
        assert len(warnings) == 1
        assert warnings[0].levelno == logging.WARNING
        lease_b.initial_promise.set_result(True)


async def test_cluster_stats_are_empty_when_a_region_does_not_answer():
    async with manual_node() as node:
        assert node.manual is not None
        replicator, answers, silent = node.probe(), node.probe(), node.probe()
        coordinator = await active(node, replicator)
        await registered(coordinator, replicator, answers)
        await registered(coordinator, replicator, silent)
        reply: TestProbe[ClusterShardingStats] = node.probe()
        coordinator.tell(GetClusterShardingStats(3.0, reply.ref))

        def is_query(m: Any) -> FishingOutcome:
            if isinstance(m, GetShardRegionStats):
                return FishingOutcomes.complete()
            return FishingOutcomes.continue_and_ignore()

        [query] = await answers.fish_for_message(3, is_query)
        await silent.fish_for_message(3, is_query)
        assert isinstance(query, GetShardRegionStats)
        query.reply_to.tell(ShardRegionStats({}, frozenset()))
        await node.manual.expect_no_message_for(2.9, reply)
        await node.manual.time_passes(0.1)
        await reply.expect_message(ClusterShardingStats({}))


def test_the_stats_cross_the_wire():
    stats = ClusterShardingStats(
        {Address("c", "h", 1): ShardRegionStats({"1": 2}, frozenset({"2"}))}
    )

    def no_refs(path: Any) -> Any:
        raise AssertionError(path)

    assert Serialization.decode(Serialization.encode(stats, no_refs), no_refs) == stats
