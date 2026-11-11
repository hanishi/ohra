"""ClusterShardingStatsSpec, ported from Apache Pekko's
cluster-sharding-typed/src/test/scala/org/apache/pekko/cluster/sharding/typed/scaladsl/
ClusterShardingStatsSpec.scala, on one node:

- "return empty statistics if there are no running sharded entities" (46);
- "allow querying of statistics of the currently running sharded entities in the entire
  cluster" (59).

Differences from the Scala spec:
- ohra has no ``sharding.shardState`` actor: ``GetClusterShardingStats(typeKey, timeout,
  replyTo)`` is told to the region as ``GetClusterShardingStats(timeout, reply_to)``, as
  ``GetShardRegionState`` is;
- in 46 Pekko asks for a type that was never initialized, which ohra cannot address, so the
  region is initialized on a node that has not joined: it has no coordinator and answers
  ``ClusterShardingStats({})``;
- the typed ``IdTestProtocol`` with its message extractor becomes ``ShardingEnvelope`` with
  ``SpecStatsHello``, and ``number-of-shards = 10`` is ``FAST_SHARDING``'s;
- the reply probes are ``ask``s with Pekko's 3 s probe default, and the ``SelfUp`` probe is
  ``all_up``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from ohra import (
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    ClusterSharding,
    ClusterShardingStats,
    Entity,
    EntityContext,
    EntityTypeKey,
    GetClusterShardingStats,
    ShardingEnvelope,
    ShardRegionStats,
    ask,
)
from ohra.cluster.core import Cluster
from ohra.cluster.sharding import shard_id_of
from ohra.remote.serialization import remotable

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from ..test_cluster_sharding import FAST_SHARDING


@remotable
@dataclass(frozen=True)
class SpecStatsHello:
    reply_to: ActorRef[str]


ID_TEST = EntityTypeKey[SpecStatsHello]("id-test")


def behavior_with_id(_entity: EntityContext[SpecStatsHello]) -> Behavior[SpecStatsHello]:
    def on_message(
        _ctx: ActorContext[SpecStatsHello], msg: SpecStatsHello
    ) -> Behavior[SpecStatsHello]:
        msg.reply_to.tell("Hello!")
        return Behaviors.same

    return Behaviors.receive(on_message)


def init(system: Any) -> ActorRef[Any]:
    entity = Entity(ID_TEST, behavior_with_id).with_settings(FAST_SHARDING)
    return cast(ActorRef[Any], ClusterSharding(system).init(entity))


async def cluster_stats(region: ActorRef[Any]) -> ClusterShardingStats:
    query_timeout = FAST_SHARDING.shard_region_query_timeout
    return await ask(region, lambda r: GetClusterShardingStats(query_timeout, r), timeout=3)


async def test_return_empty_statistics_if_there_are_no_running_sharded_entities():
    async with cluster_node("c", free_port(), []) as system:
        assert await cluster_stats(init(system)) == ClusterShardingStats({})


async def test_allow_querying_of_statistics_of_the_currently_running_sharded_entities():
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        sharding_ref = init(system)
        id1 = "id1"
        hello: str = await ask(
            sharding_ref, lambda r: ShardingEnvelope(id1, SpecStatsHello(r)), timeout=3
        )
        assert hello == "Hello!"

        stats = await cluster_stats(sharding_ref)

        expect = ClusterShardingStats(
            {
                Cluster(system).self_member.address: ShardRegionStats(
                    {shard_id_of(id1, FAST_SHARDING.number_of_shards): 1}, frozenset()
                )
            }
        )
        assert stats == expect
