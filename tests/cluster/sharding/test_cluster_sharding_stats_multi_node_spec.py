"""ClusterShardingStatsSpec, ported from Apache Pekko's multi-jvm
cluster-sharding-typed/src/multi-jvm/scala/org/apache/pekko/cluster/sharding/typed/
ClusterShardingStatsSpec.scala.

One test with the spec's cases in Scala order, since the second needs the first's cluster:
"form cluster" (82) and "get shard stats" (86).

Differences from the Scala spec:
- the roles are three systems in one process, ``cluster_node`` with the first as the seed and
  ``FAST`` and ``FAST_SHUTDOWN``, and the test body stands in for the barriers, running each
  role's steps on every node in turn;
- ``updating-state-timeout = 2s`` and ``waiting-for-state-timeout = 2s`` are set on
  ``FAST_SHARDING``;
- the typed ``Pinger`` becomes the ``@remotable`` ``SpecStatsPing`` and ``SpecStatsPong``, and
  the ``TestProbe[Pong]`` is an ``ask`` with Pekko's 3 s probe default;
- ``sharding.shardState ! GetClusterShardingStats(typeKey, ...)`` is
  ``GetClusterShardingStats(timeout, reply_to)`` told to the region, as ohra has no
  ``shardState`` actor, and ``statsProbe.receiveMessage(queryTimeout)`` is an ask with that
  timeout.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass, replace
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
    ask,
)
from ohra.remote.serialization import remotable

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from ..test_cluster_sharding import FAST_SHARDING


@remotable
@dataclass(frozen=True)
class SpecStatsPong:
    id: int


@remotable
@dataclass(frozen=True)
class SpecStatsPing:
    id: int
    ref: ActorRef[SpecStatsPong]


PING = EntityTypeKey[SpecStatsPing]("ping")

SETTINGS = replace(FAST_SHARDING, updating_state_timeout=2.0, waiting_for_state_timeout=2.0)


def pinger(_entity: EntityContext[SpecStatsPing]) -> Behavior[SpecStatsPing]:
    def on_message(
        _ctx: ActorContext[SpecStatsPing], msg: SpecStatsPing
    ) -> Behavior[SpecStatsPing]:
        msg.ref.tell(SpecStatsPong(msg.id))
        return Behaviors.same

    return Behaviors.receive(on_message)


async def test_cluster_sharding_stats_spec():
    ports = sorted(free_port() for _ in range(3))
    async with AsyncExitStack() as stack:
        trio = [await stack.enter_async_context(cluster_node("c", p, [ports[0]])) for p in ports]
        query_timeout = SETTINGS.shard_region_query_timeout * len(trio)

        # "form cluster" (82)
        await eventually(lambda: all(all_up(n, 3) for n in trio), timeout=5)

        # "get shard stats" (86)
        regions: list[ActorRef[ShardingEnvelope[SpecStatsPing]]] = [
            ClusterSharding(n).init(Entity(PING, pinger).with_settings(SETTINGS)) for n in trio
        ]
        # enterBarrier("sharding started")
        for region in regions:
            for n in (1, 2):
                pong: SpecStatsPong = await ask(
                    region, lambda r: ShardingEnvelope(f"ping-{n}", SpecStatsPing(n, r)), timeout=3
                )
                assert pong == SpecStatsPong(n)
        # enterBarrier("sharding-initialized")
        for region in regions:
            stats: ClusterShardingStats = await ask(
                cast(ActorRef[Any], region),
                lambda r: GetClusterShardingStats(query_timeout, r),
                timeout=query_timeout,
            )
            assert len(stats.regions) == 3  # 3 nodes
            assert sum(sum(s.stats.values()) for s in stats.regions.values()) == 2
            assert all(not s.failed for s in stats.regions.values())
