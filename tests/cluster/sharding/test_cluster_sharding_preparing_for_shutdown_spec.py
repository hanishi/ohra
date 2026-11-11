"""ClusterShardingPreparingForShutdownSpec, ported from Apache Pekko's multi-jvm
cluster-sharding-typed/src/multi-jvm/scala/org/apache/pekko/cluster/sharding/typed/
ClusterShardingPreparingForShutdownSpec.scala.

One test with the spec's cases in Scala order, since they build on each other: "form cluster"
(86) and "not rebalance but should still work preparing for shutdown" (90).

Differences from the Scala spec:
- the roles are three systems in one process, the first (lowest port) the oldest, and the test
  body stands in for the barriers;
- ``MultiNodeTypedClusterSpec.formCluster`` is ``cluster_node`` with the first as the seed, and
  ``all_up``;
- the typed ``Pinger`` becomes the ``@remotable`` ``SpecPrepPing`` and ``SpecPrepPong``, named
  so they clash with no other test's messages, and the ``TestProbe[Pong]`` is an ``ask``;
- ``runOn(second) { cluster.manager ! PrepareForFullClusterShutdown }`` is told to the second's
  cluster manager, and ``awaitAssert(10.seconds)`` is ``eventually(..., timeout=10)``;
- the second's ``Removed`` is its system terminating, as a node that leaves does in ohra;
- the retried ``id3`` ping (``awaitAssert``, 10 s) is a loop of one-second asks under a 10 s
  timeout;
- the AutoDowning provider with auto-down off is ohra's default of no downing provider, and
  ``leader-actions-interval = 100ms`` is FAST's.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any

from ohra import (
    ActorContext,
    ActorRef,
    AskTimeoutError,
    Behavior,
    Behaviors,
    ClusterSharding,
    Entity,
    EntityContext,
    EntityTypeKey,
    ShardingEnvelope,
    ask,
)
from ohra.cluster.core import Cluster, Leave, PrepareForFullClusterShutdown
from ohra.remote.serialization import remotable

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from ..test_cluster_sharding import FAST_SHARDING


@remotable
@dataclass(frozen=True)
class SpecPrepPong:
    id: int


@remotable
@dataclass(frozen=True)
class SpecPrepPing:
    id: int
    ref: ActorRef[SpecPrepPong]


PING = EntityTypeKey[SpecPrepPing]("ping")


def pinger(_entity: EntityContext[SpecPrepPing]) -> Behavior[SpecPrepPing]:
    def on_message(_ctx: ActorContext[SpecPrepPing], msg: SpecPrepPing) -> Behavior[SpecPrepPing]:
        msg.ref.tell(SpecPrepPong(msg.id))
        return Behaviors.same

    return Behaviors.receive(on_message)


async def ping(region: ActorRef[ShardingEnvelope[SpecPrepPing]], id: str, n: int) -> SpecPrepPong:
    return await ask(region, lambda r: ShardingEnvelope(id, SpecPrepPing(n, r)), timeout=3)


async def test_cluster_sharding_preparing_for_shutdown_spec():
    ports = sorted(free_port() for _ in range(3))
    async with AsyncExitStack() as stack:
        first, second, third = [
            await stack.enter_async_context(cluster_node("c", p, [ports[0]])) for p in ports
        ]
        trio = (first, second, third)

        # "form cluster" (86)
        await eventually(lambda: all(all_up(n, 3) for n in trio), timeout=5)

        # "not rebalance but should still work preparing for shutdown" (90)
        regions: list[ActorRef[ShardingEnvelope[SpecPrepPing]]] = [
            ClusterSharding(n).init(Entity(PING, pinger).with_settings(FAST_SHARDING))
            for n in trio
        ]
        for region in regions:
            assert await ping(region, "id1", 1) == SpecPrepPong(1)

        Cluster(second).manager.tell(PrepareForFullClusterShutdown())
        await eventually(
            lambda: all(
                Cluster(n).self_member.status == "ready_for_shutdown"
                and {m.status for m in Cluster(n).state.members} == {"ready_for_shutdown"}
                for n in trio
            ),
            timeout=10,
        )

        for region in regions:
            assert await ping(region, "id2", 2) == SpecPrepPong(2)

        Cluster(second).manager.tell(Leave(Cluster(second).self_address))
        # keep this lower than coordinated shutdown timeout
        await asyncio.wait_for(second.when_terminated(), 5)
        await eventually(
            lambda: all(len(Cluster(n).state.members) == 2 for n in (first, third)), timeout=5
        )

        # trigger creation of a new shard should be fine even though one node left
        async def ping_id3(region: ActorRef[ShardingEnvelope[SpecPrepPing]]) -> SpecPrepPong:
            async with asyncio.timeout(10):
                while True:
                    try:
                        return await ping(region, "id3", 3)
                    except AskTimeoutError:
                        pass

        for region in (regions[0], regions[2]):
            assert await ping_id3(region) == SpecPrepPong(3)

        Cluster(third).manager.tell(Leave(Cluster(first).self_address))
        Cluster(third).manager.tell(Leave(Cluster(third).self_address))
        terminated: list[Any] = [n.when_terminated() for n in (first, third)]
        await asyncio.wait_for(asyncio.gather(*terminated), 15)
