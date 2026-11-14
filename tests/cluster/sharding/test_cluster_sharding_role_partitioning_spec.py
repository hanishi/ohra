"""ClusterShardingRolePartitioningSpec, ported from Apache Pekko's cluster-sharding/src/multi-jvm/
scala/org/apache/pekko/cluster/sharding/ClusterShardingRolePartitioningSpec.scala, both configs:
``ClusterShardingMinMembersPerRoleNotConfiguredConfig`` (``min-nr-of-members = 2``) and
``ClusterShardingMinMembersPerRoleConfiguredConfig`` (``min-nr-of-members = 3``, R1's 3, R2's 2).

Five nodes, ``first`` to ``third`` with the role R1 and ``fourth`` and ``fifth`` with R2. Every
node starts ``Datatype1`` with the role R1 and ``Datatype2`` with R2, so R1's nodes are proxies
for ``Datatype2`` and R2's for ``Datatype1``:

- "start the cluster, await convergence, init sharding on every node" (136-158);
- "access role R2 (nodes 4,5) from one of the proxy nodes (1,2,3)" (161-182).

Differences from the Scala spec:

- the nodes run on one event loop; ``startSharding`` is ``ClusterSharding.init``, called before
  the cluster is up, as the Scala's "start sharding early";
- ``TestActors.echoActorProps`` is an entity that answers with its message's number;
  ``Datatype1``'s string ids and ``Datatype2``'s int ids are both strings, sharded by ohra's hash
  in place of ``extractShardId``'s one shard per id."""

from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any

import pytest

from ohra import (
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterSharding,
    ClusterShardingStats,
    Entity,
    EntityTypeKey,
    GetClusterShardingStats,
    ask,
    remotable,
)
from ohra.cluster.core import Cluster

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, node
from .test_ddata_shard_coordinator import Node


@remotable
@dataclass(frozen=True)
class PartitionEcho:
    n: int
    reply_to: ActorRef[int]


E1 = EntityTypeKey[PartitionEcho]("Datatype1")
E2 = EntityTypeKey[PartitionEcho]("Datatype2")


@Behaviors.receive_message
def echo(msg: PartitionEcho) -> Behavior[PartitionEcho]:
    msg.reply_to.tell(msg.n)
    return Behaviors.same


NOT_CONFIGURED: dict[str, Any] = {"min_nr_of_members": 2}
CONFIGURED: dict[str, Any] = {
    "min_nr_of_members": 3,
    "min_nr_of_members_of_role": {"R1": 3, "R2": 2},
}


@pytest.mark.parametrize(
    "config", [NOT_CONFIGURED, CONFIGURED], ids=["per-role-not-configured", "per-role"]
)
async def test_cluster_sharding_with_roles(config: dict[str, Any]):
    ports = [free_port() for _ in range(5)]
    async with AsyncExitStack() as stack:
        nodes: list[ActorSystem[Any]] = []
        regions: list[ActorRef[Any]] = []
        for i, p in enumerate(ports):
            role = "R1" if i < 3 else "R2"
            system = await stack.enter_async_context(
                node(p, [ports[0]], roles=frozenset({role}), **config)
            )
            nodes.append(system)
            # start sharding early
            ClusterSharding(system).init(
                Entity(E1, lambda _: echo).with_settings(FAST_SHARDING.with_role("R1"))
            )
            regions.append(
                ClusterSharding(system).init(
                    Entity(E2, lambda _: echo).with_settings(FAST_SHARDING.with_role("R2"))
                )
            )
        await eventually(lambda: all(all_up(n, 5) for n in nodes), timeout=10)

        # "access role R2 (nodes 4,5) from one of the proxy nodes (1,2,3)"
        first = nodes[0]
        for n in range(1, 21):
            entity = ClusterSharding(first).entity_ref_for(E2, str(n))
            # R2 entity received, does not timeout
            assert await ask(entity, lambda r, n=n: PartitionEcho(n, r), 5) == n

        probe = Node(first).probe()
        regions[0].tell(GetClusterShardingStats(10, probe.ref))
        stats = await probe.expect_message_type(ClusterShardingStats, 12)
        assert set(stats.regions) == {Cluster(n).self_address for n in nodes[3:]}
        assert sum(sum(s.stats.values()) for s in stats.regions.values()) == 20
