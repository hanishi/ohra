"""ClusterShardingMinMembersSpec, ported from Apache Pekko's cluster-sharding/src/multi-jvm/scala/
org/apache/pekko/cluster/sharding/ClusterShardingMinMembersSpec.scala, the DData mode: the
coordinator allocates no shard until ``min-nr-of-members`` regions have registered (Pekko's
``hasAllRegionsRegistered``, ShardCoordinator.scala 1209-1216). "use all nodes" (79-124).

Also here, the shard role's minimum (``minMembers``, 686-690).

Differences from the Scala spec:

- the nodes run on one event loop; ``startSharding`` is ``ClusterSharding.init`` with the
  ``counter`` entity of tests/cluster/test_cluster_sharding.py, and ``TestActors.echoActorProps``
  is its reply;
- entity ids are strings and the shard id is ``shard_id_of``, so the Scala's ids 1, 2 and 3 are
  three ids on distinct shards of ``FAST_SHARDING``'s ten;
- ``leastShardAllocationStrategy(absoluteLimit = 2, relativeLimit = 1.0)`` is
  ``rebalance_absolute_limit=2`` and ``rebalance_relative_limit=1.0``, and ``rebalance-interval =
  120s`` is ``rebalance_interval=120``;
- the coordinator's state is read and written with a plain majority (``majority_min_cap=0`` and
  no majority-plus), which the first two nodes' Replicators reach: the third's Replicator starts
  only with its ``init``, and with Pekko's defaults (all of three nodes, by the cap of 5) the
  coordinator would wait for it whatever the minimum, so the case would not show the minimum;
- the Scala's ``stats.regions(firstAddress)`` sum of 1 is not asserted: which of the three
  regions gets a shard on a tie is the allocation strategy's choice; the three regions and the
  three entities are."""

from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import replace
from typing import Any, cast

from ohra import ActorRef, ActorSystem, ClusterShardingStats, GetClusterShardingStats
from ohra.cluster.sharding import shard_id_of
from ohra.remote import Address

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, Count, node, ref, start
from .test_ddata_shard_coordinator import Node

SETTINGS = replace(
    FAST_SHARDING,
    rebalance_interval=120.0,
    rebalance_absolute_limit=2,
    rebalance_relative_limit=1.0,
    coordinator_state_read_majority_plus=0,
    coordinator_state_write_majority_plus=0,
    majority_min_cap=0,
)


def ids_on_distinct_shards(count: int) -> list[str]:
    ids: list[str] = []
    shards: set[str] = set()
    n = 1
    while len(ids) < count:
        shard = shard_id_of(str(n), SETTINGS.number_of_shards)
        if shard not in shards:
            shards.add(shard)
            ids.append(str(n))
        n += 1
    return ids


async def test_use_all_nodes():
    p1, p2, p3 = free_port(), free_port(), free_port()
    one, two, three = ids_on_distinct_shards(3)
    async with AsyncExitStack() as stack:
        nodes: list[ActorSystem[Any]] = []
        for p in (p1, p2, p3):
            nodes.append(await stack.enter_async_context(node(p, [p1], min_nr_of_members=3)))
            if p != p3:
                start(nodes[-1], settings=SETTINGS)
        # wait with starting sharding on third
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=10)
        first = nodes[0]
        replies = Node(first).probe()

        ref(first, one).tell(Count(1, replies.ref))
        # not allocated because third has not registered yet
        await replies.expect_no_message(2)

        start(nodes[2], settings=SETTINGS)
        # the 1 was sent above
        await replies.expect_message(1, 10)
        ref(first, two).tell(Count(1, replies.ref))
        await replies.expect_message(1, 5)
        ref(first, three).tell(Count(1, replies.ref))
        await replies.expect_message(1, 5)

        region = cast(ActorRef[Any], start(first, settings=SETTINGS))
        stats_probe = Node(first).probe()
        region.tell(GetClusterShardingStats(5, stats_probe.ref))
        stats = await stats_probe.expect_message_type(ClusterShardingStats, 6)
        assert set(stats.regions) == {Address("c", "127.0.0.1", p) for p in (p1, p2, p3)}
        assert sum(sum(s.stats.values()) for s in stats.regions.values()) == 3


async def test_the_shard_roles_minimum_is_used_when_sharding_has_a_role():
    """ShardCoordinator.scala 686-690: with a role, the coordinator waits for that role's
    ``min_nr_of_members_of_role`` (1 when it has none), not ``min_nr_of_members``. The cluster
    needs three members to come up; the one region on the role is then enough."""
    p1, p2, p3 = free_port(), free_port(), free_port()
    settings = replace(SETTINGS, role="backend")
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(
                node(
                    p,
                    [p1],
                    min_nr_of_members=3,
                    roles=frozenset({"backend"}) if p == p1 else frozenset[str](),
                )
            )
            for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=10)
        first = nodes[0]
        start(first, settings=settings)
        replies = Node(first).probe()
        ref(first, "a").tell(Count(1, replies.ref))
        await replies.expect_message(1, 5)
