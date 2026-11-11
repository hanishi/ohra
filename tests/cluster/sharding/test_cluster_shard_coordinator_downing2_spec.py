"""Ported from Apache Pekko's ClusterShardCoordinatorDowning2Spec (cluster-sharding/src/multi-jvm/
scala/org/apache/pekko/cluster/sharding/ClusterShardCoordinatorDowning2Spec.scala), its DData
variant (DDataClusterShardCoordinatorDowning2Spec): two cluster nodes, the one not running the
coordinator is cut off and downed by the one that does. The cases, in the Scala order (the Scala
file has no line-numbered cases to name beyond these three):

- join cluster
- initialize shards
- recover after downing other node (not coordinator)

One test runs the cases in order, as the Scala does: each case builds on the cluster and the entity
locations the one before it left.

Roles: the Scala's ``first`` and ``second`` are the systems ``first`` and ``second``; the test body
stands in for the barriers.

Differences from the Scala are those of tests/cluster/sharding/
test_cluster_shard_coordinator_downing_spec.py (the receive-side ``Blackhole`` on both nodes for
``testConductor.blackhole``, the shortened ``down-removal-margin`` and heartbeat pause, a wait on
"unreachable" for ``Thread.sleep(3000)``, entity answers with port and incarnation, ohra's hashed
shards), plus:

- Here ``first`` cuts the link itself (``runOn(first) { blackhole }`` in the Scala) and downs
  ``second``; the test cuts both directions, as ``Direction.Both`` does.
- The Scala reads the original locations back from ``ShardLocations`` after the additional ones;
  here they are the ``original`` dict, read before the cut.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import replace
from typing import Any

from ohra.cluster.core import Cluster, Down
from ohra.testkit import TestProbe

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, node, unreachable_ports
from .test_cluster_shard_coordinator_downing_spec import locate
from .test_cluster_sharding_leaving_spec import (
    Host,
    LvLocated,
    join,
    partition,
    ping,
    port_of,
    start_sharding,
)

SETTINGS = replace(FAST_SHARDING, number_of_shards=10, rebalance_interval=120.0)


async def test_cluster_sharding_ddata_mode_with_down_member_scenario_2():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        # join cluster
        first = await stack.enter_async_context(node(p1, [], down_removal_margin=0.3))
        join(first, first)
        await eventually(lambda: all_up(first, 1), timeout=5)
        start_sharding(first, SETTINGS)
        second = await stack.enter_async_context(node(p2, [], down_removal_margin=0.3))
        join(second, first)
        start_sharding(second, SETTINGS)
        # all Up, everywhere before continuing
        await eventually(lambda: all_up(first, 2) and all_up(second, 2), timeout=5)

        # initialize shards
        original = await locate(first, range(1, 5))
        assert any(located.port == p2 for located in original.values())  # something moves

        # recover after downing other node (not coordinator)
        partition(first, port_of(second))
        partition(second, port_of(first))
        await eventually(lambda: unreachable_ports(first) == {p2}, timeout=10)
        Cluster(first).manager.tell(Down(Cluster(second).self_address))
        await eventually(lambda: all_up(first, 1), timeout=10)
        probe = TestProbe[Any](Host(first))

        async def start_additional() -> dict[str, LvLocated]:
            # start a few more new shards, could be allocated to second but should notice that
            # it's terminated
            return await locate(first, range(5, 9))

        additional = await probe.await_assert(start_additional, max=15.0, interval=0.1)

        async def moved_only_from_second() -> None:
            for entity_id, before in (original | additional).items():
                now = await ping(first, entity_id)
                if before.port == p2:
                    assert now != before
                else:
                    assert now == before  # should not move

        await probe.await_assert(moved_only_from_second, max=15.0, interval=0.1)
