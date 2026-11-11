"""Ported from Apache Pekko's ClusterShardCoordinatorDowningSpec (cluster-sharding/src/multi-jvm/
scala/org/apache/pekko/cluster/sharding/ClusterShardCoordinatorDowningSpec.scala), its DData
variant (DDataClusterShardCoordinatorDowningSpec, Scala 112-143): two cluster nodes, the one
running the coordinator is cut off and downed by the other. The cases, in the Scala order:

- join cluster (112)
- initialize shards (129)
- recover after downing coordinator node (143)

One test runs the cases in order, as the Scala does: each case builds on the cluster and the entity
locations the one before it left.

Roles: the Scala's ``first`` and ``second`` are the systems ``first`` and ``second``; the test body
is the ``controller``, which has no system and cuts the link (``testConductor.blackhole``) and
stands in for the barriers.

Differences from the Scala:

- ``testTransport(on = true)`` and ``blackhole(first, second, Direction.Both)`` are a ``Blackhole``
  throttle on each side's receive direction (``partition`` of
  tests/cluster/sharding/test_cluster_sharding_leaving_spec.py).
- ``down-removal-margin = 3 s`` and ``acceptable-heartbeat-pause = 3s`` are shortened with the
  other cluster intervals: ``down_removal_margin=0.3`` and ``FAST``'s heartbeat pause. The
  ``Thread.sleep(3000)`` that lets the failure detector notice is a wait until the second node
  sees the first unreachable (a condition, not a duration).
- ``Ping(id)``, ``extractShardId``, ``ShardLocations`` and ``newRef should not be ref`` are as in
  tests/cluster/sharding/test_cluster_sharding_leaving_spec.py (ohra hashes ids into shards, here
  10, and an entity answers with its node's port and an incarnation token).
- ``cluster.down(first)`` is ``Cluster(second).manager.tell(Down(address))``.
- ``rebalance-interval = 120 s`` is ``rebalance_interval=120.0``. ``loglevel = INFO`` is not
  ported. The ``within(20.seconds)`` bound is the ``await_assert`` timeouts below.
- ``startPersistenceIfNeeded`` is for the persistence mode only.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import replace
from typing import Any

from ohra import ActorSystem
from ohra.cluster.core import Cluster, Down
from ohra.testkit import TestProbe

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, node, unreachable_ports
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


async def locate(system: ActorSystem[Any], ids: range) -> dict[str, LvLocated]:
    return {str(n): await ping(system, str(n)) for n in ids}


async def test_cluster_sharding_ddata_mode_with_down_member_scenario_1():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        # join cluster (112)
        first = await stack.enter_async_context(node(p1, [], down_removal_margin=0.3))
        join(first, first)
        await eventually(lambda: all_up(first, 1), timeout=5)
        start_sharding(first, SETTINGS)
        second = await stack.enter_async_context(node(p2, [], down_removal_margin=0.3))
        join(second, first)
        start_sharding(second, SETTINGS)
        # all Up, everywhere before continuing
        await eventually(lambda: all_up(first, 2) and all_up(second, 2), timeout=5)

        # initialize shards (129)
        original = await locate(first, range(1, 5))
        assert any(located.port == p1 for located in original.values())  # something moves

        # recover after downing coordinator node (143)
        partition(first, port_of(second))
        partition(second, port_of(first))
        await eventually(lambda: unreachable_ports(second) == {p1}, timeout=10)
        Cluster(second).manager.tell(Down(Cluster(first).self_address))
        await eventually(lambda: all_up(second, 1), timeout=10)
        probe = TestProbe[Any](Host(second))

        async def start_additional() -> dict[str, LvLocated]:
            # start a few more new shards, could be allocated to first but should notice that
            # it's terminated
            return await locate(second, range(5, 9))

        additional = await probe.await_assert(start_additional, max=15.0, interval=0.1)

        async def moved_only_from_first() -> None:
            for entity_id, before in (original | additional).items():
                now = await ping(second, entity_id)
                if before.port == p1:
                    assert now != before
                else:
                    assert now == before  # should not move

        await probe.await_assert(moved_only_from_first, max=15.0, interval=0.1)
