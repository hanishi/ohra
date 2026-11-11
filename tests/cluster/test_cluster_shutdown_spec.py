"""ClusterShutdownSpec, ported from Apache Pekko's multi-jvm
cluster/src/multi-jvm/scala/org/apache/pekko/cluster/ClusterShutdownSpec.scala.

One test with the spec's cases in Scala order, since they build on each other: "form cluster"
(43), "prepare for shutdown" (47), "spread around the cluster" (60), "not allow new members to
join" (68) and "be allowed to leave" (80).

Differences from the Scala spec:
- the roles are four systems, and the test body stands in for the barriers;
- MultiNodeClusterSpec.clusterConfig's 200 ms intervals are FAST's;
- Thread.sleep(3000) becomes waiting for the ignored-join log line, a condition rather than a
  duration;
- cluster.join and cluster.leave become manager.tell(Join(...)) and Leave(...);
- readView.members.unsorted.map(_.status) becomes the set of state.members statuses;
- Removed is read from a stopped node's self_member, the placeholder its stopped daemon
  leaves. In ohra a node that leaves terminates its system.
"""

from __future__ import annotations

import logging
from contextlib import AsyncExitStack

import pytest

from ohra.cluster.core import Cluster, Join, Leave

from ..util import eventually, free_port
from .test_cluster import all_up, cluster_node


async def test_cluster_shutdown_spec(caplog: pytest.LogCaptureFixture):
    ports = sorted(free_port() for _ in range(4))
    async with AsyncExitStack() as stack:
        first, second, third = [
            await stack.enter_async_context(cluster_node("c", p, [ports[0]])) for p in ports[:3]
        ]
        trio = (first, second, third)

        # "form cluster" (43)
        await eventually(lambda: all(all_up(n, 3) for n in trio), timeout=5)

        # "prepare for shutdown" (47)
        Cluster(first).prepare_for_full_cluster_shutdown()
        await eventually(
            lambda: all(Cluster(n).self_member.status == "ready_for_shutdown" for n in trio),
            timeout=10,
        )

        # "spread around the cluster" (60)
        await eventually(
            lambda: all(
                {m.status for m in Cluster(n).state.members} == {"ready_for_shutdown"}
                for n in trio
            ),
            timeout=10,
        )

        # "not allow new members to join" (68)
        first_cluster = Cluster(first)
        with caplog.at_level(logging.INFO):
            forth = await stack.enter_async_context(cluster_node("c", ports[3], []))
            forth_cluster = Cluster(forth)
            forth_cluster.manager.tell(Join(first_cluster.self_address))
            forth_unique = forth_cluster.self_unique_address
            await eventually(
                lambda: any(
                    f"Ignoring join request from [{forth_unique}]" in r.getMessage()
                    for r in caplog.records
                ),
                timeout=5,
            )
        assert forth_cluster.self_member.status == "removed"

        # "be allowed to leave" (80)
        first_cluster.manager.tell(Leave(first_cluster.self_address))
        await eventually(
            lambda: (
                len(Cluster(second).state.members) == 2
                and len(Cluster(third).state.members) == 2
                and first_cluster.self_member.status == "removed"
            ),
            timeout=10,
        )
        Cluster(second).manager.tell(Leave(Cluster(second).self_address))
        Cluster(second).manager.tell(Leave(Cluster(third).self_address))
        await eventually(
            lambda: all(Cluster(n).self_member.status == "removed" for n in (second, third)),
            timeout=10,
        )
