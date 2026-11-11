"""ClusterSingletonManagerPreparingForShutdownSpec, ported from Apache Pekko's multi-jvm
cluster-tools/src/multi-jvm/scala/org/apache/pekko/cluster/singleton/
ClusterSingletonManagerPreparingForShutdownSpec.scala.

One test with the spec's cases in Scala order, since they build on each other: "form cluster"
(104), "not handover when ready for shutdown" (108) and "last nodes should shut down" (161).

Differences from the Scala spec:
- the roles are three systems, and the test body stands in for the barriers;
- Echo and its testActor messages become test_singleton.py's counter and its start/stop log;
- expectMsgType[ActorRef] becomes the Hit reply;
- the AutoDowning provider with auto-down off is ohra's default of no downing provider;
- leader-actions-interval = 100ms is FAST's, and the singleton runs with FAST_SINGLETON;
- the cluster-exiting timeout is Pekko's default 10 s, through node(..., exiting_timeout=10.0),
  so the 8 s wait for the removal shows the exit did not wait for a hand-over;
- expectNoMessage(5.seconds) on the second's proxy becomes an ask that times out after
  max_hand_over_retries * hand_over_retry_interval + 0.5 s, the 5 s scaled to FAST_SINGLETON;
- Removed is read from a stopped node's self_member, the placeholder its stopped daemon leaves.
  In ohra a node that leaves terminates its system.
"""

from __future__ import annotations

from contextlib import AsyncExitStack

import pytest

from ohra import AskTimeoutError, ask
from ohra.cluster.core import Cluster, Leave

from ..util import eventually, free_port
from .test_cluster import all_up
from .test_singleton import FAST_SINGLETON, Hit, Log, node, start, starts


async def test_cluster_singleton_manager_preparing_for_shutdown_spec():
    ports = sorted(free_port() for _ in range(3))
    async with AsyncExitStack() as stack:
        first, second, third = [
            await stack.enter_async_context(node(p, [ports[0]], exiting_timeout=10.0))
            for p in ports
        ]
        trio = (first, second, third)

        # "form cluster" (104)
        await eventually(lambda: all(all_up(n, 3) for n in trio), timeout=5)

        # "not handover when ready for shutdown" (108)
        log: Log = []
        proxies = [start(n, log, p) for n, p in zip(trio, ports, strict=True)]
        assert await ask(proxies[0], Hit, timeout=10) == 1
        assert starts(log) == [ports[0]]

        Cluster(first).prepare_for_full_cluster_shutdown()
        await eventually(
            lambda: all(Cluster(n).self_member.status == "ready_for_shutdown" for n in trio),
            timeout=10,
        )

        Cluster(first).manager.tell(Leave(Cluster(first).self_address))
        # below the 10 s cluster-exiting timeout: the exit does not wait for a hand-over
        await eventually(
            lambda: (
                all(len(Cluster(n).state.members) == 2 for n in (second, third))
                and Cluster(first).self_member.status == "removed"
            ),
            timeout=8,
        )

        # the new oldest does not start the singleton, not even after its hand-over retries
        wait = FAST_SINGLETON.max_hand_over_retries * FAST_SINGLETON.hand_over_retry_interval
        with pytest.raises(AskTimeoutError):
            await ask(proxies[1], Hit, timeout=wait + 0.5)
        assert starts(log) == [ports[0]]

        # "last nodes should shut down" (161)
        Cluster(second).manager.tell(Leave(Cluster(third).self_address))
        Cluster(second).manager.tell(Leave(Cluster(second).self_address))
        await eventually(
            lambda: all(Cluster(n).self_member.status == "removed" for n in (second, third)),
            timeout=10,
        )
