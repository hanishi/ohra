"""StopShards on real nodes: a region forwards it to its coordinator (ShardRegion.scala 768 and
962-964, deliverCoordinatorCommand), which stops the shards and acks each with ShardStopped
(ShardCoordinator.scala 1112-1145 and 850-862). The coordinator's own steps are tested on
ManualTime in test_ddata_shard_coordinator.py."""

from __future__ import annotations

from contextlib import AsyncExitStack

from ohra import ask
from ohra.cluster.sharding import ShardStopped, StopShards, shard_id_of

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, Count, hosted, node, ref, start


async def test_the_region_forwards_stop_shards_to_the_coordinator():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all(all_up(n, 2) for n in nodes), timeout=5)
        regions = [start(n) for n in nodes]
        assert await ask(ref(nodes[0], "x"), lambda r: Count(1, r), timeout=5) == 1
        shard = shard_id_of("x", FAST_SHARDING.number_of_shards)
        stopped: ShardStopped = await ask(
            regions[1], lambda r: StopShards(frozenset({shard}), r), timeout=5
        )
        assert stopped == ShardStopped(shard)
        for region in regions:
            assert "x" not in await hosted(region)
        # the next message starts the entity again, fresh
        assert await ask(ref(nodes[1], "x"), lambda r: Count(1, r), timeout=5) == 1
