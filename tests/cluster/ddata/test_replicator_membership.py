"""The Replicator's view of the cluster, on three nodes in one process: Pekko's
receiveMemberUp, receiveMemberExiting and receiveMemberRemoved, and nodesForReadWrite.

No Pekko spec covers these alone. The read and write targets show in which node a read or a
write reaches; the long gossip interval keeps values where the writes put them."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager

from ohra.cluster import Cluster, ClusterDomainEvent, Leave, MemberExited, Subscribe
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    Replicator,
    ReplicatorSettings,
)
from ohra.remote import UniqueAddress
from ohra.testkit import TestProbe

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from .test_replicator_local import Node

KEYS = [GCounterKey(f"k{i}") for i in range(4)]


@asynccontextmanager
async def three_nodes(
    settings: ReplicatorSettings = ReplicatorSettings(),
) -> AsyncGenerator[list[Node]]:
    """Nodes by port, lowest first. The seed has the highest port and is up first, so it is the
    oldest member while its address sorts last."""
    low, mid, high = sorted(free_port() for _ in range(3))
    async with AsyncExitStack() as stack:
        seed = await stack.enter_async_context(
            cluster_node("ddata", high, [high], distributed_data=settings)
        )
        await eventually(lambda: all_up(seed, 1), timeout=3)
        systems = [
            *[
                await stack.enter_async_context(
                    cluster_node("ddata", p, [high], distributed_data=settings)
                )
                for p in (low, mid)
            ],
            seed,
        ]
        await eventually(lambda: all(all_up(s, 3) for s in systems), timeout=5)
        nodes: list[Node] = []
        for s in systems:
            dd = DistributedData(s)
            nodes.append(Node(s, dd.replicator, dd.self_unique_address))
        yield nodes


async def replica_count(node: Node) -> int:
    probe: TestProbe[Replicator.ReplicaCount] = node.probe()
    node.replicator.tell(Replicator.GetReplicaCount(probe.ref))
    return (await probe.receive_message()).n


async def replica_count_is(node: Node, n: int) -> None:
    assert await replica_count(node) == n


async def every_replicator_knows_all(nodes: list[Node]) -> None:
    """Each Replicator takes a Read or a Write only from a node it knows."""
    for node in nodes:
        await node.probe().await_assert(
            lambda node=node: replica_count_is(node, 3), max=3, interval=0.01
        )


async def update(
    node: Node, key: GCounterKey, write: Replicator.WriteConsistency
) -> Replicator.UpdateResponse[GCounter]:
    probe: TestProbe[Replicator.UpdateResponse[GCounter]] = node.probe()
    node.replicator.tell(
        Replicator.Update(
            key, GCounter(), write, probe.ref, lambda c: c.increment(node.self_address)
        )
    )
    return await probe.receive_message(5.0)


async def get(
    node: Node, key: GCounterKey, read: Replicator.ReadConsistency
) -> Replicator.GetResponse[GCounter]:
    probe: TestProbe[Replicator.GetResponse[GCounter]] = node.probe()
    node.replicator.tell(Replicator.Get(key, read, probe.ref))
    return await probe.receive_message(5.0)


def address_of(node: Node) -> UniqueAddress:
    return node.self_address.unique_address


async def test_replica_count_counts_up_members():
    async with three_nodes() as nodes:
        for node in nodes:
            await node.probe().await_assert(
                lambda node=node: replica_count_is(node, 3), max=3, interval=0.01
            )


async def test_a_removed_member_leaves_the_replicas():
    async with three_nodes() as nodes:
        low, mid, high = nodes
        await low.probe().await_assert(lambda: replica_count_is(low, 3), max=3, interval=0.01)
        Cluster(low.system).manager.tell(Leave(address_of(mid).address))
        await asyncio.wait_for(mid.system.when_terminated(), timeout=5)
        for node in (low, high):
            await node.probe().await_assert(
                lambda node=node: replica_count_is(node, 2), max=3, interval=0.01
            )


async def test_prefer_oldest_writes_to_the_oldest_first():
    async with three_nodes(ReplicatorSettings(prefer_oldest=True, gossip_interval=60.0)) as nodes:
        low, mid, high = nodes
        await every_replicator_knows_all(nodes)
        # WriteTo(2) writes to one other node: the oldest, the seed, though its address sorts
        # last. It acks long before the secondary send, so the other gets nothing.
        for key in KEYS:
            assert await update(low, key, Replicator.WriteTo(2, 20.0)) == (
                Replicator.UpdateSuccess(key)
            )
        for key in KEYS:
            assert isinstance(await get(high, key, Replicator.ReadLocal()), Replicator.GetSuccess)
            assert await get(mid, key, Replicator.ReadLocal()) == Replicator.NotFound(key)


async def test_exiting_nodes_are_excluded_from_read_all_targets():
    async with three_nodes(ReplicatorSettings(gossip_interval=60.0)) as nodes:
        low, mid, high = nodes
        await every_replicator_knows_all(nodes)
        k1, k2 = KEYS[:2]
        for key in (k1, k2):  # on the middle node only: no gossip runs
            assert await update(mid, key, Replicator.WriteLocal()) == Replicator.UpdateSuccess(key)
        exited: TestProbe[ClusterDomainEvent] = low.probe()
        Cluster(low.system).subscriptions.tell(Subscribe(exited.ref, MemberExited))
        Cluster(low.system).manager.tell(Leave(address_of(mid).address))
        # low is the leader, so it moves mid to Exiting itself; its MemberRemoved needs mid's
        # confirmation and a later leader tick, so it is still behind these reads
        assert await exited.receive_message() == MemberExited(Cluster(mid.system).self_member)
        # ReadFrom keeps the exiting node as a target, and reads its value
        assert isinstance(await get(low, k1, Replicator.ReadFrom(3, 3.0)), Replicator.GetSuccess)
        # ReadAll leaves it out, so it reads only the third node, which lacks the key
        assert await get(low, k2, Replicator.ReadAll(3.0)) == Replicator.NotFound(k2)
