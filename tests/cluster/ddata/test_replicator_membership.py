"""The Replicator's view of the cluster, on three nodes in one process: Pekko's
receiveMemberUp, receiveMemberExiting and receiveMemberRemoved, and nodesForReadWrite.

No Pekko spec covers these alone. The read and write targets are read through the private
``_NodesForReadWrite`` query, since no reply shows them before the aggregators come."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, cast

from ohra.cluster import Cluster, ClusterDomainEvent, Leave, MemberExited, Subscribe
from ohra.cluster.ddata import DistributedData, Replicator, ReplicatorSettings
from ohra.cluster.ddata.replicator import _NodesForReadWrite  # pyright: ignore[reportPrivateUsage]
from ohra.ref import ActorRef
from ohra.remote import UniqueAddress
from ohra.testkit import TestProbe

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from .test_replicator_local import Node


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


async def nodes_for_read_write(node: Node, exclude_exiting: bool) -> tuple[UniqueAddress, ...]:
    probe: TestProbe[tuple[UniqueAddress, ...]] = node.probe()
    cast(ActorRef[Any], node.replicator).tell(_NodesForReadWrite(exclude_exiting, probe.ref))
    return await probe.receive_message()


async def replica_count_is(node: Node, n: int) -> None:
    assert await replica_count(node) == n


async def targets_are(
    node: Node, exclude_exiting: bool, expected: tuple[UniqueAddress, ...]
) -> None:
    assert await nodes_for_read_write(node, exclude_exiting) == expected


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


async def test_prefer_oldest_orders_targets_by_age():
    async with three_nodes(ReplicatorSettings(prefer_oldest=True)) as nodes:
        low, mid, high = nodes
        expected = (address_of(high), address_of(mid))  # the seed is the oldest
        await low.probe().await_assert(
            lambda: targets_are(low, False, expected), max=3, interval=0.01
        )


async def test_exiting_nodes_are_excluded_from_majority_targets():
    async with three_nodes() as nodes:
        low, mid, high = nodes
        both = (address_of(mid), address_of(high))
        await low.probe().await_assert(lambda: targets_are(low, True, both), max=3, interval=0.01)
        exited: TestProbe[ClusterDomainEvent] = low.probe()
        Cluster(low.system).subscriptions.tell(Subscribe(exited.ref, MemberExited))
        Cluster(low.system).manager.tell(Leave(address_of(mid).address))
        # low is the leader, so it moves mid to Exiting itself; its MemberRemoved needs mid's
        # confirmation and a later leader tick, so it is still behind these queries
        assert await exited.receive_message() == MemberExited(Cluster(mid.system).self_member)
        assert await nodes_for_read_write(low, True) == (address_of(high),)
        assert await nodes_for_read_write(low, False) == both
