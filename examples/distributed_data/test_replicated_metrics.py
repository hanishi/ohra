"""Pekko's ReplicatedMetricsSpec (pekko-sample-distributed-data-scala), on three cluster nodes.

``test_join_cluster``, ``test_replicate_metrics`` and ``test_cleanup_removed_node`` are Pekko's
three cases. ``test_the_percent_is_used_over_max`` and ``test_memory_usage_measures_this_process``
are ohra's own: the first pins the percent a node publishes, the second the units of
``memory_usage``.
"""

from replicated_metrics import UsedHeap, node_key, memory_usage, replicated_metrics
from ohra import Address, Cluster, ClusterSettings, EventStream, Join, Leave
from ohra.cluster.ddata import DistributedData, Replicator
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

config = MultiNodeConfig()
node1 = config.role("node-1")
node2 = config.role("node-2")
node3 = config.role("node-3")
config.common_config(cluster=ClusterSettings())

one_node = MultiNodeConfig()
one_node.role("node")
one_node.common_config(cluster=ClusterSettings())


async def join_cluster(spec: MultiNodeSpec) -> None:
    for src, dst in ((node1, node1), (node2, node1), (node3, node1)):
        if spec.is_node(src):
            Cluster(spec.system).manager.tell(Join(Address.parse(spec.node(dst))))
        await spec.enter_barrier(f"{src.name}-joined")

    replicator = DistributedData(spec.system).replicator
    probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()

    async def all_are_replicas() -> None:
        replicator.tell(Replicator.GetReplicaCount(probe.ref))
        assert await probe.receive_message() == Replicator.ReplicaCount(3)

    await spec.await_assert(all_are_replicas, max=20.0)
    await spec.enter_barrier("after-1")


def used_heap_probe(spec: MultiNodeSpec) -> TestProbe[UsedHeap]:
    probe: TestProbe[UsedHeap] = spec.create_test_probe()
    spec.system.event_stream.tell(EventStream.Subscribe(UsedHeap, probe.ref))
    return probe


async def replicate_metrics(spec: MultiNodeSpec) -> None:
    probe = used_heap_probe(spec)

    async def has_three_nodes() -> None:
        assert len((await probe.expect_message_type(UsedHeap, 1.0)).percent_per_node) == 3

    await spec.await_assert(has_three_nodes, max=30.0)
    for _ in range(2):
        assert len((await probe.expect_message_type(UsedHeap)).percent_per_node) == 3
    await spec.enter_barrier("after-2")


@multi_node_spec(config)
async def test_join_cluster(spec: MultiNodeSpec):
    spec.spawn(replicated_metrics(1.0, 3.0))
    await join_cluster(spec)


@multi_node_spec(config)
async def test_replicate_metrics(spec: MultiNodeSpec):
    spec.spawn(replicated_metrics(1.0, 3.0))
    await join_cluster(spec)
    await replicate_metrics(spec)


@multi_node_spec(config)
async def test_cleanup_removed_node(spec: MultiNodeSpec):
    spec.spawn(replicated_metrics(1.0, 3.0))
    await join_cluster(spec)
    await replicate_metrics(spec)
    node3_address = Address.parse(spec.node(node3))
    if spec.is_node(node1):
        # node 3's system terminates once it is removed, and its conductor client goes with it,
        # which fails a barrier that another node has entered ("ClientLost"). So the conductor
        # takes it out of the barriers before it leaves.
        await spec.test_conductor.remove_node(node3)
        Cluster(spec.system).manager.tell(Leave(node3_address))
    if spec.is_node(node1, node2):
        probe = used_heap_probe(spec)

        async def has_two_nodes() -> None:
            assert len((await probe.expect_message_type(UsedHeap, 1.0)).percent_per_node) == 2

        await spec.await_assert(has_two_nodes, max=30.0)
        next_heap = await probe.expect_message_type(UsedHeap)
        assert node_key(node3_address) not in next_heap.percent_per_node
        await spec.enter_barrier("after-3")
    else:
        await spec.system.when_terminated()


@multi_node_spec(one_node)
async def test_the_percent_is_used_over_max(spec: MultiNodeSpec):
    spec.spawn(replicated_metrics(0.1, 3.0, measure=lambda: (25, 200)))
    Cluster(spec.system).manager.tell(Join(Cluster(spec.system).self_address))
    probe = used_heap_probe(spec)
    me = node_key(Cluster(spec.system).self_address)

    async def is_a_eighth() -> None:
        assert (await probe.expect_message_type(UsedHeap, 1.0)).percent_per_node == {me: 12.5}

    await spec.await_assert(is_a_eighth, max=10.0)


def test_memory_usage_measures_this_process():
    used, max_ = memory_usage()
    assert 0 < used < max_
