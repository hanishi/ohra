"""Nodes with deltas on and off in one cluster still converge: Pekko's rolling-upgrade case, where
a node without deltas answers ``DeltaNack`` and the others fall back to the full state. No Pekko
spec covers it alone.

Two real systems as in ``test_replicator_pruning_nodes.py``. Node A sends deltas; B has
``delta_crdt_enabled=False`` and applies none. A value written on either side reaches the other
through ``Write`` (A to B, as B takes it) and gossip (B to A).
"""

from dataclasses import replace

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import DistributedData, ORSet, ORSetKey, Replicator, ReplicatorSettings
from ohra.remote import Address
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

from ..test_cluster import FAST, FAST_SHUTDOWN

ON = ReplicatorSettings(gossip_interval=0.1)
config = MultiNodeConfig()
first, second = config.role("first"), config.role("second")
config.common_config(cluster=ClusterSettings(**FAST, distributed_data=ON), shutdown=FAST_SHUTDOWN)
config.node_config(
    second,
    cluster=ClusterSettings(**FAST, distributed_data=replace(ON, delta_crdt_enabled=False)),
)
config.test_transport(on=True)

KEY = ORSetKey[str]("set")


async def elements(spec: MultiNodeSpec) -> frozenset[str] | None:
    probe: TestProbe[Replicator.GetResponse[ORSet[str]]] = spec.create_test_probe()
    DistributedData(spec.system).replicator.tell(
        Replicator.Get(KEY, Replicator.ReadLocal(), probe.ref)
    )
    response = await probe.receive_message()
    return response.get(KEY).elements if isinstance(response, Replicator.GetSuccess) else None


async def add(spec: MultiNodeSpec, element: str) -> None:
    dd = DistributedData(spec.system)
    probe: TestProbe[Replicator.UpdateResponse[ORSet[str]]] = spec.create_test_probe()
    dd.replicator.tell(
        Replicator.Update(
            KEY,
            ORSet[str](),
            Replicator.WriteLocal(),
            probe.ref,
            lambda s: s.add(dd.self_unique_address, element),
        )
    )
    await probe.expect_message(Replicator.UpdateSuccess(KEY))


@multi_node_spec(config)
async def test_nodes_with_deltas_on_and_off_converge(spec: MultiNodeSpec) -> None:
    Cluster(spec.system).manager.tell(Join(Address.parse(spec.node(first))))
    replicas: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()

    async def both() -> None:
        DistributedData(spec.system).replicator.tell(Replicator.GetReplicaCount(replicas.ref))
        assert await replicas.receive_message() == Replicator.ReplicaCount(2)

    await spec.await_assert(both, max=10.0, interval=0.05)
    await spec.enter_barrier("joined")

    if spec.is_node(first):
        await add(spec, "from-a")
    await spec.enter_barrier("a-wrote")

    async def sees(expected: frozenset[str]) -> None:
        assert await elements(spec) == expected

    await spec.await_assert(lambda: sees(frozenset({"from-a"})), max=10.0, interval=0.05)
    await spec.enter_barrier("a-seen")

    if spec.is_node(second):
        await add(spec, "from-b")
    await spec.await_assert(lambda: sees(frozenset({"from-a", "from-b"})), max=10.0, interval=0.05)
    await spec.enter_barrier("done")
