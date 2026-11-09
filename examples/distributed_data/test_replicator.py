"""The Replicator across two nodes, and a key that expires."""

from ohra import Address, Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    Replicator,
    ReplicatorSettings,
)
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

two_nodes = MultiNodeConfig()
first = two_nodes.role("first")
second = two_nodes.role("second")
two_nodes.common_config(cluster=ClusterSettings())

VISITS = GCounterKey("visits")


@multi_node_spec(two_nodes)
async def test_a_majority_write_reaches_the_other_node(spec: MultiNodeSpec):
    replicator = DistributedData(spec.system).replicator
    node = DistributedData(spec.system).self_unique_address

    Cluster(spec.system).manager.tell(Join(Address.parse(spec.node(first))))
    count: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()

    async def both_are_replicas() -> None:
        replicator.tell(Replicator.GetReplicaCount(count.ref))
        assert await count.receive_message() == Replicator.ReplicaCount(2)

    await spec.await_assert(both_are_replicas, max=10.0)
    changes: TestProbe[Replicator.SubscribeResponse[GCounter]] = spec.create_test_probe()
    replicator.tell(Replicator.Subscribe(VISITS, changes.ref))
    await spec.enter_barrier("replicas")

    if spec.is_node(first):
        updated: TestProbe[Replicator.UpdateResponse[GCounter]] = spec.create_test_probe()
        replicator.tell(
            Replicator.Update(
                VISITS,
                GCounter(),
                Replicator.WriteMajority(timeout=3.0),
                updated.ref,
                lambda c: c.increment(node, 1),
            )
        )
        await updated.expect_message(Replicator.UpdateSuccess(VISITS))
    await spec.enter_barrier("written")

    if spec.is_node(second):
        got: TestProbe[Replicator.GetResponse[GCounter]] = spec.create_test_probe()
        replicator.tell(Replicator.Get(VISITS, Replicator.ReadLocal(), got.ref))
        reply = await got.receive_message()
        assert isinstance(reply, Replicator.GetSuccess)
        assert reply.get(VISITS).value == 1
    change = await changes.receive_message()
    assert isinstance(change, Replicator.Changed)
    assert change.get(VISITS).value == 1
    await spec.enter_barrier("read")

    if spec.is_node(second):
        deleted: TestProbe[Replicator.DeleteResponse[GCounter]] = spec.create_test_probe()
        replicator.tell(Replicator.Delete(VISITS, Replicator.WriteAll(timeout=3.0), deleted.ref))
        await deleted.expect_message(Replicator.DeleteSuccess(VISITS))
    await changes.expect_message(Replicator.Deleted(VISITS))
    await spec.enter_barrier("deleted")


one_node = MultiNodeConfig()
one_node.role("node")
one_node.common_config(
    cluster=ClusterSettings(
        distributed_data=ReplicatorSettings(
            gossip_interval=0.2, expire_keys_after_inactivity={"session-*": 1.0}
        )
    )
)

SESSION = GCounterKey("session-42")


@multi_node_spec(one_node)
async def test_an_untouched_key_expires(spec: MultiNodeSpec):
    replicator = DistributedData(spec.system).replicator
    node = DistributedData(spec.system).self_unique_address

    updated: TestProbe[Replicator.UpdateResponse[GCounter]] = spec.create_test_probe()
    replicator.tell(
        Replicator.Update(
            SESSION, GCounter(), Replicator.WriteLocal(), updated.ref, lambda c: c.increment(node)
        )
    )
    await updated.expect_message(Replicator.UpdateSuccess(SESSION))

    changes: TestProbe[Replicator.SubscribeResponse[GCounter]] = spec.create_test_probe()
    replicator.tell(Replicator.Subscribe(SESSION, changes.ref))
    await changes.expect_message(Replicator.Changed(SESSION, GCounter().increment(node)))
    await changes.expect_message(Replicator.Expired(SESSION), 3.0)

    got: TestProbe[Replicator.GetResponse[GCounter]] = spec.create_test_probe()
    replicator.tell(Replicator.Get(SESSION, Replicator.ReadLocal(), got.ref))
    await got.expect_message(Replicator.NotFound(SESSION))
