"""The Replicator's digests (spec D5): SHA-1 of a canonical encoding, with every set's elements and
every map's entries sorted, so equal data has equal digests whatever order built it. Pekko's
protobuf serializer sorts for the same reason. The spec's B5.
"""

from __future__ import annotations

from typing import Any

from ohra.cluster import Cluster, ClusterSettings
from ohra.cluster.ddata import (
    DistributedData,
    ORSet,
    ORSetKey,
    Replicator,
    ReplicatorSettings,
    SelfUniqueAddress,
)
from ohra.cluster.ddata import replicator as replicator_module
from ohra.cluster.ddata.envelope import DataEnvelope, Gossip, Status
from ohra.cluster.ddata.gset import GSet
from ohra.cluster.ddata.ormap import ORMap
from ohra.ref import ActorRef
from ohra.remote import Address, UniqueAddress
from ohra.remote.serialization import Serialization
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

from ..test_cluster import FAST, FAST_SHUTDOWN
from .test_replicator_gossip import join

node1 = SelfUniqueAddress(UniqueAddress("localhost", 7354, 1, "Sys"))
node2 = SelfUniqueAddress(UniqueAddress("localhost", 7355, 2, "Sys"))


def ref_path(ref: ActorRef[Any]) -> str:
    return ref.path


def digest(data: Any) -> str:
    return replicator_module._digest(DataEnvelope(data), ref_path)[0]  # pyright: ignore[reportPrivateUsage]


def test_equal_orsets_built_in_different_orders_have_equal_digests():
    s1 = ORSet[str].empty.add(node1, "a").add(node1, "b")
    s2 = ORSet[str].empty.add(node2, "b").add(node2, "a")
    m1, m2 = s1.merge(s2), s2.merge(s1)
    assert m1 == m2
    assert digest(m1) == digest(m2)


def test_equal_gsets_built_in_different_orders_have_equal_digests():
    # 0 and 8 collide in a small set's table, so the frozenset iterates in insertion order
    g1, g2 = GSet(frozenset([0, 8])), GSet(frozenset([8, 0]))
    assert g1 == g2 and list(g1.elements) != list(g2.elements)
    assert digest(g1) == digest(g2)


def test_equal_ormaps_from_different_histories_have_equal_digests():
    x = ORMap[str, GSet[int]].empty.put(node1, "a", GSet(frozenset([0, 8])))
    y = ORMap[str, GSet[int]].empty.put(node2, "b", GSet(frozenset([8, 0])))
    m1, m2 = x.merge(y), y.merge(x)
    assert m1 == m2
    assert digest(m1) == digest(m2)


def test_different_data_has_different_digests():
    assert digest(GSet(frozenset([0, 8]))) != digest(GSet(frozenset([0, 9])))


def test_default_encoding_is_unchanged():
    """Without ``canonical``, sets and maps keep their iteration order, as on the wire."""
    address = {
        "$type": "UniqueAddress",
        "host": "localhost",
        "port": 7355,
        "uid": 2,
        "name": "Sys",
    }
    value = {node2.unique_address: GSet(frozenset([8, 0]))}
    expected = {"$map": [[address, {"$type": "GSet", "elements": [8, 0]}]]}
    assert Serialization.encode(value, ref_path) == expected
    assert Serialization.encode(value, ref_path, canonical=False) == expected


def two_nodes() -> MultiNodeConfig:
    config = MultiNodeConfig()
    for name in ("first", "second"):
        config.role(name)
    config.common_config(
        cluster=ClusterSettings(
            **FAST,
            distributed_data=ReplicatorSettings(
                gossip_interval=0.1, notify_subscribers_interval=60.0
            ),
        ),
        shutdown=FAST_SHUTDOWN,
    )
    return config


config = two_nodes()
first, second = config.roles
KeyS: ORSetKey[str] = ORSetKey("S")


@multi_node_spec(config)
async def test_converged_key_stops_being_gossiped(spec: MultiNodeSpec):
    replicator = await join(spec, 2)
    await spec.enter_barrier("after-join")

    # one Update each, so a node that holds the other's version holds all of its state
    me = DistributedData(spec.system).self_unique_address
    order = ("a", "b") if spec.is_node(first) else ("b", "a")
    updated: TestProbe[Replicator.UpdateResponse[ORSet[str]]] = spec.create_test_probe()
    replicator.tell(
        Replicator.Update(
            KeyS,
            ORSet[str].empty,
            Replicator.WriteLocal(),
            updated.ref,
            lambda s: s.add(me, order[0]).add(me, order[1]),
        )
    )
    await updated.expect_message(Replicator.UpdateSuccess(KeyS))

    got: TestProbe[Replicator.GetResponse[ORSet[str]]] = spec.create_test_probe()

    async def merged() -> ORSet[str]:
        replicator.tell(Replicator.Get(KeyS, Replicator.ReadLocal(), got.ref))
        reply = await got.receive_message()
        assert isinstance(reply, Replicator.GetSuccess), reply
        return reply.data

    async def holds_both() -> None:
        assert (await merged()).vvector.size == 2

    await spec.await_assert(holds_both, max=10.0, interval=0.05)  # converged through gossip
    await spec.enter_barrier("converged")

    # a gossip round: this node's Status, with its digest, to the other node's Replicator. "absent"
    # is a key the other lacks, so it answers with a Status asking for it, after any Gossip.
    other = second if spec.is_node(first) else first
    other_address = Address.parse(spec.node(other))
    other_uid = next(
        m.unique_address.uid
        for m in Cluster(spec.system).state.members
        if m.unique_address.address == other_address
    )
    to: ActorRef[Status | Gossip] = spec.system.resolve(
        f"{spec.node(other)}/system/ddataReplicator"
    )
    probe: TestProbe[Status | Gossip] = spec.create_test_probe()
    digests = {KeyS.id: (digest(await merged()), 0.0), "absent": ("x", 0.0)}
    to.tell(Status(digests, 0, 1, other_uid, 4711, probe.ref))
    gossiped: list[str] = []
    while not isinstance(msg := await probe.receive_message(), Status):
        gossiped.extend(msg.updated_data)
    assert KeyS.id not in gossiped
    assert set(msg.digests) == {"absent"}
    await spec.enter_barrier("after-b5")
