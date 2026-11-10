"""The Replicator side of Pekko's delta-CRDT, on one node under ManualTime: Update records a delta
and stamps the version, the tick propagates it, and an incoming DeltaPropagation is applied in
sequence (``Replicator.scala`` 1660-1760, 1885-1912, 2062-2136). The matching specs are
``ReplicatorDeltaSpec`` and ``ReplicatorSpec``, whose multi-node cases the next files port.

A peer is a second system with a forwarder at the Replicator's path, so the Replicator's sends
reach a probe; the Replicator learns of it from a ``MemberUp``, told to it as the cluster would.
Incoming DeltaPropagation, Read and Gossip are sent as another node's Replicator sends them.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any, cast

from ohra import Behaviors
from ohra.cluster.core import MemberRemoved, MemberUp
from ohra.cluster.ddata import (
    DurableStore,
    GCounter,
    GCounterKey,
    ORSet,
    ORSetKey,
    Replicator,
    ReplicatorSettings,
    SelfUniqueAddress,
)
from ohra.cluster.ddata.envelope import (
    DataEnvelope,
    Delta,
    DeltaNack,
    DeltaPropagation,
    Gossip,
    Read,
    ReadResult,
    Status,
    Write,
    WriteAck,
    WriteNack,
)
from ohra.cluster.ddata.pruning_state import PruningInitialized
from ohra.cluster.ddata.replicated_data import ReplicatedData, RequiresCausalDeliveryOfDeltas
from ohra.cluster.ddata.replicator import _ClusterEvent  # pyright: ignore[reportPrivateUsage]
from ohra.cluster.ddata.version_vector import VersionVector
from ohra.cluster.gossip import Member
from ohra.ref import ActorRef
from ohra.remote import UniqueAddress
from ohra.system import ActorSystem
from ohra.testkit import ManualTime, TestProbe

from ...util import free_port
from .test_replicator_durable import with_store
from .test_replicator_local import Node, update_counter
from .test_replicator_subscriptions import manual_node

GOSSIP = 1.0
TICK = max(GOSSIP / 5, 0.2)
SETTINGS = ReplicatorSettings(gossip_interval=GOSSIP, notify_subscribers_interval=0.1)
KEY = ORSetKey[str]("k")
COUNTER = GCounterKey("counter")

other = UniqueAddress("localhost", 2, 98, "Sys")


def versions(*pairs: tuple[UniqueAddress, int]) -> VersionVector:
    return VersionVector.create(dict(pairs))


def sa(node: UniqueAddress) -> SelfUniqueAddress:
    return SelfUniqueAddress(node)


def grown(base: ORSet[str], node: UniqueAddress, element: str) -> ORSet[str]:
    """``base`` with ``element`` added by ``node``: its delta is that one add."""
    return base.reset_delta().add(sa(node), element)


def delta_of(base: ORSet[str], node: UniqueAddress, element: str) -> ReplicatedData:
    delta = grown(base, node, element).delta
    assert delta is not None
    return delta


def wire(node: Node) -> ActorRef[Any]:
    """The Replicator, as another node's Replicator sends to it."""
    return cast(ActorRef[Any], node.replicator)


async def write(node: Node, key_id: str, envelope: DataEnvelope) -> None:
    probe: TestProbe[WriteAck | WriteNack] = node.probe()
    wire(node).tell(Write(key_id, envelope, None, probe.ref))
    assert await probe.receive_message() == WriteAck()


async def read(node: Node, key_id: str) -> DataEnvelope | None:
    probe: TestProbe[ReadResult] = node.probe()
    wire(node).tell(Read(key_id, None, probe.ref))
    return (await probe.receive_message()).envelope


async def propagate(
    node: Node, from_node: UniqueAddress, deltas: dict[str, Delta], reply: bool = True
) -> TestProbe[WriteAck | WriteNack | DeltaNack]:
    probe: TestProbe[WriteAck | WriteNack | DeltaNack] = node.probe()
    wire(node).tell(DeltaPropagation(from_node, reply, deltas, probe.ref))
    return probe


async def add_to_set(node: Node, element: str, key: ORSetKey[str] = KEY) -> None:
    probe: TestProbe[Replicator.UpdateResponse[ORSet[str]]] = node.probe()
    node.replicator.tell(
        Replicator.Update(
            key,
            ORSet[str](),
            Replicator.WriteLocal(),
            probe.ref,
            lambda s: s.add(node.self_address, element),
        )
    )
    await probe.expect_message(Replicator.UpdateSuccess(key))


async def elements(node: Node, key: ORSetKey[str] = KEY) -> frozenset[str] | None:
    probe: TestProbe[Replicator.GetResponse[ORSet[str]]] = node.probe()
    node.replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), probe.ref))
    response = await probe.receive_message()
    match response:
        case Replicator.GetSuccess():
            return response.get(key).elements
        case Replicator.NotFound():
            return None
        case _:
            raise AssertionError(response)


@dataclass
class Peer:
    node: Node
    manual: ManualTime
    address: UniqueAddress
    inbox: TestProbe[Any]


@asynccontextmanager
async def with_peer(settings: ReplicatorSettings = SETTINGS) -> AsyncGenerator[Peer]:
    async with manual_node(settings) as (node, manual):
        port = free_port()
        async with ActorSystem(Behaviors.ignore, "ddata", address=("127.0.0.1", port)) as system:
            inbox: TestProbe[Any] = node.probe()

            def forward(msg: Any) -> Any:
                inbox.ref.tell(msg)
                return Behaviors.same

            system._system_root.spawn_child(  # pyright: ignore[reportPrivateUsage]
                Behaviors.receive_message(forward), "ddataReplicator", None
            )
            address = UniqueAddress("127.0.0.1", port, 7, "ddata")
            member = Member(address, 1, "up")
            wire(node).tell(_ClusterEvent(MemberUp(member)))
            replicas: TestProbe[Replicator.ReplicaCount] = node.probe()

            async def knows_the_peer() -> None:
                node.replicator.tell(Replicator.GetReplicaCount(replicas.ref))
                assert await replicas.receive_message() == Replicator.ReplicaCount(2)

            await replicas.await_assert(knows_the_peer, max=2.0, interval=0.01)
            yield Peer(node, manual, address, inbox)


# Update (1660-1760, setData 1885-1912)


async def test_an_update_stores_the_value_without_its_delta():
    async with manual_node(SETTINGS) as (node, _):
        me = node.self_address.unique_address
        await add_to_set(node, "a")
        envelope = await read(node, KEY.id)
        assert envelope is not None
        assert cast(ORSet[str], envelope.data).delta is None
        assert envelope.delta_versions.version_at(me) == 1

        await add_to_set(node, "b")
        envelope = await read(node, KEY.id)
        assert envelope is not None
        assert envelope.delta_versions.version_at(me) == 2


async def test_the_tick_sends_a_delta_propagation():
    async with with_peer() as p:
        wire(p.node).tell(Replicator.TestFullStateGossip(False))
        me = p.node.self_address.unique_address
        await add_to_set(p.node, "a")
        await p.manual.time_passes(TICK)

        msg = await p.inbox.receive_message()
        assert isinstance(msg, DeltaPropagation)
        assert msg.from_node == me and msg.reply is False and msg.reply_to is None
        assert list(msg.deltas) == [KEY.id]
        delta = msg.deltas[KEY.id]
        assert (delta.from_seq_nr, delta.to_seq_nr) == (1, 1)
        # the ORSet delta op: applied to the zero it gives the one element
        assert ORSet[str]().merge_delta(cast(Any, delta.envelope.data)).elements == {"a"}


async def test_the_next_tick_sends_only_what_the_peer_has_not_been_sent():
    async with with_peer() as p:
        wire(p.node).tell(Replicator.TestFullStateGossip(False))
        await add_to_set(p.node, "a")
        await p.manual.time_passes(TICK)
        await p.inbox.expect_message_type(DeltaPropagation)
        await p.manual.time_passes(TICK)
        await p.inbox.expect_no_message(0.05)

        await add_to_set(p.node, "b")
        await p.manual.time_passes(TICK)
        msg = await p.inbox.expect_message_type(DeltaPropagation)
        assert (msg.deltas[KEY.id].from_seq_nr, msg.deltas[KEY.id].to_seq_nr) == (2, 2)


async def test_delete_clears_the_selector():
    async with with_peer() as p:
        wire(p.node).tell(Replicator.TestFullStateGossip(False))
        await add_to_set(p.node, "gone", ORSetKey[str]("gone"))
        deleted: TestProbe[Replicator.DeleteResponse[ORSet[str]]] = p.node.probe()
        p.node.replicator.tell(
            Replicator.Delete(ORSetKey[str]("gone"), Replicator.WriteLocal(), deleted.ref)
        )
        await deleted.receive_message()
        await add_to_set(p.node, "a")
        await p.manual.time_passes(TICK)

        msg = await p.inbox.expect_message_type(DeltaPropagation)
        assert list(msg.deltas) == [KEY.id]


async def test_expiry_clears_the_selector():
    settings = ReplicatorSettings(
        gossip_interval=0.5,
        notify_subscribers_interval=0.1,
        expire_keys_after_inactivity={"exp*": 0.3},
    )
    async with with_peer(settings) as p:
        wire(p.node).tell(Replicator.TestFullStateGossip(False))
        await p.manual.time_passes(0.45)  # the delta ticks at 0.2 and 0.4 had nothing to send
        await add_to_set(p.node, "x", ORSetKey[str]("exp1"))
        await add_to_set(p.node, "a")
        await p.manual.time_passes(
            0.15
        )  # the gossip tick at 0.5 expires exp1, then the delta tick

        msg = await p.inbox.expect_message_type(DeltaPropagation)
        assert list(msg.deltas) == [KEY.id]


# receive_delta_propagation (2074-2136)


async def seeded(node: Node, peer: UniqueAddress, seen: int) -> ORSet[str]:
    """The key holds "a" and "b" from ``peer``, whose deltas up to ``seen`` are applied."""
    base = ORSet[str]().add(sa(peer), "a").add(sa(peer), "b").reset_delta()
    await write(node, KEY.id, DataEnvelope(base, delta_versions=versions((peer, seen))))
    return base


async def test_a_delta_already_handled_is_acked():
    async with manual_node(SETTINGS) as (node, _):
        base = await seeded(node, other, 2)
        delta = Delta(DataEnvelope(delta_of(base, other, "c")), 2, 2)
        replies = await propagate(node, other, {KEY.id: delta})
        assert await replies.receive_message() == WriteAck()
        assert await elements(node) == {"a", "b"}


async def test_a_delta_with_a_gap_is_nacked():
    async with manual_node(SETTINGS) as (node, _):
        base = await seeded(node, other, 2)
        delta = Delta(DataEnvelope(delta_of(base, other, "c")), 4, 4)
        replies = await propagate(node, other, {KEY.id: delta})
        assert await replies.receive_message() == DeltaNack()
        assert await elements(node) == {"a", "b"}


async def test_a_delta_in_sequence_is_applied():
    async with manual_node(SETTINGS) as (node, _):
        base = await seeded(node, other, 2)
        delta = Delta(DataEnvelope(delta_of(base, other, "c")), 3, 3)
        replies = await propagate(node, other, {KEY.id: delta})
        assert await replies.receive_message() == WriteAck()
        assert await elements(node) == {"a", "b", "c"}
        envelope = await read(node, KEY.id)
        assert envelope is not None and envelope.delta_versions.version_at(other) == 3


async def test_a_non_causal_delta_is_applied_as_is():
    async with manual_node(SETTINGS) as (node, _):
        counter_delta = GCounter().increment(sa(other), 1).delta
        assert counter_delta is not None
        replies = await propagate(
            node, other, {COUNTER.id: Delta(DataEnvelope(counter_delta), 5, 9)}
        )
        assert await replies.receive_message() == WriteAck()
        probe: TestProbe[Replicator.GetResponse[GCounter]] = node.probe()
        node.replicator.tell(Replicator.Get(COUNTER, Replicator.ReadLocal(), probe.ref))
        response = await probe.receive_message()
        assert isinstance(response, Replicator.GetSuccess) and response.get(COUNTER).value == 1


async def test_a_delta_to_an_unknown_key_starts_from_zero():
    async with manual_node(SETTINGS) as (node, _):
        delta = Delta(DataEnvelope(delta_of(ORSet[str](), other, "a")), 1, 1)
        replies = await propagate(node, other, {KEY.id: delta})
        assert await replies.receive_message() == WriteAck()
        assert await elements(node) == {"a"}
        envelope = await read(node, KEY.id)
        assert envelope is not None and envelope.delta_versions.version_at(other) == 1


async def test_a_delta_to_a_deleted_key_stays_deleted():
    async with manual_node(SETTINGS) as (node, _):
        await add_to_set(node, "a")
        deleted: TestProbe[Replicator.DeleteResponse[ORSet[str]]] = node.probe()
        node.replicator.tell(Replicator.Delete(KEY, Replicator.WriteLocal(), deleted.ref))
        await deleted.receive_message()

        delta = Delta(DataEnvelope(delta_of(ORSet[str](), other, "b")), 1, 1)
        replies = await propagate(node, other, {KEY.id: delta})
        assert await replies.receive_message() == WriteAck()
        probe: TestProbe[Replicator.GetResponse[ORSet[str]]] = node.probe()
        node.replicator.tell(Replicator.Get(KEY, Replicator.ReadLocal(), probe.ref))
        assert isinstance(await probe.receive_message(), Replicator.GetDataDeleted)


async def test_a_new_incarnation_starts_its_sequence_again():
    async with manual_node(SETTINGS) as (node, _):
        old = UniqueAddress("localhost", 2, 98, "Sys")
        new = UniqueAddress("localhost", 2, 99, "Sys")
        base = await seeded(node, old, 7)
        delta = Delta(DataEnvelope(delta_of(base, new, "c")), 1, 1)
        replies = await propagate(node, new, {KEY.id: delta})
        assert await replies.receive_message() == WriteAck()
        assert await elements(node) == {"a", "b", "c"}


async def test_deltas_from_a_removed_node_are_dropped():
    async with manual_node(SETTINGS) as (node, _):
        base = await seeded(node, other, 2)
        gone = UniqueAddress("localhost", 3, 97, "Sys")
        member = Member(gone, 1, "removed")
        wire(node).tell(_ClusterEvent(MemberRemoved(member, "exiting")))
        delta = Delta(DataEnvelope(delta_of(base, gone, "c")), 1, 1)
        replies = await propagate(node, gone, {KEY.id: delta})
        await replies.expect_no_message(0.1)
        assert await elements(node) == {"a", "b"}


async def test_deltas_from_a_node_named_by_pruning_are_dropped():
    async with manual_node(SETTINGS) as (node, _):
        base = ORSet[str]().add(sa(other), "a").reset_delta()
        named = UniqueAddress("localhost", 3, 97, "Sys")
        pruning = {named: PruningInitialized(other, frozenset())}
        await write(node, KEY.id, DataEnvelope(base, pruning=pruning))
        delta = Delta(DataEnvelope(delta_of(base, named, "c")), 1, 1)
        replies = await propagate(node, named, {KEY.id: delta})
        await replies.expect_no_message(0.1)
        assert await elements(node) == {"a"}


async def test_with_deltas_off_a_propagation_is_nacked():
    settings = replace(SETTINGS, delta_crdt_enabled=False)
    async with manual_node(settings) as (node, _):
        delta = Delta(DataEnvelope(delta_of(ORSet[str](), other, "a")), 1, 1)
        replies = await propagate(node, other, {KEY.id: delta})
        assert await replies.receive_message() == DeltaNack()
        silent = await propagate(node, other, {KEY.id: delta}, reply=False)
        await silent.expect_no_message(0.05)
        assert await elements(node) is None

        # an Update stores the full value with its delta, as before
        await add_to_set(node, "a")
        envelope = await read(node, KEY.id)
        assert envelope is not None
        assert cast(ORSet[str], envelope.data).delta is not None
        assert envelope.delta_versions.is_empty


async def test_with_deltas_off_no_tick_runs():
    settings = replace(SETTINGS, delta_crdt_enabled=False)
    async with with_peer(settings) as p:
        wire(p.node).tell(Replicator.TestFullStateGossip(False))
        await add_to_set(p.node, "a")
        await p.manual.time_passes(GOSSIP - 0.01)
        await p.inbox.expect_no_message(0.05)


@dataclass(frozen=True)
class Broken(RequiresCausalDeliveryOfDeltas):
    def merge(self, that: "Broken") -> "Broken":
        return self

    def zero(self) -> Any:
        raise RuntimeError("no zero")


async def test_a_propagation_that_fails_is_logged_and_dropped(
    caplog: Any,
):
    async with manual_node(SETTINGS) as (node, _):
        await add_to_set(node, "a")
        with caplog.at_level(logging.WARNING):
            replies = await propagate(node, other, {KEY.id: Delta(DataEnvelope(Broken()), 1, 1)})
            await replies.expect_no_message(0.1)
        assert "Couldn't process DeltaPropagation from" in caplog.text
        assert await elements(node) == {"a"}


# the digest and the subscribers (1885-1935)


async def test_a_subscriber_gets_no_changed_for_delta_versions_alone():
    async with manual_node(SETTINGS) as (node, manual):
        subscriber: TestProbe[Replicator.SubscribeResponse[ORSet[str]]] = node.probe()
        node.replicator.tell(Replicator.Subscribe(KEY, subscriber.ref))
        await add_to_set(node, "a")
        await manual.time_passes(0.1)
        assert isinstance(await subscriber.receive_message(), Replicator.Changed)

        envelope = await read(node, KEY.id)
        assert envelope is not None
        bumped = replace(envelope, delta_versions=versions((other, 5)))
        uid = node.self_address.unique_address.uid
        gossip_back: TestProbe[Status | Gossip] = node.probe()
        wire(node).tell(Gossip({KEY.id: (bumped, 0.0)}, False, uid, 99, gossip_back.ref))
        await manual.time_passes(0.1)
        await subscriber.expect_no_message(0.1)
        after = await read(node, KEY.id)
        assert after is not None and after.delta_versions.version_at(other) == 5


# the gossip tick (2138-2141)


async def test_full_state_gossip_can_be_turned_off():
    async with with_peer() as p:
        await p.manual.time_passes(GOSSIP)
        assert isinstance(await p.inbox.receive_message(), Status)

        wire(p.node).tell(Replicator.TestFullStateGossip(False))
        await p.manual.time_passes(2 * GOSSIP)
        await p.inbox.expect_no_message(0.05)


# load (1489-1545)


async def test_the_load_state_stashes_delta_propagations():
    async with with_store() as (node, store):
        load = await store.receive_message()
        assert isinstance(load, DurableStore.LoadAll)
        delta = Delta(DataEnvelope(delta_of(ORSet[str](), other, "a")), 1, 1)
        replies = await propagate(node, other, {KEY.id: delta})
        await replies.expect_no_message(0.1)

        load.reply_to.tell(DurableStore.LoadAllCompleted())
        assert await replies.receive_message() == WriteAck()
        assert await elements(node) == {"a"}


async def test_counter_updates_still_work_with_deltas_on():
    async with manual_node(SETTINGS) as (node, _):
        await update_counter(node, COUNTER, 2)
        envelope = await read(node, COUNTER.id)
        assert envelope is not None
        assert envelope.delta_versions.version_at(node.self_address.unique_address) == 1
