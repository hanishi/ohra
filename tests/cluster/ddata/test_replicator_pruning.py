"""The Replicator prunes the data of removed nodes, on one node under ManualTime: the steps of the
"CRDT Garbage" section of Pekko's Replicator Scaladoc, run by its receiveRemovedNodePruningTick.

Data that names a node outside the cluster is placed with a wire ``Write``, as another node's
Replicator would send it, and read back with a wire ``Read``, which shows the pruning markers.
"""

from typing import cast

from ohra.cluster.ddata import (
    Flag,
    GCounter,
    GCounterKey,
    GSet,
    Replicator,
    ReplicatorSettings,
)
from ohra.cluster.ddata.envelope import (
    DELETED_ENVELOPE,
    DataEnvelope,
    Gossip,
    Read,
    ReadResult,
    Status,
    Write,
    WriteAck,
    WriteNack,
)
from ohra.cluster.ddata.pruning_state import PruningInitialized, PruningPerformed, PruningState
from ohra.ref import ActorRef
from ohra.remote import UniqueAddress
from ohra.testkit import TestProbe

from .test_replicator_local import Node
from .test_replicator_subscriptions import manual_node

INTERVAL = 1.0
DISSEMINATION = 3.0
TTL = 10.0
SETTINGS = ReplicatorSettings(
    gossip_interval=INTERVAL,
    pruning_interval=INTERVAL,
    max_pruning_dissemination=DISSEMINATION,
    pruning_marker_time_to_live=TTL,
)
# the dissemination time, then a tick to collect the node and one to initialize and perform
PRUNED_AFTER = DISSEMINATION + 2 * INTERVAL

removed = UniqueAddress("localhost", 1, 99, "Sys")
other = UniqueAddress("localhost", 2, 98, "Sys")
KEY = GCounterKey("counter")


def wire(node: Node) -> ActorRef[Write | Read | Gossip]:
    """The Replicator, as another node's Replicator sends to it."""
    return cast(ActorRef[Write | Read | Gossip], node.replicator)


async def write(node: Node, key_id: str, envelope: DataEnvelope) -> None:
    probe: TestProbe[WriteAck | WriteNack] = node.probe()
    wire(node).tell(Write(key_id, envelope, None, probe.ref))
    assert await probe.receive_message() == WriteAck()


async def read(node: Node, key_id: str) -> DataEnvelope | None:
    probe: TestProbe[ReadResult] = node.probe()
    wire(node).tell(Read(key_id, None, probe.ref))
    return (await probe.receive_message()).envelope


async def counter_value(node: Node) -> int:
    probe: TestProbe[Replicator.GetResponse[GCounter]] = node.probe()
    node.replicator.tell(Replicator.Get(KEY, Replicator.ReadLocal(), probe.ref))
    response = await probe.receive_message()
    assert isinstance(response, Replicator.GetSuccess), response
    return response.get(KEY).value


def counter_from(node: UniqueAddress, self_node: Node, n: int = 3) -> DataEnvelope:
    """3 from ``node`` and 2 from self."""
    return DataEnvelope(
        GCounter()._increment(node, n)._increment(self_node.self_address.unique_address, 2)  # pyright: ignore[reportPrivateUsage]
    )


async def test_data_from_a_removed_node_is_moved_into_self():
    async with manual_node(SETTINGS) as (node, manual):
        await write(node, KEY.id, counter_from(removed, node))
        await manual.time_passes(PRUNED_AFTER)

        assert await counter_value(node) == 5
        envelope = await read(node, KEY.id)
        assert envelope is not None
        assert not envelope.need_pruning_from(removed)
        assert isinstance(envelope.pruning[removed], PruningPerformed)


async def test_the_performed_marker_expires():
    async with manual_node(SETTINGS) as (node, manual):
        await write(node, KEY.id, counter_from(removed, node))
        await manual.time_passes(PRUNED_AFTER)
        envelope = await read(node, KEY.id)
        assert envelope is not None and removed in envelope.pruning

        await manual.time_passes(TTL + INTERVAL)
        envelope = await read(node, KEY.id)
        assert envelope is not None
        assert envelope.pruning == {}
        assert await counter_value(node) == 5


async def test_a_stale_write_after_pruning_is_cleaned():
    async with manual_node(SETTINGS) as (node, manual):
        original = counter_from(removed, node)
        await write(node, KEY.id, original)
        await manual.time_passes(PRUNED_AFTER)

        await write(node, KEY.id, original)
        assert await counter_value(node) == 5
        envelope = await read(node, KEY.id)
        assert envelope is not None
        assert not envelope.need_pruning_from(removed)


async def test_pruning_interval_of_zero_turns_pruning_off():
    settings = ReplicatorSettings(
        gossip_interval=INTERVAL,
        pruning_interval=0.0,
        max_pruning_dissemination=DISSEMINATION,
        pruning_marker_time_to_live=TTL,
    )
    async with manual_node(settings) as (node, manual):
        await write(node, KEY.id, counter_from(removed, node))
        await manual.time_passes(PRUNED_AFTER)

        envelope = await read(node, KEY.id)
        assert envelope is not None
        assert envelope.pruning == {}
        assert envelope.need_pruning_from(removed)


async def test_keys_that_cannot_be_pruned_are_skipped():
    async with manual_node(SETTINGS) as (node, manual):
        flag = DataEnvelope(Flag(enabled=True))
        gset = DataEnvelope(GSet[str]().add("a"))
        await write(node, "flag", flag)
        await write(node, "gset", gset)
        await write(node, "deleted", counter_from(removed, node))
        await write(node, "deleted", DELETED_ENVELOPE)
        await write(node, KEY.id, counter_from(removed, node))
        await manual.time_passes(PRUNED_AFTER)

        envelope = await read(node, KEY.id)
        assert envelope is not None
        assert isinstance(envelope.pruning[removed], PruningPerformed)
        assert await read(node, "flag") == flag
        assert await read(node, "gset") == gset
        assert await read(node, "deleted") == DELETED_ENVELOPE


async def test_an_old_incarnation_of_self_is_pruned():
    async with manual_node(SETTINGS) as (node, manual):
        me = node.self_address.unique_address
        old_self = UniqueAddress(me.host, me.port, me.uid + 1, me.name)
        await write(node, KEY.id, counter_from(old_self, node))
        await manual.time_passes(PRUNED_AFTER)

        assert await counter_value(node) == 5
        envelope = await read(node, KEY.id)
        assert envelope is not None
        assert not envelope.need_pruning_from(old_self)
        assert isinstance(envelope.pruning[old_self], PruningPerformed)
        assert me not in envelope.pruning


async def test_a_marker_owned_by_another_node_is_taken_over():
    async with manual_node(SETTINGS) as (node, manual):
        data = counter_from(removed, node)
        marked = DataEnvelope(data.data, {removed: PruningInitialized(other, frozenset())})
        await write(node, KEY.id, marked)
        await manual.time_passes(PRUNED_AFTER)

        envelope = await read(node, KEY.id)
        assert envelope is not None
        assert isinstance(envelope.pruning[removed], PruningPerformed)
        # the data moved into self, the new owner, and not into the old owner
        assert isinstance(envelope.data, GCounter)
        assert envelope.data.modified_by_nodes == {node.self_address.unique_address}
        assert await counter_value(node) == 5


async def test_a_gossip_send_back_includes_a_new_key_with_pruning():
    async with manual_node(SETTINGS) as (node, _manual):
        uid = node.self_address.unique_address.uid
        probe: TestProbe[Status | Gossip] = node.probe()
        marked = DataEnvelope(
            counter_from(removed, node).data, {removed: PruningInitialized(other, frozenset())}
        )
        wire(node).tell(Gossip({KEY.id: (marked, 0.0)}, True, uid, 4711, probe.ref))

        reply = await probe.receive_message()
        assert isinstance(reply, Gossip)
        assert KEY.id in reply.updated_data
        assert not reply.send_back


async def test_a_write_adds_self_to_seen():
    async with manual_node(SETTINGS) as (node, _manual):
        me = node.self_address.unique_address
        marker: dict[UniqueAddress, PruningState] = {
            removed: PruningInitialized(other, frozenset())
        }
        data = counter_from(removed, node).data
        # a new key, and a merge into a key already held
        await write(node, "existing", DataEnvelope(data))
        for key_id in ("new", "existing"):
            await write(node, key_id, DataEnvelope(data, marker))
            envelope = await read(node, key_id)
            assert envelope is not None
            state = envelope.pruning[removed]
            assert isinstance(state, PruningInitialized)
            assert me.address in state.seen, key_id
