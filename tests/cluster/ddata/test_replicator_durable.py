"""The load phase of Pekko's ``Replicator.load`` (1489-1545), with the durable store replaced by
a probe: the Replicator asks the store for everything, holds its commands until the load is done,
and stops when the store does."""

import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any, cast

from ohra import Behavior, Behaviors
from ohra.cluster.ddata import (
    DurableDataEnvelope,
    DurableStore,
    GCounter,
    GCounterKey,
    Replicator,
    ReplicatorSettings,
)
from ohra.cluster.ddata.envelope import (
    DELETED_ENVELOPE,
    DataEnvelope,
    Gossip,
    Read,
    ReadResult,
    Write,
    WriteAck,
    WriteNack,
)
from ohra.cluster.ddata.pruning_state import PruningPerformed
from ohra.ref import ActorRef
from ohra.remote import UniqueAddress
from ohra.testkit import ActorTestKit, TestProbe

from .test_replicator_local import Node
from .test_replicator_pruning import (
    KEY as PRUNING_KEY,
)
from .test_replicator_pruning import (
    PRUNED_AFTER,
    SETTINGS as PRUNING_SETTINGS,
    TTL,
    counter_from,
    read,
    removed,
)
from .test_replicator_subscriptions import manual_node


def probe_store(probe: TestProbe[Any]) -> Callable[[], Behavior[DurableStore.Command]]:
    """A store that forwards every message to the probe."""

    def factory() -> Behavior[DurableStore.Command]:
        def forward(msg: Any) -> Behavior[Any]:
            probe.ref.tell(msg)
            return Behaviors.same

        return cast(Behavior[DurableStore.Command], Behaviors.receive_message(forward))

    return factory


def stopping_store(probe: TestProbe[Any]) -> Callable[[], Behavior[DurableStore.Command]]:
    """A store that forwards ``LoadAll`` and stops, as one whose load raised ``LoadFailed``."""

    def factory() -> Behavior[DurableStore.Command]:
        def forward(msg: Any) -> Behavior[Any]:
            probe.ref.tell(msg)
            return Behaviors.stopped()

        return cast(Behavior[DurableStore.Command], Behaviors.receive_message(forward))

    return factory


def durable_settings(**kw: Any) -> ReplicatorSettings:
    return replace(ReplicatorSettings(durable_keys=frozenset({"durable*"})), **kw)


@asynccontextmanager
async def with_store(
    store: Callable[[TestProbe[Any]], Callable[[], Behavior[DurableStore.Command]]] = probe_store,
    settings: Callable[..., ReplicatorSettings] = durable_settings,
) -> AsyncGenerator[tuple[Node, TestProbe[Any]]]:
    """A node whose durable store is ``store``, and the probe it forwards to."""
    async with ActorTestKit() as kit:
        store_probe: TestProbe[Any] = kit.create_test_probe()
        async with manual_node(settings(durable_store=store(store_probe))) as (node, _):
            yield node, store_probe


def loader(msg: DurableStore.LoadAll) -> ActorRef[Any]:
    return msg.reply_to


async def test_the_replicator_asks_the_store_to_load_all() -> None:
    async with with_store() as (node, store_probe):
        msg = await store_probe.receive_message()
        assert isinstance(msg, DurableStore.LoadAll)
        assert msg.reply_to == node.replicator


async def test_commands_before_the_load_are_answered_after_it_in_order() -> None:
    async with with_store() as (node, store_probe):
        load = await store_probe.receive_message()
        key = GCounterKey("plain")
        updates: TestProbe[Any] = node.probe()
        gets: TestProbe[Any] = node.probe()
        node.replicator.tell(
            Replicator.Update(
                key,
                GCounter(),
                Replicator.WriteLocal(),
                updates.ref,
                lambda c: c.increment(node.self_address, 1),
            )
        )
        node.replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), gets.ref))
        await updates.expect_no_message(0.1)
        await gets.expect_no_message(0.1)

        loader(load).tell(DurableStore.LoadAllCompleted())
        assert isinstance(await updates.receive_message(), Replicator.UpdateSuccess)
        got = await gets.receive_message()
        assert isinstance(got, Replicator.GetSuccess)
        assert got.get(key).value == 1


async def test_loaded_data_is_served() -> None:
    async with with_store() as (node, store_probe):
        load = await store_probe.receive_message()
        counter = GCounter()._increment(node.self_address.unique_address, 3)  # pyright: ignore[reportPrivateUsage]
        loader(load).tell(
            DurableStore.LoadData({"durable-a": DurableDataEnvelope(DataEnvelope(counter))})
        )
        loader(load).tell(DurableStore.LoadAllCompleted())
        key = GCounterKey("durable-a")
        gets: TestProbe[Any] = node.probe()
        node.replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), gets.ref))
        got = await gets.receive_message()
        assert isinstance(got, Replicator.GetSuccess)
        assert got.get(key).value == 3


async def test_replica_count_is_zero_while_loading() -> None:
    async with with_store() as (node, store_probe):
        load = await store_probe.receive_message()
        counts: TestProbe[Any] = node.probe()
        node.replicator.tell(Replicator.GetReplicaCount(counts.ref))
        assert await counts.receive_message() == Replicator.ReplicaCount(0)
        loader(load).tell(DurableStore.LoadAllCompleted())
        node.replicator.tell(Replicator.GetReplicaCount(counts.ref))
        assert await counts.receive_message() == Replicator.ReplicaCount(1)


async def test_wire_messages_are_dropped_while_loading() -> None:
    async with with_store() as (node, store_probe):
        load = await store_probe.receive_message()
        wire = cast(ActorRef[Write | Read], node.replicator)
        acks: TestProbe[WriteAck | WriteNack] = node.probe()
        wire.tell(Write("durable-a", DataEnvelope(GCounter()), None, acks.ref))
        await acks.expect_no_message(0.1)

        loader(load).tell(DurableStore.LoadAllCompleted())
        reads: TestProbe[ReadResult] = node.probe()
        wire.tell(Read("durable-a", None, reads.ref))
        assert await reads.receive_message() == ReadResult(None)


async def test_the_replicator_stops_when_its_store_terminates() -> None:
    async with with_store(stopping_store) as (node, store_probe):
        await store_probe.receive_message()
        watcher: TestProbe[Any] = node.probe()
        await watcher.expect_terminated(node.replicator)


async def test_a_replicator_without_durable_keys_has_no_store() -> None:
    async with with_store(settings=ReplicatorSettings) as (node, store_probe):
        key = GCounterKey("plain")
        gets: TestProbe[Any] = node.probe()
        node.replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), gets.ref))
        assert isinstance(await gets.receive_message(), Replicator.NotFound)
        await store_probe.expect_no_message(0.1)


async def test_the_replicator_stops_when_its_own_node_is_removed_while_loading() -> None:
    from ohra.cluster.core import MemberRemoved
    from ohra.cluster.ddata.replicator import _ClusterEvent  # pyright: ignore[reportPrivateUsage]
    from ohra.cluster.gossip import Member

    async with with_store() as (node, store_probe):
        await store_probe.receive_message()
        me = Member(node.self_address.unique_address, 1, "removed")
        cast(ActorRef[Any], node.replicator).tell(_ClusterEvent(MemberRemoved(me, "exiting")))
        watcher: TestProbe[Any] = node.probe()
        await watcher.expect_terminated(node.replicator)


# Pekko's Replicator.scala: the stores on every write path


async def loaded(store_probe: TestProbe[Any]) -> None:
    """Answer the Replicator's LoadAll with nothing, so that it serves."""
    load = await store_probe.receive_message()
    assert isinstance(load, DurableStore.LoadAll)
    load.reply_to.tell(DurableStore.LoadAllCompleted())


def update_counter(node: Node, key_id: str, reply_to: ActorRef[Any]) -> None:
    node.replicator.tell(
        Replicator.Update(
            GCounterKey(key_id),
            GCounter(),
            Replicator.WriteLocal(),
            reply_to,
            lambda c: c.increment(node.self_address, 1),
        )
    )


async def test_a_local_durable_update_is_acknowledged_after_the_store() -> None:
    async with with_store() as (node, store_probe):
        await loaded(store_probe)
        key = GCounterKey("durable-a")
        updates: TestProbe[Any] = node.probe()
        update_counter(node, "durable-a", updates.ref)
        store = await store_probe.expect_message_type(DurableStore.Store)
        assert store.key == "durable-a"
        assert store.data.data_envelope.data.value == 1  # pyright: ignore
        reply = store.reply
        assert reply == DurableStore.StoreReply(
            Replicator.UpdateSuccess(key), Replicator.StoreFailure(key), updates.ref
        )
        await updates.expect_no_message(0.1)
        reply.reply_to.tell(reply.success)
        await updates.expect_message(Replicator.UpdateSuccess(key))


async def test_a_failing_store_gives_store_failure() -> None:
    async with with_store() as (node, store_probe):
        await loaded(store_probe)
        updates: TestProbe[Any] = node.probe()
        update_counter(node, "durable-a", updates.ref)
        reply = (await store_probe.expect_message_type(DurableStore.Store)).reply
        assert reply is not None
        reply.reply_to.tell(reply.failure)
        await updates.expect_message(Replicator.StoreFailure(GCounterKey("durable-a")))


async def test_a_non_durable_key_never_reaches_the_store() -> None:
    async with with_store() as (node, store_probe):
        await loaded(store_probe)
        updates: TestProbe[Any] = node.probe()
        update_counter(node, "plain", updates.ref)
        await updates.expect_message(Replicator.UpdateSuccess(GCounterKey("plain")))
        await store_probe.expect_no_message(0.1)


async def test_a_durable_delete_is_stored() -> None:
    async with with_store() as (node, store_probe):
        await loaded(store_probe)
        key = GCounterKey("durable-a")
        deletes: TestProbe[Any] = node.probe()
        node.replicator.tell(Replicator.Delete(key, Replicator.WriteLocal(), deletes.ref))
        store = await store_probe.expect_message_type(DurableStore.Store)
        assert store.data.data_envelope == DELETED_ENVELOPE
        assert store.reply == DurableStore.StoreReply(
            Replicator.DeleteSuccess(key), Replicator.StoreFailure(key), deletes.ref
        )


async def test_a_wire_write_is_acked_after_the_store() -> None:
    async with with_store() as (node, store_probe):
        await loaded(store_probe)
        acks: TestProbe[WriteAck | WriteNack] = node.probe()
        wire = cast(ActorRef[Write], node.replicator)
        wire.tell(Write("durable-a", DataEnvelope(GCounter()), None, acks.ref))
        store = await store_probe.expect_message_type(DurableStore.Store)
        assert store.key == "durable-a"
        assert store.reply == DurableStore.StoreReply(WriteAck(), WriteNack(), acks.ref)
        await acks.expect_no_message(0.1)


async def test_a_wire_write_with_a_failing_store_is_nacked() -> None:
    async with with_store() as (node, store_probe):
        await loaded(store_probe)
        acks: TestProbe[WriteAck | WriteNack] = node.probe()
        wire = cast(ActorRef[Write], node.replicator)
        wire.tell(Write("durable-a", DataEnvelope(GCounter()), None, acks.ref))
        reply = (await store_probe.expect_message_type(DurableStore.Store)).reply
        assert reply is not None
        reply.reply_to.tell(reply.failure)
        await acks.expect_message(WriteNack())


async def test_gossip_and_read_repair_are_stored_without_a_reply() -> None:
    async with with_store() as (node, store_probe):
        await loaded(store_probe)
        sink: TestProbe[Any] = node.probe()
        wire = cast(ActorRef[Gossip], node.replicator)
        wire.tell(
            Gossip(
                {"durable-a": (DataEnvelope(GCounter()), 0.0)},
                False,
                node.self_address.unique_address.uid,
                0,
                sink.ref,
            )
        )
        store = await store_probe.expect_message_type(DurableStore.Store)
        assert store.key == "durable-a"
        assert store.reply is None


async def test_expired_durable_keys_are_expired_in_the_store() -> None:
    settings = ReplicatorSettings(
        durable_keys=frozenset({"durable-a"}),
        expire_keys_after_inactivity={"durable-a": 0.2, "plain": 0.2},
        gossip_interval=0.05,
    )
    async with ActorTestKit() as kit:
        store_probe: TestProbe[Any] = kit.create_test_probe()
        async with manual_node(replace(settings, durable_store=probe_store(store_probe))) as (
            node,
            manual,
        ):
            await loaded(store_probe)
            updates: TestProbe[Any] = node.probe()
            update_counter(node, "durable-a", updates.ref)
            update_counter(node, "plain", updates.ref)
            await store_probe.expect_message_type(DurableStore.Store)
            await manual.time_passes(0.3)
            await store_probe.expect_message(DurableStore.Expire(frozenset({"durable-a"})))


async def test_a_durable_key_is_pruned_with_the_durable_marker_and_stored_again() -> None:
    settings = replace(PRUNING_SETTINGS, durable_keys=frozenset({PRUNING_KEY.id}))
    async with ActorTestKit() as kit:
        store_probe: TestProbe[Any] = kit.create_test_probe()
        async with manual_node(replace(settings, durable_store=probe_store(store_probe))) as (
            node,
            manual,
        ):
            await loaded(store_probe)
            acks: TestProbe[WriteAck | WriteNack] = node.probe()
            wire = cast(ActorRef[Write], node.replicator)
            wire.tell(Write(PRUNING_KEY.id, counter_from(removed, node), None, acks.ref))
            first = await store_probe.expect_message_type(DurableStore.Store)
            assert first.reply is not None
            first.reply.reply_to.tell(first.reply.success)
            await acks.expect_message(WriteAck())

            await manual.time_passes(PRUNED_AFTER)
            stored = await store_probe.expect_message_type(DurableStore.Store)
            assert stored.key == PRUNING_KEY.id and stored.reply is None
            assert removed not in stored.data.data_envelope.data.state  # pyright: ignore
            envelope = await read(node, PRUNING_KEY.id)
            assert envelope is not None
            marker = envelope.pruning[removed]
            assert isinstance(marker, PruningPerformed)
            # the Replicator's clock: the scheduler's, plus the gap to wall time
            now = (
                node.system.scheduler.current_time
                + time.time()
                - asyncio.get_running_loop().time()
            )
            assert now + 10 * 86400 - PRUNED_AFTER <= marker.obsolete_time <= now + 10 * 86400 + 1
            assert marker.obsolete_time > now + TTL
