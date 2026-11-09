"""The load phase of Pekko's ``Replicator.load`` (1489-1545), with the durable store replaced by
a probe: the Replicator asks the store for everything, holds its commands until the load is done,
and stops when the store does."""

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
from ohra.cluster.ddata.envelope import DataEnvelope, Read, ReadResult, Write, WriteAck, WriteNack
from ohra.ref import ActorRef
from ohra.testkit import ActorTestKit, TestProbe

from .test_replicator_local import Node
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
