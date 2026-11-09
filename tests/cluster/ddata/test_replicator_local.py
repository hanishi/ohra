"""The Replicator on one node: Get, Update and Delete on their local paths, from Pekko's
ReplicatorSpec and LocalConcurrencySpec cases that need no other node."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from ohra.cluster import Cluster
from ohra.cluster.ddata import (
    GCounter,
    GCounterKey,
    GSet,
    GSetKey,
    ORSet,
    ORSetKey,
    SelfUniqueAddress,
)
from ohra.cluster.ddata.replicator import Replicator
from ohra.cluster.ddata.settings import ReplicatorSettings
from ohra.ref import ActorRef
from ohra.system import ActorSystem
from ohra.testkit import TestKitSettings, TestProbe

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node

A = GCounterKey("A")


@dataclass
class Node:
    system: ActorSystem[Any]
    replicator: ActorRef[Replicator.Command]
    self_address: SelfUniqueAddress
    settings: TestKitSettings = field(default_factory=TestKitSettings)

    def probe[M](self) -> TestProbe[M]:  # pyright: ignore[reportInvalidTypeVarUse]
        return TestProbe[M](self)


@asynccontextmanager
async def single_node() -> AsyncGenerator[Node]:
    port = free_port()
    async with cluster_node("ddata", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=3)
        cell = system._system_root.spawn_child(  # pyright: ignore[reportPrivateUsage]
            Replicator.behavior(ReplicatorSettings()), "ddataReplicator", None
        )
        yield Node(system, cell.ref, SelfUniqueAddress(Cluster(system).self_unique_address))


async def update_counter(node: Node, key: GCounterKey, n: int) -> None:
    probe: TestProbe[Replicator.UpdateResponse[GCounter]] = node.probe()
    node.replicator.tell(
        Replicator.Update(
            key,
            GCounter(),
            Replicator.WriteLocal(),
            probe.ref,
            lambda c: c.increment(node.self_address, n),
        )
    )
    await probe.expect_message(Replicator.UpdateSuccess(key))


async def get_counter(node: Node, key: GCounterKey) -> Replicator.GetResponse[GCounter]:
    probe: TestProbe[Replicator.GetResponse[GCounter]] = node.probe()
    node.replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), probe.ref))
    return await probe.receive_message()


def value_of(reply: Replicator.GetResponse[GCounter]) -> int:
    assert isinstance(reply, Replicator.GetSuccess)
    return reply.get(A).value


async def test_get_of_a_missing_key_replies_not_found():
    async with single_node() as node:
        assert await get_counter(node, A) == Replicator.NotFound(A)


async def test_update_then_get_returns_the_value():
    async with single_node() as node:
        await update_counter(node, A, 3)
        assert value_of(await get_counter(node, A)) == 3


async def test_update_applies_modify_to_initial_when_missing():
    async with single_node() as node:
        probe: TestProbe[Replicator.UpdateResponse[GCounter]] = node.probe()
        initial = GCounter().increment(node.self_address, 2)
        node.replicator.tell(
            Replicator.Update(
                A,
                initial,
                Replicator.WriteLocal(),
                probe.ref,
                lambda c: c.increment(node.self_address, 1),
            )
        )
        await probe.expect_message(Replicator.UpdateSuccess(A))
        assert value_of(await get_counter(node, A)) == 3


async def test_update_merges_with_the_existing_value():
    async with single_node() as node:
        key: GSetKey[str] = GSetKey("S")
        probe: TestProbe[Replicator.UpdateResponse[GSet[str]]] = node.probe()
        empty: GSet[str] = GSet()
        node.replicator.tell(
            Replicator.Update(key, empty, Replicator.WriteLocal(), probe.ref, lambda s: s.add("a"))
        )
        await probe.expect_message(Replicator.UpdateSuccess(key))
        # modify ignores the current value: the result is merged into it, not put in its place
        node.replicator.tell(
            Replicator.Update(
                key, empty, Replicator.WriteLocal(), probe.ref, lambda _: empty.add("b")
            )
        )
        await probe.expect_message(Replicator.UpdateSuccess(key))
        got: TestProbe[Replicator.GetResponse[GSet[str]]] = node.probe()
        node.replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), got.ref))
        await got.expect_message(Replicator.GetSuccess(key, empty.add("a").add("b")))


async def test_read_majority_on_a_single_node_replies_from_local():
    async with single_node() as node:
        await update_counter(node, A, 2)
        probe: TestProbe[Replicator.GetResponse[GCounter]] = node.probe()
        for consistency in (
            Replicator.ReadMajority(1.0),
            Replicator.ReadMajorityPlus(1.0, 1),
            Replicator.ReadAll(1.0),
        ):
            node.replicator.tell(Replicator.Get(A, consistency, probe.ref))
            assert value_of(await probe.receive_message(0.5)) == 2
        updated: TestProbe[Replicator.UpdateResponse[GCounter]] = node.probe()
        for write in (
            Replicator.WriteMajority(1.0),
            Replicator.WriteMajorityPlus(1.0, 1),
            Replicator.WriteAll(1.0),
        ):
            node.replicator.tell(
                Replicator.Update(
                    A, GCounter(), write, updated.ref, lambda c: c.increment(node.self_address)
                )
            )
            await updated.expect_message(Replicator.UpdateSuccess(A), 0.5)
        assert value_of(await get_counter(node, A)) == 5


async def test_write_to_more_nodes_than_exist_fails_at_once():
    async with single_node() as node:
        probe: TestProbe[Replicator.UpdateResponse[GCounter]] = node.probe()
        node.replicator.tell(
            Replicator.Update(
                A,
                GCounter(),
                Replicator.WriteTo(3, 1.0),
                probe.ref,
                lambda c: c.increment(node.self_address),
            )
        )
        await probe.expect_message(Replicator.UpdateTimeout(A), 0.5)
        # the local write stands, as in Pekko
        assert value_of(await get_counter(node, A)) == 1
        got: TestProbe[Replicator.GetResponse[GCounter]] = node.probe()
        node.replicator.tell(Replicator.Get(A, Replicator.ReadFrom(3, 1.0), got.ref))
        await got.expect_message(Replicator.GetFailure(A), 0.5)
        deleted: TestProbe[Replicator.DeleteResponse[GCounter]] = node.probe()
        node.replicator.tell(Replicator.Delete(A, Replicator.WriteTo(3, 1.0), deleted.ref))
        await deleted.expect_message(Replicator.DeleteFailure(A), 0.5)


async def test_reply_with_modify_failure_if_modify_raises():
    async with single_node() as node:
        await update_counter(node, A, 1)
        probe: TestProbe[Replicator.UpdateResponse[GCounter]] = node.probe()
        error = RuntimeError("boom")

        def fail(_: GCounter) -> GCounter:
            raise error

        node.replicator.tell(
            Replicator.Update(A, GCounter(), Replicator.WriteLocal(), probe.ref, fail)
        )
        reply = await probe.receive_message()
        assert isinstance(reply, Replicator.ModifyFailure)
        assert (reply.key, reply.cause) == (A, error)
        assert "boom" in reply.error_message
        assert value_of(await get_counter(node, A)) == 1


async def test_update_on_a_key_of_another_type_replies_modify_failure_and_keeps_serving():
    async with single_node() as node:
        await update_counter(node, A, 1)
        key: ORSetKey[str] = ORSetKey("A")
        probe: TestProbe[Replicator.UpdateResponse[ORSet[str]]] = node.probe()
        empty: ORSet[str] = ORSet()
        # modify ignores the stored GCounter, so the failure is in the merge
        node.replicator.tell(
            Replicator.Update(
                key,
                empty,
                Replicator.WriteLocal(),
                probe.ref,
                lambda _: empty.add(node.self_address, "x"),
            )
        )
        reply = await probe.receive_message()
        assert isinstance(reply, Replicator.ModifyFailure)
        assert isinstance(reply.cause, TypeError)
        assert value_of(await get_counter(node, A)) == 1
        await update_counter(node, GCounterKey("B"), 2)
        assert value_of(await get_counter(node, GCounterKey("B"))) == 2


async def test_delete_then_get_update_delete_reply_data_deleted():
    async with single_node() as node:
        await update_counter(node, A, 1)
        deleted: TestProbe[Replicator.DeleteResponse[GCounter]] = node.probe()
        node.replicator.tell(Replicator.Delete(A, Replicator.WriteLocal(), deleted.ref))
        await deleted.expect_message(Replicator.DeleteSuccess(A))
        assert await get_counter(node, A) == Replicator.GetDataDeleted(A)
        updated: TestProbe[Replicator.UpdateResponse[GCounter]] = node.probe()
        node.replicator.tell(
            Replicator.Update(
                A,
                GCounter(),
                Replicator.WriteLocal(),
                updated.ref,
                lambda c: c.increment(node.self_address),
            )
        )
        await updated.expect_message(Replicator.UpdateDataDeleted(A))
        node.replicator.tell(Replicator.Delete(A, Replicator.WriteLocal(), deleted.ref))
        await deleted.expect_message(Replicator.DataDeleted(A))


async def test_get_key_ids_lists_live_keys():
    async with single_node() as node:
        await update_counter(node, A, 1)
        await update_counter(node, GCounterKey("B"), 1)
        deleted: TestProbe[Replicator.DeleteResponse[GCounter]] = node.probe()
        node.replicator.tell(Replicator.Delete(A, Replicator.WriteLocal(), deleted.ref))
        await deleted.expect_message(Replicator.DeleteSuccess(A))
        probe: TestProbe[Replicator.GetKeyIdsResult] = node.probe()
        node.replicator.tell(Replicator.GetKeyIds(probe.ref))
        await probe.expect_message(Replicator.GetKeyIdsResult(frozenset({"B"})))


async def test_replica_count_on_one_node_is_one():
    async with single_node() as node:
        probe: TestProbe[Replicator.ReplicaCount] = node.probe()
        node.replicator.tell(Replicator.GetReplicaCount(probe.ref))
        await probe.expect_message(Replicator.ReplicaCount(1))
