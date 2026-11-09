"""Reads and writes between Replicators at every consistency level: Pekko's receiveGet,
receiveUpdate and receiveDelete with their aggregators, receiveRead, receiveWrite and
receiveReadRepair.

Three nodes under ``@multi_node_spec``, in the style of Pekko's ``ReplicatorSpec``, which waits
for gossip and so comes with it: these cases need no gossip, and the long gossip interval rules it
out. The spec's B2 (the write, its convergence comes with gossip) and B3, and Review Focus 4.
"""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import patch

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    GSet,
    GSetKey,
    Replicator,
    ReplicatorSettings,
)
from ohra.ref import ActorRef
from ohra.remote import Address
from ohra.remote import remoting as remoting_module
from ohra.testkit import Direction, MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

from ...util import errors_logged
from ..test_cluster import FAST, FAST_SHUTDOWN

config = MultiNodeConfig()
first, second, third = config.role("first"), config.role("second"), config.role("third")
config.common_config(
    cluster=ClusterSettings(**FAST, distributed_data=ReplicatorSettings(gossip_interval=60.0)),
    shutdown=FAST_SHUTDOWN,
)
config.test_transport(on=True)

KeyA = GCounterKey("A")


async def join(spec: MultiNodeSpec) -> ActorRef[Replicator.Command]:
    """Join the first node's cluster, and wait until every Replicator counts all three."""
    Cluster(spec.system).manager.tell(Join(Address.parse(spec.node(first))))
    replicator = DistributedData(spec.system).replicator
    probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()

    async def three_replicas() -> None:
        replicator.tell(Replicator.GetReplicaCount(probe.ref))
        assert await probe.receive_message() == Replicator.ReplicaCount(3)

    await spec.await_assert(three_replicas, max=10.0, interval=0.05)
    await spec.enter_barrier("after-join")
    return replicator


async def cut_off_third(spec: MultiNodeSpec) -> None:
    """Blackhole the third node, and wait until the others see it unreachable."""
    if spec.is_node(first):
        for other in (first, second):
            await spec.test_conductor.blackhole(third, other, Direction.BOTH)
    await spec.enter_barrier("blackholed")
    if not spec.is_node(third):

        def third_unreachable() -> None:
            assert len(Cluster(spec.system).state.unreachable) == 1

        await spec.await_assert(third_unreachable, max=5.0, interval=0.02)
    await spec.enter_barrier("third-unreachable")


async def reconnect_third(spec: MultiNodeSpec) -> None:
    if spec.is_node(first):
        for other in (first, second):
            await spec.test_conductor.pass_through(third, other, Direction.BOTH)
    await spec.enter_barrier("passed")


def increment(spec: MultiNodeSpec) -> Callable[[GCounter], GCounter]:
    self_address = DistributedData(spec.system).self_unique_address
    return lambda c: c.increment(self_address)


async def update(
    spec: MultiNodeSpec,
    replicator: ActorRef[Replicator.Command],
    write: Replicator.WriteConsistency,
) -> Replicator.UpdateResponse[GCounter]:
    probe: TestProbe[Replicator.UpdateResponse[GCounter]] = spec.create_test_probe()
    replicator.tell(Replicator.Update(KeyA, GCounter(), write, probe.ref, increment(spec)))
    return await probe.receive_message(10.0)


async def get(
    spec: MultiNodeSpec,
    replicator: ActorRef[Replicator.Command],
    read: Replicator.ReadConsistency = Replicator.ReadLocal(),
    key: GCounterKey = KeyA,
) -> Replicator.GetResponse[GCounter]:
    probe: TestProbe[Replicator.GetResponse[GCounter]] = spec.create_test_probe()
    replicator.tell(Replicator.Get(key, read, probe.ref))
    return await probe.receive_message(10.0)


def value(reply: Replicator.GetResponse[GCounter]) -> int:
    assert isinstance(reply, Replicator.GetSuccess), reply
    return reply.data.value


@multi_node_spec(config)
async def test_write_majority_with_one_node_blackholed_succeeds(spec: MultiNodeSpec):
    replicator = await join(spec)
    await cut_off_third(spec)

    if spec.is_node(first):
        majority = Replicator.WriteMajority(spec.settings.dilated(3.0))
        assert await update(spec, replicator, majority) == Replicator.UpdateSuccess(KeyA)
    await spec.enter_barrier("updated")

    # the second node acked the Write; the third never got it, and no gossip runs
    if spec.is_node(first, second):
        assert value(await get(spec, replicator)) == 1
    if spec.is_node(third):
        assert await get(spec, replicator) == Replicator.NotFound(KeyA)
    await spec.enter_barrier("checked")

    await reconnect_third(spec)


@multi_node_spec(config)
async def test_read_all_and_write_all_with_an_unreachable_node_time_out(spec: MultiNodeSpec):
    replicator = await join(spec)
    timeout = spec.settings.dilated(1.0)

    if spec.is_node(first):
        assert await update(spec, replicator, Replicator.WriteAll(timeout)) == (
            Replicator.UpdateSuccess(KeyA)
        )
    await spec.enter_barrier("written-to-all")
    # the second node's own increment stays there: no gossip runs
    if spec.is_node(second):
        assert await update(spec, replicator, Replicator.WriteLocal()) == (
            Replicator.UpdateSuccess(KeyA)
        )
    await spec.enter_barrier("second-updated")
    await cut_off_third(spec)

    if spec.is_node(first):
        # the second node's reply is not repaired into the local value of a failed read
        assert await get(spec, replicator, Replicator.ReadAll(timeout)) == (
            Replicator.GetFailure(KeyA)
        )
        assert value(await get(spec, replicator)) == 1
        # the local write stands when the write fails
        assert await update(spec, replicator, Replicator.WriteAll(timeout)) == (
            Replicator.UpdateTimeout(KeyA)
        )
        assert value(await get(spec, replicator)) == 2
    await spec.enter_barrier("timed-out")

    await reconnect_third(spec)


@multi_node_spec(config)
async def test_write_to_above_the_cluster_size_fails_at_once(spec: MultiNodeSpec):
    replicator = await join(spec)

    if spec.is_node(first):
        long = spec.settings.dilated(60.0)
        async with spec.within(5.0):
            assert await update(spec, replicator, Replicator.WriteTo(4, long)) == (
                Replicator.UpdateTimeout(KeyA)
            )
            assert await get(spec, replicator, Replicator.ReadFrom(4, long)) == (
                Replicator.GetFailure(KeyA)
            )
        assert value(await get(spec, replicator)) == 1  # the local write stands
        # three replicas are enough for three
        assert await update(spec, replicator, Replicator.WriteTo(3, long)) == (
            Replicator.UpdateSuccess(KeyA)
        )
    await spec.enter_barrier("written")

    if spec.is_node(second, third):
        assert value(await get(spec, replicator)) == 2
    await spec.enter_barrier("checked")


@multi_node_spec(config)
async def test_a_write_for_a_deleted_key_keeps_it_deleted(spec: MultiNodeSpec):
    replicator = await join(spec)
    timeout = spec.settings.dilated(3.0)

    if spec.is_node(first):
        assert await update(spec, replicator, Replicator.WriteAll(timeout)) == (
            Replicator.UpdateSuccess(KeyA)
        )
        # deleted here only
        deleted: TestProbe[Replicator.DeleteResponse[GCounter]] = spec.create_test_probe()
        replicator.tell(Replicator.Delete(KeyA, Replicator.WriteLocal(), deleted.ref))
        await deleted.expect_message(Replicator.DeleteSuccess(KeyA))
    await spec.enter_barrier("deleted")

    if spec.is_node(second):
        # the first node's DeletedData absorbs the Write, and it acks
        assert await update(spec, replicator, Replicator.WriteAll(timeout)) == (
            Replicator.UpdateSuccess(KeyA)
        )
    await spec.enter_barrier("written")

    if spec.is_node(first):
        assert await get(spec, replicator) == Replicator.GetDataDeleted(KeyA)
    if spec.is_node(second):
        # a read that merges the deleted replica is deleted, and read repair deletes it here
        assert await get(spec, replicator, Replicator.ReadAll(timeout)) == (
            Replicator.GetDataDeleted(KeyA)
        )
        assert await get(spec, replicator) == Replicator.GetDataDeleted(KeyA)
    await spec.enter_barrier("checked")


@multi_node_spec(config)
async def test_a_write_of_another_type_is_nacked(spec: MultiNodeSpec):
    replicator = await join(spec)
    key_x = GCounterKey("x")
    set_x: GSetKey[str] = GSetKey("x")

    if spec.is_node(first):
        probe: TestProbe[Replicator.UpdateResponse[GCounter]] = spec.create_test_probe()
        replicator.tell(
            Replicator.Update(
                key_x, GCounter(), Replicator.WriteLocal(), probe.ref, increment(spec)
            )
        )
        await probe.expect_message(Replicator.UpdateSuccess(key_x))
    await spec.enter_barrier("counter-written")

    if spec.is_node(second):
        updated: TestProbe[Replicator.UpdateResponse[GSet[str]]] = spec.create_test_probe()
        empty: GSet[str] = GSet()
        replicator.tell(
            Replicator.Update(
                set_x,
                empty,
                Replicator.WriteAll(spec.settings.dilated(60.0)),
                updated.ref,
                lambda s: s.add("a"),
            )
        )
        # the first node nacks, so the write cannot succeed and fails before its timeout
        await updated.expect_message(Replicator.UpdateTimeout(set_x), 5.0)
    await spec.enter_barrier("set-written")

    if spec.is_node(first):
        # the counter is kept, and the Replicator still serves the key
        assert value(await get(spec, replicator, key=key_x)) == 1
    if spec.is_node(third):
        got: TestProbe[Replicator.GetResponse[GSet[str]]] = spec.create_test_probe()
        replicator.tell(Replicator.Get(set_x, Replicator.ReadLocal(), got.ref))
        await got.expect_message(Replicator.GetSuccess(set_x, GSet[str]().add("a")))
    await spec.enter_barrier("checked")


@multi_node_spec(config)
async def test_an_oversized_write_is_dropped_and_the_update_times_out(spec: MultiNodeSpec):
    """Pekko's remoting drops a Write over its frame size with an error log, and the aggregator
    replies on its timeout. Here the send raises, and a raise would stop it with no reply."""
    replicator = await join(spec)

    if spec.is_node(first):
        big: GSetKey[str] = GSetKey("big")
        with patch.object(remoting_module, "MAX_FRAME", 2**16), errors_logged() as errors:
            probe: TestProbe[Replicator.UpdateResponse[GSet[str]]] = spec.create_test_probe()
            empty: GSet[str] = GSet()
            replicator.tell(
                Replicator.Update(
                    big,
                    empty,
                    Replicator.WriteAll(spec.settings.dilated(1.0)),
                    probe.ref,
                    lambda s: s.add("x" * 2**17),
                )
            )
            await probe.expect_message(Replicator.UpdateTimeout(big), 5.0)
            assert any("big" in r.getMessage() for r in errors), errors
    await spec.enter_barrier("after-oversized")
