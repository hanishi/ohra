"""Status and gossip anti-entropy between Replicators: Pekko's receiveGossipTick, gossipTo,
receiveStatus and receiveGossip, with the digest cache and the system uid check.

Three nodes under ``@multi_node_spec``, in the style of Pekko's ``ReplicatorSpec``, which converges
only through gossip. The notify interval is long, so a ``FlushChanges`` (the notify tick's own
handler) stands in for it and the count of ``Changed`` is exact. The spec's B1, B2's second half
and B4.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from types import SimpleNamespace
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
from ohra.cluster.ddata import replicator as replicator_module
from ohra.cluster.ddata.envelope import DataEnvelope, Gossip, Status
from ohra.ref import ActorRef
from ohra.remote import Address
from ohra.remote import remoting as remoting_module
from ohra.testkit import Direction, MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

from ...util import errors_logged
from ..test_cluster import FAST, FAST_SHUTDOWN

GOSSIP = 0.1
EXPIRY = 1.0


def three_nodes(replicator: ReplicatorSettings) -> MultiNodeConfig:
    config = MultiNodeConfig()
    for name in ("first", "second", "third"):
        config.role(name)
    config.common_config(
        cluster=ClusterSettings(**FAST, distributed_data=replicator), shutdown=FAST_SHUTDOWN
    )
    config.test_transport(on=True)
    return config


config = three_nodes(
    ReplicatorSettings(
        gossip_interval=GOSSIP,
        notify_subscribers_interval=60.0,
        expire_keys_after_inactivity={"expiring-*": EXPIRY},
    )
)
first, second, third = config.roles
chunked = three_nodes(ReplicatorSettings(gossip_interval=GOSSIP, max_delta_elements=2))

KeyA = GCounterKey("A")


async def join(spec: MultiNodeSpec, n: int = 3) -> ActorRef[Replicator.Command]:
    """Join the first node's cluster, and wait until the Replicator counts ``n`` replicas."""
    Cluster(spec.system).manager.tell(Join(Address.parse(spec.node(first))))
    replicator = DistributedData(spec.system).replicator
    probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()

    async def replicas() -> None:
        replicator.tell(Replicator.GetReplicaCount(probe.ref))
        assert await probe.receive_message() == Replicator.ReplicaCount(n)

    await spec.await_assert(replicas, max=10.0, interval=0.05)
    return replicator


def increment(spec: MultiNodeSpec) -> Callable[[GCounter], GCounter]:
    self_address = DistributedData(spec.system).self_unique_address
    return lambda c: c.increment(self_address)


async def update(
    spec: MultiNodeSpec,
    replicator: ActorRef[Replicator.Command],
    write: Replicator.WriteConsistency = Replicator.WriteLocal(),
    key: GCounterKey = KeyA,
) -> Replicator.UpdateResponse[GCounter]:
    probe: TestProbe[Replicator.UpdateResponse[GCounter]] = spec.create_test_probe()
    replicator.tell(Replicator.Update(key, GCounter(), write, probe.ref, increment(spec)))
    return await probe.receive_message(10.0)


async def get(
    spec: MultiNodeSpec, replicator: ActorRef[Replicator.Command], key: GCounterKey = KeyA
) -> Replicator.GetResponse[GCounter]:
    probe: TestProbe[Replicator.GetResponse[GCounter]] = spec.create_test_probe()
    replicator.tell(Replicator.Get(key, Replicator.ReadLocal(), probe.ref))
    return await probe.receive_message(10.0)


async def key_ids(spec: MultiNodeSpec, replicator: ActorRef[Replicator.Command]) -> frozenset[str]:
    """The keys held, without touching them: GetKeyIds is no Get."""
    probe: TestProbe[Replicator.GetKeyIdsResult] = spec.create_test_probe()
    replicator.tell(Replicator.GetKeyIds(probe.ref))
    return (await probe.receive_message()).key_ids


def replicator_of(spec: MultiNodeSpec) -> ActorRef[Status | Gossip]:
    """This node's Replicator, as another node's Replicator sends to it: Status and Gossip are no
    Replicator.Command."""
    return spec.system.resolve(f"{spec.node(spec.myself)}/system/ddataReplicator")


async def converged(
    spec: MultiNodeSpec,
    replicator: ActorRef[Replicator.Command],
    expected: int,
    key: GCounterKey = KeyA,
) -> None:
    async def holds() -> None:
        reply = await get(spec, replicator, key)
        assert isinstance(reply, Replicator.GetSuccess), reply
        assert reply.data.value == expected

    await spec.await_assert(holds, max=10.0, interval=0.05)


@multi_node_spec(config)
async def test_two_nodes_updating_with_write_local_converge(spec: MultiNodeSpec):
    replicator = await join(spec)
    await spec.enter_barrier("after-join")

    if spec.is_node(first, second):
        changed: TestProbe[Replicator.SubscribeResponse[GCounter]] = spec.create_test_probe()
        replicator.tell(Replicator.Subscribe(KeyA, changed.ref))
        replicator.tell(Replicator.FlushChanges())  # the subscriber is registered, no data yet
        for _ in range(5):
            assert await update(spec, replicator) == Replicator.UpdateSuccess(KeyA)
        await converged(spec, replicator, 10)  # both hold the merge, through gossip
        await spec.enter_barrier("converged")

        # ten updates, local and gossiped, give one Changed on the notify tick
        replicator.tell(Replicator.FlushChanges())
        msg = await changed.receive_message()
        assert isinstance(msg, Replicator.Changed), msg
        assert msg.get(KeyA).value == 10
        await changed.expect_no_message(0.3)
    else:
        await spec.enter_barrier("converged")
    await spec.enter_barrier("after-1")


@multi_node_spec(config)
async def test_a_node_that_joins_later_catches_up(spec: MultiNodeSpec):
    if spec.is_node(first, second):
        replicator = await join(spec, 2)
        if spec.is_node(first):
            assert await update(spec, replicator) == Replicator.UpdateSuccess(KeyA)
    await spec.enter_barrier("first-updated")

    replicator = await join(spec)
    if spec.is_node(third):
        await converged(spec, replicator, 1)
    await spec.enter_barrier("after-2")


@multi_node_spec(config)
async def test_a_blackholed_node_converges_after_pass_through(spec: MultiNodeSpec):
    replicator = await join(spec)
    await spec.enter_barrier("after-join")

    if spec.is_node(first):
        for other in (first, second):
            await spec.test_conductor.blackhole(third, other, Direction.BOTH)
    await spec.enter_barrier("blackholed")
    if spec.is_node(first):

        def third_unreachable() -> None:
            assert len(Cluster(spec.system).state.unreachable) == 1

        await spec.await_assert(third_unreachable, max=5.0, interval=0.02)
        majority = Replicator.WriteMajority(spec.settings.dilated(3.0))
        assert await update(spec, replicator, majority) == Replicator.UpdateSuccess(KeyA)
    await spec.enter_barrier("updated")

    # gossip to the third node is lost while it is blackholed
    if spec.is_node(third):
        assert await get(spec, replicator) == Replicator.NotFound(KeyA)
    await spec.enter_barrier("checked")

    if spec.is_node(first):
        for other in (first, second):
            await spec.test_conductor.pass_through(third, other, Direction.BOTH)
    await spec.enter_barrier("passed")
    if spec.is_node(third):
        await converged(spec, replicator, 1)
    await spec.enter_barrier("after-3")


@multi_node_spec(config)
async def test_status_for_another_incarnation_is_ignored(spec: MultiNodeSpec):
    replicator = await join(spec)
    await spec.enter_barrier("after-join")

    if spec.is_node(first):
        assert await update(spec, replicator) == Replicator.UpdateSuccess(KeyA)
        uid = Cluster(spec.system).self_unique_address.uid
        probe: TestProbe[Status | Gossip] = spec.create_test_probe()
        to = replicator_of(spec)
        to.tell(Status({}, 0, 1, uid + 1, 4711, probe.ref))
        await probe.expect_no_message(0.5)
        # the same Status for this incarnation gets the key this node holds and the sender lacks
        to.tell(Status({}, 0, 1, uid, 4711, probe.ref))
        gossip = await probe.receive_message()
        assert isinstance(gossip, Gossip), gossip
        assert set(gossip.updated_data) == {"A"}
        assert (gossip.to_system_uid, gossip.from_system_uid) == (4711, uid)
    await spec.enter_barrier("after-4")


@multi_node_spec(chunked)
async def test_keys_beyond_max_delta_elements_gossip_in_chunks(spec: MultiNodeSpec):
    replicator = await join(spec)
    await spec.enter_barrier("after-join")
    keys = [GCounterKey(f"K{i}") for i in range(5)]

    if spec.is_node(first):
        for key in keys:
            assert await update(spec, replicator, key=key) == Replicator.UpdateSuccess(key)
    await spec.enter_barrier("updated")

    for key in keys:
        await converged(spec, replicator, 1, key)
    await spec.enter_barrier("after-5")


@multi_node_spec(config)
async def test_gossip_for_another_incarnation_is_ignored(spec: MultiNodeSpec):
    replicator = await join(spec)
    await spec.enter_barrier("after-join")

    if spec.is_node(first):
        uid = Cluster(spec.system).self_unique_address.uid
        probe: TestProbe[Status | Gossip] = spec.create_test_probe()
        updated_data = {"B": (DataEnvelope(GCounter()), 0.0)}
        replicator_of(spec).tell(Gossip(updated_data, False, uid + 1, 4711, probe.ref))
        assert "B" not in await key_ids(spec, replicator)
        # the same Gossip for this incarnation is written
        replicator_of(spec).tell(Gossip(updated_data, False, uid, 4711, probe.ref))
        assert "B" in await key_ids(spec, replicator)
    await spec.enter_barrier("after-gossip-uid")


async def a_stamped_key_outlives_an_unstamped_one(
    spec: MultiNodeSpec,
    stamp: Callable[[str, float, int, ActorRef[Status | Gossip]], Status | Gossip],
) -> None:
    """Two keys are used together on the second node; then a message from another node carries a
    later used timestamp for one of them. The other expires, and the stamped one is kept."""
    replicator = await join(spec)
    await spec.enter_barrier("after-join")
    control, stamped = GCounterKey("expiring-control"), GCounterKey("expiring-stamped")

    if spec.is_node(second):
        for key in (control, stamped):
            assert await update(spec, replicator, key=key) == Replicator.UpdateSuccess(key)
        uid = Cluster(spec.system).self_unique_address.uid
        probe: TestProbe[Status | Gossip] = spec.create_test_probe()
        # a used timestamp on the PF3 clock, seconds since the epoch, far ahead of the control's
        replicator_of(spec).tell(stamp(stamped.id, time.time() + 100.0, uid, probe.ref))

        async def control_expired() -> None:
            assert control.id not in await key_ids(spec, replicator)

        await spec.await_assert(control_expired, max=5.0, interval=0.05)
        assert stamped.id in await key_ids(spec, replicator)
    await spec.enter_barrier("after-stamp")


@multi_node_spec(config)
async def test_a_used_timestamp_in_status_keeps_the_key_from_expiring(spec: MultiNodeSpec):
    await a_stamped_key_outlives_an_unstamped_one(
        spec, lambda key, ts, uid, reply_to: Status({key: ("", ts)}, 0, 1, uid, 4711, reply_to)
    )


@multi_node_spec(config)
async def test_a_used_timestamp_in_gossip_keeps_the_key_from_expiring(spec: MultiNodeSpec):
    await a_stamped_key_outlives_an_unstamped_one(
        spec,
        lambda key, ts, uid, reply_to: Gossip(
            {key: (DataEnvelope(GCounter()), ts)}, False, uid, 4711, reply_to
        ),
    )


@multi_node_spec(config)
async def test_status_and_gossip_carry_the_used_timestamp(spec: MultiNodeSpec):
    replicator = await join(spec)
    await spec.enter_barrier("after-join")

    if spec.is_node(first):
        assert await update(spec, replicator) == Replicator.UpdateSuccess(KeyA)
        assert isinstance(await get(spec, replicator), Replicator.GetSuccess)  # a touch
        uid = Cluster(spec.system).self_unique_address.uid
        probe: TestProbe[Status | Gossip] = spec.create_test_probe()
        replicator_of(spec).tell(Status({}, 0, 1, uid, 4711, probe.ref))
        gossip = await probe.receive_message()
        assert isinstance(gossip, Gossip), gossip
        assert gossip.updated_data["A"][1] > 0.0  # 0.0 is unset
    await spec.enter_barrier("after-carry")


@multi_node_spec(config)
async def test_the_used_timestamp_follows_wall_time_the_loop_clock_misses(spec: MultiNodeSpec):
    """A frozen container resumes with the loop's monotonic clock behind wall time: the
    Replicator reads wall time on every clock read, so its used timestamps jump with it."""
    replicator = await join(spec)
    await spec.enter_barrier("after-join")

    if spec.is_node(first):
        assert await update(spec, replicator) == Replicator.UpdateSuccess(KeyA)
        uid = Cluster(spec.system).self_unique_address.uid
        probe: TestProbe[Status | Gossip] = spec.create_test_probe()

        async def carried_timestamp() -> float:
            assert isinstance(await get(spec, replicator), Replicator.GetSuccess)  # a touch
            replicator_of(spec).tell(Status({}, 0, 1, uid, 4711, probe.ref))
            gossip = await probe.receive_message()
            assert isinstance(gossip, Gossip), gossip
            return gossip.updated_data["A"][1]

        before = await carried_timestamp()
        real_time = time.time
        # wall time steps an hour ahead, as after a resume; the loop's clock does not see it
        jumped = SimpleNamespace(time=lambda: real_time() + 3600.0)
        with patch.object(replicator_module, "time", jumped):

            async def jumped_ahead() -> None:
                assert await carried_timestamp() > before + 1800.0

            await spec.await_assert(jumped_ahead, max=5.0, interval=0.05)
    await spec.enter_barrier("after-clock")


@multi_node_spec(config)
async def test_an_oversized_gossip_is_dropped_and_the_replicator_keeps_serving(
    spec: MultiNodeSpec,
):
    """Pekko's remoting drops a payload over its frame size with an error log. Here the send
    raises, and a raise in the Replicator would restart it and wipe its data."""
    replicator = await join(spec)
    await spec.enter_barrier("after-join")

    if spec.is_node(first):
        big: GSetKey[str] = GSetKey("big")
        with patch.object(remoting_module, "MAX_FRAME", 2**16), errors_logged() as errors:
            assert await update(spec, replicator) == Replicator.UpdateSuccess(KeyA)
            probe: TestProbe[Replicator.UpdateResponse[GSet[str]]] = spec.create_test_probe()
            empty: GSet[str] = GSet()
            replicator.tell(
                Replicator.Update(
                    big, empty, Replicator.WriteLocal(), probe.ref, lambda s: s.add("x" * 2**17)
                )
            )
            await probe.expect_message(Replicator.UpdateSuccess(big))

            def dropped() -> None:
                assert any("big" in r.getMessage() for r in errors), errors

            # the other nodes lack both keys, so the next Status exchange gossips them together
            await spec.await_assert(dropped, max=5.0, interval=0.05)
            # not restarted: the Replicator still holds both keys and serves them
            reply = await get(spec, replicator)
            assert isinstance(reply, Replicator.GetSuccess), reply
            assert reply.data.value == 1
            assert {"A", "big"} <= await key_ids(spec, replicator)
    await spec.enter_barrier("after-oversized")


@multi_node_spec(config)
async def test_an_oversized_changed_to_a_remote_subscriber_is_dropped(
    spec: MultiNodeSpec,
):
    """A Changed too large to send to a subscriber on another node is dropped with an error log,
    as Pekko's remoting drops it, and the Replicator is not restarted."""
    replicator = await join(spec)
    await spec.enter_barrier("after-join")

    if spec.is_node(first):
        big: GSetKey[str] = GSetKey("big")
        remote: ActorRef[Replicator.SubscribeResponse[GSet[str]]] = spec.system.resolve(
            f"{spec.node(second)}/system/subscriber"
        )
        with patch.object(remoting_module, "MAX_FRAME", 2**16), errors_logged() as errors:
            assert await update(spec, replicator) == Replicator.UpdateSuccess(KeyA)
            probe: TestProbe[Replicator.UpdateResponse[GSet[str]]] = spec.create_test_probe()
            empty: GSet[str] = GSet()
            replicator.tell(
                Replicator.Update(
                    big, empty, Replicator.WriteLocal(), probe.ref, lambda s: s.add("x" * 2**17)
                )
            )
            await probe.expect_message(Replicator.UpdateSuccess(big))
            replicator.tell(Replicator.Subscribe(big, remote))
            replicator.tell(Replicator.FlushChanges())

            def dropped() -> None:
                assert any("Dropped Changed" in r.getMessage() for r in errors), errors

            await spec.await_assert(dropped, max=5.0, interval=0.05)
            # not restarted: the Replicator still holds both keys and serves them
            reply = await get(spec, replicator)
            assert isinstance(reply, Replicator.GetSuccess), reply
            assert reply.data.value == 1
            assert {"A", "big"} <= await key_ids(spec, replicator)
    await spec.enter_barrier("after-oversized-changed")
