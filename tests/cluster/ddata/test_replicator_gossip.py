"""Status and gossip anti-entropy between Replicators: Pekko's receiveGossipTick, gossipTo,
receiveStatus and receiveGossip, with the digest cache and the system uid check.

Three nodes under ``@multi_node_spec``, in the style of Pekko's ``ReplicatorSpec``, which converges
only through gossip. The notify interval is long, so a ``FlushChanges`` (the notify tick's own
handler) stands in for it and the count of ``Changed`` is exact. The spec's B1, B2's second half
and B4.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    Replicator,
    ReplicatorSettings,
)
from ohra.cluster.ddata.envelope import Gossip, Status
from ohra.ref import ActorRef
from ohra.remote import Address
from ohra.testkit import Direction, MultiNodeConfig, MultiNodeSpec, TestProbe, multi_node_spec

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
        # as another node's Replicator sends it: Status is no Replicator.Command
        to: ActorRef[Status] = spec.system.resolve(f"{spec.node(first)}/system/ddataReplicator")
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
async def test_a_key_used_on_one_node_does_not_expire_on_another(spec: MultiNodeSpec):
    replicator = await join(spec)
    await spec.enter_barrier("after-join")
    key = GCounterKey("expiring-a")

    # the second node creates the key, and never touches it again
    if spec.is_node(second):
        assert await update(spec, replicator, key=key) == Replicator.UpdateSuccess(key)
    await spec.enter_barrier("created")

    async def held() -> None:
        assert key.id in await key_ids(spec, replicator)

    await spec.await_assert(held, max=5.0, interval=0.05)
    await spec.enter_barrier("spread")

    # for three expiry periods the first node uses the key, and the others keep it: its used
    # timestamp reaches them with Status and Gossip
    deadline = time.monotonic() + 3 * EXPIRY
    while time.monotonic() < deadline:
        if spec.is_node(first):
            assert isinstance(await get(spec, replicator, key), Replicator.GetSuccess)
        else:
            await held()
        await asyncio.sleep(0.05)
    await spec.enter_barrier("used")

    # unused, it expires everywhere
    async def expired() -> None:
        assert key.id not in await key_ids(spec, replicator)

    await spec.await_assert(expired, max=5.0, interval=0.05)
    await spec.enter_barrier("after-6")
