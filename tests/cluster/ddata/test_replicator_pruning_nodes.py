"""Pruning across two nodes, with short real intervals: a split pauses it, and the owner of a
marker waits until every node has seen it. No Pekko spec covers these alone.

Data that names a node outside the cluster is placed on the leader with a wire ``Write``, as
another node's Replicator would send it, and read back with a wire ``Read``, which shows the
pruning markers. The leader is the Up member with the lowest address."""

from typing import cast

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import DistributedData, GCounter, Replicator, ReplicatorSettings
from ohra.cluster.ddata.envelope import DataEnvelope, Read, ReadResult, Write, WriteAck, WriteNack
from ohra.cluster.ddata.pruning_state import PruningInitialized, PruningPerformed
from ohra.ref import ActorRef
from ohra.remote import Address, UniqueAddress
from ohra.testkit import (
    Direction,
    MultiNodeConfig,
    MultiNodeSpec,
    RoleName,
    TestProbe,
    multi_node_spec,
)

from ..test_cluster import FAST, FAST_SHUTDOWN

MAX_PRUNING_DISSEMINATION = 0.5
PRUNING_INTERVAL = 0.1
SETTINGS = ReplicatorSettings(
    gossip_interval=0.1,
    pruning_interval=PRUNING_INTERVAL,
    max_pruning_dissemination=MAX_PRUNING_DISSEMINATION,
)

config = MultiNodeConfig()
first, second = config.role("first"), config.role("second")
config.common_config(
    cluster=ClusterSettings(**FAST, distributed_data=SETTINGS), shutdown=FAST_SHUTDOWN
)
config.test_transport(on=True)

removed = UniqueAddress("localhost", 1, 99, "Sys")
KEY_ID = "counter"


def port(spec: MultiNodeSpec, role: RoleName) -> int:
    return Address.parse(spec.node(role)).port


async def join_first(spec: MultiNodeSpec) -> None:
    """Both nodes join first; neither starts its Replicator."""
    Cluster(spec.system).manager.tell(Join(Address.parse(spec.node(first))))

    def both_up() -> None:
        members = Cluster(spec.system).state.members
        assert len(members) == 2 and all(m.status == "up" for m in members), members

    await spec.await_assert(both_up)


def wire(spec: MultiNodeSpec) -> ActorRef[Write | Read]:
    """This node's Replicator, as another node's Replicator sends to it."""
    return cast(ActorRef[Write | Read], DistributedData(spec.system).replicator)


async def write_from_removed(spec: MultiNodeSpec) -> None:
    """3 from the removed node and 2 from self."""
    me = Cluster(spec.system).self_unique_address
    data = GCounter()._increment(removed, 3)._increment(me, 2)  # pyright: ignore[reportPrivateUsage]
    probe: TestProbe[WriteAck | WriteNack] = spec.create_test_probe()
    wire(spec).tell(Write(KEY_ID, DataEnvelope(data), None, probe.ref))
    assert await probe.receive_message() == WriteAck()


async def read(spec: MultiNodeSpec) -> DataEnvelope | None:
    probe: TestProbe[ReadResult] = spec.create_test_probe()
    wire(spec).tell(Read(KEY_ID, None, probe.ref))
    return (await probe.receive_message()).envelope


async def pruned(spec: MultiNodeSpec) -> None:
    envelope = await read(spec)
    assert envelope is not None
    assert isinstance(envelope.pruning.get(removed), PruningPerformed), envelope
    assert not envelope.need_pruning_from(removed)
    assert isinstance(envelope.data, GCounter) and envelope.data.value == 5


async def replica_count_is_two(spec: MultiNodeSpec) -> None:
    probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()
    DistributedData(spec.system).replicator.tell(Replicator.GetReplicaCount(probe.ref))
    await probe.expect_message(Replicator.ReplicaCount(2))


@multi_node_spec(config)
async def test_pruning_waits_while_a_node_is_unreachable(spec: MultiNodeSpec):
    leader = min((first, second), key=lambda r: port(spec, r))
    await join_first(spec)
    await spec.await_assert(lambda: replica_count_is_two(spec))
    await spec.enter_barrier("up")

    if spec.is_node(first):
        await spec.test_conductor.blackhole(first, second, Direction.BOTH)
    await spec.enter_barrier("blackhole")

    if spec.is_node(leader):

        def unreachable() -> None:
            assert Cluster(spec.system).state.unreachable

        await spec.await_assert(unreachable)
        await write_from_removed(spec)
        # Pekko's expectNoMessage: nothing is collected or initialized while a node is
        # unreachable, however long the split lasts
        await spec.create_test_probe().expect_no_message(4 * MAX_PRUNING_DISSEMINATION)
        envelope = await read(spec)
        assert envelope is not None and envelope.pruning == {}
        assert envelope.need_pruning_from(removed)
    await spec.enter_barrier("no-marker")

    if spec.is_node(first):
        await spec.test_conductor.pass_through(first, second, Direction.BOTH)
    await spec.enter_barrier("pass-through")

    async with spec.within(10.0):
        await spec.await_assert(lambda: pruned(spec))
    await spec.enter_barrier("pruned")


@multi_node_spec(config)
async def test_the_owner_waits_until_every_node_has_seen_the_marker(spec: MultiNodeSpec):
    leader = min((first, second), key=lambda r: port(spec, r))
    # only the leader starts its Replicator: the other node is a reachable member without one
    await join_first(spec)
    await spec.enter_barrier("up")

    if spec.is_node(leader):
        await spec.await_assert(lambda: replica_count_is_two(spec))
        await write_from_removed(spec)

        async def initialized() -> None:
            envelope = await read(spec)
            assert envelope is not None
            assert isinstance(envelope.pruning.get(removed), PruningInitialized), envelope

        await spec.await_assert(initialized)
        # Pekko's expectNoMessage: the other node has not seen the marker, so it is not performed
        await spec.create_test_probe().expect_no_message(4 * PRUNING_INTERVAL)
        await initialized()
    await spec.enter_barrier("initialized")

    if not spec.is_node(leader):
        DistributedData(spec.system)  # starts this node's Replicator
    await spec.enter_barrier("replicator-started")

    if spec.is_node(leader):
        async with spec.within(10.0):
            await spec.await_assert(lambda: pruned(spec))
    await spec.enter_barrier("pruned")
