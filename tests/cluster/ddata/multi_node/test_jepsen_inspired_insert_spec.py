"""Pekko's multi-node ``JepsenInspiredInsertSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/JepsenInspiredInsertSpec.scala``),
as one ``@multi_node_spec`` test whose cases run in Pekko's order and share its cluster, with the
spec's own barrier names.

Each case is a section, at its line in the Scala file:

- ``setup_cluster``: "setup cluster", :107
- ``replicate_values_when_all_nodes_connected``: "replicate values when all nodes connected", :129
- ``write_read_to_majority_when_all_nodes_connected``: "write/read to majority when all nodes
  connected", :160
- ``replicate_values_after_partition``: "replicate values after partition", :196
- ``write_to_majority_during_3_2_partition``: "write to majority during 3+2 partition and read
  from majority after partition", :239

Mapping:

- Pekko's counts are kept: 200 elements, spread over five nodes, written with no delay
  (``delayMillis = 0``), so ``sleepDelay`` and ``sleepBeforePartition`` do nothing and are left
  out. The controller is not a cluster member.
- ``sleepDuringPartition`` holds the partition for five seconds, past Pekko's three-second
  acceptable heartbeat pause, so that the two sides see each other as unreachable while the nodes
  write. With ``FAST``'s pause of 0.2 s, the partition here lasts one second, an
  ``asyncio.sleep`` that paces the partition rather than waiting on an outcome.
- Pekko's ``pekko.testconductor.barrier-timeout = 60 s`` is each barrier's ``max``.
- The ``DistributedData`` extension's Replicator runs with a gossip interval of 0.1 s instead of
  Pekko's default two seconds, a notify interval of 0.05 s and ``max_delta_elements=10``.
- The typed protocol has no request context, so Pekko's ``Some(i)`` on each ``Update`` is dropped.
  Each write awaits its reply before the next, as Pekko's ``receiveOne`` does, so the replies
  arrive in ``my_data``'s order; the spec checks their count and their types, and a reply missing
  within the timeout fails the spec, where Pekko's count check would.

No assertion is skipped: none of them needs pruning, durable storage, deltas or roles.
"""

from __future__ import annotations

import asyncio

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    ORSet,
    ORSetKey,
    Replicator,
    ReplicatorSettings,
)
from ohra.remote import Address
from ohra.testkit import (
    Direction,
    MultiNodeConfig,
    MultiNodeSpec,
    RoleName,
    TestProbe,
    multi_node_spec,
)

from ...test_cluster import FAST, FAST_SHUTDOWN

config = MultiNodeConfig()
controller, n1, n2, n3, n4, n5 = (
    config.role(name) for name in ("controller", "n1", "n2", "n3", "n4", "n5")
)
config.common_config(
    cluster=ClusterSettings(
        **FAST,
        distributed_data=ReplicatorSettings(
            gossip_interval=0.1, notify_subscribers_interval=0.05, max_delta_elements=10
        ),
    ),
    shutdown=FAST_SHUTDOWN,
)
config.test_transport(on=True)

BARRIER_TIMEOUT = 60.0
TIMEOUT = 3.0
PARTITION = 1.0
READ_LOCAL = Replicator.ReadLocal()
WRITE_LOCAL = Replicator.WriteLocal()

nodes = config.roles[1:]  # controller not part of active nodes
node_count = len(nodes)
total_count = 200
expected_data = frozenset(range(total_count))
data: dict[RoleName, list[int]] = {
    n: [i for i in range(total_count) if nodes[i % node_count] == n] for n in nodes
}

type Acks = list[Replicator.UpdateResponse[ORSet[int]]]


@multi_node_spec(config)
async def test_jepsen_inspired_insert_spec(spec: MultiNodeSpec):
    cluster = Cluster(spec.system)
    replicator = DistributedData(spec.system).replicator
    self_address = DistributedData(spec.system).self_unique_address
    timeout = spec.settings.dilated(TIMEOUT)

    async def enter_barrier(name: str) -> None:
        await spec.enter_barrier(name, max=BARRIER_TIMEOUT)

    async def join(from_: RoleName, to: RoleName) -> None:
        if spec.is_node(from_):
            cluster.manager.tell(Join(Address.parse(spec.node(to))))
        await enter_barrier(from_.name + "-joined")

    async def write(
        key: ORSetKey[int], write: Replicator.WriteConsistency, max: float
    ) -> tuple[Acks, Acks]:
        """Write this node's elements one by one: the success and failure acks."""
        write_probe: TestProbe[Replicator.UpdateResponse[ORSet[int]]] = spec.create_test_probe()
        empty: ORSet[int] = ORSet()
        write_acks: Acks = []
        for i in data[spec.myself]:
            replicator.tell(
                Replicator.Update(
                    key, empty, write, write_probe.ref, lambda s, i=i: s.add(self_address, i)
                )
            )
            write_acks.append(await write_probe.receive_message(max))
        success_write_acks = [a for a in write_acks if isinstance(a, Replicator.UpdateSuccess)]
        failure_write_acks = [
            a
            for a in write_acks
            if isinstance(
                a, Replicator.UpdateTimeout | Replicator.ModifyFailure | Replicator.StoreFailure
            )
        ]
        assert len(success_write_acks) + len(failure_write_acks) == len(data[spec.myself])
        return success_write_acks, failure_write_acks

    async def read(key: ORSetKey[int], read: Replicator.ReadConsistency) -> frozenset[int]:
        read_probe: TestProbe[Replicator.GetResponse[ORSet[int]]] = spec.create_test_probe()
        replicator.tell(Replicator.Get(key, read, read_probe.ref))
        g = await read_probe.receive_message()
        assert isinstance(g, Replicator.GetSuccess) and g.key == key, g
        return g.data.elements

    async def eventually_all_nodes_have_the_data(key: ORSetKey[int]) -> None:
        async def all_data() -> None:
            assert await read(key, READ_LOCAL) == expected_data

        async with spec.within(15.0):
            await spec.await_assert(all_data, interval=0.05)

    async def partition(healed: str) -> None:
        """The controller's partition of n1, n4 and n5 from n2 and n3, for a while."""
        tc = spec.test_conductor
        for a in (n1, n4, n5):
            for b in (n2, n3):
                await tc.blackhole(a, b, Direction.BOTH)
        await asyncio.sleep(PARTITION)  # Pekko's sleepDuringPartition
        for a in (n1, n4, n5):
            for b in (n2, n3):
                await tc.pass_through(a, b, Direction.BOTH)
        await enter_barrier(healed)

    async def setup_cluster() -> None:
        if spec.is_node(*nodes):
            for n in nodes:
                await join(n, n1)

            async def replica_count() -> None:
                probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()
                replicator.tell(Replicator.GetReplicaCount(probe.ref))
                await probe.expect_message(Replicator.ReplicaCount(node_count))

            async with spec.within(10.0):
                await spec.await_assert(replica_count, interval=0.05)

        if spec.is_node(controller):
            for n in nodes:
                await enter_barrier(n.name + "-joined")

        await enter_barrier("after-setup")

    async def replicate_values_when_all_nodes_connected() -> None:
        key: ORSetKey[int] = ORSetKey("A")
        if spec.is_node(*nodes):
            success_write_acks, failure_write_acks = await write(key, WRITE_LOCAL, 3.0)
            assert len(success_write_acks) == len(data[spec.myself])
            assert failure_write_acks == []

            # eventually all nodes will have the data
            await eventually_all_nodes_have_the_data(key)

        await enter_barrier("after-test-1")

    async def write_read_to_majority_when_all_nodes_connected() -> None:
        key: ORSetKey[int] = ORSetKey("B")
        read_majority = Replicator.ReadMajority(timeout)
        write_majority = Replicator.WriteMajority(timeout)
        if spec.is_node(*nodes):
            success_write_acks, failure_write_acks = await write(
                key, write_majority, timeout + 1.0
            )
            assert len(success_write_acks) == len(data[spec.myself])
            assert failure_write_acks == []

            await enter_barrier("data-written-2")

            # read from majority of nodes, which is enough to retrieve all data
            assert await read(key, read_majority) == expected_data

        if spec.is_node(controller):
            await enter_barrier("data-written-2")

        await enter_barrier("after-test-2")

    async def replicate_values_after_partition() -> None:
        key: ORSetKey[int] = ORSetKey("C")
        if spec.is_node(controller):
            await partition("partition-healed-3")

        if spec.is_node(*nodes):
            success_write_acks, failure_write_acks = await write(key, WRITE_LOCAL, 3.0)
            assert len(success_write_acks) == len(data[spec.myself])
            assert failure_write_acks == []

            await enter_barrier("partition-healed-3")

            # eventually all nodes will have the data
            await eventually_all_nodes_have_the_data(key)

        await enter_barrier("after-test-3")

    async def write_to_majority_during_3_2_partition() -> None:
        key: ORSetKey[int] = ORSetKey("D")
        read_majority = Replicator.ReadMajority(timeout)
        write_majority = Replicator.WriteMajority(timeout)
        if spec.is_node(controller):
            await partition("partition-healed-4")

        if spec.is_node(*nodes):
            success_write_acks, failure_write_acks = await write(
                key, write_majority, timeout + 1.0
            )
            if spec.is_node(n1, n4, n5):
                assert len(success_write_acks) == len(data[spec.myself])
                assert failure_write_acks == []
            # on n2 and n3, without delays all could theoretically have been written before the
            # blackhole, so Pekko asserts no failures only with delays

            await enter_barrier("partition-healed-4")

            # on the 2 node side, read from majority of nodes is enough to read all writes
            if spec.is_node(n2, n3):
                assert await read(key, read_majority) == expected_data
            # but on the 3 node side, read from majority doesn't mean that we are guaranteed to
            # see the writes from the other side, yet

            # eventually all nodes will have the data
            await eventually_all_nodes_have_the_data(key)

        await enter_barrier("after-test-4")

    await setup_cluster()
    await replicate_values_when_all_nodes_connected()
    await write_read_to_majority_when_all_nodes_connected()
    await replicate_values_after_partition()
    await write_to_majority_during_3_2_partition()
