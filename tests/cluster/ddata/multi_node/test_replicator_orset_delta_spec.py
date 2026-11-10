"""Pekko's multi-node ``ReplicatorORSetDeltaSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/ReplicatorORSetDeltaSpec.scala``),
as one ``@multi_node_spec`` test whose cases run in Pekko's order and share its cluster, with the
spec's own barrier names.

Each case is a section, at its line in the Scala file:

- ``replicate_data_in_initial_phase``: "replicate data in initial phase", :85
- ``be_propagated_with_causal_consistency_during_network_split``: "be propagated with causal
  consistency during network split", :108

Mapping:

- Pekko starts its own Replicator under ``/user/replicator`` with a gossip interval of one
  second. Here the ``DistributedData`` extension's Replicator runs with that setting. No interval
  is shortened.
- Pekko's classic ``replicator ! Update(...)`` with ``ImplicitSender`` is the typed protocol, with
  a ``TestProbe`` as ``reply_to``; the replies carry no request context, so Pekko's
  ``UpdateSuccess(KeyA, None)`` is ``Replicator.UpdateSuccess(KeyA)``.
- ``Replicator.Internal.TestFullStateGossip`` is ``Replicator.TestFullStateGossip``.
- ``Thread.sleep(2000)`` ("all deltas sent") and ``Thread.sleep(1000)`` ("let the delta be
  propagated (will not fail if it takes longer)") are ``asyncio.sleep``: no condition shows that
  a delta was sent, and the assertions that follow them are what the sleeps are for.
- The network split is the test conductor's ``blackhole`` and ``pass_through``, as in
  ``test_replicator_chaos_spec.py``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    ORSet,
    ORSetKey,
    Replicator,
    ReplicatorSettings,
)
from ohra.ref import ActorRef
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
first, second, third = config.role("first"), config.role("second"), config.role("third")
config.common_config(
    cluster=ClusterSettings(**FAST, distributed_data=ReplicatorSettings(gossip_interval=1.0)),
    shutdown=FAST_SHUTDOWN,
)
config.test_transport(on=True)

READ_LOCAL = Replicator.ReadLocal()
WRITE_LOCAL = Replicator.WriteLocal()

KeyA: ORSetKey[str] = ORSetKey("A")


@multi_node_spec(config)
async def test_replicator_orset_delta_spec(spec: MultiNodeSpec):
    cluster = Cluster(spec.system)
    # typed Any: TestFullStateGossip is internal, and not in Replicator.Command
    replicator: ActorRef[Any] = DistributedData(spec.system).replicator
    self_address = DistributedData(spec.system).self_unique_address

    async def join(from_: RoleName, to: RoleName) -> None:
        if spec.is_node(from_):
            cluster.manager.tell(Join(Address.parse(spec.node(to))))
        await spec.enter_barrier(from_.name + "-joined")

    async def add(elem: str) -> None:
        probe: TestProbe[Replicator.UpdateResponse[ORSet[str]]] = spec.create_test_probe()
        replicator.tell(
            Replicator.Update(
                KeyA, ORSet[str](), WRITE_LOCAL, probe.ref, lambda s: s.add(self_address, elem)
            )
        )
        await probe.expect_message(Replicator.UpdateSuccess(KeyA))

    async def assert_value(key: ORSetKey[str], expected: set[str]) -> None:
        async def value() -> None:
            probe: TestProbe[Replicator.GetResponse[ORSet[str]]] = spec.create_test_probe()
            replicator.tell(Replicator.Get(key, READ_LOCAL, probe.ref))
            g = await probe.receive_message()
            assert isinstance(g, Replicator.GetSuccess), g
            assert g.get(key).elements == expected

        async with spec.within(10.0):
            await spec.await_assert(value)

    async def replicate_data_in_initial_phase() -> None:
        await join(first, first)
        await join(second, first)
        await join(third, first)

        replicator.tell(Replicator.TestFullStateGossip(enabled=False))

        async def replica_count() -> None:
            probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()
            replicator.tell(Replicator.GetReplicaCount(probe.ref))
            await probe.expect_message(Replicator.ReplicaCount(3))

        async with spec.within(10.0):
            await spec.await_assert(replica_count)

        if spec.is_node(first):
            await add("a")

        await spec.enter_barrier("initial-updates-done")

        await assert_value(KeyA, {"a"})

        await spec.enter_barrier("after-1")

    async def be_propagated_with_causal_consistency_during_network_split() -> None:
        if spec.is_node(first):
            # third is isolated
            await spec.test_conductor.blackhole(first, third, Direction.BOTH)
            await spec.test_conductor.blackhole(second, third, Direction.BOTH)
        await spec.enter_barrier("split")

        if spec.is_node(first):
            await add("b")
        if spec.is_node(second):
            await add("d")
        if spec.is_node(first, second):
            await assert_value(KeyA, {"a", "b", "d"})
            await asyncio.sleep(2.0)  # all deltas sent
        await spec.enter_barrier("added-b-and-d")

        if spec.is_node(first):
            await spec.test_conductor.pass_through(first, third, Direction.BOTH)
            await spec.test_conductor.pass_through(second, third, Direction.BOTH)
        await spec.enter_barrier("healed")

        if spec.is_node(first):
            # delta for "c" will be sent to third, but it has not received the previous delta
            # for "b"
            await add("c")
            # let the delta be propagated (will not fail if it takes longer)
            await asyncio.sleep(1.0)
        await spec.enter_barrier("added-c")

        if spec.is_node(first, second):
            await assert_value(KeyA, {"a", "b", "c", "d"})
        if spec.is_node(third):
            # the delta for "c" should not be applied because it has not received previous delta
            # for "b" and full gossip is turned off so far
            await assert_value(KeyA, {"a"})
        await spec.enter_barrier("verified-before-full-gossip")

        replicator.tell(Replicator.TestFullStateGossip(enabled=True))
        await assert_value(KeyA, {"a", "b", "c", "d"})
        await spec.enter_barrier("verified-after-full-gossip")

        replicator.tell(Replicator.TestFullStateGossip(enabled=False))

        # and now the delta seqNr should be in sync again
        if spec.is_node(first):
            await add("e")
        await assert_value(KeyA, {"a", "b", "c", "d", "e"})

        await spec.enter_barrier("after-2")

    await replicate_data_in_initial_phase()
    await be_propagated_with_causal_consistency_during_network_split()
