"""Pekko's multi-node ``ReplicatorPruningSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/ReplicatorPruningSpec.scala``),
as one ``@multi_node_spec`` test with the spec's own barrier names and its one case, "move data
from removed node" (:82), whole: the data of the removed third node is moved into the leader, its
values survive, an update that injects the old data does not count third twice
(``updateAfterPruning``, 10 then 11, and 12 then 13 after the dissemination time), and the final
wait of ``max_pruning_dissemination + 3`` seconds.

Mapping:

- Pekko starts its own Replicator under ``/user/replicator`` with a gossip interval of one second
  and ``withPruning(pruningInterval = 1.second, maxPruningDissemination = 3.seconds)``. Here the
  ``DistributedData`` extension's Replicator runs with those settings.
- Pekko's classic ``replicator ! Get(...)`` with ``expectMsg`` is the typed protocol, with a
  ``TestProbe`` as ``reply_to``. The typed protocol has no request context, so Pekko's ``None``
  request is dropped.
- ``GetSuccess(...).needPruningFrom(third)`` is ``need_pruning_from(third_unique_address)`` on the
  data. Pekko fishes third's ``UniqueAddress`` from a ``MemberUp`` subscription; here it is read
  from ``Cluster(system).state``, once the Replicator counts three replicas.
- ``cluster.leave(node(third).address)`` is the typed ``cluster.manager ! Leave(...)``.
- Pekko's ``MultiNodeSpec`` sets ``coordinated-shutdown.terminate-actor-system = off``, so third's
  system keeps running after it left, and third goes on through the barriers. ohra's coordinated
  shutdown has no such setting: third's system terminates once it is removed, and a terminated
  system cannot enter a barrier. So third waits for its own termination and returns, and first
  takes third out of the barriers with ``test_conductor.remove_node(third)`` as soon as it has
  sent the ``Leave``, before anyone waits at a barrier that third's termination could fail with
  ``ClientLost``. The barriers after "remove-element" are entered by first and second only, as
  their ``runOn(first, second)`` blocks are in Pekko.
- ``expectNoMessage(maxPruningDissemination + 3.seconds)`` is ``expect_no_message`` on a probe.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from ohra.cluster import Cluster, ClusterSettings, Join, Leave
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    GSet,
    Key,
    ORMap,
    ORMapKey,
    ORMultiMap,
    ORMultiMapKey,
    ORSet,
    ORSetKey,
    PNCounterMap,
    PNCounterMapKey,
    ReplicatedData,
    Replicator,
    ReplicatorSettings,
)
from ohra.remote import Address
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, RoleName, TestProbe, multi_node_spec

from ...test_cluster import FAST, FAST_SHUTDOWN

MAX_PRUNING_DISSEMINATION = 3.0

config = MultiNodeConfig()
first, second, third = config.role("first"), config.role("second"), config.role("third")
config.common_config(
    cluster=ClusterSettings(
        **FAST,
        distributed_data=ReplicatorSettings(
            gossip_interval=1.0,
            pruning_interval=1.0,
            max_pruning_dissemination=MAX_PRUNING_DISSEMINATION,
        ),
    ),
    shutdown=FAST_SHUTDOWN,
)

READ_LOCAL = Replicator.ReadLocal()
WRITE_LOCAL = Replicator.WriteLocal()

KeyA = GCounterKey("A")
KeyB: ORSetKey[str] = ORSetKey("B")
KeyC: PNCounterMapKey[str] = PNCounterMapKey("C")
KeyD: ORMultiMapKey[str, str] = ORMultiMapKey("D")
KeyE: ORMapKey[str, GSet[str]] = ORMapKey("E")


@multi_node_spec(config)
async def test_replicator_pruning_spec(spec: MultiNodeSpec):
    cluster = Cluster(spec.system)
    replicator = DistributedData(spec.system).replicator
    self_address = DistributedData(spec.system).self_unique_address
    timeout = spec.settings.dilated(3.0)
    write_all = Replicator.WriteAll(timeout)

    async def join(from_: RoleName, to: RoleName) -> None:
        if spec.is_node(from_):
            cluster.manager.tell(Join(Address.parse(spec.node(to))))
        await spec.enter_barrier(from_.name + "-joined")

    async def get[A: ReplicatedData](key: Key[A]) -> A:
        probe: TestProbe[Replicator.GetResponse[A]] = spec.create_test_probe()
        replicator.tell(Replicator.Get(key, READ_LOCAL, probe.ref))
        reply = await probe.receive_message()
        assert isinstance(reply, Replicator.GetSuccess) and reply.key == key, reply
        return reply.get(key)

    async def update[A: ReplicatedData](
        key: Key[A], initial: A, write: Replicator.WriteConsistency, modify: Callable[[A], A]
    ) -> None:
        probe: TestProbe[Replicator.UpdateResponse[A]] = spec.create_test_probe()
        replicator.tell(Replicator.Update(key, initial, write, probe.ref, modify))
        assert await probe.receive_message() == Replicator.UpdateSuccess(key)

    async def await_assert(fn: Callable[[], Awaitable[None]], max: float) -> None:
        async with spec.within(max):
            await spec.await_assert(fn)

    async def await_replica_count(n: int, max: float) -> None:
        async def replica_count() -> None:
            probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()
            replicator.tell(Replicator.GetReplicaCount(probe.ref))
            await probe.expect_message(Replicator.ReplicaCount(n))

        await await_assert(replica_count, max)

    # "move data from removed node"
    await join(first, first)
    await join(second, first)
    await join(third, first)

    await await_replica_count(3, 5.0)

    # we need the UniqueAddress
    third_address = Address.parse(spec.node(third))
    (third_unique_address,) = (
        m.unique_address
        for m in cluster.state.members
        if m.unique_address.address == third_address
    )

    await update(KeyA, GCounter(), write_all, lambda c: c.increment(self_address, 3))
    await update(
        KeyB,
        ORSet[str](),
        write_all,
        lambda s: s.add(self_address, "a").add(self_address, "b").add(self_address, "c"),
    )
    await update(
        KeyC,
        PNCounterMap[str].empty,
        write_all,
        lambda m: m.increment(self_address, "x", 1).increment(self_address, "y", 1),
    )
    await update(
        KeyD,
        ORMultiMap[str, str].empty,
        write_all,
        lambda m: m.put(self_address, "a", frozenset({"A"})),
    )
    await update(
        KeyE,
        ORMap[str, GSet[str]].empty,
        write_all,
        lambda m: m.put(self_address, "a", GSet[str]().add("A")),
    )

    await spec.enter_barrier("updates-done")

    old_counter = await get(KeyA)
    assert old_counter.value == 9

    old_set = await get(KeyB)
    assert old_set.elements == {"a", "b", "c"}

    old_map = await get(KeyC)
    assert old_map.get("x") == 3
    assert old_map.get("y") == 3

    old_multi_map = await get(KeyD)
    assert old_multi_map.get("a") == {"A"}

    old_or_map = await get(KeyE)
    assert old_or_map.entries["a"].elements == {"A"}

    await spec.enter_barrier("get-old")

    if spec.is_node(third):
        await update(
            KeyE, ORMap[str, GSet[str]].empty, WRITE_LOCAL, lambda m: m.remove(self_address, "a")
        )

    await spec.enter_barrier("remove-element")

    if spec.is_node(third):
        # removed, third's system terminates and its body can enter no more barriers
        await asyncio.wait_for(spec.system.when_terminated(), 15.0)
        return

    if spec.is_node(first):
        cluster.manager.tell(Leave(third_address))
        await spec.test_conductor.remove_node(third)

    await await_replica_count(2, 15.0)
    await spec.enter_barrier("third-removed")

    values: set[int] = set()

    async def a_pruned() -> None:
        counter = await get(KeyA)
        values.add(counter.value)
        assert counter.value == 9
        assert not counter.need_pruning_from(third_unique_address)

    await await_assert(a_pruned, 15.0)
    assert values == {9}

    async def b_pruned() -> None:
        s = await get(KeyB)
        assert s.elements == {"a", "b", "c"}
        assert not s.need_pruning_from(third_unique_address)

    await await_assert(b_pruned, 5.0)

    async def c_pruned() -> None:
        m = await get(KeyC)
        assert m.entries == {"x": 3, "y": 3}
        assert not m.need_pruning_from(third_unique_address)

    await await_assert(c_pruned, 5.0)

    async def d_pruned() -> None:
        m = await get(KeyD)
        assert m.entries["a"] == {"A"}
        assert not m.need_pruning_from(third_unique_address)

    await await_assert(d_pruned, 5.0)

    async def e_pruned() -> None:
        m = await get(KeyE)
        assert not m.need_pruning_from(third_unique_address)

    await await_assert(e_pruned, 5.0)

    await spec.enter_barrier("pruning-done")

    # after pruning performed we should not be able to update with data from removed node
    async def update_after_pruning(expected_value: int) -> None:
        # inject data from removed node to simulate bad data
        await update(
            KeyA,
            GCounter(),
            write_all,
            lambda existing: existing.merge(old_counter).increment(self_address, 1),
        )
        assert (await get(KeyA)).value == expected_value

    async def check_value_propagated(expected_value: int) -> None:
        async def propagated() -> None:
            assert (await get(KeyA)).value == expected_value

        await spec.await_assert(propagated)

    if spec.is_node(first):
        await update_after_pruning(expected_value=10)
    if spec.is_node(second):
        await check_value_propagated(10)

    await spec.enter_barrier("update-first-after-pruning")

    if spec.is_node(second):
        await update_after_pruning(expected_value=11)
    await spec.enter_barrier("update-second-after-pruning")

    # after full replication should still not be able to update with data from removed node
    # but it would not work after removal of the PruningPerformed markers
    await spec.create_test_probe().expect_no_message(MAX_PRUNING_DISSEMINATION + 3.0)

    if spec.is_node(first):
        await update_after_pruning(expected_value=12)
    if spec.is_node(second):
        await check_value_propagated(12)

    await spec.enter_barrier("update-first-after-dissemination")

    if spec.is_node(second):
        await update_after_pruning(expected_value=13)
    await spec.enter_barrier("update-second-after-dissemination")

    await spec.enter_barrier("after-1")
