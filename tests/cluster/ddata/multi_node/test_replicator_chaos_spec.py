"""Pekko's multi-node ``ReplicatorChaosSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/ReplicatorChaosSpec.scala``),
as one ``@multi_node_spec`` test whose cases run in Pekko's order and share its cluster, with the
spec's own barrier names.

Each case is a section, at its line in the Scala file:

- ``replicate_data_in_initial_phase``: "replicate data in initial phase", :107
- ``be_available_during_network_split``: "be available during network split", :176
- ``converge_after_partition``: "converge after partition", :229

Mapping:

- Pekko sets ``pekko.cluster.roles = ["backend"]`` and starts its own Replicator under
  ``/user/replicator`` with ``withRole("backend")``. Replicator roles are not ported, so every node
  is a replica, as every node in Pekko's spec has the role anyway. The ``DistributedData``
  extension's Replicator runs with a gossip interval of 0.1 s instead of Pekko's one second, and a
  notify interval of 0.05 s.
- Pekko's classic ``replicator ! Update(...)`` with ``ImplicitSender`` is the typed protocol here,
  with a ``TestProbe`` as ``reply_to``. The replies carry no request context, so Pekko's
  ``UpdateSuccess(KeyA, None)`` is ``Replicator.UpdateSuccess(KeyA)``.
- ``testConductor.exit(fourth, 0)`` crashes fourth's system and cancels its body, as an exited JVM
  runs nothing more. With manual downing, as in Pekko, fourth stays an unreachable member.

No assertion is skipped: none of them needs pruning, durable storage, deltas or roles.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    GCounter,
    GCounterKey,
    GSet,
    GSetKey,
    Key,
    ORSet,
    ORSetKey,
    PNCounter,
    PNCounterKey,
    ReplicatedData,
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
first, second, third, fourth, fifth = (
    config.role(name) for name in ("first", "second", "third", "fourth", "fifth")
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

TIMEOUT = 3.0
READ_LOCAL = Replicator.ReadLocal()
WRITE_LOCAL = Replicator.WriteLocal()

KeyA = GCounterKey("A")
KeyB = PNCounterKey("B")
KeyC = GCounterKey("C")
KeyD = GCounterKey("D")
KeyE: GSetKey[str] = GSetKey("E")
KeyF: ORSetKey[str] = ORSetKey("F")
KeyX = GCounterKey("X")


@multi_node_spec(config)
async def test_replicator_chaos_spec(spec: MultiNodeSpec):
    cluster = Cluster(spec.system)
    replicator = DistributedData(spec.system).replicator
    self_address = DistributedData(spec.system).self_unique_address
    timeout = spec.settings.dilated(TIMEOUT)
    write_two = Replicator.WriteTo(2, timeout)
    write_all = Replicator.WriteAll(timeout)

    async def join(from_: RoleName, to: RoleName) -> None:
        if spec.is_node(from_):
            cluster.manager.tell(Join(Address.parse(spec.node(to))))
        await spec.enter_barrier(from_.name + "-joined")

    def plus(n: int) -> Callable[[GCounter], GCounter]:
        """Pekko's ``_ :+ n``."""
        return lambda c: c.increment(self_address, n)

    async def update[A: ReplicatedData](
        key: Key[A], initial: A, write: Replicator.WriteConsistency, modify: Callable[[A], A]
    ) -> Replicator.UpdateResponse[A]:
        probe: TestProbe[Replicator.UpdateResponse[A]] = spec.create_test_probe()
        replicator.tell(Replicator.Update(key, initial, write, probe.ref, modify))
        return await probe.receive_message()

    async def assert_value(key: Key[Any], expected: object) -> None:
        async def value() -> None:
            probe: TestProbe[Replicator.GetResponse[Any]] = spec.create_test_probe()
            replicator.tell(Replicator.Get(key, READ_LOCAL, probe.ref))
            g = await probe.receive_message()
            assert isinstance(g, Replicator.GetSuccess) and g.key == key, g
            match g.data:
                case GCounter() | PNCounter() as c:
                    v: object = c.value
                case GSet() | ORSet() as s:
                    v = s.elements
                case other:
                    raise AssertionError(f"unexpected {other!r}")
            assert v == expected, v

        async with spec.within(10.0):
            await spec.await_assert(value, interval=0.05)

    async def assert_deleted(key: Key[Any]) -> None:
        async def deleted() -> None:
            probe: TestProbe[Replicator.GetResponse[Any]] = spec.create_test_probe()
            replicator.tell(Replicator.Get(key, READ_LOCAL, probe.ref))
            await probe.expect_message(Replicator.GetDataDeleted(key))

        async with spec.within(5.0):
            await spec.await_assert(deleted, interval=0.05)

    async def replicate_data_in_initial_phase() -> None:
        for role in (first, second, third, fourth, fifth):
            await join(role, first)

        async def replica_count() -> None:
            probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()
            replicator.tell(Replicator.GetReplicaCount(probe.ref))
            await probe.expect_message(Replicator.ReplicaCount(5))

        async with spec.within(10.0):
            await spec.await_assert(replica_count, interval=0.05)
        await spec.enter_barrier("all-joined")

        if spec.is_node(first):
            probe: TestProbe[Any] = spec.create_test_probe()
            for _ in range(5):
                replicator.tell(
                    Replicator.Update(KeyA, GCounter(), WRITE_LOCAL, probe.ref, plus(1))
                )
                replicator.tell(
                    Replicator.Update(
                        KeyB,
                        PNCounter(),
                        WRITE_LOCAL,
                        probe.ref,
                        lambda c: c.decrement(self_address, 1),
                    )
                )
                replicator.tell(Replicator.Update(KeyC, GCounter(), write_all, probe.ref, plus(1)))
            replies = await probe.receive_messages(15)
            assert {type(r) for r in replies} == {Replicator.UpdateSuccess}

        if spec.is_node(second):
            probe = spec.create_test_probe()
            replicator.tell(Replicator.Update(KeyA, GCounter(), WRITE_LOCAL, probe.ref, plus(20)))
            replicator.tell(
                Replicator.Update(
                    KeyB,
                    PNCounter(),
                    write_two,
                    probe.ref,
                    lambda c: c.increment(self_address, 20),
                )
            )
            replicator.tell(Replicator.Update(KeyC, GCounter(), write_all, probe.ref, plus(20)))
            assert set(await probe.receive_messages(3)) == {
                Replicator.UpdateSuccess(KeyA),
                Replicator.UpdateSuccess(KeyB),
                Replicator.UpdateSuccess(KeyC),
            }

            e: GSet[str] = GSet()
            assert await update(KeyE, e, WRITE_LOCAL, lambda s: s.add("e1").add("e2")) == (
                Replicator.UpdateSuccess(KeyE)
            )

            f: ORSet[str] = ORSet()
            assert await update(
                KeyF, f, WRITE_LOCAL, lambda s: s.add(self_address, "e1").add(self_address, "e2")
            ) == Replicator.UpdateSuccess(KeyF)

        if spec.is_node(fourth):
            assert await update(KeyD, GCounter(), WRITE_LOCAL, plus(40)) == (
                Replicator.UpdateSuccess(KeyD)
            )

            e = GSet()
            assert await update(KeyE, e, WRITE_LOCAL, lambda s: s.add("e2").add("e3")) == (
                Replicator.UpdateSuccess(KeyE)
            )

            f = ORSet()
            assert await update(
                KeyF, f, WRITE_LOCAL, lambda s: s.add(self_address, "e2").add(self_address, "e3")
            ) == Replicator.UpdateSuccess(KeyF)

        if spec.is_node(fifth):
            assert await update(KeyX, GCounter(), write_two, plus(50)) == (
                Replicator.UpdateSuccess(KeyX)
            )
            deleted: TestProbe[Replicator.DeleteResponse[GCounter]] = spec.create_test_probe()
            replicator.tell(Replicator.Delete(KeyX, WRITE_LOCAL, deleted.ref))
            await deleted.expect_message(Replicator.DeleteSuccess(KeyX))

        await spec.enter_barrier("initial-updates-done")

        await assert_value(KeyA, 25)
        await assert_value(KeyB, 15)
        await assert_value(KeyC, 25)
        await assert_value(KeyD, 40)
        await assert_value(KeyE, {"e1", "e2", "e3"})
        await assert_value(KeyF, {"e1", "e2", "e3"})
        await assert_deleted(KeyX)

        await spec.enter_barrier("after-1")

    async def be_available_during_network_split() -> None:
        side1 = (first, second)
        side2 = (third, fourth, fifth)
        if spec.is_node(first):
            for a in side1:
                for b in side2:
                    await spec.test_conductor.blackhole(a, b, Direction.BOTH)
        await spec.enter_barrier("split")

        if spec.is_node(first):
            assert await update(KeyA, GCounter(), write_two, plus(1)) == (
                Replicator.UpdateSuccess(KeyA)
            )

        if spec.is_node(third):
            assert await update(KeyA, GCounter(), write_two, plus(2)) == (
                Replicator.UpdateSuccess(KeyA)
            )

            e: GSet[str] = GSet()
            assert await update(KeyE, e, write_two, lambda s: s.add("e4")) == (
                Replicator.UpdateSuccess(KeyE)
            )

            f: ORSet[str] = ORSet()
            assert await update(KeyF, f, write_two, lambda s: s.remove(self_address, "e2")) == (
                Replicator.UpdateSuccess(KeyF)
            )

        if spec.is_node(fourth):
            assert await update(KeyD, GCounter(), write_two, plus(1)) == (
                Replicator.UpdateSuccess(KeyD)
            )
        await spec.enter_barrier("update-during-split")

        if spec.is_node(*side1):
            await assert_value(KeyA, 26)
            await assert_value(KeyB, 15)
            await assert_value(KeyD, 40)
            await assert_value(KeyE, {"e1", "e2", "e3"})
            await assert_value(KeyF, {"e1", "e2", "e3"})
        if spec.is_node(*side2):
            await assert_value(KeyA, 27)
            await assert_value(KeyB, 15)
            await assert_value(KeyD, 41)
            await assert_value(KeyE, {"e1", "e2", "e3", "e4"})
            await assert_value(KeyF, {"e1", "e3"})
        await spec.enter_barrier("update-during-split-verified")

        if spec.is_node(first):
            await spec.test_conductor.exit(fourth, 0)

        await spec.enter_barrier("after-2")

    async def converge_after_partition() -> None:
        side1 = (first, second)
        side2 = (third, fifth)  # fourth was shutdown
        if spec.is_node(first):
            for a in side1:
                for b in side2:
                    await spec.test_conductor.pass_through(a, b, Direction.BOTH)
        await spec.enter_barrier("split-repaired")

        await assert_value(KeyA, 28)
        await assert_value(KeyB, 15)
        await assert_value(KeyC, 25)
        await assert_value(KeyD, 41)
        await assert_value(KeyE, {"e1", "e2", "e3", "e4"})
        await assert_value(KeyF, {"e1", "e3"})
        await assert_deleted(KeyX)

        await spec.enter_barrier("after-3")

    await replicate_data_in_initial_phase()
    await be_available_during_network_split()
    await converge_after_partition()
