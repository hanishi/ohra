"""Pekko's multi-node ``ReplicatorSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/ReplicatorSpec.scala``),
as one ``@multi_node_spec`` test whose cases run in Pekko's order and share its cluster, with the
spec's own barrier names.

Each case is a section, at its line in the Scala file:

- ``work_in_single_node_cluster``: "work in single node cluster", :103
- ``merge_the_update_with_existing_value``: "merge the update with existing value", :191
- ``reply_with_modify_failure``: "reply with ModifyFailure if exception is thrown by modify
  function", :205
- ``replicate_values_to_new_node``: "replicate values to new node", :211
- ``work_in_2_node_cluster``: "work in 2 node cluster", :243
- ``be_replicated_after_successful_update``: "be replicated after successful update", :293
- ``converge_after_partition``: "converge after partition", :368
- ``support_majority_quorum_with_1_unreachable``: "support majority quorum write and read with 3
  nodes with 1 unreachable", :419
- ``converge_after_many_concurrent_updates``: "converge after many concurrent updates", :522
- ``read_repair_happens_before_get_success``: "read-repair happens before GetSuccess", :541
- ``remote_and_local_update_both_emit_change``: "check that a remote update and a local update
  both cause a change event to emit with the merged data", :556
- ``avoid_duplicate_change_events``: "avoid duplicate change events for same data", :582
- ``support_prefer_oldest_members``: "support prefer oldest members", :606

Mapping:

- Pekko's classic ``replicator ! Get(...)`` with ``expectMsg`` is the typed protocol here, with a
  ``TestProbe`` as ``reply_to``. The typed protocol has no request context, so Pekko's
  ``Some(777)`` and the like are dropped and the replies are compared without them.
- Pekko's spec starts its own Replicator under ``/user/replicator`` with a gossip interval of one
  second and ``withMaxDeltaElements(10)``. Here the ``DistributedData`` extension's Replicator
  runs with ``ReplicatorSettings(gossip_interval=0.1, notify_subscribers_interval=0.05)``, so
  ``max_delta_elements`` keeps its default: deltas are not ported, and the setting only sizes the
  Status chunks.
- "support prefer oldest members" starts a second Replicator under ``/user/oldestReplicator``, as
  Pekko does. Its long gossip interval is Pekko's; ``withDeltaCrdtEnabled(false)`` has nothing to
  turn off.
- The two ``ask``-``mapTo`` checks of issue #27371 use ``ohra.ask`` and the reply's class.

No assertion is skipped: none of them needs pruning, durable storage, deltas or roles.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from ohra import ask
from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    Flag,
    GCounter,
    GCounterKey,
    GSet,
    GSetKey,
    Key,
    LWWRegister,
    LWWRegisterKey,
    ORMap,
    ORMapKey,
    ORSet,
    ORSetKey,
    ReplicatedData,
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
    cluster=ClusterSettings(
        **FAST,
        distributed_data=ReplicatorSettings(gossip_interval=0.1, notify_subscribers_interval=0.05),
    ),
    shutdown=FAST_SHUTDOWN,
)
config.test_transport(on=True)

TIMEOUT = 3.0
ASK_TIMEOUT = 5.0
READ_LOCAL = Replicator.ReadLocal()
WRITE_LOCAL = Replicator.WriteLocal()

KeyA = GCounterKey("A")
KeyB = GCounterKey("B")
KeyC = GCounterKey("C")
KeyD = GCounterKey("D")
KeyE = GCounterKey("E")
KeyE2 = GCounterKey("E2")
KeyF = GCounterKey("F")
KeyG: ORSetKey[str] = ORSetKey("G")
KeyH: ORMapKey[str, Flag] = ORMapKey("H")
KeyI: GSetKey[str] = GSetKey("I")
KeyJ: GSetKey[str] = GSetKey("J")
KeyK: LWWRegisterKey[str] = LWWRegisterKey("K")
KeyX = GCounterKey("X")
KeyY = GCounterKey("Y")
KeyZ = GCounterKey("Z")


def data[A: ReplicatedData](reply: Replicator.GetResponse[A], key: Key[A]) -> A:
    """Pekko's ``expectMsgPF { case g @ GetSuccess(key, _) => g.get(key) }``."""
    assert isinstance(reply, Replicator.GetSuccess) and reply.key == key, reply
    return reply.data


async def changed[A: ReplicatedData](
    probe: TestProbe[Replicator.SubscribeResponse[A]], key: Key[A]
) -> A:
    """Pekko's ``changedProbe.expectMsgPF { case c @ Changed(key) => c.get(key) }``."""
    c = await probe.receive_message()
    assert isinstance(c, Replicator.Changed) and c.key == key, c
    return c.data


@multi_node_spec(config)
async def test_replicator_spec(spec: MultiNodeSpec):
    cluster = Cluster(spec.system)
    replicator = DistributedData(spec.system).replicator
    self_address = DistributedData(spec.system).self_unique_address
    timeout = spec.settings.dilated(TIMEOUT)
    write_two = Replicator.WriteTo(2, timeout)
    write_majority = Replicator.WriteMajority(timeout)
    write_all = Replicator.WriteAll(timeout)
    read_two = Replicator.ReadFrom(2, timeout)
    read_all = Replicator.ReadAll(timeout)
    read_majority = Replicator.ReadMajority(timeout)

    after_counter = 0

    async def enter_barrier_after_test_step() -> None:
        nonlocal after_counter
        after_counter += 1
        await spec.enter_barrier(f"after-{after_counter}")

    async def join(from_: RoleName, to: RoleName) -> None:
        if spec.is_node(from_):
            cluster.manager.tell(Join(Address.parse(spec.node(to))))
        await spec.enter_barrier(from_.name + "-joined")

    def plus(n: int) -> Callable[[GCounter], GCounter]:
        """Pekko's ``_ :+ n``."""
        return lambda c: c.increment(self_address, n)

    async def get[A: ReplicatedData](
        key: Key[A],
        read: Replicator.ReadConsistency = READ_LOCAL,
        max: float | None = None,
        to: ActorRef[Replicator.Command] = replicator,
    ) -> Replicator.GetResponse[A]:
        probe: TestProbe[Replicator.GetResponse[A]] = spec.create_test_probe()
        to.tell(Replicator.Get(key, read, probe.ref))
        return await probe.receive_message(max)

    async def update[A: ReplicatedData](
        key: Key[A],
        initial: A,
        write: Replicator.WriteConsistency,
        modify: Callable[[A], A],
        max: float | None = None,
    ) -> Replicator.UpdateResponse[A]:
        probe: TestProbe[Replicator.UpdateResponse[A]] = spec.create_test_probe()
        replicator.tell(Replicator.Update(key, initial, write, probe.ref, modify))
        return await probe.receive_message(max)

    async def delete[A: ReplicatedData](
        key: Key[A], write: Replicator.WriteConsistency
    ) -> Replicator.DeleteResponse[A]:
        probe: TestProbe[Replicator.DeleteResponse[A]] = spec.create_test_probe()
        replicator.tell(Replicator.Delete(key, write, probe.ref))
        return await probe.receive_message()

    async def await_replica_count(
        n: int, max: float, to: ActorRef[Replicator.Command] = replicator
    ) -> None:
        probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()

        async def replica_count() -> None:
            to.tell(Replicator.GetReplicaCount(probe.ref))
            assert await probe.receive_message() == Replicator.ReplicaCount(n)

        async with spec.within(max):
            await spec.await_assert(replica_count, interval=0.05)

    async def await_assert(fn: Callable[[], Awaitable[None]], max: float | None = None) -> None:
        if max is None:
            await spec.await_assert(fn, interval=0.05)
        else:
            async with spec.within(max):
                await spec.await_assert(fn, interval=0.05)

    async def work_in_single_node_cluster() -> None:
        await join(first, first)

        if spec.is_node(first):
            await await_replica_count(1, 5.0)

            changed_probe: TestProbe[Replicator.SubscribeResponse[GCounter]] = (
                spec.create_test_probe()
            )
            replicator.tell(Replicator.Subscribe(KeyA, changed_probe.ref))
            replicator.tell(Replicator.Subscribe(KeyX, changed_probe.ref))

            assert await get(KeyA) == Replicator.NotFound(KeyA)

            c3 = GCounter().increment(self_address, 3)
            assert await update(KeyA, GCounter(), WRITE_LOCAL, plus(3)) == (
                Replicator.UpdateSuccess(KeyA)
            )
            assert data(await get(KeyA), KeyA) == c3
            assert await changed(changed_probe, KeyA) == c3

            changed_probe2: TestProbe[Replicator.SubscribeResponse[GCounter]] = (
                spec.create_test_probe()
            )
            replicator.tell(Replicator.Subscribe(KeyA, changed_probe2.ref))
            assert await changed(changed_probe2, KeyA) == c3

            c4 = c3.increment(self_address, 1)
            # too strong consistency level
            assert await update(KeyA, GCounter(), write_two, plus(1), TIMEOUT + 1.0) == (
                Replicator.UpdateTimeout(KeyA)
            )
            assert data(await get(KeyA), KeyA) == c4
            assert await changed(changed_probe, KeyA) == c4

            c5 = c4.increment(self_address, 1)
            # too strong consistency level
            assert await update(KeyA, GCounter(), write_majority, plus(1)) == (
                Replicator.UpdateSuccess(KeyA)
            )
            assert data(await get(KeyA, read_majority), KeyA) == c5
            assert await changed(changed_probe, KeyA) == c5

            c6 = c5.increment(self_address, 1)
            assert await update(KeyA, GCounter(), write_all, plus(1)) == (
                Replicator.UpdateSuccess(KeyA)
            )
            assert data(await get(KeyA, read_all), KeyA) == c6
            assert await changed(changed_probe, KeyA) == c6

            c9 = GCounter().increment(self_address, 9)
            assert await update(KeyX, GCounter(), WRITE_LOCAL, plus(9)) == (
                Replicator.UpdateSuccess(KeyX)
            )
            assert await changed(changed_probe, KeyX) == c9
            assert await delete(KeyX, WRITE_LOCAL) == Replicator.DeleteSuccess(KeyX)
            await changed_probe.expect_message(Replicator.Deleted(KeyX))
            assert await get(KeyX) == Replicator.GetDataDeleted(KeyX)
            assert await get(KeyX, read_all) == Replicator.GetDataDeleted(KeyX)
            # verify ask-mapTo for GetDataDeleted, issue #27371
            got = await ask(replicator, lambda r: Replicator.Get(KeyX, READ_LOCAL, r), ASK_TIMEOUT)
            assert type(got) is Replicator.GetDataDeleted

            assert await update(KeyX, GCounter(), WRITE_LOCAL, plus(1)) == (
                Replicator.UpdateDataDeleted(KeyX)
            )
            assert await delete(KeyX, WRITE_LOCAL) == Replicator.DataDeleted(KeyX)
            # verify ask-mapTo for UpdateDataDeleted, issue #27371
            updated = await ask(
                replicator,
                lambda r: Replicator.Update(KeyX, GCounter(), WRITE_LOCAL, r, plus(1)),
                ASK_TIMEOUT,
            )
            assert type(updated) is Replicator.UpdateDataDeleted

            key_ids: TestProbe[Replicator.GetKeyIdsResult] = spec.create_test_probe()
            replicator.tell(Replicator.GetKeyIds(key_ids.ref))
            await key_ids.expect_message(Replicator.GetKeyIdsResult(frozenset({"A"})))

        await enter_barrier_after_test_step()

    async def merge_the_update_with_existing_value() -> None:
        if spec.is_node(first):
            empty: GSet[str] = GSet()
            # in case user is not using the passed in existing value
            assert await update(KeyJ, empty, WRITE_LOCAL, lambda s: s.add("a").add("b")) == (
                Replicator.UpdateSuccess(KeyJ)
            )
            # normal usage would be `lambda s: s.add("c")`
            assert await update(KeyJ, empty, WRITE_LOCAL, lambda _: empty.add("c")) == (
                Replicator.UpdateSuccess(KeyJ)
            )
            assert data(await get(KeyJ), KeyJ) == empty.add("a").add("b").add("c")
        await enter_barrier_after_test_step()

    async def reply_with_modify_failure() -> None:
        e = RuntimeError("errr")

        def fail(_: GCounter) -> GCounter:
            raise e

        failure = await update(KeyA, GCounter(), WRITE_LOCAL, fail)
        assert isinstance(failure, Replicator.ModifyFailure), failure
        assert failure.cause is e

    async def replicate_values_to_new_node() -> None:
        await join(second, first)

        if spec.is_node(first, second):
            await await_replica_count(2, 10.0)

        await spec.enter_barrier("2-nodes")

        if spec.is_node(second):
            changed_probe: TestProbe[Replicator.SubscribeResponse[GCounter]] = (
                spec.create_test_probe()
            )
            replicator.tell(Replicator.Subscribe(KeyA, changed_probe.ref))

            # "A" should be replicated via gossip to the new node
            async def replicated() -> None:
                assert data(await get(KeyA), KeyA).value == 6

            await await_assert(replicated, 5.0)
            assert (await changed(changed_probe, KeyA)).value == 6

        await enter_barrier_after_test_step()

    async def work_in_2_node_cluster() -> None:
        async def read_b(read: Replicator.ReadConsistency, value: int) -> None:
            async def total() -> None:
                assert data(await get(KeyB, read), KeyB).value == value

            await await_assert(total)

        if spec.is_node(first, second):
            # start with 20 on both nodes
            assert await update(KeyB, GCounter(), WRITE_LOCAL, plus(20)) == (
                Replicator.UpdateSuccess(KeyB)
            )
            # add 1 on both nodes using WriteTwo
            assert await update(KeyB, GCounter(), write_two, plus(1)) == (
                Replicator.UpdateSuccess(KeyB)
            )
            # the total, after replication should be 42
            await read_b(read_two, 42)
        await spec.enter_barrier("update-42")

        if spec.is_node(first, second):
            # add 1 on both nodes using WriteAll
            assert await update(KeyB, GCounter(), write_all, plus(1)) == (
                Replicator.UpdateSuccess(KeyB)
            )
            # the total, after replication should be 44
            await read_b(read_all, 44)
        await spec.enter_barrier("update-44")

        if spec.is_node(first, second):
            # add 1 on both nodes using WriteMajority
            assert await update(KeyB, GCounter(), write_majority, plus(1)) == (
                Replicator.UpdateSuccess(KeyB)
            )
            # the total, after replication should be 46
            await read_b(read_majority, 46)

        await enter_barrier_after_test_step()

    async def be_replicated_after_successful_update() -> None:
        changed_probe: TestProbe[Replicator.SubscribeResponse[GCounter]] = spec.create_test_probe()
        if spec.is_node(first, second):
            replicator.tell(Replicator.Subscribe(KeyC, changed_probe.ref))

        if spec.is_node(first):
            assert await update(KeyC, GCounter(), write_two, plus(30)) == (
                Replicator.UpdateSuccess(KeyC)
            )
            assert (await changed(changed_probe, KeyC)).value == 30

            assert await update(KeyY, GCounter(), write_two, plus(30)) == (
                Replicator.UpdateSuccess(KeyY)
            )
            assert await update(KeyZ, GCounter(), write_majority, plus(30)) == (
                Replicator.UpdateSuccess(KeyZ)
            )
        await spec.enter_barrier("update-c30")

        if spec.is_node(second):
            assert data(await get(KeyC), KeyC).value == 30
            assert (await changed(changed_probe, KeyC)).value == 30

            # replicate with gossip after WriteLocal
            assert await update(KeyC, GCounter(), WRITE_LOCAL, plus(1)) == (
                Replicator.UpdateSuccess(KeyC)
            )
            assert (await changed(changed_probe, KeyC)).value == 31

            assert await delete(KeyY, WRITE_LOCAL) == Replicator.DeleteSuccess(KeyY)

            assert data(await get(KeyZ, read_majority), KeyZ).value == 30
        await spec.enter_barrier("update-c31")

        if spec.is_node(first):
            # KeyC and deleted KeyY should be replicated via gossip to the other node
            async def replicated() -> None:
                assert data(await get(KeyC), KeyC).value == 31
                assert await get(KeyY) == Replicator.GetDataDeleted(KeyY)

            await await_assert(replicated, 5.0)
            assert (await changed(changed_probe, KeyC)).value == 31
        await spec.enter_barrier("verified-c31")

        # and also for concurrent updates
        if spec.is_node(first, second):
            assert data(await get(KeyC), KeyC).value == 31

            assert await update(KeyC, GCounter(), WRITE_LOCAL, plus(1)) == (
                Replicator.UpdateSuccess(KeyC)
            )

            async def converged() -> None:
                assert data(await get(KeyC), KeyC).value == 33

            await await_assert(converged, 5.0)

        await enter_barrier_after_test_step()

    async def converge_after_partition() -> None:
        if spec.is_node(first):
            assert await update(KeyD, GCounter(), write_two, plus(40)) == (
                Replicator.UpdateSuccess(KeyD)
            )
            await spec.test_conductor.blackhole(first, second, Direction.BOTH)
        await spec.enter_barrier("blackhole-first-second")

        if spec.is_node(first, second):
            assert data(await get(KeyD), KeyD).value == 40
            initial = GCounter().increment(self_address, 1)
            assert await update(KeyD, initial, write_two, plus(1), TIMEOUT + 1.0) == (
                Replicator.UpdateTimeout(KeyD)
            )
            assert await update(KeyD, GCounter(), write_two, plus(1), TIMEOUT + 1.0) == (
                Replicator.UpdateTimeout(KeyD)
            )
        if spec.is_node(first):
            for n in range(1, 31):
                key_dn = GCounterKey(f"D{n}")
                assert await update(key_dn, GCounter(), WRITE_LOCAL, plus(n)) == (
                    Replicator.UpdateSuccess(key_dn)
                )
        await spec.enter_barrier("updates-during-partion")

        if spec.is_node(first):
            await spec.test_conductor.pass_through(first, second, Direction.BOTH)
        await spec.enter_barrier("passThrough-first-second")

        if spec.is_node(first, second):
            assert data(await get(KeyD, read_two), KeyD).value == 44

            async def converged() -> None:
                for n in range(1, 31):
                    key_dn = GCounterKey(f"D{n}")
                    assert data(await get(key_dn), key_dn).value == n

            await await_assert(converged, 10.0)

        await enter_barrier_after_test_step()

    async def support_majority_quorum_with_1_unreachable() -> None:
        await join(third, first)

        await await_replica_count(3, 10.0)
        await spec.enter_barrier("3-nodes")

        assert await update(KeyE, GCounter(), write_majority, plus(50)) == (
            Replicator.UpdateSuccess(KeyE)
        )
        await spec.enter_barrier("write-initial-majority")

        assert data(await get(KeyE, read_majority), KeyE).value == 150
        await spec.enter_barrier("read-initial-majority")

        if spec.is_node(first):
            await spec.test_conductor.blackhole(first, third, Direction.BOTH)
            await spec.test_conductor.blackhole(second, third, Direction.BOTH)
        await spec.enter_barrier("blackhole-third")

        if spec.is_node(second):
            assert await update(KeyE, GCounter(), WRITE_LOCAL, plus(1)) == (
                Replicator.UpdateSuccess(KeyE)
            )
        await spec.enter_barrier("local-update-from-second")

        async def majority_update_from_first() -> None:
            # ReadMajority should retrieve the previous update from second, before applying the
            # modification
            probe1: TestProbe[int] = spec.create_test_probe()
            probe2: TestProbe[Any] = spec.create_test_probe()
            replicator.tell(Replicator.Get(KeyE, read_majority, probe2.ref))
            await probe2.expect_message_type(Replicator.GetSuccess)

            def modify(c: GCounter) -> GCounter:
                probe1.ref.tell(c.value)
                return c.increment(self_address, 1)

            replicator.tell(
                Replicator.Update(KeyE, GCounter(), write_majority, probe2.ref, modify)
            )
            # verify read your own writes, without waiting for the UpdateSuccess reply
            # note that the order of the replies are not defined, and therefore we use separate
            # probes
            probe3: TestProbe[Replicator.GetResponse[GCounter]] = spec.create_test_probe()
            replicator.tell(Replicator.Get(KeyE, read_majority, probe3.ref))
            await probe1.expect_message(151)
            await probe2.expect_message(Replicator.UpdateSuccess(KeyE))
            assert data(await probe3.receive_message(), KeyE).value == 152

        async def majority_update_from_second() -> None:
            probe1: TestProbe[Any] = spec.create_test_probe()
            replicator.tell(Replicator.Get(KeyE, read_majority, probe1.ref))
            await probe1.expect_message_type(Replicator.GetSuccess)
            replicator.tell(
                Replicator.Update(KeyE, GCounter(), write_majority, probe1.ref, plus(1))
            )
            # verify read your own writes, without waiting for the UpdateSuccess reply
            # note that the order of the replies are not defined, and therefore we use separate
            # probes
            probe2: TestProbe[Replicator.UpdateResponse[GCounter]] = spec.create_test_probe()
            replicator.tell(
                Replicator.Update(KeyE, GCounter(), write_majority, probe2.ref, plus(1))
            )
            probe3: TestProbe[Replicator.UpdateResponse[GCounter]] = spec.create_test_probe()
            replicator.tell(
                Replicator.Update(KeyE, GCounter(), write_majority, probe3.ref, plus(1))
            )
            probe5: TestProbe[Replicator.GetResponse[GCounter]] = spec.create_test_probe()
            replicator.tell(Replicator.Get(KeyE, read_majority, probe5.ref))
            await probe1.expect_message(Replicator.UpdateSuccess(KeyE))
            await probe2.expect_message(Replicator.UpdateSuccess(KeyE))
            await probe3.expect_message(Replicator.UpdateSuccess(KeyE))
            assert data(await probe5.receive_message(), KeyE).value == 155

        await spec.run_on(first, thunk=majority_update_from_first)
        await spec.enter_barrier("majority-update-from-first")

        await spec.run_on(second, thunk=majority_update_from_second)
        await spec.enter_barrier("majority-update-from-second")

        if spec.is_node(first, second):
            assert await get(KeyE2, read_all, TIMEOUT + 1.0) == Replicator.GetFailure(KeyE2)
            assert await get(KeyE2) == Replicator.NotFound(KeyE2)
        await spec.enter_barrier("read-all-fail-update")

        if spec.is_node(first):
            await spec.test_conductor.pass_through(first, third, Direction.BOTH)
            await spec.test_conductor.pass_through(second, third, Direction.BOTH)
        await spec.enter_barrier("passThrough-third")

        if spec.is_node(third):
            assert data(await get(KeyE, read_majority), KeyE).value == 155

        await enter_barrier_after_test_step()

    async def converge_after_many_concurrent_updates() -> None:
        async with spec.within(10.0):
            probe: TestProbe[Replicator.UpdateResponse[GCounter]] = spec.create_test_probe()
            for _ in range(100):
                replicator.tell(Replicator.Update(KeyF, GCounter(), write_two, probe.ref, plus(1)))
            results = await probe.receive_messages(100, 10.0)
            assert {type(r) for r in results} == {Replicator.UpdateSuccess}
            await spec.enter_barrier("100-updates-done")
            assert data(await get(KeyF, read_two), KeyF).value == 3 * 100
            await enter_barrier_after_test_step()

    async def read_repair_happens_before_get_success() -> None:
        if spec.is_node(first):
            empty: ORSet[str] = ORSet()
            reply = await update(
                KeyG, empty, write_two, lambda s: s.add(self_address, "a").add(self_address, "b")
            )
            assert isinstance(reply, Replicator.UpdateSuccess), reply
        await spec.enter_barrier("a-b-added-to-G")
        if spec.is_node(second):
            assert data(await get(KeyG, read_all), KeyG).elements == {"a", "b"}
            assert data(await get(KeyG), KeyG).elements == {"a", "b"}
        await enter_barrier_after_test_step()

    async def remote_and_local_update_both_emit_change() -> None:
        changed_probe: TestProbe[Replicator.SubscribeResponse[ORMap[str, Flag]]] = (
            spec.create_test_probe()
        )
        # Pekko sends the replies to testActor and never reads them
        ignored: TestProbe[Replicator.UpdateResponse[ORMap[str, Flag]]] = spec.create_test_probe()
        empty: ORMap[str, Flag] = ORMap()

        def put(key: str, flag: Flag) -> Replicator.Update[ORMap[str, Flag]]:
            return Replicator.Update(
                KeyH, empty, write_two, ignored.ref, lambda m: m.put(self_address, key, flag)
            )

        if spec.is_node(second):
            replicator.tell(Replicator.Subscribe(KeyH, changed_probe.ref))
            replicator.tell(put("a", Flag.Disabled))
            assert (await changed(changed_probe, KeyH)).entries == {"a": Flag.Disabled}

        await spec.enter_barrier("update-h1")

        if spec.is_node(first):
            replicator.tell(put("a", Flag.Enabled))

        if spec.is_node(second):
            assert (await changed(changed_probe, KeyH)).entries == {"a": Flag.Enabled}

            replicator.tell(put("b", Flag.Enabled))
            assert (await changed(changed_probe, KeyH)).entries == {
                "a": Flag.Enabled,
                "b": Flag.Enabled,
            }

        await enter_barrier_after_test_step()

    async def avoid_duplicate_change_events() -> None:
        changed_probe: TestProbe[Replicator.SubscribeResponse[GSet[str]]] = (
            spec.create_test_probe()
        )
        ignored: TestProbe[Replicator.UpdateResponse[GSet[str]]] = spec.create_test_probe()
        empty: GSet[str] = GSet()
        replicator.tell(Replicator.Subscribe(KeyI, changed_probe.ref))
        await spec.enter_barrier("subscribed-I")

        if spec.is_node(second):
            replicator.tell(
                Replicator.Update(KeyI, empty, write_two, ignored.ref, lambda a: a.add("a"))
            )

        async with spec.within(5.0):  # gossip to third
            c = await changed_probe.receive_message(5.0)
            assert isinstance(c, Replicator.Changed) and c.key == KeyI, c
            assert c.data.elements == {"a"}

        await spec.enter_barrier("update-I")

        if spec.is_node(first):
            replicator.tell(
                Replicator.Update(KeyI, empty, write_two, ignored.ref, lambda s: s.add("a"))
            )

        await changed_probe.expect_no_message(1.0)

        await enter_barrier_after_test_step()

    async def support_prefer_oldest_members() -> None:
        # disable gossip and delta replication to only verify the write and read operations
        oldest_replicator = spec.spawn(
            Replicator.behavior(ReplicatorSettings(prefer_oldest=True, gossip_interval=60.0)),
            "oldestReplicator",
        )
        await await_replica_count(3, 5.0, oldest_replicator)
        await spec.enter_barrier("oldest-replicator-started")

        if spec.is_node(second):
            probe: TestProbe[Replicator.UpdateResponse[LWWRegister[str]]] = (
                spec.create_test_probe()
            )
            oldest_replicator.tell(
                Replicator.Update(
                    KeyK,
                    LWWRegister.create(self_address, "0"),
                    write_two,
                    probe.ref,
                    lambda r: r.with_value(self_address, "1"),
                )
            )
            await probe.expect_message(Replicator.UpdateSuccess(KeyK))
        await spec.enter_barrier("updated-1")

        if spec.is_node(first):
            # replicated to oldest
            assert data(await get(KeyK, to=oldest_replicator), KeyK).value == "1"

        if spec.is_node(third):
            # not replicated to third (not among the two oldest)
            assert await get(KeyK, to=oldest_replicator) == Replicator.NotFound(KeyK)

            # read from oldest
            assert data(await get(KeyK, read_two, to=oldest_replicator), KeyK).value == "1"

        await enter_barrier_after_test_step()

    await work_in_single_node_cluster()
    await merge_the_update_with_existing_value()
    await reply_with_modify_failure()
    await replicate_values_to_new_node()
    await work_in_2_node_cluster()
    await be_replicated_after_successful_update()
    await converge_after_partition()
    await support_majority_quorum_with_1_unreachable()
    await converge_after_many_concurrent_updates()
    await read_repair_happens_before_get_success()
    await remote_and_local_update_both_emit_change()
    await avoid_duplicate_change_events()
    await support_prefer_oldest_members()
