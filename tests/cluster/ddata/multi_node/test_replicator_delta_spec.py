"""Pekko's multi-node ``ReplicatorDeltaSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/ReplicatorDeltaSpec.scala``),
as one ``@multi_node_spec`` test whose cases run in Pekko's order and share its cluster, with the
spec's own barrier names.

Each case is a section, at its line in the Scala file:

- ``join_cluster``: "join cluster", :148
- ``propagate_delta``: "propagate delta", :165
- ``work_with_write_consistency``: "work with write consistency", :217
- ``preserve_causal_consistency_for_none_delta``: "preserve causal consistency for None delta",
  :260
- ``be_eventually_consistent``: "be eventually consistent", :355

The operations generator (:68-:128, ``generateOperations``) is ported whole: 31 to 40 operations
of a delay, an increment or a decrement of a PNCounter, and an add or a remove on an ORSet, with
the write consistency drawn 90/5/5 from ``WriteLocal``, ``WriteTo(2)`` and ``WriteMajority``.

Mapping:

- Pekko starts two Replicators per node with ``system.actorOf``: ``fullStateReplicator`` (gossip
  interval one second, ``withDeltaCrdtEnabled(false)``) and ``deltaReplicator`` (default
  settings, then ``TestFullStateGossip(enabled = false)``). Here both are
  ``Replicator.behavior(settings)`` spawned with ``spec.spawn`` under the same names, so each one
  finds its peers on the other nodes by its own path. Nothing is shortened: the intervals are
  Pekko's (gossip one second for the full-state one, the default two for the delta one, so its
  delta propagation tick is 0.4 s).
- Pekko's classic ``replicator ! Update(...)`` with ``ImplicitSender`` is the typed protocol, with
  a ``TestProbe`` as ``reply_to``; the replies carry no request context, so Pekko's
  ``UpdateSuccess(key, None)`` is ``Replicator.UpdateSuccess(key)``. The random operations are
  fire and forget in Pekko (the replies go to the unread testActor); here their replies go to a
  probe that is never read.
- ``Replicator.Internal.DeltaPropagationTick`` is the Replicator's private
  ``_DeltaPropagationTick``.
- ``Highest`` is a frozen ``@remotable`` dataclass. Pekko's ``delta: Option[Highest]`` field is a
  ``transient()`` field, which ohra never sends, as it never sends a ``delta`` (the delta itself
  is a ``Highest`` without a delta, as in Pekko).
- ``ThreadLocalRandom`` is ``random.Random(seed)`` with a seed drawn per node and per run. The
  seed is in the failure message, as a note on the exception, together with the operations, as
  Pekko's ``info(...)`` prints them.
- ``Thread.sleep(d)`` of the ``Delay`` operation is ``asyncio.sleep(d / 1000)``.
- ``join`` is called again in "propagate delta", as in Pekko: a ``Join`` to the node the sender
  is already a member of changes nothing, and the barriers are entered again.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DeltaReplicatedData,
    DistributedData,
    Key,
    ORSet,
    ORSetKey,
    PNCounter,
    PNCounterKey,
    ReplicatedData,
    Replicator,
    ReplicatorSettings,
)
from ohra.cluster.ddata.replicated_data import RequiresCausalDeliveryOfDeltas
from ohra.cluster.ddata.replicator import _DeltaPropagationTick  # pyright: ignore[reportPrivateUsage]
from ohra.ref import ActorRef
from ohra.remote import Address
from ohra.remote.serialization import remotable, transient
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, RoleName, TestProbe, multi_node_spec

from ...test_cluster import FAST, FAST_SHUTDOWN

config = MultiNodeConfig()
first, second, third, fourth = (
    config.role(name) for name in ("first", "second", "third", "fourth")
)
config.common_config(cluster=ClusterSettings(**FAST), shutdown=FAST_SHUTDOWN)
config.test_transport(on=True)


@remotable
@dataclass(frozen=True)
class Highest(DeltaReplicatedData["Highest"], RequiresCausalDeliveryOfDeltas):
    n: int
    delta: Highest | None = transient()

    def merge(self, that: Highest) -> Highest:  # pyright: ignore[reportIncompatibleMethodOverride]
        return self if self.n >= that.n else that

    def merge_delta(self, that_delta: Highest) -> Highest:
        return self.merge(that_delta)

    def zero(self) -> Highest:
        return Highest(0)

    def reset_delta(self) -> Highest:
        return Highest(self.n)

    def incr(self, i: int) -> Highest:
        return Highest(self.n + i, Highest(self.n + i))

    def incr_no_delta(self, i: int) -> Highest:
        return Highest(self.n + i, None)


@dataclass(frozen=True)
class Delay:
    n: int


@dataclass(frozen=True)
class Incr:
    key: PNCounterKey
    n: int
    consistency: Replicator.WriteConsistency


@dataclass(frozen=True)
class Decr:
    key: PNCounterKey
    n: int
    consistency: Replicator.WriteConsistency


@dataclass(frozen=True)
class Add:
    key: ORSetKey[str]
    elem: str
    consistency: Replicator.WriteConsistency


@dataclass(frozen=True)
class Remove:
    key: ORSetKey[str]
    elem: str
    consistency: Replicator.WriteConsistency


type Op = Delay | Incr | Decr | Add | Remove

TIMEOUT = 5.0
WRITE_TWO = Replicator.WriteTo(2, TIMEOUT)
WRITE_MAJORITY = Replicator.WriteMajority(TIMEOUT)
WRITE_LOCAL = Replicator.WriteLocal()
READ_LOCAL = Replicator.ReadLocal()

KeyHigh: Key[Highest] = Key("High")
KeyA = PNCounterKey("A")
KeyB = PNCounterKey("B")
KeyC = PNCounterKey("C")
KeyD: ORSetKey[str] = ORSetKey("D")
KeyE: ORSetKey[str] = ORSetKey("E")
KeyF: ORSetKey[str] = ORSetKey("F")


def generate_operations(rnd: random.Random, on_node: RoleName) -> list[Op]:
    def consistency() -> Replicator.WriteConsistency:
        n = rnd.randrange(100)
        if n < 90:
            return WRITE_LOCAL
        if n < 95:
            return WRITE_TWO
        return WRITE_MAJORITY

    def rnd_pn_counter_key() -> PNCounterKey:
        return (KeyA, KeyB, KeyC)[rnd.randrange(3)]

    def rnd_or_set_key() -> ORSetKey[str]:
        return (KeyD, KeyE, KeyF)[rnd.randrange(3)]

    available_for_remove: set[str] = set()

    def rnd_add_element() -> str:
        # lower case a - j
        s = chr(97 + rnd.randrange(10))
        available_for_remove.add(s)
        return s

    def rnd_remove_element() -> str:
        if not available_for_remove:
            return "a"
        return list(available_for_remove)[rnd.randrange(len(available_for_remove))]

    def operation() -> Op:
        match rnd.randrange(4):
            case 0:
                return Delay(rnd.randrange(500))
            case 1:
                return Incr(rnd_pn_counter_key(), rnd.randrange(100), consistency())
            case 2:
                return Decr(rnd_pn_counter_key(), rnd.randrange(10), consistency())
            case _:
                # ORSet
                key = rnd_or_set_key()
                # only removals for KeyF on node first
                if key == KeyF and on_node == first and rnd.choice((True, False)):
                    return Remove(key, rnd_remove_element(), consistency())
                return Add(key, rnd_add_element(), consistency())

    # Scala's (0 to (30 + rnd.nextInt(10))) is inclusive
    return [operation() for _ in range(31 + rnd.randrange(10))]


@multi_node_spec(config)
async def test_replicator_delta_spec(spec: MultiNodeSpec):
    cluster = Cluster(spec.system)
    self_address = DistributedData(spec.system).self_unique_address

    full_state_replicator: ActorRef[Any] = spec.spawn(
        Replicator.behavior(ReplicatorSettings(gossip_interval=1.0, delta_crdt_enabled=False)),
        "fullStateReplicator",
    )
    delta_replicator: ActorRef[Any] = spec.spawn(
        Replicator.behavior(ReplicatorSettings()), "deltaReplicator"
    )
    delta_replicator.tell(Replicator.TestFullStateGossip(enabled=False))

    write_all = Replicator.WriteAll(5.0)
    ignored: TestProbe[Any] = spec.create_test_probe()  # the replies of fire and forget updates

    after_counter = 0

    async def enter_barrier_after_test_step() -> None:
        nonlocal after_counter
        after_counter += 1
        await spec.enter_barrier("after-" + str(after_counter))

    async def join(from_: RoleName, to: RoleName) -> None:
        if spec.is_node(from_):
            cluster.manager.tell(Join(Address.parse(spec.node(to))))
        await spec.enter_barrier(from_.name + "-joined")

    def update[A: ReplicatedData](
        replicator: ActorRef[Any],
        key: Key[A],
        initial: A,
        write: Replicator.WriteConsistency,
        modify: Callable[[A], A],
        reply_to: TestProbe[Any] | None = None,
    ) -> None:
        reply_to = ignored if reply_to is None else reply_to
        replicator.tell(Replicator.Update(key, initial, write, reply_to.ref, modify))

    async def get[A: ReplicatedData](replicator: ActorRef[Any], key: Key[A]) -> A:
        probe: TestProbe[Replicator.GetResponse[A]] = spec.create_test_probe()
        replicator.tell(Replicator.Get(key, READ_LOCAL, probe.ref))
        reply = await probe.receive_message()
        assert isinstance(reply, Replicator.GetSuccess), reply
        return reply.get(key)

    async def await_replica_count(n: int) -> None:
        async def replica_count() -> None:
            probe: TestProbe[Replicator.ReplicaCount] = spec.create_test_probe()
            full_state_replicator.tell(Replicator.GetReplicaCount(probe.ref))
            await probe.expect_message(Replicator.ReplicaCount(n))

        async with spec.within(15.0):
            await spec.await_assert(replica_count)

    async def join_cluster() -> None:
        await join(first, first)
        await join(second, first)
        await join(third, first)
        await join(fourth, first)

        await await_replica_count(4)

        await enter_barrier_after_test_step()

    async def propagate_delta() -> None:
        await join(first, first)
        await join(second, first)
        await join(third, first)
        await join(fourth, first)

        await await_replica_count(4)
        await spec.enter_barrier("ready")

        if spec.is_node(first):
            # by setting something for each key we don't have to worry about NotFound
            for key in (KeyA, KeyB, KeyC):
                for r in (full_state_replicator, delta_replicator):
                    update(
                        r, key, PNCounter(), WRITE_LOCAL, lambda c: c.increment(self_address, 1)
                    )
            for set_key in (KeyD, KeyE, KeyF):
                for r in (full_state_replicator, delta_replicator):
                    update(
                        r,
                        set_key,
                        ORSet[str](),
                        WRITE_LOCAL,
                        lambda s: s.add(self_address, "a"),
                    )
        await spec.enter_barrier("updated-1")

        async def counters() -> None:
            for key in (KeyA, KeyB, KeyC):
                assert (await get(full_state_replicator, key)).value == 1

        async def sets() -> None:
            for set_key in (KeyD, KeyE, KeyF):
                assert (await get(delta_replicator, set_key)).elements == {"a"}

        async with spec.within(5.0):
            await spec.await_assert(counters)
            await spec.await_assert(sets)

        await enter_barrier_after_test_step()

    async def work_with_write_consistency() -> None:
        if spec.is_node(first):
            p1: TestProbe[Any] = spec.create_test_probe()
            for r in (full_state_replicator, delta_replicator):
                update(
                    r,
                    KeyD,
                    ORSet[str](),
                    write_all,
                    lambda s: s.add(self_address, "A"),
                    p1,
                )
            await p1.expect_message_type(Replicator.UpdateSuccess)
            await p1.expect_message_type(Replicator.UpdateSuccess)
        await spec.enter_barrier("write-1")

        assert (await get(delta_replicator, KeyD)).elements == {"a", "A"}
        await spec.enter_barrier("read-1")

        # and also when doing several at the same time (deltas may be reordered) and then we
        # retry with full state to sort it out
        if spec.is_node(first):
            p1 = spec.create_test_probe()
            for elem in ("B", "C", "D"):
                update(
                    delta_replicator,
                    KeyD,
                    ORSet[str](),
                    write_all,
                    lambda s, e=elem: s.add(self_address, e),
                    p1,
                )
            await p1.expect_message_type(Replicator.UpdateSuccess)
            await p1.expect_message_type(Replicator.UpdateSuccess)
            await p1.expect_message_type(Replicator.UpdateSuccess)
        await spec.enter_barrier("write-2")
        assert (await get(delta_replicator, KeyD)).elements == {"a", "A", "B", "C", "D"}

        # add same to the fullStateReplicator so they are in sync
        if spec.is_node(first):
            p1 = spec.create_test_probe()
            update(
                full_state_replicator,
                KeyD,
                ORSet[str](),
                write_all,
                lambda s: (
                    s.add(self_address, "A")
                    .add(self_address, "B")
                    .add(self_address, "C")
                    .add(self_address, "D")
                ),
                p1,
            )
            await p1.expect_message_type(Replicator.UpdateSuccess)
        await spec.enter_barrier("write-3")
        assert (await get(full_state_replicator, KeyD)).elements == {"a", "A", "B", "C", "D"}

        await enter_barrier_after_test_step()

    async def preserve_causal_consistency_for_none_delta() -> None:
        async def high(n: int) -> None:
            assert (await get(delta_replicator, KeyHigh)).n == n

        if spec.is_node(first):
            p1: TestProbe[Any] = spec.create_test_probe()
            update(delta_replicator, KeyHigh, Highest(0), WRITE_LOCAL, lambda h: h.incr(1), p1)
            await p1.expect_message_type(Replicator.UpdateSuccess)
        await spec.enter_barrier("write-1")

        if spec.is_node(first):
            await high(1)
        if spec.is_node(second, third, fourth):
            async with spec.within(5.0):
                await spec.await_assert(lambda: high(1))
        await spec.enter_barrier("read-1")

        if spec.is_node(first):
            p1 = spec.create_test_probe()
            update(delta_replicator, KeyHigh, Highest(0), write_all, lambda h: h.incr(2), p1)
            await p1.expect_message_type(Replicator.UpdateSuccess)
            update(
                delta_replicator,
                KeyHigh,
                Highest(0),
                WRITE_LOCAL,
                lambda h: h.incr_no_delta(5),
                p1,
            )
            update(delta_replicator, KeyHigh, Highest(0), WRITE_LOCAL, lambda h: h.incr(10), p1)
            await p1.expect_message_type(Replicator.UpdateSuccess)
            await p1.expect_message_type(Replicator.UpdateSuccess)
        await spec.enter_barrier("write-2")

        if spec.is_node(first):
            await high(18)
        if spec.is_node(second, third, fourth):
            # the incrNoDelta(5) is not propagated as delta, and then incr(10) is also skipped
            async with spec.within(5.0):
                await spec.await_assert(lambda: high(3))
        await spec.enter_barrier("read-2")

        if spec.is_node(first):
            p1 = spec.create_test_probe()
            # WriteAll will send full state when delta can't be applied and thereby syncing the
            # delta versions again. Same would happen via full state gossip.
            # Thereafter delta can be propagated and applied again.
            update(delta_replicator, KeyHigh, Highest(0), write_all, lambda h: h.incr(100), p1)
            await p1.expect_message_type(Replicator.UpdateSuccess)
            # Flush the deltaPropagation buffer, otherwise it will contain
            # NoDeltaPlaceholder from previous updates and the incr(4) delta will also
            # be folded into NoDeltaPlaceholder and not propagated as delta. A few
            # DeltaPropagationTick are needed to send to all and flush buffer.
            for _ in spec.roles:
                delta_replicator.tell(_DeltaPropagationTick())
            update(delta_replicator, KeyHigh, Highest(0), WRITE_LOCAL, lambda h: h.incr(4), p1)
            await p1.expect_message_type(Replicator.UpdateSuccess)
        await spec.enter_barrier("write-3")

        async with spec.within(5.0):
            await spec.await_assert(lambda: high(122))

        await enter_barrier_after_test_step()

    async def be_eventually_consistent() -> None:
        seed = random.randrange(2**32)
        operations = generate_operations(random.Random(seed), on_node=spec.myself)
        try:
            # perform random operations with both delta and full-state replicators
            # and compare that the end result is the same
            replicators = (full_state_replicator, delta_replicator)

            for op in operations:
                match op:
                    case Delay(d):
                        await asyncio.sleep(d / 1000)
                    case Incr(key, n, consistency):
                        for r in replicators:
                            update(
                                r,
                                key,
                                PNCounter(),
                                consistency,
                                lambda c, n=n: c.increment(self_address, n),
                            )
                    case Decr(key, n, consistency):
                        for r in replicators:
                            update(
                                r,
                                key,
                                PNCounter(),
                                consistency,
                                lambda c, n=n: c.decrement(self_address, n),
                            )
                    case Add(key, elem, consistency):
                        # to have an deterministic result when mixing add/remove we can only
                        # perform the ORSet operations from one node
                        nodes = (first,) if key == KeyF else (first, second, third)
                        if spec.is_node(*nodes):
                            for r in replicators:
                                update(
                                    r,
                                    key,
                                    ORSet[str](),
                                    consistency,
                                    lambda s, elem=elem: s.add(self_address, elem),
                                )
                    case Remove(key, elem, consistency):
                        if spec.is_node(first):
                            for r in replicators:
                                update(
                                    r,
                                    key,
                                    ORSet[str](),
                                    consistency,
                                    lambda s, elem=elem: s.remove(self_address, elem),
                                )

            await spec.enter_barrier("updated-2")

            for counter_key in (KeyA, KeyB, KeyC):

                async def same_counter(k: PNCounterKey = counter_key) -> None:
                    full_state_value = await get(full_state_replicator, k)
                    delta_value = await get(delta_replicator, k)
                    assert delta_value == full_state_value, f"seed {seed}: {k}"

                async with spec.within(5.0):
                    await spec.await_assert(same_counter)

            for set_key in (KeyD, KeyE, KeyF):

                async def same_set(k: ORSetKey[str] = set_key) -> None:
                    full_state_value = (await get(full_state_replicator, k)).elements
                    delta_value = (await get(delta_replicator, k)).elements
                    assert delta_value == full_state_value, f"seed {seed}: {k}"

                async with spec.within(5.0):
                    await spec.await_assert(same_set)

            await enter_barrier_after_test_step()
        except Exception as e:
            e.add_note(f"seed {seed}, random operations on [{spec.myself.name}]: {operations}")
            raise

    await join_cluster()
    await propagate_delta()
    await work_with_write_consistency()
    await preserve_causal_consistency_for_none_delta()
    await be_eventually_consistent()
