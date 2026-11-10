"""Pekko's multi-node ``ReplicatorMapDeltaSpec``
(``distributed-data/src/multi-jvm/scala/org/apache/pekko/cluster/ddata/ReplicatorMapDeltaSpec.scala``),
as one ``@multi_node_spec`` test whose cases run in Pekko's order and share its cluster, with the
spec's own barrier names.

Each case is a section, at its line in the Scala file:

- ``join_cluster``: "join cluster", :166
- ``propagate_delta``: "propagate delta", :183
- ``replicate_high_throughput_changes``: "replicate high throughput changes without
  OversizedPayloadException", :264
- ``be_eventually_consistent``: "be eventually consistent", :292

The operations generator (:90-:167, ``generateOperations``) is ported whole: 51 to 60 operations
of a delay, an increment or a decrement of a PNCounterMap, and an add or a remove on an
ORMultiMap with value deltas, a vanilla ORMultiMap and a vanilla ORMap holding ORSets, with the
write consistency drawn 90/5/5 (it is drawn, and then ignored, as in Pekko, which updates with
``WriteLocal``).

Mapping:

- Pekko starts three Replicators per node with ``system.actorOf``: ``fullStateReplicator`` (gossip
  interval one second, deltas off), ``deltaReplicator`` (default settings, full-state gossip off)
  and ``ordinaryReplicator`` (gossip interval one second, both). Here all three are
  ``Replicator.behavior(settings)`` spawned with ``spec.spawn`` under the same names, so each
  finds its peers on the other nodes by its own path. No interval is shortened.
- Pekko's classic ``replicator ! Update(...)`` with ``ImplicitSender`` is the typed protocol, with
  a ``TestProbe`` as ``reply_to``. The random operations are fire and forget in Pekko; here their
  replies go to a probe that is never read.
- Pekko's ``errorLogProbe`` subscribes to the event stream's ``Error`` events, to see that no
  ``OversizedPayloadException`` is logged. ohra has no such exception: a replica message over the
  frame limit (``MAX_FRAME``) is logged as an error by ``send_to_replica`` and not sent. So the
  check is ``errors_logged()``: no record at ERROR or above from the start of that case. All
  nodes log to the same logger, so it sees every node's errors, not only this one's.
- ``Thread.sleep(d)`` of the ``Delay`` operation is ``asyncio.sleep(d / 1000)``.
- ``ThreadLocalRandom`` is ``random.Random(seed)`` with a seed drawn per node and per run. The
  seed is in the failure message, as a note on the exception, together with the operations, as
  Pekko's ``info(...)`` prints them.
- ``join`` is called again in "propagate delta", as in Pekko.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ohra.cluster import Cluster, ClusterSettings, Join
from ohra.cluster.ddata import (
    DistributedData,
    Key,
    ORMap,
    ORMapKey,
    ORMultiMap,
    ORMultiMapKey,
    ORSet,
    PNCounterMap,
    PNCounterMapKey,
    ReplicatedData,
    Replicator,
    ReplicatorSettings,
)
from ohra.ref import ActorRef
from ohra.remote import Address
from ohra.testkit import MultiNodeConfig, MultiNodeSpec, RoleName, TestProbe, multi_node_spec

from ....util import errors_logged
from ...test_cluster import FAST, FAST_SHUTDOWN

config = MultiNodeConfig()
first, second, third, fourth = (
    config.role(name) for name in ("first", "second", "third", "fourth")
)
config.common_config(cluster=ClusterSettings(**FAST), shutdown=FAST_SHUTDOWN)
config.test_transport(on=True)

type PNKey = tuple[PNCounterMapKey[str], str]
type MMKey = tuple[ORMultiMapKey[str, str], str]
type OMKey = tuple[ORMapKey[str, ORSet[str]], str]


@dataclass(frozen=True)
class Delay:
    n: int


@dataclass(frozen=True)
class Incr:
    ki: PNKey
    n: int
    consistency: Replicator.WriteConsistency


@dataclass(frozen=True)
class Decr:
    ki: PNKey
    n: int
    consistency: Replicator.WriteConsistency


# AddVD and RemoveVD for variant of ORMultiMap with Value Deltas, NoVD - for the vanilla ORMultiMap
@dataclass(frozen=True)
class AddVD:
    ki: MMKey
    elem: str
    consistency: Replicator.WriteConsistency


@dataclass(frozen=True)
class RemoveVD:
    ki: MMKey
    elem: str
    consistency: Replicator.WriteConsistency


@dataclass(frozen=True)
class AddNoVD:
    ki: MMKey
    elem: str
    consistency: Replicator.WriteConsistency


@dataclass(frozen=True)
class RemoveNoVD:
    ki: MMKey
    elem: str
    consistency: Replicator.WriteConsistency


# AddOM and RemoveOM for Vanilla ORMap holding ORSet inside
@dataclass(frozen=True)
class AddOM:
    ki: OMKey
    elem: str
    consistency: Replicator.WriteConsistency


@dataclass(frozen=True)
class RemoveOM:
    ki: OMKey
    elem: str
    consistency: Replicator.WriteConsistency


type Op = Delay | Incr | Decr | AddVD | RemoveVD | AddNoVD | RemoveNoVD | AddOM | RemoveOM

TIMEOUT = 5.0
WRITE_TWO = Replicator.WriteTo(2, TIMEOUT)
WRITE_MAJORITY = Replicator.WriteMajority(TIMEOUT)
WRITE_LOCAL = Replicator.WriteLocal()
READ_LOCAL = Replicator.ReadLocal()

KeyPN: PNCounterMapKey[str] = PNCounterMapKey("A")
# VD and NoVD as above
KeyMMVD: ORMultiMapKey[str, str] = ORMultiMapKey("D")
KeyMMNoVD: ORMultiMapKey[str, str] = ORMultiMapKey("G")
# OM as above
KeyOM: ORMapKey[str, ORSet[str]] = ORMapKey("J")

KeyA: PNKey = (KeyPN, "a")
KeyB: PNKey = (KeyPN, "b")
KeyC: PNKey = (KeyPN, "c")
KeyD: MMKey = (KeyMMVD, "d")
KeyE: MMKey = (KeyMMVD, "e")
KeyF: MMKey = (KeyMMVD, "f")
KeyG: MMKey = (KeyMMNoVD, "g")
KeyH: MMKey = (KeyMMNoVD, "h")
KeyI: MMKey = (KeyMMNoVD, "i")
KeyJ: OMKey = (KeyOM, "j")
KeyK: OMKey = (KeyOM, "k")
KeyL: OMKey = (KeyOM, "l")


def generate_operations(rnd: random.Random, on_node: RoleName) -> list[Op]:
    def consistency() -> Replicator.WriteConsistency:
        n = rnd.randrange(100)
        if n < 90:
            return WRITE_LOCAL
        if n < 95:
            return WRITE_TWO
        return WRITE_MAJORITY

    def rnd_pn_counter_key() -> PNKey:
        return (KeyA, KeyB, KeyC)[rnd.randrange(3)]

    def rnd_or_set_key_vd() -> MMKey:
        return (KeyD, KeyE, KeyF)[rnd.randrange(3)]

    def rnd_or_set_key_no_vd() -> MMKey:
        return (KeyG, KeyH, KeyI)[rnd.randrange(3)]

    def rnd_or_set_key_om() -> OMKey:
        return (KeyJ, KeyK, KeyL)[rnd.randrange(3)]

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
        match rnd.randrange(6):
            case 0:
                return Delay(rnd.randrange(500))
            case 1:
                return Incr(rnd_pn_counter_key(), rnd.randrange(100), consistency())
            case 2:
                return Decr(rnd_pn_counter_key(), rnd.randrange(10), consistency())
            case 3:
                # ORMultiMap.withValueDeltas
                vd = rnd_or_set_key_vd()
                # only removals for KeyF on node first
                if vd == KeyF and on_node == first and rnd.choice((True, False)):
                    return RemoveVD(vd, rnd_remove_element(), consistency())
                return AddVD(vd, rnd_add_element(), consistency())
            case 4:
                # ORMultiMap - vanilla variant - without Value Deltas
                no_vd = rnd_or_set_key_no_vd()
                # only removals for KeyI on node first
                if no_vd == KeyI and on_node == first and rnd.choice((True, False)):
                    return RemoveNoVD(no_vd, rnd_remove_element(), consistency())
                return AddNoVD(no_vd, rnd_add_element(), consistency())
            case _:
                # Vanilla ORMap - with ORSet inside
                om = rnd_or_set_key_om()
                # only removals for KeyL on node first
                if om == KeyL and on_node == first and rnd.choice((True, False)):
                    return RemoveOM(om, rnd_remove_element(), consistency())
                return AddOM(om, rnd_add_element(), consistency())

    # Scala's (0 to (50 + rnd.nextInt(10))) is inclusive
    return [operation() for _ in range(51 + rnd.randrange(10))]


@multi_node_spec(config)
async def test_replicator_map_delta_spec(spec: MultiNodeSpec):
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
    # both deltas and full state
    ordinary_replicator: ActorRef[Any] = spec.spawn(
        Replicator.behavior(ReplicatorSettings(gossip_interval=1.0)), "ordinaryReplicator"
    )

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
        modify: Callable[[A], A],
    ) -> None:
        # the consistency of the random operations is ignored, as in Pekko: WriteLocal
        replicator.tell(Replicator.Update(key, initial, WRITE_LOCAL, ignored.ref, modify))

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
            for r in (full_state_replicator, delta_replicator):
                for pn, name in (KeyA, KeyB, KeyC):
                    update(
                        r,
                        pn,
                        PNCounterMap[str].empty,
                        lambda m, name=name: m.increment(self_address, name, 1),
                    )
                for vd, name in (KeyD, KeyE, KeyF):
                    update(
                        r,
                        vd,
                        ORMultiMap[str, str].empty_with_value_deltas,
                        lambda m, name=name: m.put(self_address, name, frozenset({"a"})),
                    )
                for no_vd, name in (KeyG, KeyH, KeyI):
                    update(
                        r,
                        no_vd,
                        ORMultiMap[str, str].empty,
                        lambda m, name=name: m.put(self_address, name, frozenset({"a"})),
                    )
                for om, name in (KeyJ, KeyK, KeyL):
                    update(
                        r,
                        om,
                        ORMap[str, ORSet[str]].empty,
                        lambda m, name=name: m.put(
                            self_address, name, ORSet[str]().add(self_address, "a")
                        ),
                    )
        await spec.enter_barrier("updated-1")

        async def pn_counters() -> None:
            for pn, name in (KeyA, KeyB, KeyC):
                assert (await get(full_state_replicator, pn)).get(name) == 1

        async def multi_maps_vd() -> None:
            for vd, name in (KeyD, KeyE, KeyF):
                assert (await get(full_state_replicator, vd)).get(name) == {"a"}

        async def multi_maps_no_vd() -> None:
            for no_vd, name in (KeyG, KeyH, KeyI):
                assert (await get(full_state_replicator, no_vd)).get(name) == {"a"}

        async def or_maps() -> None:
            for om, name in (KeyJ, KeyK, KeyL):
                res = (await get(full_state_replicator, om)).get(name)
                assert (None if res is None else res.elements) == {"a"}

        async with spec.within(5.0):
            await spec.await_assert(pn_counters)
            await spec.await_assert(multi_maps_vd)
            await spec.await_assert(multi_maps_no_vd)
            await spec.await_assert(or_maps)

        await enter_barrier_after_test_step()

    async def replicate_high_throughput_changes(errors: list[Any]) -> None:
        n = 1000
        errors_before = len(errors)
        if spec.is_node(first):
            for _ in range(n):
                for pn, name in (KeyA, KeyB):
                    update(
                        ordinary_replicator,
                        pn,
                        PNCounterMap[str].empty,
                        lambda m, name=name: m.increment(self_address, name, 1),
                    )
        await spec.enter_barrier("updated-2")

        async def replicated() -> None:
            for pn, name in (KeyA, KeyB):
                assert (await get(ordinary_replicator, pn)).get(name) == n

        async with spec.within(5.0):
            await spec.await_assert(replicated)

        await spec.enter_barrier("replicated-2")
        # no OversizedPayloadException logging
        assert errors[errors_before:] == []

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
                    case Incr((key, name), n, _):
                        for r in replicators:
                            update(
                                r,
                                key,
                                PNCounterMap[str].empty,
                                lambda m, name=name, n=n: m.increment(self_address, name, n),
                            )
                    case Decr((key, name), n, _):
                        for r in replicators:
                            update(
                                r,
                                key,
                                PNCounterMap[str].empty,
                                lambda m, name=name, n=n: m.decrement(self_address, name, n),
                            )
                    case AddVD((key, name), elem, _):
                        # to have an deterministic result when mixing add/remove we can only
                        # perform the ORSet operations from one node
                        nodes = (first,) if (key, name) == KeyF else (first, second, third)
                        if spec.is_node(*nodes):
                            for r in replicators:
                                update(
                                    r,
                                    key,
                                    ORMultiMap[str, str].empty_with_value_deltas,
                                    lambda m, name=name, elem=elem: m.add_binding(
                                        self_address, name, elem
                                    ),
                                )
                    case RemoveVD((key, name), elem, _):
                        if spec.is_node(first):
                            for r in replicators:
                                update(
                                    r,
                                    key,
                                    ORMultiMap[str, str].empty_with_value_deltas,
                                    lambda m, name=name, elem=elem: m.remove_binding(
                                        self_address, name, elem
                                    ),
                                )
                    case AddNoVD((key, name), elem, _):
                        nodes = (first,) if (key, name) == KeyI else (first, second, third)
                        if spec.is_node(*nodes):
                            for r in replicators:
                                update(
                                    r,
                                    key,
                                    ORMultiMap[str, str].empty,
                                    lambda m, name=name, elem=elem: m.add_binding(
                                        self_address, name, elem
                                    ),
                                )
                    case RemoveNoVD((key, name), elem, _):
                        if spec.is_node(first):
                            for r in replicators:
                                update(
                                    r,
                                    key,
                                    ORMultiMap[str, str].empty,
                                    lambda m, name=name, elem=elem: m.remove_binding(
                                        self_address, name, elem
                                    ),
                                )
                    case AddOM((key, name), elem, _):
                        nodes = (first,) if (key, name) == KeyL else (first, second, third)
                        if spec.is_node(*nodes):
                            for r in replicators:
                                update(
                                    r,
                                    key,
                                    ORMap[str, ORSet[str]].empty,
                                    lambda om, name=name, elem=elem: add_element_to_or_map(
                                        om, name, elem
                                    ),
                                )
                    case RemoveOM((key, name), elem, _):
                        if spec.is_node(first):
                            for r in replicators:
                                update(
                                    r,
                                    key,
                                    ORMap[str, ORSet[str]].empty,
                                    lambda om, name=name, elem=elem: remove_element_from_or_map(
                                        om, name, elem
                                    ),
                                )

            await spec.enter_barrier("updated-3")

            for pn, name in (KeyA, KeyB, KeyC):

                async def same_counter(pn: PNCounterMapKey[str] = pn, name: str = name) -> None:
                    full_state_value = (await get(full_state_replicator, pn)).get(name)
                    delta_value = (await get(delta_replicator, pn)).get(name)
                    assert full_state_value is not None
                    assert delta_value == full_state_value, f"seed {seed}: {pn} {name}"

                async with spec.within(5.0):
                    await spec.await_assert(same_counter)

            for mm, name in (KeyD, KeyE, KeyF, KeyG, KeyH, KeyI):

                async def same_multi_map(
                    mm: ORMultiMapKey[str, str] = mm, name: str = name
                ) -> None:
                    full_state_value = (await get(full_state_replicator, mm)).get(name)
                    delta_value = (await get(delta_replicator, mm)).get(name)
                    assert delta_value == full_state_value, f"seed {seed}: {mm} {name}"

                async with spec.within(5.0):
                    await spec.await_assert(same_multi_map)

            for om, name in (KeyJ, KeyK, KeyL):

                async def same_or_map(
                    om: ORMapKey[str, ORSet[str]] = om, name: str = name
                ) -> None:
                    full_state_value = (await get(full_state_replicator, om)).get(name)
                    delta_value = (await get(delta_replicator, om)).get(name)
                    assert (None if delta_value is None else delta_value.elements) == (
                        None if full_state_value is None else full_state_value.elements
                    ), f"seed {seed}: {om} {name}"

                async with spec.within(5.0):
                    await spec.await_assert(same_or_map)

            await enter_barrier_after_test_step()
        except Exception as e:
            e.add_note(f"seed {seed}, random operations on [{spec.myself.name}]: {operations}")
            raise

    def add_element_to_or_map(
        om: ORMap[str, ORSet[str]], key: str, element: str
    ) -> ORMap[str, ORSet[str]]:
        return om.update(self_address, key, ORSet[str](), lambda s: s.add(self_address, element))

    def remove_element_from_or_map(
        om: ORMap[str, ORSet[str]], key: str, element: str
    ) -> ORMap[str, ORSet[str]]:
        return om.update(
            self_address, key, ORSet[str](), lambda s: s.remove(self_address, element)
        )

    with errors_logged() as errors:
        await join_cluster()
        await propagate_delta()
        await replicate_high_throughput_changes(errors)
        await be_eventually_consistent()
