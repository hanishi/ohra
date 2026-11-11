"""Ported from Apache Pekko's ClusterShardingSpec (cluster-sharding/src/multi-jvm/scala/org/apache/
pekko/cluster/sharding/ClusterShardingSpec.scala), its DData variant (DDataClusterShardingSpec,
Scala 322): up to six cluster nodes in one process, each with the sharding coordinator on
Distributed Data. The cases, in the Scala order:

- work in single node cluster (433)
- use second node (451)
- failover shards on crashed node (537)
- use third and fourth node (570)
- recover coordinator state after coordinator crash (621)
- rebalance to nodes with less shards (652)

One test runs the cases in order, as the Scala does: each case builds on the cluster the one
before it left (the second node joins the first, then crashes; the third and fourth join; the
first, which runs the coordinator, crashes). Splitting it would make every case rebuild the
previous ones, and the point of the later cases is the state the earlier ones allocated.

Roles: the Scala's ``first`` to ``sixth`` are the systems ``first`` to ``sixth``; the test body is
the ``controller``, which has no system and runs ``testConductor.exit`` (a crash) and the barriers
(plain sequencing). ``runOn(x)`` is code that talks through system x's refs.

``ClusterShardingSpecConfig`` (Scala 119-166) and ``createCoordinator``/``createRegion`` (Scala
342-411) map as follows:

- retry-interval = 1 s, handoff-timeout = 10 s, shard-start-timeout = 5 s, rebalance-interval = 2
  s, the least-shard strategy's absolute limit 2 and relative limit 1.0 (the coordinator's own
  props): ``FAST_SHARDING`` of tests/cluster/test_cluster_sharding.py, which shortens the
  intervals, with ``rebalance_absolute_limit=2`` and ``rebalance_relative_limit=1.0``. The counter
  type has a 3600 s rebalance interval and rebalancingCounter a short one, as the Scala
  coordinators (``rebalanceEnabled``).
- numberOfShards = 100 (Scala 217): ``number_of_shards=100``.
- AutoDowning with auto-down-unreachable-after = 0s (MultiNodeClusterShardingConfig 120): a crash
  helper that waits for the node to be unreachable and then sends ``Down``.
- distributed-data.gossip-interval = 1s: ``FAST``'s cluster gossip and ddata's own interval, as the
  other cluster tests shorten them.

Differences from the Scala:

- ``EntityEnvelope(id, payload)`` is ``ShardingEnvelope(entity_id, message)``, through
  ``ClusterSharding.entity_ref_for``. Entity ids are strings; ``extractEntityId`` and
  ``extractShardId`` (id % numberOfShards) have no counterpart: ohra hashes the id (crc32) into
  ``number_of_shards`` shards. The test checks that the ids it uses are on distinct shards, which
  the Scala's ids (1, 2, 3, 4, 11, 12, 1 to 10) are by the modulo.
- The Counter is a PersistentActor on a SharedLeveldbStore in the Scala; here its count is kept in
  a dict shared by all the systems (the shared store), written on each change and read when the
  entity starts. ``Get`` carries a ``reply_to`` and the reply names the node the entity runs on
  (``lastSender.path`` in the Scala; ohra's entity has no sender, and the shard's path is not
  visible). ``Stop`` is part of the Counter; passivation is not ported (the "support passivation"
  case is outside this port), so the count of entity 2 is 3 where the Scala has 4 (451, 537).
- ``ReceiveTimeout`` is not sent either.
- "setup shared journal" (Scala 421), "support passivation and activation of entities" (498),
  "support proxy only mode" (516), "easy to use with extensions" and "easy API for starting" are
  not ported. ``GetCurrentRegions``/``CurrentRegions`` (Scala 446) has no counterpart in ohra's
  public API, so that assertion is not ported.
- ``ClusterSharding.init`` starts the coordinator singleton and a region on a node together. The
  Scala creates the coordinators of every type on every node at ``join`` and a region on first
  use. The rebalancing type is therefore started on the third, fourth and fifth nodes (the oldest
  node must run its coordinator), so its ten shards are spread over those three before the sixth
  joins, where the Scala has them all on the fourth.
- Roles (``backend`` and the sixth's ``frontend``) have no counterpart: ohra's sharding has no
  roles.
- Nodes are crashed with the ``crash`` of tests/cluster/test_cluster_sharding.py: the transport is
  closed and the system terminated, so nothing the node says on its way down gets out
  (``testConductor.exit``).
- The ``within`` bounds (20, 30, 15, 60 s) are the ``await_assert`` and ask timeouts below, kept
  short because the intervals are.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from typing import Any

from ohra import (
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterSharding,
    ClusterShardingSettings,
    Entity,
    EntityContext,
    EntityTypeKey,
    ask,
    remotable,
)
from ohra.cluster.core import Cluster, Down, Join
from ohra.cluster.sharding import shard_id_of
from ohra.remote import Address
from ohra.testkit import TestProbe
from ohra.testkit.settings import TestKitSettings

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, crash, node, unreachable_ports

NUMBER_OF_SHARDS = 100


@remotable
@dataclass(frozen=True)
class CsIncrement: ...


@remotable
@dataclass(frozen=True)
class CsDecrement: ...


@remotable
@dataclass(frozen=True)
class CsStop: ...


@remotable
@dataclass(frozen=True)
class CsCounted:
    """What a Counter answers to a Get: its count and the port of the node it runs on."""

    count: int
    node: int


@remotable
@dataclass(frozen=True)
class CsGet:
    reply_to: ActorRef[CsCounted]


type CounterCommand = CsIncrement | CsDecrement | CsGet | CsStop

COUNTER = EntityTypeKey[CounterCommand]("counter")
REBALANCING_COUNTER = EntityTypeKey[CounterCommand]("rebalancingCounter")

type Journal = dict[tuple[str, str], int]
"""The shared store: (type name, entity id) to count."""


def counter(
    journal: Journal, port: int
) -> Callable[[EntityContext[CounterCommand]], Behavior[CounterCommand]]:
    """Pekko's Counter: a persistent count that recovers on whichever node it starts."""

    def create(entity: EntityContext[CounterCommand]) -> Behavior[CounterCommand]:
        key = (entity.entity_type_key.name, entity.entity_id)

        def active(count: int) -> Behavior[CounterCommand]:
            @Behaviors.receive_message
            def on_message(msg: CounterCommand) -> Behavior[CounterCommand]:
                match msg:
                    case CsIncrement():
                        journal[key] = count + 1
                        return active(count + 1)
                    case CsDecrement():
                        journal[key] = count - 1
                        return active(count - 1)
                    case CsGet(reply_to):
                        reply_to.tell(CsCounted(count, port))
                        return Behaviors.same
                    case CsStop():
                        return Behaviors.stopped()

            return on_message

        return active(journal.get(key, 0))

    return create


@dataclass
class Host:
    """What a TestProbe needs of a test kit."""

    system: ActorSystem[Any]
    settings: TestKitSettings = field(default_factory=TestKitSettings)


def settings_of(*, rebalancing: bool) -> ClusterShardingSettings:
    return replace(
        FAST_SHARDING,
        number_of_shards=NUMBER_OF_SHARDS,
        rebalance_interval=0.2 if rebalancing else 3600.0,
        rebalance_absolute_limit=2,
        rebalance_relative_limit=1.0,
    )


def port_of(system: ActorSystem[Any]) -> int:
    return Cluster(system).self_address.port


def join(system: ActorSystem[Any], seed: ActorSystem[Any]) -> None:
    Cluster(system).manager.tell(Join(Cluster(seed).self_address))


def start(
    system: ActorSystem[Any], journal: Journal, *keys: EntityTypeKey[CounterCommand]
) -> None:
    """``createCoordinator`` and the region: sharding's init for each type on this node."""
    for key in keys:
        entity = Entity(key, counter(journal, port_of(system)))
        ClusterSharding(system).init(
            entity.with_settings(settings_of(rebalancing=key is REBALANCING_COUNTER))
        )


async def get(
    system: ActorSystem[Any], key: EntityTypeKey[CounterCommand], entity_id: int
) -> CsCounted:
    ref = ClusterSharding(system).entity_ref_for(key, str(entity_id))
    return await ask(ref, CsGet, timeout=1.0)


def tell(system: ActorSystem[Any], entity_id: int, msg: CounterCommand) -> None:
    ClusterSharding(system).entity_ref_for(COUNTER, str(entity_id)).tell(msg)


async def crash_and_down(survivor: ActorSystem[Any], victim: ActorSystem[Any]) -> None:
    """``testConductor.exit(victim, 0)``, then the testkit's AutoDowning with 0 s."""
    victim_port = port_of(victim)
    await crash(victim)
    await eventually(lambda: victim_port in unreachable_ports(survivor), timeout=5)
    Cluster(survivor).manager.tell(Down(Address("c", "127.0.0.1", victim_port)))


async def test_cluster_sharding_ddata_mode():
    assert len({shard_id_of(str(i), NUMBER_OF_SHARDS) for i in (1, 2, 3, 4, 11, 12)}) == 6
    assert len({shard_id_of(str(i), NUMBER_OF_SHARDS) for i in range(1, 11)}) == 10
    journal: Journal = {}
    p1, p2, p3, p4, p5, p6 = (free_port() for _ in range(6))
    async with AsyncExitStack() as stack:

        async def new_node(
            port: int, seed: ActorSystem[Any] | None, size: int
        ) -> ActorSystem[Any]:
            """``join(from, to)``: the node joins ``seed`` (itself for the first), and every node
            sees ``size`` members Up."""
            system = await stack.enter_async_context(node(port, []))
            join(system, seed or system)
            await eventually(lambda: all_up(system, size), timeout=5)
            return system

        # work in single node cluster (433)
        first = await new_node(p1, None, 1)
        start(first, journal, COUNTER)
        for msg in (CsIncrement(), CsIncrement(), CsIncrement(), CsDecrement()):
            tell(first, 1, msg)
        assert (await get(first, COUNTER, 1)).count == 2

        # use second node (451)
        second = await new_node(p2, first, 2)
        await eventually(lambda: all_up(first, 2), timeout=5)
        start(second, journal, COUNTER)
        for msg in (CsIncrement(), CsIncrement(), CsIncrement(), CsDecrement()):
            tell(second, 2, msg)
        assert (await get(second, COUNTER, 2)).count == 2
        tell(second, 11, CsIncrement())
        tell(second, 12, CsIncrement())
        assert (await get(second, COUNTER, 11)).count == 1
        assert (await get(second, COUNTER, 12)).count == 1
        # second-update
        tell(first, 2, CsIncrement())
        answer = await get(first, COUNTER, 2)
        assert (answer.count, answer.node) == (3, p2)
        answer = await get(first, COUNTER, 11)
        assert (answer.count, answer.node) == (1, p1)  # local on first
        answer = await get(first, COUNTER, 12)
        assert (answer.count, answer.node) == (1, p2)
        # first-update
        answer = await get(second, COUNTER, 2)
        assert (answer.count, answer.node) == (3, p2)

        # failover shards on crashed node (537)
        await crash_and_down(first, second)
        probe = TestProbe[Any](Host(first))

        async def entity_2_on_first() -> None:
            answer = await get(first, COUNTER, 2)
            assert (answer.count, answer.node) == (3, p1)

        async def entity_12_on_first() -> None:
            answer = await get(first, COUNTER, 12)
            assert (answer.count, answer.node) == (1, p1)

        await probe.await_assert(entity_2_on_first, max=10.0, interval=0.05)
        await probe.await_assert(entity_12_on_first, max=10.0, interval=0.05)

        # use third and fourth node (570)
        third = await new_node(p3, first, 2)
        start(third, journal, COUNTER)
        for _ in range(10):
            tell(third, 3, CsIncrement())
        answer = await get(third, COUNTER, 3)
        assert (answer.count, answer.node) == (10, p3)  # local
        # third-update
        fourth = await new_node(p4, first, 3)
        start(fourth, journal, COUNTER)
        for _ in range(20):
            tell(fourth, 4, CsIncrement())
        answer = await get(fourth, COUNTER, 4)
        assert (answer.count, answer.node) == (20, p4)  # local
        # fourth-update
        tell(first, 3, CsIncrement())
        answer = await get(first, COUNTER, 3)
        assert (answer.count, answer.node) == (11, p3)
        tell(first, 4, CsIncrement())
        answer = await get(first, COUNTER, 4)
        assert (answer.count, answer.node) == (21, p4)
        # first-update
        answer = await get(third, COUNTER, 3)
        assert (answer.count, answer.node) == (11, p3)
        answer = await get(fourth, COUNTER, 4)
        assert (answer.count, answer.node) == (21, p4)

        # recover coordinator state after coordinator crash (621)
        fifth = await new_node(p5, fourth, 4)
        start(fifth, journal, COUNTER)
        await crash_and_down(third, first)
        probe = TestProbe[Any](Host(fifth))

        async def entity_3_on_third() -> None:
            answer = await get(fifth, COUNTER, 3)
            assert (answer.count, answer.node) == (11, p3)

        async def entity_4_on_fourth() -> None:
            answer = await get(fifth, COUNTER, 4)
            assert (answer.count, answer.node) == (21, p4)

        await probe.await_assert(entity_3_on_third, max=10.0, interval=0.05)
        await probe.await_assert(entity_4_on_fourth, max=10.0, interval=0.05)

        # rebalance to nodes with less shards (652)
        await eventually(lambda: all(all_up(n, 3) for n in (third, fourth, fifth)), timeout=5)
        for system in (third, fourth, fifth):
            start(system, journal, REBALANCING_COUNTER)
        for n in range(1, 11):
            ref = ClusterSharding(fourth).entity_ref_for(REBALANCING_COUNTER, str(n))
            ref.tell(CsIncrement())
            assert (await get(fourth, REBALANCING_COUNTER, n)).count == 1
        # rebalancing-shards-allocated
        sixth = await new_node(p6, third, 4)
        start(sixth, journal, REBALANCING_COUNTER)
        probe = TestProbe[Any](Host(sixth))

        async def sixth_hosts_at_least_two() -> None:
            local = 0
            for n in range(1, 11):
                answer = await get(sixth, REBALANCING_COUNTER, n)
                assert answer.count == 1
                local += answer.node == p6
            assert local >= 2

        await probe.await_assert(sixth_hosts_at_least_two, max=10.0, interval=0.1)
