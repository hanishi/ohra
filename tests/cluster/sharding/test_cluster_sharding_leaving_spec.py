"""Ported from Apache Pekko's ClusterShardingLeavingSpec (cluster-sharding/src/multi-jvm/scala/org/
apache/pekko/cluster/sharding/ClusterShardingLeavingSpec.scala), its DData variant
(DDataClusterShardingLeavingSpec, Scala 114-146): five cluster nodes in one process, each running
sharding with the coordinator on Distributed Data. The cases, in the Scala order:

- join cluster (114)
- initialize shards (132)
- recover after leaving coordinator node (146)

One test runs the cases in order, as the Scala does: each case builds on the cluster and the entity
locations the one before it left. Splitting it would make every case rebuild the previous ones.

Roles: the Scala's ``first`` to ``fifth`` are the systems ``first`` to ``fifth``; the test body
stands in for the barriers. ``runOn(x)`` is code that talks through system x's refs.

The Scala config maps as follows:

- ``rebalance-interval = 1s``: ``rebalance_interval=1.0``, so rebalancing can happen while the two
  nodes leave. The other intervals are ``FAST_SHARDING``'s.
- ``majority-min-cap = 1``, ``coordinator-state.write-majority-plus = 1`` and ``read-majority-plus
  = 1``: ``majority_min_cap=1`` and the two ``coordinator_state_*_majority_plus=1`` settings.
- ``verbose-debug-logging`` and ``loglevel = DEBUG`` are not ported.

Differences from the Scala:

- ``Ping(id)`` is ``LvPing(reply_to)`` sent through ``ClusterSharding.entity_ref_for``; the entity
  replies ``LvLocated`` instead of ``self``, naming the node it runs on
  (``ref.path.address`` in the Scala) and a token that is new for each entity incarnation
  (so ``newRef should not be ref`` is ``located != before``). ``extractShardId`` (the first
  character of the id) has no counterpart: ohra hashes the id (crc32) into ``number_of_shards``
  shards, here 10.
- ``ShardLocations`` (an actor holding the locations, read through an actor selection) is the
  ``original`` dict of the test.
- The ``cluster.leave`` of the two leaving nodes comes from the fifth, as in the Scala, through
  ``Cluster(fifth).manager.tell(Leave(address))``. ``expectTerminated(region, 15.seconds)`` on the
  leaving nodes is a TestProbe on each leaving system watching its region.
- The Scala asserts on each remaining node in parallel; here one node after the other, in the
  same ``await_assert``.
- ``expectMsgType[ActorRef]`` in "initialize shards" is a ping with the test kit's
  ``single_expect_default`` (3 s); the 1 s of ``expectMsgType[ActorRef](1.second)`` is the
  ping's timeout in "recover after leaving coordinator node".
- The ``within`` bounds (20, 15 s) are the ``eventually`` and ``await_assert`` timeouts below.
- ``startPersistenceIfNeeded`` is for the persistence mode only.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from itertools import count
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
from ohra.cluster.core import Cluster, Join, Leave
from ohra.remote.throttler import Blackhole, Direction, Throttler
from ohra.testkit import TestProbe
from ohra.testkit.settings import TestKitSettings

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, node

_incarnations = count(1)


@remotable
@dataclass(frozen=True)
class LvLocated:
    """What an entity answers to a ping: the port of its node and its incarnation."""

    port: int
    incarnation: int


@remotable
@dataclass(frozen=True)
class LvPing:
    reply_to: ActorRef[LvLocated]


ENTITY = EntityTypeKey[LvPing]("Entity")


def entity_behavior(port: int) -> Any:
    def create(_entity: EntityContext[LvPing]) -> Behavior[LvPing]:
        located = LvLocated(port, next(_incarnations))

        @Behaviors.receive_message
        def on_message(msg: LvPing) -> Behavior[LvPing]:
            msg.reply_to.tell(located)
            return Behaviors.same

        return on_message

    return create


@dataclass
class Host:
    """What a TestProbe needs of a test kit."""

    system: ActorSystem[Any]
    settings: TestKitSettings = field(default_factory=TestKitSettings)


def port_of(system: ActorSystem[Any]) -> int:
    return Cluster(system).self_address.port


def join(system: ActorSystem[Any], seed: ActorSystem[Any]) -> None:
    Cluster(system).manager.tell(Join(Cluster(seed).self_address))


def start_sharding(system: ActorSystem[Any], settings: ClusterShardingSettings) -> ActorRef[Any]:
    """``startSharding``: type "Entity" on this node."""
    entity = Entity(ENTITY, entity_behavior(port_of(system))).with_settings(settings)
    return ClusterSharding(system).init(entity)


async def ping(system: ActorSystem[Any], entity_id: str, timeout: float = 1.0) -> LvLocated:
    ref = ClusterSharding(system).entity_ref_for(ENTITY, entity_id)
    return await ask(ref, LvPing, timeout=timeout)


def partition(system: ActorSystem[Any], blocked_port: int) -> None:
    """``testConductor.blackhole`` for one side: drop every frame received from the peer."""
    remoting = system.remoting
    assert remoting is not None
    if remoting.throttler is None:
        remoting.throttler = Throttler()
    remoting.throttler.set_throttle("127.0.0.1", blocked_port, Direction.RECEIVE, Blackhole())


SETTINGS = replace(
    FAST_SHARDING,
    number_of_shards=10,
    rebalance_interval=1.0,
    majority_min_cap=1,
    coordinator_state_read_majority_plus=1,
    coordinator_state_write_majority_plus=1,
)


async def test_cluster_sharding_ddata_mode_with_leaving_member():
    ports = [free_port() for _ in range(5)]
    async with AsyncExitStack() as stack:
        systems: list[ActorSystem[Any]] = []
        regions: list[ActorRef[Any]] = []

        # join cluster (114)
        for index, port in enumerate(ports):
            system = await stack.enter_async_context(node(port, []))
            join(system, systems[0] if systems else system)
            systems.append(system)
            if index == 0:
                await eventually(lambda: all_up(system, 1), timeout=5)
            regions.append(start_sharding(system, SETTINGS))
        # all Up, everywhere before continuing
        await eventually(lambda: all(all_up(s, 5) for s in systems), timeout=10)
        first, second, third, fourth, fifth = systems

        # initialize shards (132)
        # expectMsgType's default, single-expect-default: the first ping waits for the coordinator
        expect_default = TestKitSettings().single_expect_default_dilated
        original = {str(n): await ping(first, str(n), expect_default) for n in range(1, 11)}
        assert any(located.port in {ports[0], ports[1]} for located in original.values())

        # recover after leaving coordinator node (146)
        leaving = (first, second)
        remaining = (third, fourth, fifth)
        terminated = [
            asyncio.create_task(TestProbe[Any](Host(s)).expect_terminated(region, max=15.0))
            for s, region in zip(leaving, regions, strict=False)
        ]
        for s in leaving:
            Cluster(fifth).manager.tell(Leave(Cluster(s).self_address))
        await asyncio.gather(*terminated)
        # more stress by not having the barrier here

        leaving_ports = {port_of(s) for s in leaving}
        probe = TestProbe[Any](Host(fifth))

        async def moved_only_from_leaving_nodes() -> None:
            for system in remaining:
                for entity_id, before in original.items():
                    now = await ping(system, entity_id)
                    if before.port in leaving_ports:
                        assert now != before
                    else:
                        assert now == before  # should not move

        await probe.await_assert(moved_only_from_leaving_nodes, max=15.0, interval=0.1)
        await eventually(lambda: all(all_up(s, 3) for s in remaining), timeout=5)
