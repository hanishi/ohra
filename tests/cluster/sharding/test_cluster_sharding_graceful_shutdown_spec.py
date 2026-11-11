"""Ported from Apache Pekko's ClusterShardingGracefulShutdownSpec (cluster-sharding/src/multi-jvm/
scala/org/apache/pekko/cluster/sharding/ClusterShardingGracefulShutdownSpec.scala), its DData
variant (DDataClusterShardingGracefulShutdownSpec, Scala 88-150): two cluster nodes, the newest
shuts down through CoordinatedShutdown. The cases, in the Scala order:

- start some shards in both regions (88)
- gracefully shutdown the region on the newest node (109)
- gracefully shutdown empty region (150)

One test runs the cases in order, as the Scala does: each case builds on the cluster the one before
it left (the second case shuts the second node down, the third runs on the first alone).

Roles: the Scala's ``first`` and ``second`` are the systems ``first`` and ``second``; the test body
stands in for the barriers.

Differences from the Scala:

- ``ShardedEntity`` echoes the int it gets; here the entity (type "Entity", ``GsPing``) answers
  ``GsPong`` with the port of its node, which stands in for ``lastSender.path.address``
  (``p.lastSender.path should be(region.path / n / n)`` is "the answer names first's port").
  ``intExtractShardId`` (the id itself) has no counterpart: ohra hashes the ids into
  ``number_of_shards = 100`` shards. ``ShardedEntity.Stop`` is ``GsStop``, the entity's stop
  message (``with_stop_message``). The strategy is the least-shard one with absolute limit 2 and
  relative limit 1.0 (``rebalance_absolute_limit``, ``rebalance_relative_limit``).
- ``GetCurrentRegions``/``CurrentRegions`` (Scala 108, 130) has no counterpart in ohra's public
  API, so "two regions" is "the entities answer from both nodes" and "the coordinator notices
  the region stopped, without waiting for the member to be removed" is that the second
  region terminates and first serves every entity while the phase is still held open by the
  postponing task.
- The Scala sets ``cluster-sharding-shutdown-region`` to 60 s and postpones it by 40 s; here the
  phase is 8 s and the postponing task 6 s (the ``exiting_timeout`` of ``node``). The handoff and
  ``expectTerminated(region)`` are awaited for 3 s, far under the phase, so a handoff that waited
  for the phase or the member removal fails.
- ``CoordinatedShutdown.unknownReason`` is ``ActorSystemTerminateReason()``.
- ``runOn(second) { watch(region); expectTerminated(region) }`` is a TestProbe on the
  second system.
- The "empty region" case sends ``GracefulShutdown`` to the region, ohra's ``_GracefulShutdown``
  (private: Pekko's ``ShardRegion.GracefulShutdown`` has no public ohra counterpart; the region's
  own coordinated-shutdown task sends it).
- Range 1 to 200 in the second case is as in the Scala; the ``within(30.seconds)`` bounds are the
  ``await_assert`` and ``eventually`` timeouts below.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from typing import Any

from ohra import (
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterSharding,
    Entity,
    EntityContext,
    EntityTypeKey,
    ask,
    remotable,
)
from ohra.cluster.core import Cluster, Join
from ohra.cluster.sharding.region import _GracefulShutdown  # pyright: ignore[reportPrivateUsage]
from ohra.coordinated_shutdown import ActorSystemTerminateReason, CoordinatedShutdown
from ohra.testkit import TestProbe
from ohra.testkit.settings import TestKitSettings

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, node

NUMBER_OF_SHARDS = 100
SETTINGS = replace(
    FAST_SHARDING,
    number_of_shards=NUMBER_OF_SHARDS,
    rebalance_absolute_limit=2,
    rebalance_relative_limit=1.0,
)


@remotable
@dataclass(frozen=True)
class GsPong:
    port: int


@remotable
@dataclass(frozen=True)
class GsPing:
    reply_to: ActorRef[GsPong]


@remotable
@dataclass(frozen=True)
class GsStop: ...


type EntityMessage = GsPing | GsStop


@dataclass
class Host:
    """What a TestProbe needs of a test kit."""

    system: ActorSystem[Any]
    settings: TestKitSettings = field(default_factory=TestKitSettings)


def start_sharding(
    system: ActorSystem[Any], type_name: str
) -> tuple[EntityTypeKey[EntityMessage], ActorRef[Any]]:
    port = Cluster(system).self_address.port

    def create(_entity: EntityContext[EntityMessage]) -> Behavior[EntityMessage]:
        @Behaviors.receive_message
        def on_message(msg: EntityMessage) -> Behavior[EntityMessage]:
            match msg:
                case GsPing(reply_to):
                    reply_to.tell(GsPong(port))
                    return Behaviors.same
                case GsStop():
                    return Behaviors.stopped()

        return on_message

    key = EntityTypeKey[EntityMessage](type_name)
    entity = Entity(key, create).with_stop_message(GsStop()).with_settings(SETTINGS)
    return key, ClusterSharding(system).init(entity)


async def port_of_entity(
    system: ActorSystem[Any], key: EntityTypeKey[EntityMessage], n: int
) -> int:
    ref = ClusterSharding(system).entity_ref_for(key, str(n))
    return (await ask(ref, GsPing, timeout=1.0)).port


async def test_cluster_sharding_ddata_mode_graceful_shutdown():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        # start some shards in both regions (88)
        first = await stack.enter_async_context(node(p1, [], exiting_timeout=8.0))
        Cluster(first).manager.tell(Join(Cluster(first).self_address))  # oldest
        await eventually(lambda: all_up(first, 1), timeout=5)
        key, _ = start_sharding(first, "Entity")
        second = await stack.enter_async_context(node(p2, [], exiting_timeout=8.0))
        Cluster(second).manager.tell(Join(Cluster(first).self_address))
        _, region_second = start_sharding(second, "Entity")
        await eventually(lambda: all_up(first, 2) and all_up(second, 2), timeout=5)
        probe = TestProbe[Any](Host(first))

        async def entities_in_both_regions() -> None:
            ports = {await port_of_entity(first, key, n) for n in range(1, 101)}
            assert len(ports) == 2

        await probe.await_assert(entities_in_both_regions, max=30.0, interval=0.1)

        # gracefully shutdown the region on the newest node (109)
        # make sure the 'cluster-sharding-shutdown-region' phase takes at least 6 seconds, to
        # validate region shutdown completion is propagated immediately and not postponed until
        # when the cluster member leaves
        async def postpone_actual_stop() -> None:
            await asyncio.sleep(6.0)

        CoordinatedShutdown(second).add_task(
            "cluster-sharding-shutdown-region", "postpone-actual-stop", postpone_actual_stop
        )
        shutdown = CoordinatedShutdown(second).run(ActorSystemTerminateReason())

        async def handoff_completed() -> None:
            for n in range(1, 201):
                assert await port_of_entity(first, key, n) == p1

        await probe.await_assert(handoff_completed, max=3.0, interval=0.1)
        # the second node's region has stopped, without waiting for the member to be removed
        await TestProbe[Any](Host(second)).expect_terminated(region_second, max=3.0)
        assert not shutdown.done()
        await shutdown

        # gracefully shutdown empty region (150)
        _, region_empty = start_sharding(first, "EntityEmpty")
        region_empty.tell(_GracefulShutdown())
        await probe.expect_terminated(region_empty, max=5.0)
