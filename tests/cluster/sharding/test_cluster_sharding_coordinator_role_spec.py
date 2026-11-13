"""ClusterShardingCoordinatorRoleSpec, ported from Apache Pekko's cluster-sharding/src/multi-jvm/
scala/org/apache/pekko/cluster/sharding/ClusterShardingCoordinatorRoleSpec.scala, the DData mode,
remember entities off (``DDataClusterShardingCoordinatorRoleSpec``) and on
(``DDataRememberClusterShardingCoordinatorRoleSpec``):

- "use nodes with shard role" (113-155);
- "restart remembered entities" (157-178), with remember entities only.

The config (36-43): sharding's role is ``shard``, ``coordinator-singleton-role-override = off``
and the coordinator singleton's role is ``coordinator``, ``entity-restart-backoff = 100 ms``.
``first`` and ``second`` have the role ``coordinator``, ``third`` and ``fourth`` ``shard``.

Differences from the Scala spec:

- the nodes run on one event loop; ``startSharding`` is ``ClusterSharding.init``, with the
  ``EntityActor`` of MultiNodeClusterShardingSpec: ``Started`` to a probe of its node, then an
  echo, here of the port of its node, which stands for ``lastSender.path``;
- ``PoisonPill`` is a ``RoleStop`` the entity stops on, and ``Started`` carries the entity id
  where the Scala's carries the ref, whose path is the same on the restart;
- the remember variant keeps sharding's Replicators in memory (no durable directory), as the
  coordinator state of the DData mode;
- ``expectNoMessage`` is one second."""

from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass, replace
from typing import Any

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterSharding,
    ClusterShardingSettings,
    ClusterSingletonSettings,
    Entity,
    EntityContext,
    EntityTypeKey,
    ReplicatorSettings,
    ask,
    remotable,
)
from ohra.cluster.core import Cluster, Join, Leave

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, node
from .test_ddata_shard_coordinator import Node


@remotable
@dataclass(frozen=True)
class RoleEcho:
    reply_to: ActorRef[int]


@remotable
@dataclass(frozen=True)
class RoleStop: ...


@dataclass(frozen=True)
class Started:
    entity_id: str


type Command = RoleEcho | RoleStop

KEY = EntityTypeKey[Command]("Entity")


def settings(remember: bool) -> ClusterShardingSettings:
    return replace(
        FAST_SHARDING,
        role="shard",
        coordinator_singleton_override_role=False,
        coordinator_singleton_settings=replace(
            FAST_SHARDING.coordinator_singleton_settings or ClusterSingletonSettings(),
            role="coordinator",
        ),
        entity_restart_backoff=0.1,
        remember_entities=remember,
        distributed_data=(
            ReplicatorSettings(prefer_oldest=True, max_delta_elements=5) if remember else None
        ),
    )


def entity_actor(
    entity: EntityContext[Command], probe: ActorRef[Started], port: int
) -> Behavior[Command]:
    @Behaviors.setup
    def setup(_ctx: ActorContext[Command]) -> Behavior[Command]:
        probe.tell(Started(entity.entity_id))

        @Behaviors.receive_message
        def receive(msg: Command) -> Behavior[Command]:
            match msg:
                case RoleEcho(reply_to):
                    reply_to.tell(port)
                    return Behaviors.same
                case RoleStop():
                    return Behaviors.stopped()

        return receive

    return setup


def start_sharding(
    system: ActorSystem[Any], probe: ActorRef[Started], remember: bool
) -> ActorRef[Any]:
    port = Cluster(system).self_address.port
    entity = Entity(KEY, lambda e: entity_actor(e, probe, port))
    return ClusterSharding(system).init(entity.with_settings(settings(remember)))


async def echo(system: ActorSystem[Any], entity_id: str) -> int:
    return await ask(ClusterSharding(system).entity_ref_for(KEY, entity_id), RoleEcho, 5)


@pytest.mark.parametrize("remember", [False, True], ids=["ddata", "ddata-remember"])
async def test_cluster_sharding_with_separate_coordinator_role(remember: bool):
    p1, p2, p3, p4 = (free_port() for _ in range(4))
    coordinator, shard = frozenset({"coordinator"}), frozenset({"shard"})
    async with AsyncExitStack() as stack:
        first = await stack.enter_async_context(node(p1, [p1], roles=coordinator))
        second = await stack.enter_async_context(node(p2, [p1], roles=coordinator))
        third = await stack.enter_async_context(node(p3, [p1], roles=shard))
        fourth = await stack.enter_async_context(node(p4, [], roles=shard))
        nodes = [first, second, third, fourth]
        probes = [Node(n).probe() for n in nodes]
        for n, probe in zip(nodes, probes, strict=True):
            start_sharding(n, probe.ref, remember)
        await eventually(lambda: all(all_up(n, 3) for n in nodes[:3]), timeout=5)

        # "use nodes with shard role"
        assert await echo(third, "1") == p3
        await probes[2].expect_message(Started("1"), 3)
        assert await echo(first, "1") == p3
        assert await echo(second, "1") == p3
        await probes[0].expect_no_message(1)
        await probes[1].expect_no_message(1)

        if not remember:
            return

        assert await echo(third, "2") == p3
        await probes[2].expect_message(Started("2"), 3)
        ClusterSharding(third).entity_ref_for(KEY, "2").tell(RoleStop())
        # and then started again by remember entities
        await probes[2].expect_message(Started("2"), 5)
        await probes[0].expect_no_message(1)
        await probes[1].expect_no_message(1)

        # "restart remembered entities"
        Cluster(fourth).manager.tell(Join(Cluster(second).self_address))
        await eventually(lambda: all_up(second, 4), timeout=5)
        Cluster(first).manager.tell(Leave(Cluster(third).self_address))
        Cluster(first).manager.tell(Leave(Cluster(first).self_address))
        # started by remember entities on new node
        await probes[3].expect_message_type(Started, 20)
        await probes[1].expect_no_message(1)
