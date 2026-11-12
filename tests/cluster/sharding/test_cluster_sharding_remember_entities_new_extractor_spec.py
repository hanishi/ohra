"""ClusterShardingRememberEntitiesNewExtractorSpec, ported from Apache Pekko's cluster-sharding/
src/multi-jvm/scala/org/apache/pekko/cluster/sharding/
ClusterShardingRememberEntitiesNewExtractorSpec.scala, the DData mode
(``DDataClusterShardingRememberEntitiesNewExtractorSpec``).

A cluster that remembers its entities restarts with a shard id extractor that puts them on other
shards, and each entity runs on the shard the new extractor gives it. The Scala's three cases
share one cluster's state, so they are one test, with the cases as its phases:

- "start up first cluster and sharding" (155-180);
- "shutdown sharding nodes" (182-195);
- "start new nodes with different extractor, and have the entities running on the right shards"
  (197-253).

Differences from the Scala spec:

- ohra has no roles, so the role-less seed node ``first``, which runs no sharding there, is
  dropped: the seed is ``second``, one of the two sharding nodes, and ``settings.withRole
  ("sharding")`` goes with it. With ``first`` gone, the new systems form a new cluster in place of
  joining ``first``'s;
- ``testConductor.exit`` is a crash of both systems, and the new systems run on the same ports
  and durable directories, which are SQLite under ``tmp_path`` (``sharding-node-a`` and
  ``sharding-node-b``);
- ohra's shard id is ``shard_id_of(entity_id, number_of_shards)``, so the extractor change
  (``id % 5`` to ``(id + 1) % 5``) is a change of ``number_of_shards`` from 5 to 7;
- the entity reports ``Started`` with its id in place of its ref, and answers ``Echo`` with its
  number in place of echoing the message;
- each shard's entity ids, which Pekko's ``CurrentShardRegionState`` lists per shard, come from
  ``GetShardState`` to each shard the region's ``GetShardRegionState`` lists;
- the settings are ``FAST_SHARDING``'s, with remember entities and
  ``fail_on_invalid_entity_state_transition`` on."""

from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterSharding,
    ClusterShardingSettings,
    CurrentShardRegionState,
    Entity,
    EntityContext,
    EntityTypeKey,
    GetShardRegionState,
    ShardingEnvelope,
    ask,
    remotable,
)
from ohra.cluster.ddata import ReplicatorSettings, sqlite_durable_store
from ohra.cluster.sharding import shard_id_of
from ohra.cluster.sharding.shard import CurrentShardState, GetShardState

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, crash, node
from .test_ddata_shard_coordinator import Node


def settings(dir: Path, number_of_shards: int) -> ClusterShardingSettings:
    return replace(
        FAST_SHARDING,
        number_of_shards=number_of_shards,
        remember_entities=True,
        fail_on_invalid_entity_state_transition=True,
        distributed_data=ReplicatorSettings(
            prefer_oldest=True,
            max_delta_elements=5,
            durable_keys=frozenset({"shard-*"}),
            durable_store=lambda: sqlite_durable_store(dir),
        ),
    )


@remotable
@dataclass(frozen=True)
class NewExtractorEcho:
    n: int
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Started:
    entity_id: str


KEY = EntityTypeKey[NewExtractorEcho]("Entity")


def entity_actor(
    entity: EntityContext[NewExtractorEcho], probe: ActorRef[Started] | None
) -> Behavior[NewExtractorEcho]:
    """``TestEntity`` (30-39): ``Started`` to ``probe`` if any, then echoes."""

    @Behaviors.setup
    def setup(ctx: ActorContext[NewExtractorEcho]) -> Behavior[NewExtractorEcho]:
        ctx.log.info("Entity started: %s", ctx.self.path)
        if probe is not None:
            probe.tell(Started(entity.entity_id))

        @Behaviors.receive_message
        def receive(msg: NewExtractorEcho) -> Behavior[NewExtractorEcho]:
            msg.reply_to.tell(msg.n)
            return Behaviors.same

        return receive

    return setup


def start_sharding(
    system: ActorSystem[Any], probe: ActorRef[Started] | None, settings: ClusterShardingSettings
) -> ActorRef[ShardingEnvelope[NewExtractorEcho]]:
    """``startShardingWithExtractor1`` and ``startShardingWithExtractor2`` (130-148)."""
    entity = Entity(KEY, lambda e: entity_actor(e, probe))
    return ClusterSharding(system).init(entity.with_settings(settings))


async def test_start_new_nodes_with_different_extractor_and_have_the_entities_running_on_the_right_shards(  # noqa: E501
    tmp_path: Path,
):
    p2, p3 = free_port(), free_port()
    dirs = (tmp_path / "sharding-node-a", tmp_path / "sharding-node-b")

    # "start up first cluster and sharding"
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p2])) for p in (p2, p3)]
        await eventually(lambda: all(all_up(n, 2) for n in nodes), timeout=5)
        for system, d in zip(nodes, dirs):
            start_sharding(system, None, settings(d, 5))
        for system in nodes:
            # one entity for each shard id
            for n in range(1, 11):
                entity = ClusterSharding(system).entity_ref_for(KEY, str(n))
                assert await ask(entity, lambda r: NewExtractorEcho(n, r), 5) == n

        # "shutdown sharding nodes"
        for system in nodes:
            await crash(system)

    # "start new nodes with different extractor, and have the entities running on the right
    # shards": no sharding nodes left of the original cluster, start new nodes
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p2])) for p in (p2, p3)]
        await eventually(lambda: all(all_up(n, 2) for n in nodes), timeout=5)
        probes = [Node(n).probe() for n in nodes]
        regions = [
            start_sharding(n, probe.ref, settings(d, 7))
            for n, probe, d in zip(nodes, probes, dirs)
        ]
        for probe in probes:
            await probe.expect_message_type(Started, 20)

        for system, region, probe in zip(nodes, regions, probes):

            async def shards_not_empty() -> frozenset[str]:
                reply: CurrentShardRegionState = await ask(region, GetShardRegionState, 3)
                assert reply.shard_ids
                return reply.shard_ids

            shard_ids = await probe.await_assert(shards_not_empty, max=10)
            for shard_id in shard_ids:
                shard = system.resolve(f"{region.path}/{shard_id}")
                shard_state: CurrentShardState = await ask(shard, GetShardState, 3)
                for entity_id in shard_state.entity_ids:
                    assert shard_id_of(entity_id, 7) == shard_state.shard
