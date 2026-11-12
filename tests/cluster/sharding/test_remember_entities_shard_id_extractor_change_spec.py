"""RememberEntitiesShardIdExtractorChangeSpec, ported from Apache Pekko's cluster-sharding/src/
test/scala/org/apache/pekko/cluster/sharding/RememberEntitiesShardIdExtractorChangeSpec.scala.

Remembered entities move to their new shards when the shard id extractor changes across a full
restart of the cluster, and stay there: "allow a change to the shard id extractor" (101-139).

Differences from the Scala spec:

- Pekko's event-sourced remember-entities store (``remember-entities-store = "eventsourced"``)
  is the DData store, with sharding's Replicator keeping ``shard-*`` in a SQLite durable directory
  under ``tmp_path``; the three incarnations share it, and one port. The incarnations keep the
  Scala's three system names, so the directory's name does not end in ``ddata``, which the store
  would suffix with the system's name, as Pekko's ``LmdbDurableStore``;
- ohra's shard id is ``shard_id_of(entity_id, number_of_shards)``, so the extractor change
  (``id % 10`` to ``id % 10 + 1``) is a change of ``number_of_shards`` from 10 to 11, and the
  Scala's entities 1, 11 and 21, of shard 1 then shard 2, are three ids the test finds that share
  a shard under 10 shards and another under 11;
- ohra has no ``GetCurrentRegions``, so ``assertRegionRegistrationComplete`` is left to the acks:
  the region answers only once it is registered;
- each shard's entity ids, which Pekko's ``CurrentShardRegionState`` lists per shard, come from
  ``GetShardState`` to each shard the region's ``GetShardRegionState`` lists;
- the entity ``PA`` is a typed entity that answers ``"ack"`` to the ``reply_to`` of its message;
- the settings are ``FAST_SHARDING``'s, with remember entities and
  ``fail_on_invalid_entity_state_transition`` on."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from ohra import (
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterSharding,
    CurrentShardRegionState,
    Entity,
    EntityContext,
    EntityTypeKey,
    GetShardRegionState,
    ShardingEnvelope,
    ask,
)
from ohra.cluster.ddata import ReplicatorSettings, sqlite_durable_store
from ohra.cluster.sharding import shard_id_of
from ohra.cluster.sharding.shard import CurrentShardState, GetShardState

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from ..test_cluster_sharding import FAST_SHARDING
from .test_ddata_shard_coordinator import Node


@dataclass(frozen=True)
class Message:
    reply_to: ActorRef[str]


TYPE_KEY = EntityTypeKey[Message]("ShardIdExtractorChange")


def pa(_entity: EntityContext[Message]) -> Behavior[Message]:
    @Behaviors.receive_message
    def receive(msg: Message) -> Behavior[Message]:
        msg.reply_to.tell("ack")
        return Behaviors.same

    return receive


def ids_sharing_shards() -> tuple[list[str], str, str]:
    """Three ids in one shard under 10 shards and in one other shard under 11: the ids, the first
    shard and the second."""
    by_shards: dict[tuple[str, str], list[str]] = {}
    for n in range(1000):
        entity_id = str(n)
        shards = (shard_id_of(entity_id, 10), shard_id_of(entity_id, 11))
        if shards[0] == shards[1]:
            continue
        ids = by_shards.setdefault(shards, [])
        ids.append(entity_id)
        if len(ids) == 3:
            return ids, *shards
    raise AssertionError("no three ids share a shard under both counts")


IDS, FIRST_SHARD, SECOND_SHARD = ids_sharing_shards()


@asynccontextmanager
async def with_system(
    name: str, port: int, dir: Path, number_of_shards: int
) -> AsyncGenerator[tuple[ActorSystem[Any], ActorRef[ShardingEnvelope[Message]]]]:
    """``withSystem`` (141-151): a one-node cluster that joins itself, and the region."""
    settings = replace(
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
    async with cluster_node(name, port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        region = ClusterSharding(system).init(Entity(TYPE_KEY, pa).with_settings(settings))
        yield system, region


async def assert_migrated(system: ActorSystem[Any], region: ActorRef[Any]) -> None:
    """The first shard is remembered and empty, the second holds the three ids."""

    async def shard_states() -> None:
        reply: CurrentShardRegionState = await ask(region, GetShardRegionState, 3)
        states: dict[str, frozenset[str]] = {}
        for shard_id in reply.shard_ids:
            shard = system.resolve(f"{region.path}/{shard_id}")
            state: CurrentShardState = await ask(shard, GetShardState, 3)
            states[state.shard] = state.entity_ids
        assert states.get(FIRST_SHARD) == frozenset()
        assert states.get(SECOND_SHARD) == frozenset(IDS)

    await Node(system).probe().await_assert(shard_states, max=10)


async def test_allow_a_change_to_the_shard_id_extractor(tmp_path: Path):
    port, dir = free_port(), tmp_path / "remember-entities"

    async with with_system("FirstShardIdExtractor", port, dir, 10) as (system, _):
        for entity_id in IDS:
            entity = ClusterSharding(system).entity_ref_for(TYPE_KEY, entity_id)
            assert await ask(entity, Message, 5) == "ack"

    async with with_system("SecondShardIdExtractor", port, dir, 11) as (system, region):
        # shards should have been remembered but migrated over to the second shard
        await assert_migrated(system, region)

    async with with_system("ThirdIncarnation", port, dir, 11) as (system, region):
        # started as remembered: the first shard loads none, the second all three
        await assert_migrated(system, region)
