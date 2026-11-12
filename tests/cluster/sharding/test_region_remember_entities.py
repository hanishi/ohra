"""The region's remember-entities branches (ShardRegion.scala 674-705, 889, 1018-1026, 1250-1299):
the region hands its shards the store provider and the starter manager, restarts a remembering
shard that stopped while not being handed off after ``shard_failure_backoff``, takes the public
``StartEntity``, and logs that remember entities turns automatic passivation off. Pekko has no
spec of its own for these branches; the tests run on one real cluster node, and
``_LeaseLost(None)`` stops a shard at once, as a failed store write or Pekko's ``PoisonPill``
would."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    ClusterSharding,
    ClusterShardingSettings,
    Entity,
    EntityContext,
    EntityTypeKey,
    Sharding,
    ShardingEnvelope,
    StartEntity,
)
from ohra.cluster.ddata import ReplicatorSettings
from ohra.cluster.sharding import shard_id_of
from ohra.cluster.sharding.shard import (
    HandOff,
    ShardStopped,
    _LeaseLost,  # pyright: ignore[reportPrivateUsage]
)

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node
from ..test_cluster_sharding import FAST_SHARDING, shards_on
from .test_ddata_shard_coordinator import Node

REMEMBER = replace(
    FAST_SHARDING,
    remember_entities=True,
    verbose_debug_logging=True,
    fail_on_invalid_entity_state_transition=True,
    shard_failure_backoff=0.2,
    distributed_data=ReplicatorSettings(durable_keys=frozenset()),
)


@dataclass(frozen=True)
class Started:
    id: str


@dataclass(frozen=True)
class Got:
    msg: object


@dataclass(frozen=True)
class WhereIsShard:
    reply_to: ActorRef[ActorRef[Any]]


KEY = EntityTypeKey[WhereIsShard]("remembering")


def reporting(entity: EntityContext[WhereIsShard], events: ActorRef[Any]) -> Behavior[Any]:
    """Reports ``Started`` when set up, its shard on ``WhereIsShard``, and any other message as
    ``Got``."""

    @Behaviors.setup
    def setup(_ctx: ActorContext[Any]) -> Behavior[Any]:
        events.tell(Started(entity.entity_id))

        @Behaviors.receive_message
        def receive(msg: Any) -> Behavior[Any]:
            if isinstance(msg, WhereIsShard):
                msg.reply_to.tell(entity.shard)
            else:
                events.tell(Got(msg))
            return Behaviors.same

        return receive

    return setup


def init(system: Any, events: ActorRef[Any], settings: ClusterShardingSettings = REMEMBER) -> Any:
    entity = Entity(KEY, lambda e: cast(Behavior[WhereIsShard], reporting(e, events)))
    return ClusterSharding(system).init(entity.with_settings(settings))


async def shard_of(region: Any, entity_id: str, node: Node) -> ActorRef[Any]:
    shards = node.probe()
    region.tell(ShardingEnvelope(entity_id, WhereIsShard(shards.ref)))
    return await shards.receive_message(5)


async def test_a_shard_that_stops_while_not_handed_off_is_restarted_after_the_backoff():
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        node = Node(system)
        events = node.probe()
        region = init(system, events.ref)
        shard = await shard_of(region, "a", node)
        await events.expect_message(Started("a"), 5)

        shard.tell(_LeaseLost(None))
        await events.expect_terminated(shard, 5)
        # no message sent: the region restarts the shard, which starts what it remembers
        await events.expect_message(Started("a"), REMEMBER.shard_failure_backoff + 3)


async def test_a_handed_off_shard_is_not_restarted():
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        node = Node(system)
        events, probe = node.probe(), node.probe()
        region = init(system, events.ref)
        await shard_of(region, "a", node)
        await events.expect_message(Started("a"), 5)
        shard = shard_id_of("a", REMEMBER.number_of_shards)

        region.tell(HandOff(shard, probe.ref))
        await probe.expect_message(ShardStopped(shard), 5)
        # a non-event: wait out twice the backoff a restart would take
        await asyncio.sleep(2 * REMEMBER.shard_failure_backoff)
        assert shard not in await shards_on(region)
        await events.expect_no_message(0.1)


async def test_start_entity_starts_and_remembers_an_entity():
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        node = Node(system)
        events = node.probe()
        region = init(system, events.ref)

        region.tell(StartEntity("a"))
        await events.expect_message(Started("a"), 5)
        shard = await shard_of(region, "a", node)

        shard.tell(_LeaseLost(None))
        await events.expect_terminated(shard, 5)
        await events.expect_message(Started("a"), REMEMBER.shard_failure_backoff + 3)


async def test_start_entity_starts_an_entity_of_a_non_remembering_shard_without_delivering_it():
    """With remember entities off, the shard takes the enveloped ``StartEntity`` as Pekko's
    ``deliverMessage`` does: the entity starts, and it gets no ``StartEntity``."""
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        node = Node(system)
        events = node.probe()
        region = init(system, events.ref, replace(REMEMBER, remember_entities=False))

        region.tell(StartEntity("b"))
        await events.expect_message(Started("b"), 5)
        # the entity handles messages in order: a StartEntity would have been reported before it
        # answers this
        await shard_of(region, "b", node)
        await events.expect_no_message(0.1)


async def test_the_region_logs_that_remember_entities_turns_passivation_off(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        init(system, Node(system).probe().ref)
        text = (
            "remembering: Entities will not be passivated automatically because "
            "'rememberEntities' is enabled."
        )
        await eventually(
            lambda: any(
                r.levelno == logging.DEBUG and r.getMessage() == text for r in caplog.records
            ),
            timeout=5,
        )
        assert not [
            r
            for r in caplog.records
            if r.getMessage().startswith("remembering: Automatic entity passivation")
        ]


def test_single_node_sharding_refuses_remember_entities():
    def create(_e: EntityContext[WhereIsShard]) -> Behavior[WhereIsShard]:
        return Behaviors.ignore

    with pytest.raises(ValueError, match="ClusterSharding"):
        Sharding.init(
            Entity(KEY, create).with_settings(ClusterShardingSettings(remember_entities=True))
        )
