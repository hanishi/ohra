"""Pekko Typed's ShardingMessageExtractor (ShardingMessageExtractor.scala) and the region's use of
it, as ClusterShardingImpl's ExtractorAdapter: a ``ShardingEnvelope`` or ``StartEntity`` gives
its entity id directly, any other message goes through ``entity_id`` and ``unwrap_message``, and
every shard id comes from ``shard_id``."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
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
    StartEntity,
    ask,
)
from ohra.cluster.sharding import (
    ExtractedEntity,
    HashCodeMessageExtractor,
    HashCodeNoEnvelopeMessageExtractor,
    ShardingMessageExtractor,
    shard_id_of,
)
from ohra.cluster.sharding.shard import Sharding
from ohra.remote.serialization import remotable
from ohra.testkit import ActorTestKit, TestProbe

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, node


@remotable
@dataclass(frozen=True)
class ExtractorWhere:
    reply_to: ActorRef[str]


@remotable
@dataclass(frozen=True)
class ExtractorPing:
    id: str
    reply_to: ActorRef[str]


def where(entity: EntityContext[ExtractorWhere]) -> Behavior[ExtractorWhere]:
    @Behaviors.receive_message
    def on_message(msg: ExtractorWhere) -> Behavior[ExtractorWhere]:
        msg.reply_to.tell(entity.entity_id)
        return Behaviors.same

    return on_message


def pinged(entity: EntityContext[ExtractorPing]) -> Behavior[ExtractorPing]:
    @Behaviors.receive_message
    def on_message(msg: ExtractorPing) -> Behavior[ExtractorPing]:
        msg.reply_to.tell(entity.entity_id)
        return Behaviors.same

    return on_message


class PrefixExtractor(ShardingMessageExtractor[ShardingEnvelope[ExtractorWhere], ExtractorWhere]):
    """The shard of ``"a-1"`` is ``"a"``."""

    def entity_id(self, message: ShardingEnvelope[ExtractorWhere]) -> str:
        return message.entity_id

    def shard_id(self, entity_id: str) -> str:
        return entity_id.split("-")[0]

    def unwrap_message(self, message: ShardingEnvelope[ExtractorWhere]) -> ExtractorWhere:
        return message.message


def ping_id(message: ExtractorPing) -> str:
    return message.id


WHERE = EntityTypeKey[ExtractorWhere]("ExtractorWhere")
PING = EntityTypeKey[ExtractorPing]("ExtractorPing")


async def region_shards(region: ActorRef[Any]) -> frozenset[str]:
    state: CurrentShardRegionState = await ask(region, GetShardRegionState, timeout=5)
    return state.shard_ids


async def shards_become(region: ActorRef[Any], expected: frozenset[str]) -> None:
    for _ in range(500):
        if await region_shards(region) == expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"the region's shards did not become {expected}")


def _up(system: ActorSystem[Any]) -> bool:
    return all_up(system, 1)


async def test_a_custom_extractor_puts_entities_on_its_shards():
    p = free_port()
    async with node(p, [p]) as system:
        await eventually(lambda: _up(system), timeout=3)
        sharding = ClusterSharding(system)
        region = sharding.init(
            Entity(WHERE, where)
            .with_settings(FAST_SHARDING)
            .with_message_extractor(PrefixExtractor())
        )
        for entity_id in ("a-1", "a-2", "b-1"):
            ref = sharding.entity_ref_for(WHERE, entity_id)
            assert await ask(ref, ExtractorWhere, timeout=5) == entity_id
        assert await region_shards(region) == {"a", "b"}


async def test_a_no_envelope_extractor_delivers_its_own_messages():
    p = free_port()
    async with node(p, [p]) as system:
        await eventually(lambda: _up(system), timeout=3)
        extractor = ShardingMessageExtractor.no_envelope(10, None, ping_id)
        region = ClusterSharding(system).init(
            Entity(PING, pinged).with_settings(FAST_SHARDING).with_message_extractor(extractor)
        )
        assert await ask(region, lambda r: ExtractorPing("x", r), timeout=5) == "x"
        assert await region_shards(region) == {shard_id_of("x", 10)}


async def test_entity_ref_for_and_start_entity_use_the_extractors_shard():
    p = free_port()
    async with node(p, [p]) as system:
        await eventually(lambda: _up(system), timeout=3)
        sharding = ClusterSharding(system)
        region = sharding.init(
            Entity(WHERE, where)
            .with_settings(FAST_SHARDING)
            .with_message_extractor(PrefixExtractor())
        )
        assert await ask(sharding.entity_ref_for(WHERE, "c-9"), ExtractorWhere, timeout=5) == "c-9"
        region.tell(StartEntity("d-1"))
        await shards_become(region, frozenset({"c", "d"}))


def test_with_methods_keep_the_extractor():
    x = PrefixExtractor()
    entity = Entity(WHERE, where).with_message_extractor(x).with_settings(FAST_SHARDING)
    entity = entity.with_role("r").with_buffer_size(5)
    assert isinstance(entity, ExtractedEntity)
    assert entity.message_extractor is x
    assert entity.role == "r" and entity.buffer_size == 5


async def test_single_node_sharding_uses_the_extractor():
    async with ActorTestKit() as testkit:
        entity = Entity(WHERE, where).with_message_extractor(PrefixExtractor())
        region = testkit.spawn(Sharding.init(entity))
        probe: TestProbe[str] = testkit.create_test_probe()
        for entity_id in ("a-1", "a-2", "b-1"):
            Sharding.entity_ref_for(region, entity_id).tell(ExtractorWhere(probe.ref))
            await probe.expect_message(entity_id)
        assert await region_shards(region) == {"a", "b"}


async def test_single_node_sharding_takes_a_no_envelope_extractors_messages():
    async with ActorTestKit() as testkit:
        extractor = ShardingMessageExtractor.no_envelope(10, None, ping_id)
        region = testkit.spawn(
            Sharding.init(Entity(PING, pinged).with_message_extractor(extractor))
        )
        probe: TestProbe[str] = testkit.create_test_probe()
        region.tell(ExtractorPing("y", probe.ref))
        await probe.expect_message("y")


def test_the_default_extractor_is_crc32():
    extractor = HashCodeMessageExtractor[Any](10)
    assert extractor.shard_id("abc") == shard_id_of("abc", 10)
    assert extractor.entity_id(ShardingEnvelope("abc", 1)) == "abc"
    assert extractor.unwrap_message(ShardingEnvelope("abc", 1)) == 1


def test_a_no_envelope_extractor_unwraps_to_itself():
    class ById(HashCodeNoEnvelopeMessageExtractor[ExtractorPing]):
        def entity_id(self, message: ExtractorPing) -> str:
            return message.id

    probe_ref: Any = None
    msg = ExtractorPing("z", probe_ref)
    extractor = ById(7)
    assert extractor.entity_id(msg) == "z"
    assert extractor.unwrap_message(msg) is msg
    assert extractor.shard_id("z") == shard_id_of("z", 7)
    assert repr(extractor) == "HashCodeNoEnvelopeMessageExtractor(7)"
