"""RememberEntitiesStarterSpec (cluster-sharding internal/RememberEntitiesStarterSpec.scala):
the starter starts remembered entities through the region, all at once or at a constant rate,
retries what is not acked and tells the shard which entities moved to other shards; the manager
throttles the constant rate across shards. Batches are taken in sorted id order."""

from __future__ import annotations

from typing import Any

import pytest

from ohra.cluster.sharding.remember_entity_starter import (
    remember_entity_starter,
    remember_entity_starter_manager,
)
from ohra.cluster.sharding.shard import (
    ClusterShardingSettings,
    EntitiesMovedToOtherShard,
    StartEntities,
    StartEntityAck,
    _StartEntity,  # pyright: ignore[reportPrivateUsage]
)
from ohra.testkit import ActorTestKit, ManualTime, TestProbe

ALL = ClusterShardingSettings(retry_interval=1.0)
# Pekko's spec uses retry-interval = 1 s and real time, where a retry tick that lands on a batch
# tick is luck. ManualTime makes the 2 s batch ticks and 1 s retry ticks coincide, and the retry
# would resend the batch just started, so the constant cases retry at 1.3 s.
CONSTANT = ClusterShardingSettings(
    retry_interval=1.3,
    entity_recovery_strategy="constant",
    entity_recovery_constant_rate_frequency=2.0,
    entity_recovery_constant_rate_number_of_entities=2,
)
IDS = frozenset({"1", "2", "3"})


async def ack(region: TestProbe[Any], shard_id: str) -> str:
    start = await region.expect_message_type(_StartEntity)
    assert start.ack_to is not None
    start.ack_to.tell(StartEntityAck(start.entity_id, shard_id))
    return start.entity_id


async def test_try_start_all_entities_directly_with_entity_recovery_strategy_all():
    async with ActorTestKit(manual_time=True) as testkit:
        region: TestProbe[Any] = testkit.create_test_probe()
        shard: TestProbe[Any] = testkit.create_test_probe()
        starter = testkit.spawn(
            remember_entity_starter(region.ref, shard.ref, "s1", IDS, False, ALL)
        )
        started = {await ack(region, "s1") for _ in range(3)}
        assert started == IDS
        await region.expect_terminated(starter)
        await shard.expect_no_message(0.1)
        await region.expect_no_message(0.1)


async def test_retry_start_all_entities_with_no_ack_with_entity_recovery_strategy_all():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        region: TestProbe[Any] = testkit.create_test_probe()
        shard: TestProbe[Any] = testkit.create_test_probe()
        starter = testkit.spawn(
            remember_entity_starter(region.ref, shard.ref, "s1", IDS, False, ALL)
        )
        for _ in range(3):
            await region.expect_message_type(_StartEntity)
        await manual.time_passes(1.0)
        started = {await ack(region, "s1") for _ in range(3)}
        assert started == IDS
        await region.expect_terminated(starter)
        await shard.expect_no_message(0.1)


async def test_inform_the_shard_when_entities_have_been_reallocated_to_a_different_shard_id():
    async with ActorTestKit(manual_time=True) as testkit:
        region: TestProbe[Any] = testkit.create_test_probe()
        shard: TestProbe[Any] = testkit.create_test_probe()
        starter = testkit.spawn(
            remember_entity_starter(region.ref, shard.ref, "s1", IDS, False, ALL)
        )
        await ack(region, "s1")
        await ack(region, "Relocated1")
        await ack(region, "Relocated2")
        await shard.expect_message(EntitiesMovedToOtherShard(frozenset({"2", "3"})))
        await region.expect_terminated(starter)


async def test_try_start_all_entities_in_a_throttled_way_with_entity_recovery_strategy_constant():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        region: TestProbe[Any] = testkit.create_test_probe()
        shard: TestProbe[Any] = testkit.create_test_probe()
        ids = frozenset({"1", "2", "3", "4", "5"})
        starter = testkit.spawn(
            remember_entity_starter(region.ref, shard.ref, "s1", ids, True, CONSTANT)
        )
        assert [await ack(region, "s1") for _ in range(2)] == ["1", "2"]
        await manual.expect_no_message_for(0.6, region)
        await manual.time_passes(1.4)
        assert [await ack(region, "s1") for _ in range(2)] == ["3", "4"]
        await manual.expect_no_message_for(0.6, region)
        await manual.time_passes(1.4)
        assert await ack(region, "s1") == "5"
        await region.expect_terminated(starter)
        await shard.expect_no_message(0.1)
        await region.expect_no_message(0.1)


async def test_the_manager_starts_entities_for_all_shards_immediately_with_strategy_all():
    async with ActorTestKit(manual_time=True) as testkit:
        region: TestProbe[Any] = testkit.create_test_probe()
        shard1: TestProbe[Any] = testkit.create_test_probe()
        shard2: TestProbe[Any] = testkit.create_test_probe()
        manager = testkit.spawn(remember_entity_starter_manager(region.ref, ALL))
        manager.tell(StartEntities(shard1.ref, "s1", frozenset({"1", "2"})))
        manager.tell(StartEntities(shard2.ref, "s2", frozenset({"3", "4"})))
        started: set[str] = set()
        for _ in range(4):
            start = await region.expect_message_type(_StartEntity)
            assert start.ack_to is not None
            start.ack_to.tell(
                StartEntityAck(start.entity_id, "s1" if start.entity_id in "12" else "s2")
            )
            started.add(start.entity_id)
        assert started == {"1", "2", "3", "4"}


async def test_the_manager_throttles_entity_starting_across_shards_with_strategy_constant():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        region: TestProbe[Any] = testkit.create_test_probe()
        shard1: TestProbe[Any] = testkit.create_test_probe()
        shard2: TestProbe[Any] = testkit.create_test_probe()
        manager = testkit.spawn(remember_entity_starter_manager(region.ref, CONSTANT))
        manager.tell(StartEntities(shard1.ref, "s1", frozenset({"1", "2", "3", "4", "5"})))
        manager.tell(StartEntities(shard2.ref, "s2", frozenset({"6", "7", "8"})))

        async def receive_start_and_ack() -> str:
            start = await region.expect_message_type(_StartEntity)
            assert start.ack_to is not None
            shard_id = "s1" if int(start.entity_id) <= 5 else "s2"
            start.ack_to.tell(StartEntityAck(start.entity_id, shard_id))
            return start.entity_id

        started = [await receive_start_and_ack() for _ in range(2)]
        await manual.expect_no_message_for(0.6, region)
        await manual.time_passes(1.4)
        started += [await receive_start_and_ack() for _ in range(2)]
        await manual.expect_no_message_for(0.6, region)
        await manual.time_passes(1.4)
        started.append(await receive_start_and_ack())
        assert started == ["1", "2", "3", "4", "5"]
        # shard 2 waits for the delay after shard 1's starter stopped
        await manual.expect_no_message_for(0.6, region)
        await manual.time_passes(1.4)
        started += [await receive_start_and_ack() for _ in range(2)]
        await manual.expect_no_message_for(0.6, region)
        await manual.time_passes(1.4)
        started.append(await receive_start_and_ack())
        assert started == ["1", "2", "3", "4", "5", "6", "7", "8"]


async def test_a_starter_with_no_ids_is_refused():
    region: Any = None
    with pytest.raises(ValueError):
        remember_entity_starter(region, region, "s1", frozenset(), False, ALL)
