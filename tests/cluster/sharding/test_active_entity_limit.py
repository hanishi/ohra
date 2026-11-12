"""The active-entity limit through the region: Apache Pekko's ShardRegion passes
``SetActiveEntityLimit`` to every shard (ShardRegion.scala 767 and 1278) and tells every shard
``ShardsUpdated`` with the shard count when a shard starts (1356-1360) or terminates
(1011-1016), so a limit-based strategy divides its per-region limit across the shards. The
region also logs ``PassivationStrategy.describe`` at start (698-705, classic
ClusterShardingSettings.scala 782-860). Pekko has no spec for these; the tests are ohra's own.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    ClusterShardingSettings,
    CurrentShardRegionState,
    Entity,
    GetShardRegionState,
    PassivationStrategySettings,
    SetActiveEntityLimit,
    Sharding,
    ShardingEnvelope,
    ask,
)
from ohra.cluster.sharding import region as region_module
from ohra.cluster.sharding.coordinator import HostShard, ShardStarted
from ohra.cluster.sharding.passivation_settings import (
    describe_passivation_strategy,
    passivation_strategy,
)
from ohra.cluster.sharding.region import (
    _ExitingProgress,  # pyright: ignore[reportPrivateUsage]
    region_behavior,
)
from ohra.cluster.sharding.shard import (
    HandOff,
    ShardInitialized,
    ShardsUpdated,
    ShardStopped,
    shard_id_of,
)
from ohra.testkit import ActorTestKit, TestProbe

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, Count, hosted, node, ref, start
from .test_ddata_shard_coordinator import manual_node
from .test_shard import Add, Cmd, Counter, Get, counter

N = 10
X, Y = "0", "1"

S = PassivationStrategySettings
LRU_4 = S.defaults.with_active_entity_limit(4).with_least_recently_used_replacement()


def ids_in_shard(shard: str, count: int) -> list[str]:
    """The first ``count`` ids of ``shard``, a module helper like ``tests/test_sharding.py``'s
    ``_id_in_another_shard_than``."""
    return [i for i in (f"x{k}" for k in range(10_000)) if shard_id_of(i, N) == shard][:count]


def lru_region(events: TestProbe[str]) -> Behavior[Any]:
    settings = ClusterShardingSettings(number_of_shards=N).with_passivation_strategy(LRU_4)
    entity = Entity(Counter, counter(events.ref, reports_start=True)).with_settings(settings)
    return Sharding.init(entity)


async def add(region: ActorRef[Any], events: TestProbe[str], ids: list[str]) -> None:
    for entity_id in ids:
        Sharding.entity_ref_for(region, entity_id).tell(Add(1))
    assert {await events.receive_message() for _ in ids} == {f"started {i}" for i in ids}


async def total(region: ActorRef[Any], replies: TestProbe[int], entity_id: str) -> int:
    """The entity's total: 0 after a passivation, since a restarted counter starts over."""
    Sharding.entity_ref_for(region, entity_id).tell(Get(replies.ref))
    return await replies.receive_message()


async def test_set_active_entity_limit_reaches_every_shard():
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        replies: TestProbe[int] = testkit.create_test_probe()
        region = testkit.spawn(lru_region(events))
        a, b, c, d = ids_in_shard(X, 4)
        await add(region, events, [a, b, c, d])
        region.tell(SetActiveEntityLimit(2))
        assert {await events.receive_message() for _ in range(2)} == {
            f"stopping {a}",
            f"stopping {b}",
        }
        assert await total(region, replies, c) == 1
        assert await total(region, replies, d) == 1


async def test_a_new_shard_divides_the_limit():
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        replies: TestProbe[int] = testkit.create_test_probe()
        region = testkit.spawn(lru_region(events))
        a, b, c, d = ids_in_shard(X, 4)
        await add(region, events, [a, b, c, d])
        (y,) = ids_in_shard(Y, 1)
        Sharding.entity_ref_for(region, y).tell(Add(1))
        assert {await events.receive_message() for _ in range(3)} == {
            f"started {y}",
            f"stopping {a}",
            f"stopping {b}",
        }
        assert await total(region, replies, c) == 1
        assert await total(region, replies, d) == 1


async def test_a_shard_that_stops_raises_the_others_limit():
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        replies: TestProbe[int] = testkit.create_test_probe()
        stopped: TestProbe[ShardStopped] = testkit.create_test_probe()
        region = testkit.spawn(lru_region(events))
        a, b, e, f, g = ids_in_shard(X, 5)
        c, d = ids_in_shard(Y, 2)
        # two shards: each is at its limit of 2
        await add(region, events, [a, b])
        await add(region, events, [c, d])
        shard_y = testkit.system.resolve(f"{region.path}/{Y}")
        shard_y.tell(HandOff(Y, stopped.ref))
        assert {await events.receive_message() for _ in range(2)} == {
            f"stopping {c}",
            f"stopping {d}",
        }
        await stopped.expect_message(ShardStopped(Y))
        await stopped.expect_terminated(shard_y)
        # one shard left: its limit is 4, so two more entities passivate nothing
        await add(region, events, [e, f])
        assert await total(region, replies, a) == 1
        assert await total(region, replies, b) == 1
        # and a fifth passivates the least recently used one
        Sharding.entity_ref_for(region, g).tell(Add(1))
        assert {await events.receive_message() for _ in range(2)} == {
            f"started {g}",
            f"stopping {e}",
        }


def stand_in(probe: TestProbe[Any]) -> Any:
    """``shard_behavior`` replaced by a shard that is initialized at once and passes every
    message to ``probe``."""

    def shard_behavior(
        _entity: Any, shard: str, region: ActorRef[Any], *_remember: Any
    ) -> Behavior[Any]:
        def setup(_ctx: ActorContext[Any]) -> Behavior[Any]:
            region.tell(ShardInitialized(shard))

            def forward(msg: Any) -> Behavior[Any]:
                probe.ref.tell(msg)
                return Behaviors.same

            return Behaviors.receive_message(forward)

        return Behaviors.setup(setup)

    return shard_behavior


@pytest.mark.parametrize(
    ("strategy", "first"),
    [
        (S.disabled, ShardingEnvelope("a", Add(1))),
        (LRU_4, ShardsUpdated(1)),
    ],
    ids=["no strategy", "lru"],
)
async def test_no_shards_updated_without_a_passivation_strategy(
    monkeypatch: pytest.MonkeyPatch, strategy: PassivationStrategySettings, first: object
):
    async with manual_node() as n:
        shard = n.probe()
        started = n.probe()
        monkeypatch.setattr(region_module, "shard_behavior", stand_in(shard))
        settings = ClusterShardingSettings().with_passivation_strategy(strategy)
        entity = Entity(Counter, counter(n.probe().ref)).with_settings(settings)
        region = n.system._system_root.spawn_child(  # pyright: ignore[reportPrivateUsage]
            region_behavior(entity, settings, _ExitingProgress()), "Counter", None
        ).ref
        home = shard_id_of("a", settings.number_of_shards)
        region.tell(HostShard(home, started.ref))
        await started.expect_message(ShardStarted(home))
        region.tell(ShardingEnvelope[Cmd]("a", Add(1)))
        assert await shard.receive_message() == first


async def test_the_cluster_region_forwards_the_limit_to_its_shards():
    port = free_port()
    async with node(port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=3)
        region = start(system, settings=FAST_SHARDING.with_passivation_strategy(LRU_4))
        ids = ids_in_shard(X, 4)
        for entity_id in ids:
            assert await ask(ref(system, entity_id), lambda r: Count(1, r), timeout=5) == 1
        assert await hosted(region) == set(ids)
        cast(ActorRef[Any], region).tell(SetActiveEntityLimit(2))
        async with asyncio.timeout(5):  # the two least recently used passivate
            while await hosted(region) != set(ids[2:]):
                await asyncio.sleep(0.01)


@pytest.mark.parametrize(
    ("settings", "text"),
    [
        (S.disabled, "disabled"),
        (S.default_idle_strategy, "idle entities after [120 s], checked every [60 s]"),
        (
            S.defaults.with_active_entity_limit(10)
            .with_least_recently_used_replacement()
            .with_idle_entity_passivation(30.0, 5.0),
            "least recently used entities when over [10] entities and idle entities after "
            "[30 s], checked every [5 s]",
        ),
        (
            S.defaults.with_active_entity_limit(10).with_replacement_policy(
                S.LeastRecentlyUsedSettings.defaults.with_segmented((0.25, 0.75))
            ),
            "least recently used entities when over [10] entities (segmented with [2] levels "
            "with proportions of [0.25, 0.75])",
        ),
        (
            S.defaults.with_active_entity_limit(10).with_most_recently_used_replacement(),
            "most recently used entities when over [10] entities",
        ),
        (
            S.defaults.with_active_entity_limit(10).with_replacement_policy(
                S.LeastFrequentlyUsedSettings.defaults.with_dynamic_aging()
            ),
            "least frequently used entities when over [10] entities (with dynamic aging)",
        ),
        (
            S.defaults.with_active_entity_limit(10).with_admission(S.AdmissionSettings.defaults),
            "composite strategy with limit of [10] active entities, no admission window, "
            "always admit, main (disabled)",
        ),
        (
            S.default_strategy,
            "composite strategy with limit of [100000] active entities, admission window "
            "(least recently used entities) with proportions [initial = 0.01, min = 0.01, "
            "max = 1.0] adapting with hill-climbing optimizer [adjust multiplier = 10.0, "
            "initial step = 0.0625, restart threshold = 0.05, step decay = 0.98], admit using "
            "frequency sketch [width multiplier = 4, reset multiplier = 10.0, depth = 4, "
            "counter bits = 4], main (least recently used entities (segmented with [2] levels "
            "with proportions of [0.20, 0.80]))",
        ),
    ],
)
def test_describe_passivation_strategy(settings: PassivationStrategySettings, text: str):
    assert describe_passivation_strategy(passivation_strategy(settings)) == text


async def test_the_region_logs_its_passivation_strategy_at_start(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.INFO)
    async with ActorTestKit(manual_time=True) as testkit:
        events: TestProbe[str] = testkit.create_test_probe()
        state: TestProbe[CurrentShardRegionState] = testkit.create_test_probe()
        region = testkit.spawn(Sharding.init(Entity(Counter, counter(events.ref))))
        region.tell(GetShardRegionState(state.ref))
        await state.receive_message()
    assert (
        "Counter: Automatic entity passivation: idle entities after [120 s], checked every "
        "[60 s]" in caplog.messages
    )
