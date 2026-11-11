"""Ported from Apache Pekko's EntityPassivationSpec (cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/passivation/EntityPassivationSpec.scala). The case:

- DisabledEntityPassivationSpec: not passivate when passivation is disabled (152)

Then tests of ohra's own: the scheduler's clock driving idle passivation, ``entity_terminated``
for an id a strategy does not track, and a per-shard limit of 0.

Porting rules, shared by the passivation spec ports:

- Each Scala config becomes a ``PassivationStrategySettings`` expression; the disabled spec's
  ``strategy = none`` is ``PassivationStrategySettings.disabled``.
- The specs drive the strategies through ``harness.Region``, not real shards and entities.
  ``region ! Envelope(shard, id, _)`` is ``region.tell(shard, id)``,
  ``ShardRegion.SetActiveEntityLimit(n)`` is ``set_active_entity_limit(n)`` and
  ``ManuallyPassivate`` is ``manually_passivate``. Each ``expectReceived(id, Stop)`` asserts the
  id is in what that step passivated, and each step (a ``tell``, a limit update or a ``passes``)
  asserts exactly what it passivated, empty where Scala expects no ``Stop``: Scala's per-entity
  probes fail on a stray ``Stop``. A module's docstring names any step Scala leaves unchecked.
  Each ``expectState`` asserts ``region.state()``.
- A ``Thread.sleep(timeout / 2)`` is ``region.passes(0.5)`` on a ``TestClock``. Each idle
  expectation asserts the entity is passivated by the first ``passes`` after which more than the
  1.0 s timeout has gone since its last touch, and not by an earlier one: Scala's
  ``> configuredIdleTimeout`` bound, at Pekko's interval granularity. The harness ticks once per
  ``passes``, so each step is the strategy's 0.5 s interval.

Differences from the Scala: the disabled case asserts that a touch passivates nothing and that
no interval is scheduled, where Pekko waits a second for no ``Stop``.
"""

import pytest

from ohra.cluster.sharding.passivation import EntityPassivationStrategy
from ohra.cluster.sharding.passivation_settings import (
    NoPassivationStrategy,
    PassivationStrategySettings,
)
from ohra.cluster.sharding.shard import ClusterShardingSettings
from ohra.testkit import ActorTestKit, ManualTime

from ....util import TestClock
from .harness import Region

P = PassivationStrategySettings


def test_disabled_entity_passivation_not_passivate_when_passivation_is_disabled() -> None:
    settings = ClusterShardingSettings().with_passivation_strategy(P.disabled)
    assert settings.passivation_strategy == NoPassivationStrategy()
    region = Region(P.disabled, TestClock())
    assert region.tell(1, 1) == set()
    assert region.state() == {1: {1}}
    assert EntityPassivationStrategy.create(settings, TestClock()).scheduled_interval is None


async def test_the_scheduler_clock_drives_idle_passivation() -> None:
    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        settings = ClusterShardingSettings().with_passivation_strategy(
            P.defaults.with_idle_entity_passivation(1.0)
        )
        strategy = EntityPassivationStrategy.create(
            settings, lambda: testkit.system.scheduler.current_time
        )
        assert strategy.entity_touched("1") == []
        await manual_time.time_passes(1.0)
        assert strategy.interval_passed() == []
        await manual_time.time_passes(0.5)
        assert strategy.interval_passed() == ["1"]


_IDLE = P.defaults.with_idle_entity_passivation(1.0)
_LIMIT_1 = P.defaults.with_active_entity_limit(1)
_LRU = _LIMIT_1.with_least_recently_used_replacement()
_SLRU = _LIMIT_1.with_replacement_policy(
    P.LeastRecentlyUsedSettings.defaults.with_segmented([0.2, 0.8])
)
_MRU = _LIMIT_1.with_most_recently_used_replacement()
_LFU = _LIMIT_1.with_least_frequently_used_replacement()


def _idle(s: PassivationStrategySettings) -> PassivationStrategySettings:
    return s.with_idle_entity_passivation(1.0)


@pytest.mark.parametrize(
    "passivation",
    [_IDLE, _LRU, _idle(_LRU), _SLRU, _idle(_SLRU), _MRU, _idle(_MRU), _LFU, _idle(_LFU)],
    ids=["idle", "lru", "lru-idle", "slru", "slru-idle", "mru", "mru-idle", "lfu", "lfu-idle"],
)
def test_terminating_an_untracked_entity_is_a_no_op(
    passivation: PassivationStrategySettings,
) -> None:
    settings = ClusterShardingSettings().with_passivation_strategy(passivation)
    clock = TestClock()
    control = EntityPassivationStrategy.create(settings, clock)
    strategy = EntityPassivationStrategy.create(settings, clock)
    strategy.entity_terminated("never-seen")

    def passivate_the_first(s: EntityPassivationStrategy) -> list[str]:
        clock.now = 0.0
        passivated = s.entity_touched("1")
        clock.now = 2.0
        passivated += s.entity_touched("2") + s.interval_passed()
        for entity_id in passivated:
            s.entity_terminated(entity_id)  # the shard, on the entity's Terminated
        return passivated

    assert passivate_the_first(control) == ["1"]
    assert passivate_the_first(strategy) == ["1"]
    strategy.entity_terminated("1")
    strategy.entity_terminated("never-seen")

    for entity_id in ["1", "3", "2", "4"]:
        clock.now += 0.5
        assert strategy.entity_touched(entity_id) == control.entity_touched(entity_id)
        assert strategy.interval_passed() == control.interval_passed()


@pytest.mark.parametrize(
    ("passivation", "first", "second"),
    [(_LRU, ["1"], ["2"]), (_SLRU, ["1"], ["2"]), (_MRU, [], ["1"]), (_LFU, [], ["1"])],
    ids=["lru", "slru", "mru", "lfu"],
)
def test_a_per_shard_limit_of_zero(
    passivation: PassivationStrategySettings, first: list[str], second: list[str]
) -> None:
    settings = ClusterShardingSettings().with_passivation_strategy(passivation)
    strategy = EntityPassivationStrategy.create(settings, TestClock())
    assert strategy.shards_updated(2) == []
    assert strategy.entity_touched("1") == first
    for entity_id in first:
        strategy.entity_terminated(entity_id)  # the shard, on the entity's Terminated
    assert strategy.entity_touched("2") == second
