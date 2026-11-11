"""Ported from Apache Pekko's LeastFrequentlyUsedSpec (cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/passivation/LeastFrequentlyUsedSpec.scala). The cases:

- LeastFrequentlyUsedSpec: passivate the least frequently used entities when the per-shard
  entity limit is reached (76)
- LeastFrequentlyUsedWithDynamicAgingSpec: passivate the least frequently used entities when the
  per-shard entity limit is reached (226)
- LeastFrequentlyUsedWithIdleSpec: passivate entities when they haven't seen messages for the
  configured timeout (303)
- LeastFrequentlyUsedLimitAdjustmentSpec: adjust per-shard entity limits when the per-region
  limit is dynamically adjusted (346)

Porting rules, as in ``test_entity_passivation_spec``:

- ``config`` is
  ``P.defaults.with_active_entity_limit(10).with_least_frequently_used_replacement()``;
  ``dynamicAgingConfig`` is limit 10 with
  ``LeastFrequentlyUsedSettings.defaults.with_dynamic_aging()``; ``idleConfig`` is limit 3, LFU
  and ``.with_idle_entity_passivation(1.0)``.
- The specs drive ``harness.Region``. Each ``expectReceived(id, Stop)`` asserts the id is in what
  that step passivated (an id's several accesses are one step); where Scala expects no ``Stop``
  in a step, the test asserts nothing about it. Each ``expectState`` asserts ``region.state()``.
- A ``Thread.sleep(timeout / 2)`` is ``region.passes(0.5)``. Each idle expectation asserts the
  entity is passivated by the first ``passes`` after which more than the 1.0 s timeout has gone
  since its last touch, and not by an earlier one.

Differences from the Scala: none beyond the porting rules.
"""

from ohra.cluster.sharding.passivation_settings import PassivationStrategySettings

from ....util import TestClock
from .harness import Region

P = PassivationStrategySettings

CONFIG = P.defaults.with_active_entity_limit(10).with_least_frequently_used_replacement()
DYNAMIC_AGING_CONFIG = P.defaults.with_active_entity_limit(10).with_replacement_policy(
    P.LeastFrequentlyUsedSettings.defaults.with_dynamic_aging()
)
IDLE_CONFIG = (
    P.defaults.with_active_entity_limit(3)
    .with_least_frequently_used_replacement()
    .with_idle_entity_passivation(1.0)
)


def _accesses(region: Region, shard: int, id: int, accesses: int) -> set[int]:
    """``accesses`` messages to one entity: what they passivated."""
    passivated: set[int] = set()
    for _ in range(accesses):
        passivated |= region.tell(shard, id)
    return passivated


def test_lfu_passivate_the_least_frequently_used_entities_when_the_shard_limit_is_reached() -> (
    None
):
    region = Region(CONFIG, TestClock())

    # only one active shard at first, least frequently used entities passivated once the limit
    # is reached
    expect_passivated = {
        11: 1,
        12: 5,
        13: 9,
        14: 13,
        15: 2,
        16: 6,
        17: 10,
        18: 17,
        19: 14,
        20: 18,
    }
    for id in range(1, 21):
        accesses = (id - 1) % 4 + 1  # vary frequency between 1 and 4 accesses
        passivated = _accesses(region, 1, id, accesses)
        if id in expect_passivated:
            assert expect_passivated[id] in passivated

    # shard 1: frequency 3 = (3, 7, 11, 15, 19), frequency 4 = (4, 8, 12, 16, 20)
    assert region.state() == {1: {3, 7, 11, 15, 19, 4, 8, 12, 16, 20}}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert {3, 7, 11, 15, 19} <= region.tell(2, 21)

    # shard 1: frequency 4 = (4, 8, 12, 16, 20)
    # shard 2: frequency 1 = (21)
    assert region.state() == {1: {4, 8, 12, 16, 20}, 2: {21}}

    # shards now have a limit of 5 entities
    # note: newly added entities are not counted as least frequent for passivation
    for id in range(1, 21):
        accesses = (id - 1) % 4 + 1  # vary frequency between 1 and 4 accesses
        passivated_id = id + 3 if accesses == 1 else id - 1
        assert passivated_id in _accesses(region, 1, id, accesses)

    # shard 1: frequency 4 = (4, 8, 12, 16, 20)
    # shard 2: frequency 1 = (21)
    assert region.state() == {1: {4, 8, 12, 16, 20}, 2: {21}}

    # shards now have a limit of 5 entities
    for id in range(21, 25):
        region.tell(2, id)

    # shard 1: frequency 4 = (4, 8, 12, 16, 20)
    # shard 2: frequency 1 = (22, 23, 24), frequency 2 = (21)
    assert region.state() == {1: {4, 8, 12, 16, 20}, 2: {22, 23, 24, 21}}

    # activating a third shard will divide the per-shard limit in three, passivating entities
    # over the new limits
    assert {4, 8, 22} <= region.tell(3, 31)

    # shard 1: frequency 4 = (12, 16, 20)
    # shard 2: frequency 1 = (23, 24), frequency 2 = (21)
    # shard 3: frequency 1 = (31)
    assert region.state() == {1: {12, 16, 20}, 2: {23, 24, 21}, 3: {31}}

    # shards now have a limit of 3 entities
    expect_passivated = {22: 23, 23: 24, 24: 22, 25: 21, 26: 25, 27: 26, 28: 23, 29: 27, 30: 29}
    for id in range(21, 31):
        accesses = (id - 1) % 4 + 1  # vary frequency between 1 and 4 accesses
        passivated = _accesses(region, 2, id, accesses)
        if id in expect_passivated:
            assert expect_passivated[id] in passivated

    # shard 1: frequency 4 = (12, 16, 20)
    # shard 2: frequency 2 = (30), frequency 4 = (24, 28)
    # shard 3: frequency 1 = (31)
    assert region.state() == {1: {12, 16, 20}, 2: {30, 24, 28}, 3: {31}}

    # shards now have a limit of 3 entities
    expect_passivated = {34: 32, 35: 31, 36: 35, 37: 36, 38: 34, 39: 38, 40: 39}
    for id in range(31, 41):
        accesses = 4 - (id - 1) % 4  # vary frequency between 1 and 4 accesses
        passivated = _accesses(region, 3, id, accesses)
        if id in expect_passivated:
            assert expect_passivated[id] in passivated

    # shard 1: frequency 4 = (12, 16, 20)
    # shard 2: frequency 2 = (30), frequency 4 = (24, 28)
    # shard 3: frequency 1 = (40), frequency 4 = (33, 37)
    assert region.state() == {1: {12, 16, 20}, 2: {30, 24, 28}, 3: {40, 33, 37}}

    # manually passivate some entities
    region.manually_passivate(1, 16)
    region.manually_passivate(2, 24)
    region.manually_passivate(3, 33)

    # shard 1: frequency 4 = (12, 20)
    # shard 2: frequency 2 = (30), frequency 4 = (28)
    # shard 3: frequency 1 = (40), frequency 4 = (37)
    assert region.state() == {1: {12, 20}, 2: {30, 28}, 3: {40, 37}}

    for i in range(1, 4):
        passivated: set[int] = set()
        for _ in range(i):
            passivated |= region.tell(1, 10 + i) | region.tell(2, 20 + i) | region.tell(3, 30 + i)
        if i == 2:
            assert {21, 40} <= passivated
        elif i == 3:
            assert {11, 30, 31} <= passivated

    # shard 1: frequency 3 = (13), frequency 4 = (20), frequency 6 = (12)
    # shard 2: frequency 2 = (22), frequency 3 = (23), frequency 4 = (28)
    # shard 3: frequency 2 = (32), frequency 3 = (33), frequency 4 = (37)
    assert region.state() == {1: {13, 20, 12}, 2: {22, 23, 28}, 3: {32, 33, 37}}


def test_lfu_with_dynamic_aging_passivate_the_least_frequently_used_entities() -> None:
    region = Region(DYNAMIC_AGING_CONFIG, TestClock())

    # only one active shard at first
    # ids 1 and 2 are quite popular initially
    for id in range(1, 3):
        _accesses(region, 1, id, 5)
    # ids 3, 4, and 5 are very popular initially
    for id in range(3, 6):
        _accesses(region, 1, id, 10)

    # shard 1: age = 0, @5 (5 + 0) = (1, 2), @10 (10 + 0) = (3, 4, 5)
    assert region.state() == {1: {1, 2, 3, 4, 5}}

    for id in range(6, 21):
        passivated = region.tell(1, id)
        if id > 10:
            assert id - 5 in passivated

    # shard 1: age = 2, @3 (1 + 2) = (16, 17, 18, 19, 20), @5 (5 + 0) = (1, 2),
    # @10 (10 + 0) = (3, 4, 5)
    assert region.state() == {1: {1, 2, 3, 4, 5, 16, 17, 18, 19, 20}}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert set(range(16, 21)) <= region.tell(2, 21)

    # shard 1: age = 3, @5 (5 + 0) = (1, 2), @10 (10 + 0) = (3, 4, 5)
    # shard 2: age = 0, @1 (1 + 0) = (21)
    assert region.state() == {1: {1, 2, 3, 4, 5}, 2: {21}}

    for id in range(6, 11):
        region.tell(1, id)

    # shard 1: age = 7, @7 (1 + 6) = (9), @8 (1 + 7) = (10), @10 (10 + 0) = (3, 4, 5)
    # shard 2: age = 0, @1 (1 + 0) = (21)
    assert region.state() == {1: {9, 10, 3, 4, 5}, 2: {21}}

    for id in range(11, 16):
        region.tell(1, id)

    # shard 1: age = 9, @10 (10 + 0) = (3, 4, 5), @10 (1 + 9) = (14, 15)
    # shard 2: age = 0, @1 (1 + 0) = (21)
    assert region.state() == {1: {3, 4, 5, 14, 15}, 2: {21}}

    for id in range(16, 21):
        region.tell(1, id)

    # shard 1: age = 10, @11 (1 + 10) = (16, 17, 18, 19, 20)
    # shard 2: age = 0, @1 (1 + 0) = (21)
    assert region.state() == {1: {16, 17, 18, 19, 20}, 2: {21}}


def test_lfu_with_idle_passivate_entities_when_they_havent_seen_messages_for_the_timeout() -> None:
    region = Region(IDLE_CONFIG, TestClock())

    region.tell(1, 1)  # t = 0.0, lastSendNanoTime1
    region.tell(1, 2)

    # keep entity 3 active to prevent idle passivation
    region.tell(1, 3)
    assert region.passes(0.5).isdisjoint({1, 2})
    region.tell(1, 3)
    assert region.passes(0.5).isdisjoint({1, 2})  # t = 1.0: 1.0 s since the touch, not more
    region.tell(1, 3)
    assert {1, 2} <= region.passes(0.5)  # t = 1.5
    region.tell(1, 3)  # lastSendNanoTime2
    assert 3 not in region.passes(0.5)
    assert 3 not in region.passes(0.5)
    assert 3 in region.passes(0.5)  # t = 3.0


def test_lfu_limit_adjustment_adjust_per_shard_limits_when_the_per_region_limit_is_adjusted() -> (
    None
):
    region = Region(CONFIG, TestClock())

    # only one active shard at first, initial per-shard limit of 10
    for id in range(1, 21):
        passivated = region.tell(1, id)
        if id > 10:
            assert id - 10 in passivated

    assert region.state() == {1: set(range(11, 21))}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert set(range(11, 16)) <= region.tell(2, 21)

    assert region.state() == {1: set(range(16, 21)), 2: {21}}

    # reduce the per-region limit from 10 to 6, per-shard limit becomes 3
    assert {16, 17} <= region.set_active_entity_limit(6)  # passivate entities over new limit

    assert region.state() == {1: set(range(18, 21)), 2: {21}}

    for id in range(1, 11):
        passivated_id = id + 17 if id < 4 else id - 3
        assert passivated_id in region.tell(1, id)

    assert region.state() == {1: set(range(8, 11)), 2: {21}}

    # increase the per-region limit from 6 to 12, per-shard limit becomes 6
    region.set_active_entity_limit(12)

    for id in range(11, 21):
        passivated = region.tell(1, id)
        if id > 13:  # start passivating at new higher limit of 6
            assert id - 6 in passivated

    assert region.state() == {1: set(range(15, 21)), 2: {21}}
