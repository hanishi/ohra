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
  that step passivated (an id's several accesses are one step), and each step asserts exactly
  what it passivated, empty where Scala expects no ``Stop``: Scala's per-entity probes fail on a
  stray ``Stop``. The exception is the dynamic aging case's last three loops, where Scala expects
  no ``Stop`` yet entities are passivated (its ``expectState`` shows them gone): there the test,
  as Scala, asserts only the state. Each ``expectState`` asserts ``region.state()``.
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
        expected = {expect_passivated[id]} if id in expect_passivated else set()
        assert passivated == expected

    # shard 1: frequency 3 = (3, 7, 11, 15, 19), frequency 4 = (4, 8, 12, 16, 20)
    assert region.state() == {1: {3, 7, 11, 15, 19, 4, 8, 12, 16, 20}}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 21) == {3, 7, 11, 15, 19}

    # shard 1: frequency 4 = (4, 8, 12, 16, 20)
    # shard 2: frequency 1 = (21)
    assert region.state() == {1: {4, 8, 12, 16, 20}, 2: {21}}

    # shards now have a limit of 5 entities
    # note: newly added entities are not counted as least frequent for passivation
    for id in range(1, 21):
        accesses = (id - 1) % 4 + 1  # vary frequency between 1 and 4 accesses
        passivated_id = id + 3 if accesses == 1 else id - 1
        assert _accesses(region, 1, id, accesses) == {passivated_id}

    # shard 1: frequency 4 = (4, 8, 12, 16, 20)
    # shard 2: frequency 1 = (21)
    assert region.state() == {1: {4, 8, 12, 16, 20}, 2: {21}}

    # shards now have a limit of 5 entities
    for id in range(21, 25):
        assert region.tell(2, id) == set()

    # shard 1: frequency 4 = (4, 8, 12, 16, 20)
    # shard 2: frequency 1 = (22, 23, 24), frequency 2 = (21)
    assert region.state() == {1: {4, 8, 12, 16, 20}, 2: {22, 23, 24, 21}}

    # activating a third shard will divide the per-shard limit in three, passivating entities
    # over the new limits
    assert region.tell(3, 31) == {4, 8, 22}

    # shard 1: frequency 4 = (12, 16, 20)
    # shard 2: frequency 1 = (23, 24), frequency 2 = (21)
    # shard 3: frequency 1 = (31)
    assert region.state() == {1: {12, 16, 20}, 2: {23, 24, 21}, 3: {31}}

    # shards now have a limit of 3 entities
    expect_passivated = {22: 23, 23: 24, 24: 22, 25: 21, 26: 25, 27: 26, 28: 23, 29: 27, 30: 29}
    for id in range(21, 31):
        accesses = (id - 1) % 4 + 1  # vary frequency between 1 and 4 accesses
        passivated = _accesses(region, 2, id, accesses)
        expected = {expect_passivated[id]} if id in expect_passivated else set()
        assert passivated == expected

    # shard 1: frequency 4 = (12, 16, 20)
    # shard 2: frequency 2 = (30), frequency 4 = (24, 28)
    # shard 3: frequency 1 = (31)
    assert region.state() == {1: {12, 16, 20}, 2: {30, 24, 28}, 3: {31}}

    # shards now have a limit of 3 entities
    expect_passivated = {34: 32, 35: 31, 36: 35, 37: 36, 38: 34, 39: 38, 40: 39}
    for id in range(31, 41):
        accesses = 4 - (id - 1) % 4  # vary frequency between 1 and 4 accesses
        passivated = _accesses(region, 3, id, accesses)
        expected = {expect_passivated[id]} if id in expect_passivated else set()
        assert passivated == expected

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
        assert passivated == ({21, 40} if i == 2 else {11, 30, 31} if i == 3 else set())

    # shard 1: frequency 3 = (13), frequency 4 = (20), frequency 6 = (12)
    # shard 2: frequency 2 = (22), frequency 3 = (23), frequency 4 = (28)
    # shard 3: frequency 2 = (32), frequency 3 = (33), frequency 4 = (37)
    assert region.state() == {1: {13, 20, 12}, 2: {22, 23, 28}, 3: {32, 33, 37}}


def test_lfu_with_dynamic_aging_passivate_the_least_frequently_used_entities() -> None:
    region = Region(DYNAMIC_AGING_CONFIG, TestClock())

    # only one active shard at first
    # ids 1 and 2 are quite popular initially
    for id in range(1, 3):
        assert _accesses(region, 1, id, 5) == set()
    # ids 3, 4, and 5 are very popular initially
    for id in range(3, 6):
        assert _accesses(region, 1, id, 10) == set()

    # shard 1: age = 0, @5 (5 + 0) = (1, 2), @10 (10 + 0) = (3, 4, 5)
    assert region.state() == {1: {1, 2, 3, 4, 5}}

    for id in range(6, 21):
        assert region.tell(1, id) == ({id - 5} if id > 10 else set())

    # shard 1: age = 2, @3 (1 + 2) = (16, 17, 18, 19, 20), @5 (5 + 0) = (1, 2),
    # @10 (10 + 0) = (3, 4, 5)
    assert region.state() == {1: {1, 2, 3, 4, 5, 16, 17, 18, 19, 20}}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 21) == set(range(16, 21))

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

    assert region.tell(1, 1) == set()  # t = 0.0, lastSendNanoTime1
    assert region.tell(1, 2) == set()

    # keep entity 3 active to prevent idle passivation
    assert region.tell(1, 3) == set()
    assert region.passes(0.5) == set()
    assert region.tell(1, 3) == set()
    assert region.passes(0.5) == set()  # t = 1.0: 1.0 s since the touch, not more
    assert region.tell(1, 3) == set()
    assert region.passes(0.5) == {1, 2}  # t = 1.5
    assert region.tell(1, 3) == set()  # lastSendNanoTime2
    assert region.passes(0.5) == set()
    assert region.passes(0.5) == set()
    assert region.passes(0.5) == {3}  # t = 3.0


def test_lfu_limit_adjustment_adjust_per_shard_limits_when_the_per_region_limit_is_adjusted() -> (
    None
):
    region = Region(CONFIG, TestClock())

    # only one active shard at first, initial per-shard limit of 10
    for id in range(1, 21):
        assert region.tell(1, id) == ({id - 10} if id > 10 else set())

    assert region.state() == {1: set(range(11, 21))}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 21) == set(range(11, 16))

    assert region.state() == {1: set(range(16, 21)), 2: {21}}

    # reduce the per-region limit from 10 to 6, per-shard limit becomes 3
    assert region.set_active_entity_limit(6) == {16, 17}  # passivate entities over new limit

    assert region.state() == {1: set(range(18, 21)), 2: {21}}

    for id in range(1, 11):
        passivated_id = id + 17 if id < 4 else id - 3
        assert region.tell(1, id) == {passivated_id}

    assert region.state() == {1: set(range(8, 11)), 2: {21}}

    # increase the per-region limit from 6 to 12, per-shard limit becomes 6
    assert region.set_active_entity_limit(12) == set()

    for id in range(11, 21):
        # start passivating at new higher limit of 6
        assert region.tell(1, id) == ({id - 6} if id > 13 else set())

    assert region.state() == {1: set(range(15, 21)), 2: {21}}
