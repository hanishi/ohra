"""Ported from Apache Pekko's LeastRecentlyUsedSpec (cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/passivation/LeastRecentlyUsedSpec.scala). The cases:

- LeastRecentlyUsedSpec: passivate the least recently used entities when the per-shard entity
  limit is reached (83)
- SegmentedLeastRecentlyUsedSpec: passivate the (segmented) least recently used entities when
  the per-shard entity limit is reached (192)
- LeastRecentlyUsedWithIdleSpec: passivate entities when they haven't seen messages for the
  configured timeout (263)
- LeastRecentlyUsedLimitAdjustmentSpec: adjust per-shard entity limits when the per-region limit
  is dynamically adjusted (306)
- SegmentedLeastRecentlyUsedLimitAdjustmentSpec: adjust per-shard entity limits when the
  per-region limit is dynamically adjusted (367)

Porting rules, as in ``test_entity_passivation_spec``:

- ``config`` is ``P.defaults.with_active_entity_limit(10).with_least_recently_used_replacement()``;
  ``segmentedConfig`` is limit 10 with
  ``LeastRecentlyUsedSettings.defaults.with_segmented([0.2, 0.8])``, and
  ``segmentedInitialLimitConfig`` the same with limit 20; ``idleConfig`` is limit 3, LRU and
  ``.with_idle_entity_passivation(1.0)``.
- The specs drive ``harness.Region``. Each ``expectReceived(id, Stop)`` asserts the id is in what
  that step passivated, and each step asserts exactly what it passivated, empty where Scala
  expects no ``Stop``: Scala's per-entity probes fail on a stray ``Stop``. Each ``expectState``
  asserts ``region.state()``.
- A ``Thread.sleep(timeout / 2)`` is ``region.passes(0.5)``. Each idle expectation asserts the
  entity is passivated by the first ``passes`` after which more than the 1.0 s timeout has gone
  since its last touch, and not by an earlier one.

Differences from the Scala: none beyond the porting rules.
"""

from ohra.cluster.sharding.passivation_settings import PassivationStrategySettings

from ....util import TestClock
from .harness import Region

P = PassivationStrategySettings

CONFIG = P.defaults.with_active_entity_limit(10).with_least_recently_used_replacement()
SEGMENTED_CONFIG = P.defaults.with_active_entity_limit(10).with_replacement_policy(
    P.LeastRecentlyUsedSettings.defaults.with_segmented([0.2, 0.8])
)
SEGMENTED_INITIAL_LIMIT_CONFIG = SEGMENTED_CONFIG.with_active_entity_limit(20)
IDLE_CONFIG = (
    P.defaults.with_active_entity_limit(3)
    .with_least_recently_used_replacement()
    .with_idle_entity_passivation(1.0)
)


def test_lru_passivate_the_least_recently_used_entities_when_the_per_shard_limit_is_reached() -> (
    None
):
    region = Region(CONFIG, TestClock())

    # only one active shard at first, least recently used entities passivated once the limit is
    # reached
    for id in range(1, 21):
        assert region.tell(1, id) == ({id - 10} if id > 10 else set())

    assert region.state() == {1: set(range(11, 21))}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 21) == set(range(11, 16))

    assert region.state() == {1: set(range(16, 21)), 2: {21}}

    # shards now have a limit of 5 entities
    for id in range(1, 21):
        passivated_id = id + 15 if id <= 5 else id - 5
        assert region.tell(1, id) == {passivated_id}

    assert region.state() == {1: set(range(16, 21)), 2: {21}}

    # shards now have a limit of 5 entities
    for id in range(21, 25):
        assert region.tell(2, id) == set()

    assert region.state() == {1: set(range(16, 21)), 2: set(range(21, 25))}

    # activating a third shard will divide the per-shard limit in three, passivating entities
    # over the new limits
    assert region.tell(3, 31) == {16, 17, 21}

    assert region.state() == {1: {18, 19, 20}, 2: {22, 23, 24}, 3: {31}}

    # shards now have a limit of 3 entities
    for id in range(25, 31):
        assert region.tell(2, id) == {id - 3}

    assert region.state() == {1: {18, 19, 20}, 2: {28, 29, 30}, 3: {31}}

    # shards now have a limit of 3 entities
    for id in range(31, 41):
        assert region.tell(3, id) == ({id - 3} if id > 33 else set())

    assert region.state() == {1: {18, 19, 20}, 2: {28, 29, 30}, 3: {38, 39, 40}}

    # manually passivate some entities
    region.manually_passivate(1, 19)
    region.manually_passivate(2, 29)
    region.manually_passivate(3, 39)

    assert region.state() == {1: {18, 20}, 2: {28, 30}, 3: {38, 40}}

    for i in range(1, 4):
        passivated = region.tell(1, 10 + i) | region.tell(2, 20 + i) | region.tell(3, 30 + i)
        assert passivated == ({18, 28, 38} if i == 2 else {20, 30, 40} if i == 3 else set())

    assert region.state() == {1: {11, 12, 13}, 2: {21, 22, 23}, 3: {31, 32, 33}}


def test_segmented_lru_passivate_the_segmented_least_recently_used_entities() -> None:
    region = Region(SEGMENTED_CONFIG, TestClock())

    # only one active shard at first
    # entities are only accessed once (so all in lowest segment)
    # least recently used entities passivated once the total limit is reached
    for id in range(1, 21):
        assert region.tell(1, id) == ({id - 10} if id > 10 else set())

    # shard 1: level 0: 11-20, level 1: empty
    assert region.state() == {1: set(range(11, 21))}

    # accessing entities a second time moves them to the higher "protected" segment
    # when limit for higher segment is reached, entities are demoted to lower "probationary"
    # segment
    for id in range(11, 21):
        assert region.tell(1, id) == set()

    # shard 1: level 0: 11-12, level 1: 13-20
    assert region.state() == {1: set(range(11, 21))}

    # newly activated entities will just cycle through the lower segment
    for id in range(1, 6):
        passivated_id = id + 10 if id < 3 else id - 2
        assert region.tell(1, id) == {passivated_id}

    # shard 1: level 0: 4-5, level 1: 13-20
    assert region.state() == {1: {4, 5} | set(range(13, 21))}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 21) == {4, 5, 13, 14, 15}

    # shard 1: level 0: 16, level 1: 17-20
    # shard 2: level 0: 21, level 1: empty
    assert region.state() == {1: set(range(16, 21)), 2: {21}}

    # entities in higher "protected" segment accessed again move to most recent position
    # entities in lower "probationary" segment accessed again are promoted
    # lower segment only has space for one entity
    for id in reversed(range(11, 21)):
        expected = {20} if id == 15 else {id + 1} if id < 15 else set()
        assert region.tell(1, id) == expected

    # shard 1: level 0: 11, level 1: 19, 18, 17, 16
    # shard 2: level 0: 21, level 1: empty
    assert region.state() == {1: {11, 19, 18, 17, 16}, 2: {21}}


def test_lru_with_idle_passivate_entities_when_they_havent_seen_messages_for_the_timeout() -> None:
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


def test_lru_limit_adjustment_adjust_per_shard_limits_when_the_per_region_limit_is_adjusted() -> (
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


def test_segmented_lru_limit_adjustment_adjust_per_shard_limits_when_the_limit_is_adjusted() -> (
    None
):
    region = Region(SEGMENTED_INITIAL_LIMIT_CONFIG, TestClock())

    # only one active shard at first, initial per-shard limit of 20
    for id in range(1, 31):
        assert region.tell(1, id) == ({id - 20} if id > 20 else set())

    assert region.state() == {1: set(range(11, 31))}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 31) == set(range(11, 21))

    assert region.state() == {1: set(range(21, 31)), 2: {31}}

    # reduce the per-region limit from 20 to 10, per-shard limit becomes 5
    assert region.set_active_entity_limit(10) == set(range(21, 26))

    assert region.state() == {1: set(range(26, 31)), 2: {31}}

    for id in range(1, 11):
        passivated_id = id + 25 if id < 6 else id - 5
        assert region.tell(1, id) == {passivated_id}

    assert region.state() == {1: set(range(6, 11)), 2: {31}}

    # increase the per-region limit from 10 to 30, per-shard limit becomes 15
    assert region.set_active_entity_limit(30) == set()

    for id in range(11, 31):
        # start passivating at new higher limit of 15
        assert region.tell(1, id) == ({id - 15} if id > 20 else set())

    assert region.state() == {1: set(range(16, 31)), 2: {31}}
