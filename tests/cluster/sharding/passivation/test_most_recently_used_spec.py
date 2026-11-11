"""Ported from Apache Pekko's MostRecentlyUsedSpec (cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/passivation/MostRecentlyUsedSpec.scala). The cases:

- MostRecentlyUsedSpec: passivate the most recently used entities when the per-shard entity
  limit is reached (58)
- MostRecentlyUsedWithIdleSpec: passivate entities when they haven't seen messages for the
  configured timeout (166)
- MostRecentlyUsedLimitAdjustmentSpec: adjust per-shard entity limits when the per-region limit
  is dynamically adjusted (209)

Porting rules, as in ``test_entity_passivation_spec``:

- ``config`` is ``P.defaults.with_active_entity_limit(10).with_most_recently_used_replacement()``;
  ``idleConfig`` is limit 3, MRU and ``.with_idle_entity_passivation(1.0)``.
- The specs drive ``harness.Region``. Each ``expectReceived(id, Stop)`` asserts the id is in what
  that step passivated, and each step asserts exactly what it passivated, empty where Scala
  expects no ``Stop``: Scala's per-entity probes fail on a stray ``Stop``. Each ``expectState``
  asserts ``region.state()``.
- A ``Thread.sleep(timeout / 2)`` is ``region.passes(0.5)``. Each idle expectation asserts the
  entity is passivated by the first ``passes`` after which more than the 1.0 s timeout has gone
  since its last touch, and not by an earlier one.

Differences from the Scala: in the limit adjustment case, the loop after the limit drops to 3
expects a ``Stop`` from id 6 on, but the touches of 4 and 5 already passivate 3 and 4 (Scala's
``expectState`` shows them gone, and never reads their probes); the port asserts both.
"""

from ohra.cluster.sharding.passivation_settings import PassivationStrategySettings

from ....util import TestClock
from .harness import Region

P = PassivationStrategySettings

CONFIG = P.defaults.with_active_entity_limit(10).with_most_recently_used_replacement()
IDLE_CONFIG = (
    P.defaults.with_active_entity_limit(3)
    .with_most_recently_used_replacement()
    .with_idle_entity_passivation(1.0)
)


def test_mru_passivate_the_most_recently_used_entities_when_the_per_shard_limit_is_reached() -> (
    None
):
    region = Region(CONFIG, TestClock())

    # only one active shard at first, most recently used entities passivated once the limit is
    # reached
    for id in range(1, 21):
        assert region.tell(1, id) == ({id - 1} if id > 10 else set())

    assert region.state() == {1: {1, 2, 3, 4, 5, 6, 7, 8, 9, 20}}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 21) == {20, 9, 8, 7, 6}

    assert region.state() == {1: {1, 2, 3, 4, 5}, 2: {21}}

    # shards now have a limit of 5 entities
    for id in range(1, 21):
        assert region.tell(1, id) == ({id - 1} if id > 5 else set())

    assert region.state() == {1: {1, 2, 3, 4, 20}, 2: {21}}

    # shards now have a limit of 5 entities
    for id in range(21, 25):
        assert region.tell(2, id) == set()

    assert region.state() == {1: {1, 2, 3, 4, 20}, 2: {21, 22, 23, 24}}

    # activating a third shard will divide the per-shard limit in three, passivating entities
    # over the new limits
    assert region.tell(3, 31) == {24, 20, 4}

    assert region.state() == {1: {1, 2, 3}, 2: {21, 22, 23}, 3: {31}}

    # shards now have a limit of 3 entities
    for id in range(24, 31):
        assert region.tell(2, id) == {id - 1}

    assert region.state() == {1: {1, 2, 3}, 2: {21, 22, 30}, 3: {31}}

    # shards now have a limit of 3 entities
    for id in range(31, 41):
        assert region.tell(3, id) == ({id - 1} if id > 33 else set())

    assert region.state() == {1: {1, 2, 3}, 2: {21, 22, 30}, 3: {31, 32, 40}}

    # manually passivate some entities
    region.manually_passivate(1, 2)
    region.manually_passivate(2, 22)
    region.manually_passivate(3, 32)

    assert region.state() == {1: {1, 3}, 2: {21, 30}, 3: {31, 40}}

    for i in range(1, 4):
        passivated = region.tell(1, 11 + i) | region.tell(2, 22 + i) | region.tell(3, 33 + i)
        assert passivated == ({12, 23, 34} if i == 2 else {13, 24, 35} if i == 3 else set())

    assert region.state() == {1: {1, 3, 14}, 2: {21, 30, 25}, 3: {31, 40, 36}}


def test_mru_with_idle_passivate_entities_when_they_havent_seen_messages_for_the_timeout() -> None:
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


def test_mru_limit_adjustment_adjust_per_shard_limits_when_the_per_region_limit_is_adjusted() -> (
    None
):
    region = Region(CONFIG, TestClock())

    # only one active shard at first, initial per-shard limit of 10
    for id in range(1, 21):
        assert region.tell(1, id) == ({id - 1} if id > 10 else set())

    assert region.state() == {1: {1, 2, 3, 4, 5, 6, 7, 8, 9, 20}}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 21) == {20, 9, 8, 7, 6}

    assert region.state() == {1: {1, 2, 3, 4, 5}, 2: {21}}

    # reduce the per-region limit from 10 to 6, per-shard limit becomes 3
    assert region.set_active_entity_limit(6) == {5, 4}  # passivate entities over new limit

    assert region.state() == {1: {1, 2, 3}, 2: {21}}

    for id in range(1, 11):
        # Scala expects a Stop from id 6 on; 3 and 4 are passivated too, unchecked by Scala
        assert region.tell(1, id) == ({id - 1} if id > 3 else set())

    assert region.state() == {1: {1, 2, 10}, 2: {21}}

    # increase the per-region limit from 6 to 12, per-shard limit becomes 6
    assert region.set_active_entity_limit(12) == set()

    for id in range(11, 21):
        # start passivating at new higher limit of 6
        assert region.tell(1, id) == ({id - 1} if id > 13 else set())

    assert region.state() == {1: {1, 2, 10, 11, 12, 20}, 2: {21}}
