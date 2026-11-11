"""Ported from Apache Pekko's CompositeSpec (cluster-sharding/src/test/scala/org/apache/pekko/
cluster/sharding/passivation/CompositeSpec.scala). The cases:

- AdmissionWindowAndFilterSpec: passivate the least recently or frequently used entities when the
  per-shard entity limit is reached (120)
- AdmissionFilterNoWindowSpec: passivate the least recently or frequently used entities when the
  per-shard entity limit is reached (213)
- AdaptiveAdmissionWindowSpec: adjust the admission window size when optimizing with
  hill-climbing algorithm (276)
- CompositeWithIdleSpec: passivate entities when they haven't seen messages for the configured
  timeout (390)
- CompositeLimitAdjustmentSpec: adjust per-shard entity limits when the per-region limit is
  dynamically adjusted (433)

Porting rules, as in ``test_entity_passivation_spec``:

- ``admissionWindowAndFilterConfig`` is limit 20, an LRU admission window of proportion 0.2 with
  no optimizer, a frequency sketch filter and a segmented LRU main area of proportions
  ``[0.25, 0.75]``. ``admissionFilterNoWindowConfig`` is limit 10, a frequency sketch filter, no
  window and an LRU main area. ``adaptiveWindowConfig`` is limit 10, an LRU window of proportion
  0.1 (minimum 0.1, maximum 1.0) with hill climbing (adjust multiplier 2, initial step 0.4,
  restart threshold 0.8, step decay 0.5), a frequency sketch filter with reset multiplier 100 and
  an LRU main area. ``idleConfig`` is ``default_strategy`` with limit 3 and a 1.0 s idle timeout.
- The specs drive ``harness.Region``. Each ``expectReceived(id, Stop)`` asserts the id is in what
  that step passivated, and each step asserts exactly what it passivated, empty where Scala
  expects no ``Stop``: Scala's per-entity probes fail on a stray ``Stop``. Each ``expectState``
  asserts ``region.state()``.
- A ``Thread.sleep(timeout / 2)`` is ``region.passes(0.5)``. Each idle expectation asserts the
  entity is passivated by the first ``passes`` after which more than the 1.0 s timeout has gone
  since its last touch, and not by an earlier one.

The admission decisions compare the frequency sketch's counts, so these expectations hold only
with the sketch's exact hashes (``ohra.util.frequency_sketch``, bit-exact with the JVM).

Then a test of ohra's own: the frequency sketch filter of a depth other than 4 (Scala's general
``FrequencySketchAdmissionFilter``, where depth 4 and 4-bit counters take the fast one). Its
expectations come from Pekko 1.4.0 run on the JVM, whose composite strategy and sketch match the
ported source apart from overflow guards on the sketch's width.

Differences from the Scala: in AdmissionFilterNoWindowSpec's last loop Scala leaves the ``Stop``
of entities 6-10 unchecked (no later message reaches their probes); its final ``expectState``
shows them passivated, and the port asserts each one passivates itself. The comment above that
``expectState`` says "shard 1: 16-20" in Scala, stale against the 1-5 it asserts; the port
corrects it.
"""

from ohra.cluster.sharding.passivation_settings import PassivationStrategySettings

from ....util import TestClock
from .harness import Region

P = PassivationStrategySettings
A = P.AdmissionSettings

ADMISSION_WINDOW_AND_FILTER_CONFIG = (
    P.defaults.with_active_entity_limit(20)
    .with_admission(
        A.defaults.with_window(
            A.WindowSettings.defaults.with_policy(
                P.LeastRecentlyUsedSettings.defaults
            ).with_initial_proportion(0.2)
        ).with_filter(A.FrequencySketchSettings.defaults)
    )
    .with_replacement_policy(P.LeastRecentlyUsedSettings.defaults.with_segmented([0.25, 0.75]))
)
ADMISSION_FILTER_NO_WINDOW_CONFIG = (
    P.defaults.with_active_entity_limit(10)
    .with_admission(A.defaults.with_filter(A.FrequencySketchSettings.defaults))
    .with_least_recently_used_replacement()
)
ADAPTIVE_WINDOW_CONFIG = (
    P.defaults.with_active_entity_limit(10)
    .with_admission(
        A.defaults.with_window(
            A.WindowSettings.defaults.with_policy(P.LeastRecentlyUsedSettings.defaults)
            .with_initial_proportion(0.1)
            .with_minimum_proportion(0.1)
            .with_maximum_proportion(1.0)
            .with_optimizer(A.HillClimbingSettings(2.0, 0.4, 0.8, 0.5))
        ).with_filter(A.FrequencySketchSettings.defaults.with_reset_multiplier(100.0))
    )
    .with_least_recently_used_replacement()
)
DEPTH_3_FILTER_CONFIG = (
    P.defaults.with_active_entity_limit(5)
    .with_admission(A.defaults.with_filter(A.FrequencySketchSettings.defaults.with_depth(3)))
    .with_least_recently_used_replacement()
)
IDLE_CONFIG = P.default_strategy.with_active_entity_limit(3).with_idle_entity_passivation(1.0)


def test_admission_window_and_filter_passivate_the_least_recently_or_frequently_used() -> None:
    region = Region(ADMISSION_WINDOW_AND_FILTER_CONFIG, TestClock())

    # only one active shard at first
    # entities move through the LRU window before moving to main
    # candidate entities from window passivated due to admission filter (and same frequency)
    # entities are only accessed once (so all in lowest segment)
    for id in range(1, 41):
        assert region.tell(1, id) == ({id - 4} if id > 20 else set())

    # shard 1: window: 37-40, main level 0: 1-16, main level 1: empty
    assert region.state() == {1: set(range(1, 17)) | set(range(37, 41))}

    # increase the frequency of the entities in the main area
    # accessing entities a second time moves them to the higher "protected" segment in main area
    # when the limit for higher segment is reached, entities are demoted to lower "probationary"
    # segment
    # any activated entities will passivate candidates from the window (which have lower
    # frequency)
    for id in range(1, 21):
        assert region.tell(1, id) == ({id + 20} if id > 16 else set())

    # shard 1: window: 17-20, main level 0: 1-4, level 1: 5-16
    assert region.state() == {1: set(range(1, 21))}

    # cycle through the entities in the window to increase their counts in the frequency sketch
    for id in range(17, 21):
        for _ in range(5):
            assert region.tell(1, id) == set()

    # shard 1: window: 17-20, main level 0: 1-4, level 1: 5-16
    assert region.state() == {1: set(range(1, 21))}

    # activating new entities will promote candidates from the window to main (higher
    # frequencies now)
    for id in range(21, 25):
        assert region.tell(1, id) == {id - 20}

    # shard 1: window: 21-24, main level 0: 17-20, level 1: 5-16
    assert region.state() == {1: set(range(5, 25))}

    # activating more new entities will passivate candidates from the window (lower frequencies
    # than main)
    for id in range(25, 29):
        assert region.tell(1, id) == {id - 4}

    # shard 1: window: 25-28, main level 0: 17-20, level 1: 5-16
    assert region.state() == {1: set(range(5, 21)) | set(range(25, 29))}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 41) == {25, 26, 17, 18, 19, 20, 5, 6, 7, 8}

    # shard 1: window: 27-28, main level 0: 9-10, level 1: 11-16
    # shard 2: window: 41, main level 0: empty, level 1: empty
    assert region.state() == {1: set(range(9, 17)) | {27, 28}, 2: {41}}

    # ids 17 and 18 still have higher frequencies, will be promoted through to main from window
    for id in range(17, 21):
        assert region.tell(1, id) == {{17: 27, 18: 28, 19: 9, 20: 10}[id]}

    # shard 1: window: 19-20, main level 0: 17-18, level 1: 11-16
    # shard 2: window: 41, main level 0: empty, level 1: empty
    assert region.state() == {1: set(range(11, 21)), 2: {41}}


def test_admission_filter_no_window_passivate_the_least_recently_or_frequently_used() -> None:
    region = Region(ADMISSION_FILTER_NO_WINDOW_CONFIG, TestClock())

    # only one active shard at first
    # as entities have same frequency, when limit is reached the candidate entities (11-20) are
    # not admitted to main
    for id in range(1, 21):
        assert region.tell(1, id) == ({id} if id > 10 else set())

    # shard 1: 1-10
    assert region.state() == {1: set(range(1, 11))}

    # entities 11-20 on second access, so admitted to main, replacing current entities
    # cycle through multiple times to increase frequency counts
    for id in range(11, 21):
        for i in range(1, 6):
            assert region.tell(1, id) == ({id - 10} if i == 1 else set())

    # shard 1: 11-20
    assert region.state() == {1: set(range(11, 21))}

    # entities 1-10 on second and third access, but not admitted to main as lower frequency
    for id in range(1, 11):
        for _ in range(2):
            assert region.tell(1, id) == {id}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 21) == set(range(11, 16))

    # shard 1: 16-20, shard 2: 21
    assert region.state() == {1: set(range(16, 21)), 2: {21}}

    # frequency sketch is recreated on limit changes, so all frequencies are 0 again
    # candidate entities will replace old entities, but then retain their position
    for id in range(1, 11):
        assert region.tell(1, id) == ({id + 15} if id <= 5 else {id})

    # shard 1: 1-5, shard 2: 21
    assert region.state() == {1: set(range(1, 6)), 2: {21}}


def test_adaptive_admission_window_adjust_the_window_size_with_hill_climbing() -> None:
    region = Region(ADAPTIVE_WINDOW_CONFIG, TestClock())

    # fill admission window and main areas
    # increase frequency counts for entities 1-10
    for id in range(1, 11):
        for _ in range(10):
            assert region.tell(1, id) == set()

    assert region.state() == {1: set(range(1, 11))}

    # window = [10], main = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    # window limit is currently 1 (window proportion of 0.1)
    # these entities will be passivated as they have lower frequencies
    # given drop in active rate the adjustment direction flips to increased (for next step)
    # so that window proportion is increased by step of 0.4 (to window limit of 5) on next cycle
    for i in range(1, 5):
        for id in range(11, 21):
            passivated = 20 if id == 11 and i > 1 else id - 1
            assert region.tell(1, id) == {passivated}

    assert region.state() == {1: set(range(1, 10)) | {20}}

    # window = [20, 1, 2, 3, 4], main = [5, 6, 7, 8, 9]
    # window limit is currently 5 (window proportion of 0.5), direction is increasing
    # continue to maintain positive active rate delta to increase window by decayed step of 0.2
    for id in range(21, 31):
        for i in range(1, 3):
            if i == 1:
                if id == 21:
                    passivated = 20  # in window from previous loop
                elif id <= 25:
                    passivated = id - 21  # demoted from main
                else:
                    passivated = id - 5  # window size
                assert region.tell(1, id) == {passivated}
            else:
                assert region.tell(1, id) == set()

    assert region.state() == {1: set(range(5, 10)) | set(range(26, 31))}

    # window = [26, 27, 28, 29, 30, 5, 6], main = [7, 8, 9]
    # window proportion is currently 0.7, direction is increasing
    # continue to maintain positive active rate delta to increase window by decayed step of 0.1
    # increase to high active rate to trigger subsequent restart on delta over threshold
    for id in range(21, 25):
        for i in range(1, 6):
            assert region.tell(1, id) == ({id + 5} if i == 1 else set())

    assert region.state() == {1: set(range(5, 10)) | set(range(21, 25)) | {30}}

    # window = [30, 5, 6, 21, 22, 23, 24, 7], main = [8, 9]
    # window proportion is currently 0.8, direction is increasing
    # drop the active rate to zero to trigger a direction change (decrease of 0.05 to window of 7)
    # and hill-climbing restart triggered for next step (change is over restart threshold)
    previous_window = [30, 5, 6, 21, 22, 23, 24, 7]
    for i in range(1, 3):
        for id in range(31, 41):
            if i == 1 and id <= 38:
                passivated = previous_window[id - 31]
            elif id <= 38:
                passivated = id + 2
            else:
                passivated = id - 8  # window size
            assert region.tell(1, id) == {passivated}

    assert region.state() == {1: {8, 9} | set(range(33, 41))}

    # window = [34, 35, 36, 37, 38, 39, 40], main = [8, 9, 33]
    # window proportion is currently 0.7, direction is decreasing and restarted at initial step
    # stable or increasing active rate will keep decreasing window now (step of -0.4)
    for id in range(41, 51):
        for i in range(1, 3):
            assert region.tell(1, id) == ({id - 7} if i == 1 else set())  # window size

    assert region.state() == {1: {8, 9, 33} | set(range(44, 51))}

    # window = [48, 49, 50], main = [8, 9, 33, 44, 45, 46, 47]
    # window proportion is currently 0.3, direction is decreasing, with next step decayed to -0.2
    for id in range(51, 61):
        for i in range(1, 3):
            assert region.tell(1, id) == ({id - 3} if i == 1 else set())  # window size

    assert region.state() == {1: {60} | {8, 9, 33, 44, 45, 46, 47, 58, 59}}

    # window = [60], main = [8, 9, 33, 44, 45, 46, 47, 58, 59]
    # window proportion is currently 0.1, direction is decreasing, with next step decayed to -0.1
    for id in range(61, 71):
        assert region.tell(1, id) == {id - 1}  # window size

    assert region.state() == {1: {70} | {8, 9, 33, 44, 45, 46, 47, 58, 59}}


def test_composite_with_idle_passivate_entities_when_they_havent_seen_messages() -> None:
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


def test_composite_limit_adjustment_adjust_per_shard_limits_when_the_limit_is_adjusted() -> None:
    region = Region(ADMISSION_WINDOW_AND_FILTER_CONFIG, TestClock())

    # only one active shard at first, initial per-shard limit of 20
    # entities move through the LRU window before moving to main
    # candidate entities from window passivated due to admission filter (and same frequency)
    for id in range(1, 41):
        assert region.tell(1, id) == ({id - 4} if id > 20 else set())

    assert region.state() == {1: set(range(1, 17)) | set(range(37, 41))}

    # activating a second shard will divide the per-shard limit in two, passivating half of the
    # first shard
    assert region.tell(2, 41) == {37, 38} | set(range(1, 9))

    assert region.state() == {1: set(range(9, 17)) | {39, 40}, 2: {41}}

    # reduce the per-region limit from 20 to 10, per-shard limit becomes 5
    # passivate entities over new limit
    assert region.set_active_entity_limit(10) == {39} | set(range(9, 13))

    assert region.state() == {1: set(range(13, 17)) | {40}, 2: {41}}

    # note: frequency sketch is recreated when limit changes, all frequency counts are 0 again
    for id in range(1, 11):
        passivated = 40 if id == 1 else id + 11 if id <= 5 else id - 1
        assert region.tell(1, id) == {passivated}

    assert region.state() == {1: set(range(1, 5)) | {10}, 2: {41}}

    # increase the per-region limit from 10 to 30, per-shard limit becomes 15
    assert region.set_active_entity_limit(30) == set()

    # note: frequency sketch is recreated when limit changes, all frequency counts are 0 again
    for id in range(1, 21):
        # start passivating window candidates at new higher limit of 15
        assert region.tell(1, id) == ({id - 3} if id > 15 else set())  # new window size

    assert region.state() == {1: set(range(1, 13)) | set(range(18, 21)), 2: {41}}


def test_a_frequency_sketch_filter_of_depth_3_admits_the_more_frequent_entities() -> None:
    region = Region(DEPTH_3_FILTER_CONFIG, TestClock())

    # candidates of the same frequency are not admitted to main
    for id in range(1, 11):
        assert region.tell(1, id) == ({id} if id > 5 else set())

    # on their second access 6-10 are more frequent, so admitted, replacing 1-5
    for id in range(6, 11):
        for i in range(1, 4):
            assert region.tell(1, id) == ({id - 5} if i == 1 else set())

    # 1-5 are less frequent than 6-10, so not admitted
    for id in range(1, 6):
        assert region.tell(1, id) == {id}

    # a second shard halves the per-shard limit to 2, passivating the least recently used
    assert region.tell(2, 11) == {6, 7, 8}

    assert region.state() == {1: {9, 10}, 2: {11}}
