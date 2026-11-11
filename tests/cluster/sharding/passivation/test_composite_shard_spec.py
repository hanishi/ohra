"""Ported from Apache Pekko's CompositeSpec (cluster-sharding/src/test/scala/org/apache/pekko/
cluster/sharding/passivation/CompositeSpec.scala), on the shard. The cases:

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

The configs are ``test_composite_spec``'s. The porting rules are those of
``test_entity_passivation_shard_spec``: the specs drive ``shard_harness.shard_region`` on
``ManualTime``; a ``Thread.sleep(timeout / 2)`` is ``time_passes(0.5)``; and
``expectReceived(id, Stop, within = timeout * 2)`` is ``time_passes(2.0)`` and then
``expect_received``. The admission decisions compare the frequency sketch's counts of the ids'
hashes, so the ids are Pekko's.

Differences from the Scala: the harness places each id on the shard the spec names. As in
Scala, AdmissionFilterNoWindowSpec's last loop leaves the ``Stop`` of entities 6-10 unread, and
the comment above its last ``expectState`` is corrected to the 1-5 it asserts.
"""

from .shard_harness import Stop, shard_region
from .test_composite_spec import (
    ADAPTIVE_WINDOW_CONFIG,
    ADMISSION_FILTER_NO_WINDOW_CONFIG,
    ADMISSION_WINDOW_AND_FILTER_CONFIG,
    IDLE_CONFIG,
)

CONFIGURED_IDLE_TIMEOUT = 1.0


async def test_admission_window_and_filter_passivate_the_lru_or_lfu_on_the_shard() -> None:
    async with shard_region(ADMISSION_WINDOW_AND_FILTER_CONFIG, expected_entities=41) as region:
        # only one active shard at first
        # entities move through the LRU window before moving to main
        # candidate entities from window passivated due to admission filter (and same frequency)
        # entities are only accessed once (so all in lowest segment)
        for id in range(1, 41):
            region.tell(1, id, "A")
            await region.expect_received(id, "A")
            if id > 20:
                await region.expect_received(id - 4, Stop())

        # shard 1: window: 37-40, main level 0: 1-16, main level 1: empty
        await region.expect_state({1: [*range(1, 17), *range(37, 41)]})

        # increase the frequency of the entities in the main area
        # accessing entities a second time moves them to the higher "protected" segment in main
        # area
        # when the limit for higher segment is reached, entities are demoted to lower
        # "probationary" segment
        # any activated entities will passivate candidates from the window (which have lower
        # frequency)
        for id in range(1, 21):
            region.tell(1, id, "B")
            await region.expect_received(id, "B")
            if id > 16:
                await region.expect_received(id + 20, Stop())

        # shard 1: window: 17-20, main level 0: 1-4, level 1: 5-16
        await region.expect_state({1: range(1, 21)})

        # cycle through the entities in the window to increase their counts in the frequency
        # sketch
        for id in range(17, 21):
            for _ in range(1, 6):
                region.tell(1, id, "C")
                await region.expect_received(id, "C")

        # shard 1: window: 17-20, main level 0: 1-4, level 1: 5-16
        await region.expect_state({1: range(1, 21)})

        # activating new entities will promote candidates from the window to main (higher
        # frequencies now)
        for id in range(21, 25):
            region.tell(1, id, "D")
            await region.expect_received(id, "D")
            await region.expect_received(id - 20, Stop())

        # shard 1: window: 21-24, main level 0: 17-20, level 1: 5-16
        await region.expect_state({1: range(5, 25)})

        # activating more new entities will passivate candidates from the window (lower
        # frequencies than main)
        for id in range(25, 29):
            region.tell(1, id, "E")
            await region.expect_received(id, "E")
            await region.expect_received(id - 4, Stop())

        # shard 1: window: 25-28, main level 0: 17-20, level 1: 5-16
        await region.expect_state({1: [*range(5, 21), *range(25, 29)]})

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 41, "F")
        await region.expect_received(41, "F")
        for id in [25, 26, 17, 18, 19, 20, 5, 6, 7, 8]:
            await region.expect_received(id, Stop())

        # shard 1: window: 27-28, main level 0: 9-10, level 1: 11-16
        # shard 2: window: 41, main level 0: empty, level 1: empty
        await region.expect_state({1: [*range(9, 17), *range(27, 29)], 2: {41}})

        # ids 17 and 18 still have higher frequencies, will be promoted through to main from
        # window
        for id in range(17, 21):
            region.tell(1, id, "G")
            await region.expect_received(id, "G")
            if id == 17:
                await region.expect_received(27, Stop())
            if id == 18:
                await region.expect_received(28, Stop())
            if id == 19:
                await region.expect_received(9, Stop())
            if id == 20:
                await region.expect_received(10, Stop())

        # shard 1: window: 19-20, main level 0: 17-18, level 1: 11-16
        # shard 2: window: 41, main level 0: empty, level 1: empty
        await region.expect_state({1: range(11, 21), 2: {41}})


async def test_admission_filter_no_window_passivate_the_lru_or_lfu_on_the_shard() -> None:
    async with shard_region(ADMISSION_FILTER_NO_WINDOW_CONFIG, expected_entities=21) as region:
        # only one active shard at first
        # as entities have same frequency, when limit is reached the candidate entities (11-20)
        # are not admitted to main
        for id in range(1, 21):
            region.tell(1, id, "A")
            await region.expect_received(id, "A")
            if id > 10:
                await region.expect_received(id, Stop())

        # shard 1: 1-10
        await region.expect_state({1: range(1, 11)})

        # entities 11-20 on second access, so admitted to main, replacing current entities
        # cycle through multiple times to increase frequency counts
        for id in range(11, 21):
            for i in range(1, 6):
                region.tell(1, id, "B")
                await region.expect_received(id, "B")
                if i == 1:
                    await region.expect_received(id - 10, Stop())

        # shard 1: 11-20
        await region.expect_state({1: range(11, 21)})

        # entities 1-10 on second and third access, but not admitted to main as lower frequency
        for id in range(1, 11):
            for _ in range(1, 3):
                region.tell(1, id, "C")
                await region.expect_received(id, "C")
                await region.expect_received(id, Stop())

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 21, "D")
        await region.expect_received(21, "D")
        for id in range(11, 16):
            await region.expect_received(id, Stop())

        # shard 1: 16-20, shard 2: 21
        await region.expect_state({1: range(16, 21), 2: {21}})

        # frequency sketch is recreated on limit changes, so all frequencies are 0 again
        # candidate entities will replace old entities, but then retain their position
        for id in range(1, 11):
            region.tell(1, id, "E")
            await region.expect_received(id, "E")
            if id <= 5:
                await region.expect_received(id + 15, Stop())

        # shard 1: 1-5, shard 2: 21
        await region.expect_state({1: range(1, 6), 2: {21}})


async def test_adaptive_admission_window_adjust_the_window_with_hill_climbing_on_the_shard() -> (
    None
):
    async with shard_region(ADAPTIVE_WINDOW_CONFIG, expected_entities=70) as region:
        # fill admission window and main areas
        # increase frequency counts for entities 1-10
        for id in range(1, 11):
            for i in range(1, 11):
                region.tell(1, id, f"A{i}")
                await region.expect_received(id, f"A{i}")

        await region.expect_state({1: range(1, 11)})

        # window = [10], main = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        # window limit is currently 1 (window proportion of 0.1)
        # these entities will be passivated as they have lower frequencies
        # given drop in active rate the adjustment direction flips to increased (for next step)
        # so that window proportion is increased by step of 0.4 (to window limit of 5) on next
        # cycle
        for i in range(1, 5):
            for id in range(11, 21):
                region.tell(1, id, f"B{i}")
                await region.expect_received(id, f"B{i}")
                passivated = 20 if id == 11 and i > 1 else id - 1
                await region.expect_received(passivated, Stop())

        await region.expect_state({1: [*range(1, 10), 20]})

        # window = [20, 1, 2, 3, 4], main = [5, 6, 7, 8, 9]
        # window limit is currently 5 (window proportion of 0.5), direction is increasing
        # continue to maintain positive active rate delta to increase window by decayed step of
        # 0.2
        for id in range(21, 31):
            for i in range(1, 3):
                region.tell(1, id, f"C{i}")
                await region.expect_received(id, f"C{i}")
                if i == 1:
                    if id == 21:
                        passivated = 20  # in window from previous loop
                    elif id <= 25:
                        passivated = id - 21  # demoted from main
                    else:
                        passivated = id - 5  # window size
                    await region.expect_received(passivated, Stop())

        await region.expect_state({1: [*range(5, 10), *range(26, 31)]})

        # window = [26, 27, 28, 29, 30, 5, 6], main = [7, 8, 9]
        # window proportion is currently 0.7, direction is increasing
        # continue to maintain positive active rate delta to increase window by decayed step of
        # 0.1
        # increase to high active rate to trigger subsequent restart on delta over threshold
        for id in range(21, 25):
            for i in range(1, 6):
                region.tell(1, id, f"D{i}")
                await region.expect_received(id, f"D{i}")
                if i == 1:
                    await region.expect_received(id + 5, Stop())

        await region.expect_state({1: [*range(5, 10), *range(21, 25), 30]})

        # window = [30, 5, 6, 21, 22, 23, 24, 7], main = [8, 9]
        # window proportion is currently 0.8, direction is increasing
        # drop the active rate to zero to trigger a direction change (decrease of 0.05 to window
        # of 7)
        # and hill-climbing restart triggered for next step (change is over restart threshold)
        for i in range(1, 3):
            for id in range(31, 41):
                region.tell(1, id, f"E{i}")
                await region.expect_received(id, f"E{i}")
                previous_window = [30, 5, 6, 21, 22, 23, 24, 7]
                if i == 1 and id <= 38:
                    passivated = previous_window[id - 31]
                elif id <= 38:
                    passivated = id + 2
                else:
                    passivated = id - 8  # window size
                await region.expect_received(passivated, Stop())

        await region.expect_state({1: [*range(8, 10), *range(33, 41)]})

        # window = [34, 35, 36, 37, 38, 39, 40], main = [8, 9, 33]
        # window proportion is currently 0.7, direction is decreasing and restarted at initial
        # step
        # stable or increasing active rate will keep decreasing window now (step of -0.4)
        for id in range(41, 51):
            for i in range(1, 3):
                region.tell(1, id, f"F{i}")
                await region.expect_received(id, f"F{i}")
                if i == 1:
                    await region.expect_received(id - 7, Stop())  # window size

        await region.expect_state({1: [*range(8, 10), 33, *range(44, 51)]})

        # window = [48, 49, 50], main = [8, 9, 33, 44, 45, 46, 47]
        # window proportion is currently 0.3, direction is decreasing, with next step decayed to
        # -0.2
        for id in range(51, 61):
            for i in range(1, 3):
                region.tell(1, id, f"G{i}")
                await region.expect_received(id, f"G{i}")
                if i == 1:
                    await region.expect_received(id - 3, Stop())  # window size

        await region.expect_state({1: {60, 8, 9, 33, 44, 45, 46, 47, 58, 59}})

        # window = [60], main = [8, 9, 33, 44, 45, 46, 47, 58, 59]
        # window proportion is currently 0.1, direction is decreasing, with next step decayed to
        # -0.1
        for id in range(61, 71):
            region.tell(1, id, "H")
            await region.expect_received(id, "H")
            await region.expect_received(id - 1, Stop())  # window size

        await region.expect_state({1: {70, 8, 9, 33, 44, 45, 46, 47, 58, 59}})


async def test_composite_with_idle_passivate_entities_when_they_are_idle_on_the_shard() -> None:
    async with shard_region(IDLE_CONFIG, expected_entities=3) as region:
        last_send_time_1 = region.now()
        region.tell(1, 1, "A")
        region.tell(1, 2, "B")

        # keep entity 3 active to prevent idle passivation
        region.tell(1, 3, "C")
        await region.time_passes(CONFIGURED_IDLE_TIMEOUT / 2)
        region.tell(1, 3, "D")
        await region.time_passes(CONFIGURED_IDLE_TIMEOUT / 2)
        region.tell(1, 3, "E")
        await region.time_passes(CONFIGURED_IDLE_TIMEOUT / 2)
        last_send_time_2 = region.now()
        region.tell(1, 3, "F")

        await region.expect_received(1, "A")
        await region.expect_received(2, "B")
        await region.expect_received(3, "C")
        await region.expect_received(3, "D")
        await region.expect_received(3, "E")
        await region.expect_received(3, "F")
        passivate_1 = await region.expect_received(1, Stop())
        passivate_2 = await region.expect_received(2, Stop())
        await region.time_passes(CONFIGURED_IDLE_TIMEOUT * 2)
        passivate_3 = await region.expect_received(3, Stop())

        # note: touched timestamps are when the shard receives the message, not the entity
        # itself so look at the time from before sending the last message until receiving the
        # passivate message
        assert passivate_1.time - last_send_time_1 > CONFIGURED_IDLE_TIMEOUT
        assert passivate_2.time - last_send_time_1 > CONFIGURED_IDLE_TIMEOUT
        assert passivate_3.time - last_send_time_2 > CONFIGURED_IDLE_TIMEOUT


async def test_composite_limit_adjustment_adjust_per_shard_limits_on_the_shard() -> None:
    async with shard_region(ADMISSION_WINDOW_AND_FILTER_CONFIG, expected_entities=41) as region:
        # only one active shard at first, initial per-shard limit of 20
        # entities move through the LRU window before moving to main
        # candidate entities from window passivated due to admission filter (and same frequency)
        for id in range(1, 41):
            region.tell(1, id, "A")
            await region.expect_received(id, "A")
            if id > 20:
                await region.expect_received(id - 4, Stop())

        await region.expect_state({1: [*range(1, 17), *range(37, 41)]})

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 41, "B")
        await region.expect_received(41, "B")
        for id in [37, 38, *range(1, 9)]:
            await region.expect_received(id, Stop())

        await region.expect_state({1: [*range(9, 17), *range(39, 41)], 2: {41}})

        # reduce the per-region limit from 20 to 10, per-shard limit becomes 5
        region.set_active_entity_limit(10)
        for id in [39, *range(9, 13)]:  # passivate entities over new limit
            await region.expect_received(id, Stop())

        await region.expect_state({1: [*range(13, 17), 40], 2: {41}})

        # note: frequency sketch is recreated when limit changes, all frequency counts are 0
        # again
        for id in range(1, 11):
            region.tell(1, id, "C")
            await region.expect_received(id, "C")
            passivated = 40 if id == 1 else id + 11 if id <= 5 else id - 1
            await region.expect_received(passivated, Stop())

        await region.expect_state({1: [*range(1, 5), 10], 2: {41}})

        # increase the per-region limit from 10 to 30, per-shard limit becomes 15
        region.set_active_entity_limit(30)

        # note: frequency sketch is recreated when limit changes, all frequency counts are 0
        # again
        for id in range(1, 21):
            region.tell(1, id, "D")
            await region.expect_received(id, "D")
            if id > 15:  # start passivating window candidates at new higher limit of 15
                await region.expect_received(id - 3, Stop())  # new window size

        await region.expect_state({1: [*range(1, 13), *range(18, 21)], 2: {41}})
