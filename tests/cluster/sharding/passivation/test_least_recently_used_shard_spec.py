"""Ported from Apache Pekko's LeastRecentlyUsedSpec (cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/passivation/LeastRecentlyUsedSpec.scala), on the shard. The cases:

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

The configs are ``test_least_recently_used_spec``'s. The porting rules are those of
``test_entity_passivation_shard_spec``: the specs drive ``shard_harness.shard_region`` on
``ManualTime``; a ``Thread.sleep(timeout / 2)`` is ``time_passes(0.5)``; and
``expectReceived(id, Stop, within = timeout * 2)`` is ``time_passes(2.0)`` and then
``expect_received``.

Differences from the Scala: the harness places each id on the shard the spec names.
"""

from .shard_harness import ManuallyPassivate, Stop, shard_region
from .test_least_recently_used_spec import (
    CONFIG,
    IDLE_CONFIG,
    SEGMENTED_CONFIG,
    SEGMENTED_INITIAL_LIMIT_CONFIG,
)

CONFIGURED_IDLE_TIMEOUT = 1.0


async def test_lru_passivate_the_least_recently_used_entities_on_the_shard() -> None:
    async with shard_region(CONFIG, expected_entities=40) as region:
        # only one active shard at first, least recently used entities passivated once the
        # limit is reached
        for id in range(1, 21):
            region.tell(1, id, "A")
            await region.expect_received(id, "A")
            if id > 10:
                await region.expect_received(id - 10, Stop())

        await region.expect_state({1: range(11, 21)})

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 21, "B")
        await region.expect_received(21, "B")
        for id in range(11, 16):
            await region.expect_received(id, Stop())

        await region.expect_state({1: range(16, 21), 2: {21}})

        # shards now have a limit of 5 entities
        for id in range(1, 21):
            region.tell(1, id, "C")
            await region.expect_received(id, "C")
            passivated_id = id + 15 if id <= 5 else id - 5
            await region.expect_received(passivated_id, Stop())

        await region.expect_state({1: range(16, 21), 2: {21}})

        # shards now have a limit of 5 entities
        for id in range(21, 25):
            region.tell(2, id, "D")
            await region.expect_received(id, "D")

        await region.expect_state({1: range(16, 21), 2: range(21, 25)})

        # activating a third shard will divide the per-shard limit in three, passivating
        # entities over the new limits
        region.tell(3, 31, "E")
        await region.expect_received(31, "E")
        for id in [16, 17, 21]:
            await region.expect_received(id, Stop())

        await region.expect_state({1: {18, 19, 20}, 2: {22, 23, 24}, 3: {31}})

        # shards now have a limit of 3 entities
        for id in range(25, 31):
            region.tell(2, id, "F")
            await region.expect_received(id, "F")
            await region.expect_received(id - 3, Stop())

        await region.expect_state({1: {18, 19, 20}, 2: {28, 29, 30}, 3: {31}})

        # shards now have a limit of 3 entities
        for id in range(31, 41):
            region.tell(3, id, "G")
            await region.expect_received(id, "G")
            if id > 33:
                await region.expect_received(id - 3, Stop())

        await region.expect_state({1: {18, 19, 20}, 2: {28, 29, 30}, 3: {38, 39, 40}})

        # manually passivate some entities
        region.tell(1, 19, ManuallyPassivate())
        region.tell(2, 29, ManuallyPassivate())
        region.tell(3, 39, ManuallyPassivate())
        await region.expect_received(19, ManuallyPassivate())
        await region.expect_received(29, ManuallyPassivate())
        await region.expect_received(39, ManuallyPassivate())
        await region.expect_received(19, Stop())
        await region.expect_received(29, Stop())
        await region.expect_received(39, Stop())

        await region.expect_state({1: {18, 20}, 2: {28, 30}, 3: {38, 40}})

        for i in range(1, 4):
            region.tell(1, 10 + i, "H")
            region.tell(2, 20 + i, "H")
            region.tell(3, 30 + i, "H")
            await region.expect_received(10 + i, "H")
            await region.expect_received(20 + i, "H")
            await region.expect_received(30 + i, "H")
            if i == 2:
                await region.expect_received(18, Stop())
                await region.expect_received(28, Stop())
                await region.expect_received(38, Stop())
            elif i == 3:
                await region.expect_received(20, Stop())
                await region.expect_received(30, Stop())
                await region.expect_received(40, Stop())

        await region.expect_state({1: {11, 12, 13}, 2: {21, 22, 23}, 3: {31, 32, 33}})


async def test_segmented_lru_passivate_the_segmented_least_recently_used_on_the_shard() -> None:
    async with shard_region(SEGMENTED_CONFIG, expected_entities=21) as region:
        # only one active shard at first
        # entities are only accessed once (so all in lowest segment)
        # least recently used entities passivated once the total limit is reached
        for id in range(1, 21):
            region.tell(1, id, "A")
            await region.expect_received(id, "A")
            if id > 10:
                await region.expect_received(id - 10, Stop())

        # shard 1: level 0: 11-20, level 1: empty
        await region.expect_state({1: range(11, 21)})

        # accessing entities a second time moves them to the higher "protected" segment
        # when limit for higher segment is reached, entities are demoted to lower
        # "probationary" segment
        for id in range(11, 21):
            region.tell(1, id, "B")
            await region.expect_received(id, "B")

        # shard 1: level 0: 11-12, level 1: 13-20
        await region.expect_state({1: range(11, 21)})

        # newly activated entities will just cycle through the lower segment
        for id in range(1, 6):
            region.tell(1, id, "C")
            await region.expect_received(id, "C")
            passivated_id = id + 10 if id < 3 else id - 2
            await region.expect_received(passivated_id, Stop())

        # shard 1: level 0: 4-5, level 1: 13-20
        await region.expect_state({1: [4, 5, *range(13, 21)]})

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 21, "D")
        await region.expect_received(21, "D")
        for id in [4, 5, 13, 14, 15]:
            await region.expect_received(id, Stop())

        # shard 1: level 0: 16, level 1: 17-20
        # shard 2: level 0: 21, level 1: empty
        await region.expect_state({1: range(16, 21), 2: {21}})

        # entities in higher "protected" segment accessed again move to most recent position
        # entities in lower "probationary" segment accessed again are promoted
        # lower segment only has space for one entity
        for id in reversed(range(11, 21)):
            region.tell(1, id, "E")
            await region.expect_received(id, "E")
            if id == 15:
                await region.expect_received(20, Stop())
            elif id < 15:
                await region.expect_received(id + 1, Stop())

        # shard 1: level 0: 11, level 1: 19, 18, 17, 16
        # shard 2: level 0: 21, level 1: empty
        await region.expect_state({1: {11, 19, 18, 17, 16}, 2: {21}})


async def test_lru_with_idle_passivate_entities_when_they_havent_seen_messages_on_the_shard() -> (
    None
):
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


async def test_lru_limit_adjustment_adjust_per_shard_limits_on_the_shard() -> None:
    async with shard_region(CONFIG, expected_entities=21) as region:
        # only one active shard at first, initial per-shard limit of 10
        for id in range(1, 21):
            region.tell(1, id, "A")
            await region.expect_received(id, "A")
            if id > 10:
                await region.expect_received(id - 10, Stop())

        await region.expect_state({1: range(11, 21)})

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 21, "B")
        await region.expect_received(21, "B")
        for id in range(11, 16):
            await region.expect_received(id, Stop())

        await region.expect_state({1: range(16, 21), 2: {21}})

        # reduce the per-region limit from 10 to 6, per-shard limit becomes 3
        region.set_active_entity_limit(6)
        for id in range(16, 18):  # passivate entities over new limit
            await region.expect_received(id, Stop())

        await region.expect_state({1: range(18, 21), 2: {21}})

        for id in range(1, 11):
            region.tell(1, id, "C")
            await region.expect_received(id, "C")
            passivated = id + 17 if id < 4 else id - 3
            await region.expect_received(passivated, Stop())

        await region.expect_state({1: range(8, 11), 2: {21}})

        # increase the per-region limit from 6 to 12, per-shard limit becomes 6
        region.set_active_entity_limit(12)

        for id in range(11, 21):
            region.tell(1, id, "D")
            await region.expect_received(id, "D")
            if id > 13:  # start passivating at new higher limit of 6
                await region.expect_received(id - 6, Stop())

        await region.expect_state({1: range(15, 21), 2: {21}})


async def test_segmented_lru_limit_adjustment_adjust_per_shard_limits_on_the_shard() -> None:
    async with shard_region(SEGMENTED_INITIAL_LIMIT_CONFIG, expected_entities=31) as region:
        # only one active shard at first, initial per-shard limit of 20
        for id in range(1, 31):
            region.tell(1, id, "A")
            await region.expect_received(id, "A")
            if id > 20:
                await region.expect_received(id - 20, Stop())

        await region.expect_state({1: range(11, 31)})

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 31, "B")
        await region.expect_received(31, "B")
        for id in range(11, 21):
            await region.expect_received(id, Stop())

        await region.expect_state({1: range(21, 31), 2: {31}})

        # reduce the per-region limit from 20 to 10, per-shard limit becomes 5
        region.set_active_entity_limit(10)
        for id in range(21, 26):  # passivate entities over new limit
            await region.expect_received(id, Stop())

        await region.expect_state({1: range(26, 31), 2: {31}})

        for id in range(1, 11):
            region.tell(1, id, "C")
            await region.expect_received(id, "C")
            passivated = id + 25 if id < 6 else id - 5
            await region.expect_received(passivated, Stop())

        await region.expect_state({1: range(6, 11), 2: {31}})

        # increase the per-region limit from 10 to 30, per-shard limit becomes 15
        region.set_active_entity_limit(30)

        for id in range(11, 31):
            region.tell(1, id, "D")
            await region.expect_received(id, "D")
            if id > 20:  # start passivating at new higher limit of 15
                await region.expect_received(id - 15, Stop())

        await region.expect_state({1: range(16, 31), 2: {31}})
