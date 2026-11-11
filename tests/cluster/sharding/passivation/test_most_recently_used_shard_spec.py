"""Ported from Apache Pekko's MostRecentlyUsedSpec (cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/passivation/MostRecentlyUsedSpec.scala), on the shard. The cases:

- MostRecentlyUsedSpec: passivate the most recently used entities when the per-shard entity
  limit is reached (58)
- MostRecentlyUsedWithIdleSpec: passivate entities when they haven't seen messages for the
  configured timeout (166)
- MostRecentlyUsedLimitAdjustmentSpec: adjust per-shard entity limits when the per-region limit
  is dynamically adjusted (209)

The configs are ``test_most_recently_used_spec``'s. The porting rules are those of
``test_entity_passivation_shard_spec``: the specs drive ``shard_harness.shard_region`` on
``ManualTime``; a ``Thread.sleep(timeout / 2)`` is ``time_passes(0.5)``; and
``expectReceived(id, Stop, within = timeout * 2)`` is ``time_passes(2.0)`` and then
``expect_received``.

Differences from the Scala: the harness places each id on the shard the spec names. As in
Scala, the limit adjustment case leaves the ``Stop`` of 3 and 4 unread (the touches of 4 and 5
passivate them, and no later expectation reads their probes).
"""

from .shard_harness import ManuallyPassivate, Stop, shard_region
from .test_most_recently_used_spec import CONFIG, IDLE_CONFIG

CONFIGURED_IDLE_TIMEOUT = 1.0


async def test_mru_passivate_the_most_recently_used_entities_on_the_shard() -> None:
    async with shard_region(CONFIG, expected_entities=40) as region:
        # only one active shard at first, most recently used entities passivated once the limit
        # is reached
        for id in range(1, 21):
            region.tell(1, id, "A")
            await region.expect_received(id, "A")
            if id > 10:
                await region.expect_received(id - 1, Stop())

        await region.expect_state({1: {1, 2, 3, 4, 5, 6, 7, 8, 9, 20}})

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 21, "B")
        await region.expect_received(21, "B")
        for id in [20, 9, 8, 7, 6]:
            await region.expect_received(id, Stop())

        await region.expect_state({1: {1, 2, 3, 4, 5}, 2: {21}})

        # shards now have a limit of 5 entities
        for id in range(1, 21):
            region.tell(1, id, "C")
            await region.expect_received(id, "C")
            if id > 5:
                await region.expect_received(id - 1, Stop())

        await region.expect_state({1: {1, 2, 3, 4, 20}, 2: {21}})

        # shards now have a limit of 5 entities
        for id in range(21, 25):
            region.tell(2, id, "D")
            await region.expect_received(id, "D")

        await region.expect_state({1: {1, 2, 3, 4, 20}, 2: {21, 22, 23, 24}})

        # activating a third shard will divide the per-shard limit in three, passivating
        # entities over the new limits
        region.tell(3, 31, "E")
        await region.expect_received(31, "E")
        for id in [24, 20, 4]:
            await region.expect_received(id, Stop())

        await region.expect_state({1: {1, 2, 3}, 2: {21, 22, 23}, 3: {31}})

        # shards now have a limit of 3 entities
        for id in range(24, 31):
            region.tell(2, id, "F")
            await region.expect_received(id, "F")
            await region.expect_received(id - 1, Stop())

        await region.expect_state({1: {1, 2, 3}, 2: {21, 22, 30}, 3: {31}})

        # shards now have a limit of 3 entities
        for id in range(31, 41):
            region.tell(3, id, "G")
            await region.expect_received(id, "G")
            if id > 33:
                await region.expect_received(id - 1, Stop())

        await region.expect_state({1: {1, 2, 3}, 2: {21, 22, 30}, 3: {31, 32, 40}})

        # manually passivate some entities
        region.tell(1, 2, ManuallyPassivate())
        region.tell(2, 22, ManuallyPassivate())
        region.tell(3, 32, ManuallyPassivate())
        await region.expect_received(2, ManuallyPassivate())
        await region.expect_received(22, ManuallyPassivate())
        await region.expect_received(32, ManuallyPassivate())
        await region.expect_received(2, Stop())
        await region.expect_received(22, Stop())
        await region.expect_received(32, Stop())

        await region.expect_state({1: {1, 3}, 2: {21, 30}, 3: {31, 40}})

        for i in range(1, 4):
            region.tell(1, 11 + i, "H")
            region.tell(2, 22 + i, "H")
            region.tell(3, 33 + i, "H")
            await region.expect_received(11 + i, "H")
            await region.expect_received(22 + i, "H")
            await region.expect_received(33 + i, "H")
            if i == 2:
                await region.expect_received(12, Stop())
                await region.expect_received(23, Stop())
                await region.expect_received(34, Stop())
            elif i == 3:
                await region.expect_received(13, Stop())
                await region.expect_received(24, Stop())
                await region.expect_received(35, Stop())

        await region.expect_state({1: {1, 3, 14}, 2: {21, 30, 25}, 3: {31, 40, 36}})


async def test_mru_with_idle_passivate_entities_when_they_havent_seen_messages_on_the_shard() -> (
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


async def test_mru_limit_adjustment_adjust_per_shard_limits_on_the_shard() -> None:
    async with shard_region(CONFIG, expected_entities=21) as region:
        # only one active shard at first, initial per-shard limit of 10
        for id in range(1, 21):
            region.tell(1, id, "A")
            await region.expect_received(id, "A")
            if id > 10:
                await region.expect_received(id - 1, Stop())

        await region.expect_state({1: {1, 2, 3, 4, 5, 6, 7, 8, 9, 20}})

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 21, "B")
        await region.expect_received(21, "B")
        for id in [20, 9, 8, 7, 6]:
            await region.expect_received(id, Stop())

        await region.expect_state({1: {1, 2, 3, 4, 5}, 2: {21}})

        # reduce the per-region limit from 10 to 6, per-shard limit becomes 3
        region.set_active_entity_limit(6)
        for id in [5, 4]:  # passivate entities over new limit
            await region.expect_received(id, Stop())

        await region.expect_state({1: {1, 2, 3}, 2: {21}})

        for id in range(1, 11):
            region.tell(1, id, "C")
            await region.expect_received(id, "C")
            if id > 5:
                await region.expect_received(id - 1, Stop())

        await region.expect_state({1: {1, 2, 10}, 2: {21}})

        # increase the per-region limit from 6 to 12, per-shard limit becomes 6
        region.set_active_entity_limit(12)

        for id in range(11, 21):
            region.tell(1, id, "D")
            await region.expect_received(id, "D")
            if id > 13:  # start passivating at new higher limit of 6
                await region.expect_received(id - 1, Stop())

        await region.expect_state({1: {1, 2, 10, 11, 12, 20}, 2: {21}})
