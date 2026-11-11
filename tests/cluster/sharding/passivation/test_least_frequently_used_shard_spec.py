"""Ported from Apache Pekko's LeastFrequentlyUsedSpec (cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/passivation/LeastFrequentlyUsedSpec.scala), on the shard. The cases:

- LeastFrequentlyUsedSpec: passivate the least frequently used entities when the per-shard
  entity limit is reached (76)
- LeastFrequentlyUsedWithDynamicAgingSpec: passivate the least frequently used entities when the
  per-shard entity limit is reached (226)
- LeastFrequentlyUsedWithIdleSpec: passivate entities when they haven't seen messages for the
  configured timeout (303)
- LeastFrequentlyUsedLimitAdjustmentSpec: adjust per-shard entity limits when the per-region
  limit is dynamically adjusted (346)

The configs are ``test_least_frequently_used_spec``'s. The porting rules are those of
``test_entity_passivation_shard_spec``: the specs drive ``shard_harness.shard_region`` on
``ManualTime``; a ``Thread.sleep(timeout / 2)`` is ``time_passes(0.5)``; and
``expectReceived(id, Stop, within = timeout * 2)`` is ``time_passes(2.0)`` and then
``expect_received``.

Differences from the Scala: the harness places each id on the shard the spec names. As in
Scala, the dynamic aging case's last three loops expect no ``Stop`` yet passivate entities, and
only its ``expectState`` shows them gone.
"""

from .shard_harness import ManuallyPassivate, Stop, shard_region
from .test_least_frequently_used_spec import CONFIG, DYNAMIC_AGING_CONFIG, IDLE_CONFIG

CONFIGURED_IDLE_TIMEOUT = 1.0


async def test_lfu_passivate_the_least_frequently_used_entities_on_the_shard() -> None:
    async with shard_region(CONFIG, expected_entities=40) as region:
        # only one active shard at first, least frequently used entities passivated once the
        # limit is reached
        for id in range(1, 21):
            accesses = (id - 1) % 4 + 1  # vary frequency between 1 and 4 accesses
            for x in range(1, accesses + 1):
                region.tell(1, id, f"A{x}")
                await region.expect_received(id, f"A{x}")
            expect_passivated = {
                11: 1, 12: 5, 13: 9, 14: 13, 15: 2, 16: 6, 17: 10, 18: 17, 19: 14, 20: 18,
            }  # fmt: skip
            if id in expect_passivated:
                await region.expect_received(expect_passivated[id], Stop())

        # shard 1: frequency 3 = (3, 7, 11, 15, 19), frequency 4 = (4, 8, 12, 16, 20)
        await region.expect_state({1: {3, 7, 11, 15, 19, 4, 8, 12, 16, 20}})

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 21, "B")
        await region.expect_received(21, "B")
        for id in [3, 7, 11, 15, 19]:
            await region.expect_received(id, Stop())

        # shard 1: frequency 4 = (4, 8, 12, 16, 20)
        # shard 2: frequency 1 = (21)
        await region.expect_state({1: {4, 8, 12, 16, 20}, 2: {21}})

        # shards now have a limit of 5 entities
        # note: newly added entities are not counted as least frequent for passivation
        for id in range(1, 21):
            accesses = (id - 1) % 4 + 1  # vary frequency between 1 and 4 accesses
            for x in range(1, accesses + 1):
                region.tell(1, id, f"C{x}")
                await region.expect_received(id, f"C{x}")
            passivated = id + 3 if accesses == 1 else id - 1
            await region.expect_received(passivated, Stop())

        # shard 1: frequency 4 = (4, 8, 12, 16, 20)
        # shard 2: frequency 1 = (21)
        await region.expect_state({1: {4, 8, 12, 16, 20}, 2: {21}})

        # shards now have a limit of 5 entities
        for id in range(21, 25):
            region.tell(2, id, "D")
            await region.expect_received(id, "D")

        # shard 1: frequency 4 = (4, 8, 12, 16, 20)
        # shard 2: frequency 1 = (22, 23, 24), frequency 2 = (21)
        await region.expect_state({1: {4, 8, 12, 16, 20}, 2: {22, 23, 24, 21}})

        # activating a third shard will divide the per-shard limit in three, passivating
        # entities over the new limits
        region.tell(3, 31, "E")
        await region.expect_received(31, "E")
        for id in [4, 8, 22]:
            await region.expect_received(id, Stop())

        # shard 1: frequency 4 = (12, 16, 20)
        # shard 2: frequency 1 = (23, 24), frequency 2 = (21)
        # shard 3: frequency 1 = (31)
        await region.expect_state({1: {12, 16, 20}, 2: {23, 24, 21}, 3: {31}})

        # shards now have a limit of 3 entities
        for id in range(21, 31):
            accesses = (id - 1) % 4 + 1  # vary frequency between 1 and 4 accesses
            for x in range(1, accesses + 1):
                region.tell(2, id, f"F{x}")
                await region.expect_received(id, f"F{x}")
            expect_passivated = {
                22: 23, 23: 24, 24: 22, 25: 21, 26: 25, 27: 26, 28: 23, 29: 27, 30: 29,
            }  # fmt: skip
            if id in expect_passivated:
                await region.expect_received(expect_passivated[id], Stop())

        # shard 1: frequency 4 = (12, 16, 20)
        # shard 2: frequency 2 = (30), frequency 4 = (24, 28)
        # shard 3: frequency 1 = (31)
        await region.expect_state({1: {12, 16, 20}, 2: {30, 24, 28}, 3: {31}})

        # shards now have a limit of 3 entities
        for id in range(31, 41):
            accesses = 4 - (id - 1) % 4  # vary frequency between 1 and 4 accesses
            for x in range(1, accesses + 1):
                region.tell(3, id, f"G{x}")
                await region.expect_received(id, f"G{x}")
            expect_passivated = {34: 32, 35: 31, 36: 35, 37: 36, 38: 34, 39: 38, 40: 39}
            if id in expect_passivated:
                await region.expect_received(expect_passivated[id], Stop())

        # shard 1: frequency 4 = (12, 16, 20)
        # shard 2: frequency 2 = (30), frequency 4 = (24, 28)
        # shard 3: frequency 1 = (40), frequency 4 = (33, 37)
        await region.expect_state({1: {12, 16, 20}, 2: {30, 24, 28}, 3: {40, 33, 37}})

        # manually passivate some entities
        region.tell(1, 16, ManuallyPassivate())
        region.tell(2, 24, ManuallyPassivate())
        region.tell(3, 33, ManuallyPassivate())
        await region.expect_received(16, ManuallyPassivate())
        await region.expect_received(24, ManuallyPassivate())
        await region.expect_received(33, ManuallyPassivate())
        await region.expect_received(16, Stop())
        await region.expect_received(24, Stop())
        await region.expect_received(33, Stop())

        # shard 1: frequency 4 = (12, 20)
        # shard 2: frequency 2 = (30), frequency 4 = (28)
        # shard 3: frequency 1 = (40), frequency 4 = (37)
        await region.expect_state({1: {12, 20}, 2: {30, 28}, 3: {40, 37}})

        for i in range(1, 4):
            for x in range(1, i + 1):
                region.tell(1, 10 + i, f"H{x}")
                region.tell(2, 20 + i, f"H{x}")
                region.tell(3, 30 + i, f"H{x}")
                await region.expect_received(10 + i, f"H{x}")
                await region.expect_received(20 + i, f"H{x}")
                await region.expect_received(30 + i, f"H{x}")
            if i == 2:
                await region.expect_received(21, Stop())
                await region.expect_received(40, Stop())
            elif i == 3:
                await region.expect_received(11, Stop())
                await region.expect_received(30, Stop())
                await region.expect_received(31, Stop())

        # shard 1: frequency 3 = (13), frequency 4 = (20), frequency 6 = (12)
        # shard 2: frequency 2 = (22), frequency 3 = (23), frequency 4 = (28)
        # shard 3: frequency 2 = (32), frequency 3 = (33), frequency 4 = (37)
        await region.expect_state({1: {13, 20, 12}, 2: {22, 23, 28}, 3: {32, 33, 37}})


async def test_lfu_with_dynamic_aging_passivate_the_least_frequently_used_on_the_shard() -> None:
    async with shard_region(DYNAMIC_AGING_CONFIG, expected_entities=21) as region:
        # only one active shard at first
        # ids 1 and 2 are quite popular initially
        for id in range(1, 3):
            for x in range(1, 6):
                region.tell(1, id, f"A{x}")
                await region.expect_received(id, f"A{x}")
        # ids 3, 4, and 5 are very popular initially
        for id in range(3, 6):
            for x in range(1, 11):
                region.tell(1, id, f"A{x}")
                await region.expect_received(id, f"A{x}")

        # shard 1: age = 0, @5 (5 + 0) = (1, 2), @10 (10 + 0) = (3, 4, 5)
        await region.expect_state({1: {1, 2, 3, 4, 5}})

        for id in range(6, 21):
            region.tell(1, id, "B")
            await region.expect_received(id, "B")
            if id > 10:
                await region.expect_received(id - 5, Stop())

        # shard 1: age = 2, @3 (1 + 2) = (16, 17, 18, 19, 20), @5 (5 + 0) = (1, 2),
        # @10 (10 + 0) = (3, 4, 5)
        await region.expect_state({1: {1, 2, 3, 4, 5, 16, 17, 18, 19, 20}})

        # activating a second shard will divide the per-shard limit in two, passivating half of
        # the first shard
        region.tell(2, 21, "C")
        await region.expect_received(21, "C")
        for id in range(16, 21):
            await region.expect_received(id, Stop())

        # shard 1: age = 3, @5 (5 + 0) = (1, 2), @10 (10 + 0) = (3, 4, 5)
        # shard 2: age = 0, @1 (1 + 0) = (21)
        await region.expect_state({1: {1, 2, 3, 4, 5}, 2: {21}})

        for id in range(6, 11):
            region.tell(1, id, "D")
            await region.expect_received(id, "D")

        # shard 1: age = 7, @7 (1 + 6) = (9), @8 (1 + 7) = (10), @10 (10 + 0) = (3, 4, 5)
        # shard 2: age = 0, @1 (1 + 0) = (21)
        await region.expect_state({1: {9, 10, 3, 4, 5}, 2: {21}})

        for id in range(11, 16):
            region.tell(1, id, "E")
            await region.expect_received(id, "E")

        # shard 1: age = 9, @10 (10 + 0) = (3, 4, 5), @10 (1 + 9) = (14, 15)
        # shard 2: age = 0, @1 (1 + 0) = (21)
        await region.expect_state({1: {3, 4, 5, 14, 15}, 2: {21}})

        for id in range(16, 21):
            region.tell(1, id, "F")
            await region.expect_received(id, "F")

        # shard 1: age = 10, @11 (1 + 10) = (16, 17, 18, 19, 20)
        # shard 2: age = 0, @1 (1 + 0) = (21)
        await region.expect_state({1: {16, 17, 18, 19, 20}, 2: {21}})


async def test_lfu_with_idle_passivate_entities_when_they_havent_seen_messages_on_the_shard() -> (
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


async def test_lfu_limit_adjustment_adjust_per_shard_limits_on_the_shard() -> None:
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
