"""Ported from Apache Pekko's IdleSpec (cluster-sharding/src/test/scala/org/apache/pekko/cluster/
sharding/passivation/IdleSpec.scala), on the shard. The case:

- passivate entities when they haven't seen messages for the configured duration (37)

The config is ``test_idle_spec``'s,
``PassivationStrategySettings.default_idle_strategy.with_idle_entity_passivation(1.0)``, checked
every 0.5 s. The porting rules are those of ``test_entity_passivation_shard_spec``: the spec
drives ``shard_harness.shard_region`` on ``ManualTime``; a ``Thread.sleep(timeout / 2)`` is
``time_passes(0.5)``; and ``expectReceived(id, Stop, within = timeout * 2)`` is
``time_passes(2.0)`` and then ``expect_received``.

Differences from the Scala: the harness places each id on the shard the spec names.
"""

from ohra.cluster.sharding.passivation_settings import PassivationStrategySettings

from .shard_harness import Stop, shard_region

P = PassivationStrategySettings

CONFIGURED_IDLE_TIMEOUT = 1.0


async def test_idle_passivate_entities_when_they_havent_seen_messages_on_the_shard() -> None:
    passivation = P.default_idle_strategy.with_idle_entity_passivation(CONFIGURED_IDLE_TIMEOUT)
    async with shard_region(passivation, expected_entities=2) as region:
        last_send_time_1 = region.now()
        region.tell(1, 1, "A")
        region.tell(2, 2, "B")
        await region.time_passes(CONFIGURED_IDLE_TIMEOUT / 2)
        region.tell(2, 2, "C")
        await region.time_passes(CONFIGURED_IDLE_TIMEOUT / 2)
        region.tell(2, 2, "D")
        await region.time_passes(CONFIGURED_IDLE_TIMEOUT / 2)
        last_send_time_2 = region.now()
        region.tell(2, 2, "E")

        await region.expect_received(1, "A")
        await region.expect_received(2, "B")
        await region.expect_received(2, "C")
        await region.expect_received(2, "D")
        await region.expect_received(2, "E")
        passivate_1 = await region.expect_received(1, Stop())
        await region.time_passes(CONFIGURED_IDLE_TIMEOUT * 2)
        passivate_2 = await region.expect_received(2, Stop())

        # note: touched timestamps are when the shard receives the message, not the entity
        # itself so look at the time from before sending the last message until receiving the
        # passivate message
        assert passivate_1.time - last_send_time_1 > CONFIGURED_IDLE_TIMEOUT
        assert passivate_2.time - last_send_time_2 > CONFIGURED_IDLE_TIMEOUT

        # entities can be re-activated
        region.tell(1, 1, "X")
        region.tell(2, 2, "Y")
        region.tell(1, 1, "Z")

        await region.expect_received(1, "X")
        await region.expect_received(2, "Y")
        await region.expect_received(1, "Z")
        await region.time_passes(CONFIGURED_IDLE_TIMEOUT * 2)
        await region.expect_received(1, Stop())
        await region.time_passes(CONFIGURED_IDLE_TIMEOUT * 2)
        await region.expect_received(2, Stop())
