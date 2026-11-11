"""Ported from Apache Pekko's EntityPassivationSpec (cluster-sharding/src/test/scala/org/apache/
pekko/cluster/sharding/passivation/EntityPassivationSpec.scala), on the shard. The case:

- DisabledEntityPassivationSpec: not passivate when passivation is disabled (152)

Porting rules, shared by the shard-level passivation spec ports:

- Each Scala config is the ``PassivationStrategySettings`` expression of the matching
  strategy-level port (``test_entity_passivation_spec`` and its siblings); the disabled spec's
  ``strategy = none`` is ``PassivationStrategySettings.disabled``.
- The specs drive ``shard_harness.shard_region``, a stand-in for Pekko's ``ShardRegion`` with
  real shards and entities. ``region ! Envelope(shard, id, message)`` is
  ``region.tell(shard, id, message)``, and ``ShardRegion.SetActiveEntityLimit(n)`` is
  ``region.set_active_entity_limit(n)``. ``expectReceived``, ``expectNoMessage`` and
  ``expectState`` are the harness's, each entity with its own probe as in Scala.
- The test kit runs on ``ManualTime``, and Pekko's ``Clock`` nanoTime is the scheduler's
  seconds. A ``Thread.sleep(d)`` is ``time_passes(d)``; ``expectNoMessage(id, 1.second)`` is
  ``time_passes(1.0)`` and then ``expect_no_message(id)``; and
  ``expectReceived(id, Stop, within = configuredIdleTimeout * 2)`` is ``time_passes(2.0)`` and
  then ``expect_received(id, Stop())``, as the clock moves only when told.

Differences from the Scala: the harness places each id on the shard the spec names, where
Pekko's region extracts the shard from the envelope; ohra's regions route by ``shard_id_of``.
"""

from ohra.cluster.sharding.passivation_settings import (
    NoPassivationStrategy,
    PassivationStrategySettings,
)

from .shard_harness import shard_region

P = PassivationStrategySettings


async def test_disabled_entity_passivation_not_passivate_when_disabled_on_the_shard() -> None:
    async with shard_region(P.disabled, expected_entities=1) as region:
        assert region.settings.passivation_strategy == NoPassivationStrategy()
        region.tell(1, 1, "A")
        await region.expect_received(1, "A")
        await region.time_passes(1.0)
        await region.expect_no_message(1)
