"""Ported from Apache Pekko's IdleSpec (cluster-sharding/src/test/scala/org/apache/pekko/cluster/
sharding/passivation/IdleSpec.scala). The case:

- passivate entities when they haven't seen messages for the configured duration (37)

The config, ``default-idle-strategy.idle-entity.timeout = 1s``, is
``PassivationStrategySettings.default_idle_strategy.with_idle_entity_passivation(1.0)``, checked
every 0.5 s. The porting rules are those of ``test_entity_passivation_spec``: the spec drives
``harness.Region``; a ``Thread.sleep(timeout / 2)`` is ``region.passes(0.5)``; and each idle
expectation asserts the entity is passivated by the first ``passes`` after which more than the
1.0 s timeout has gone since its last touch, and not by an earlier one.

Differences from the Scala: re-activated entities are passivated, as Scala waits up to twice
the timeout for, by the same bound.
"""

from ohra.cluster.sharding.passivation_settings import PassivationStrategySettings

from ....util import TestClock
from .harness import Region

P = PassivationStrategySettings


def test_idle_passivate_entities_when_they_havent_seen_messages_for_the_configured_duration() -> (
    None
):
    region = Region(P.default_idle_strategy.with_idle_entity_passivation(1.0), TestClock())

    region.tell(1, 1)  # t = 0.0, lastSendNanoTime1
    region.tell(2, 2)
    assert 1 not in region.passes(0.5)
    region.tell(2, 2)
    assert 1 not in region.passes(0.5)  # t = 1.0: 1.0 s since 1's touch, not more
    region.tell(2, 2)
    assert 1 in region.passes(0.5)  # t = 1.5
    region.tell(2, 2)  # lastSendNanoTime2
    assert 2 not in region.passes(0.5)
    assert 2 not in region.passes(0.5)
    assert 2 in region.passes(0.5)  # t = 3.0

    # entities can be re-activated
    region.tell(1, 1)
    region.tell(2, 2)
    region.tell(1, 1)
    assert region.passes(0.5).isdisjoint({1, 2})
    assert region.passes(0.5).isdisjoint({1, 2})
    assert {1, 2} <= region.passes(0.5)
