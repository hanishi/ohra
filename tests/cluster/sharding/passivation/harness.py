"""The stand-in for Pekko's region and shards that the strategy-level passivation specs drive.

Pekko's specs (``AbstractEntityPassivationSpec``, EntityPassivationSpec.scala 93-144) start a
region of real shards and entities and watch for ``Stop``. Here the region keeps one strategy per
shard and calls it where Pekko's region and shard do, so the ports assert what each step
passivates.
"""

from ohra.cluster.sharding.passivation import EntityPassivationStrategy, PassivateEntities
from ohra.cluster.sharding.passivation_settings import PassivationStrategySettings
from ohra.cluster.sharding.shard import ClusterShardingSettings

from ....util import TestClock


class Region:
    """A region of shards, each with its strategy and its active entities."""

    def __init__(self, passivation: PassivationStrategySettings, clock: TestClock) -> None:
        self._settings = ClusterShardingSettings().with_passivation_strategy(passivation)
        self._clock = clock
        self._strategies: dict[int, EntityPassivationStrategy] = {}  # in start order
        self._active: dict[int, set[int]] = {}

    def tell(self, shard: int, entity_id: int) -> set[int]:
        """``region ! Envelope(shard, id, _)``: a new shard gets a strategy and every shard,
        in start order, ``shards_updated`` (ShardRegion.scala 1356-1362); then the shard touches
        the entity (Shard.scala 1075, 1093). Returns what every shard passivated."""
        passivated: set[int] = set()
        if shard not in self._strategies:
            self._strategies[shard] = EntityPassivationStrategy.create(self._settings, self._clock)
            self._active[shard] = set()
            for s, strategy in self._strategies.items():
                passivated |= self._passivate(s, strategy.shards_updated(len(self._strategies)))
        self._active[shard].add(entity_id)
        touched = self._strategies[shard].entity_touched(str(entity_id))
        return passivated | self._passivate(shard, touched)

    def set_active_entity_limit(self, limit: int) -> set[int]:
        """``ShardRegion.SetActiveEntityLimit``: ``limit_updated`` on every shard (767)."""
        passivated: set[int] = set()
        for s, strategy in self._strategies.items():
            passivated |= self._passivate(s, strategy.limit_updated(limit))
        return passivated

    def manually_passivate(self, shard: int, entity_id: int) -> None:
        """The entity's ``ManuallyPassivate``: it is touched, then its ``Passivate`` stops it."""
        self.tell(shard, entity_id)
        self._passivate(shard, [str(entity_id)])

    def passes(self, seconds: float) -> set[int]:
        """Advance the clock, then the shards' ``PassivateIntervalTick`` (Shard.scala 479-481,
        1009-1012). One tick per call, whatever ``scheduled_interval`` is: it assumes
        ``seconds`` is the strategy's interval."""
        self._clock.now += seconds
        passivated: set[int] = set()
        for s, strategy in self._strategies.items():
            if strategy.scheduled_interval is not None:
                passivated |= self._passivate(s, strategy.interval_passed())
        return passivated

    def state(self) -> dict[int, set[int]]:
        """Pekko's ``expectState``: each shard's active entities."""
        return {s: set(active) for s, active in self._active.items()}

    def _passivate(self, shard: int, entity_ids: PassivateEntities) -> set[int]:
        """Each passivated entity stops before the spec's next message, so the shard sees its
        ``Terminated`` at once (Shard.scala 913-917)."""
        for entity_id in entity_ids:
            self._active[shard].discard(int(entity_id))
            self._strategies[shard].entity_terminated(entity_id)
        return {int(entity_id) for entity_id in entity_ids}
