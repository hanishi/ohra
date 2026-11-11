"""Entity passivation strategies, one per active shard.

Port of Apache Pekko's ``EntityPassivationStrategy.scala`` (cluster-sharding/src/main/scala/org/
apache/pekko/cluster/sharding/internal/), internal API as in Pekko. Durations are seconds.
Scala's ``() => Clock`` factory and ``Clock`` are both a ``Clock`` here; a replacement policy
without idle passivation gives its list no clock.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

from ohra.util import Clock
from ohra.util.frequency_list import FrequencyList
from ohra.util.recency_list import RecencyList
from ohra.util.segmented_recency_list import SegmentedRecencyList

from .passivation_settings import (
    IdlePassivationStrategy,
    LeastFrequentlyUsedPassivationStrategy,
    LeastRecentlyUsedPassivationStrategy,
    MostRecentlyUsedPassivationStrategy,
    PassivationStrategy,
)
from .passivation_settings import (
    PassivationStrategySettings as PassivationStrategySettings,  # public, re-exported
)
from .shard import ClusterShardingSettings

type PassivateEntities = list[str]
"""Scala 34-38; Pekko's ``PassivateEntities.none`` is ``[]``."""


def _idle_check(idle: IdlePassivationStrategy | None) -> IdleCheck | None:
    return None if idle is None else IdleCheck(idle.timeout, idle.interval)


class EntityPassivationStrategy(ABC):
    """Scala 88-129: an entity passivation strategy, instantiated per active shard."""

    @staticmethod
    def create(settings: ClusterShardingSettings, clock: Clock) -> EntityPassivationStrategy:
        """Scala 40-81 (``EntityPassivationStrategy.apply``). The composite strategy (54-78)
        gives ``DisabledEntityPassivationStrategy`` until it is ported."""
        match settings.passivation_strategy:
            case IdlePassivationStrategy(timeout, interval):
                return IdleEntityPassivationStrategy(IdleCheck(timeout, interval), clock)
            case LeastRecentlyUsedPassivationStrategy(limit, segmented, idle):
                if not segmented:
                    return LeastRecentlyUsedEntityPassivationStrategy(
                        limit, _idle_check(idle), clock
                    )
                return SegmentedLeastRecentlyUsedEntityPassivationStrategy(
                    limit, segmented, _idle_check(idle), clock
                )
            case MostRecentlyUsedPassivationStrategy(limit, idle):
                return MostRecentlyUsedEntityPassivationStrategy(limit, _idle_check(idle), clock)
            case LeastFrequentlyUsedPassivationStrategy(limit, dynamic_aging, idle):
                return LeastFrequentlyUsedEntityPassivationStrategy(
                    limit, dynamic_aging, _idle_check(idle), clock
                )
            case _:
                return DisabledEntityPassivationStrategy()

    @abstractmethod
    def limit_updated(self, new_limit: int) -> PassivateEntities:
        """Scala 96: the per-region active entity limit has been updated."""

    @abstractmethod
    def shards_updated(self, active_shards: int) -> PassivateEntities:
        """Scala 103: the number of active shards in this region has been updated."""

    @abstractmethod
    def entity_touched(self, entity_id: str) -> PassivateEntities:
        """Scala 110: an entity has been touched, recorded before message delivery."""

    @abstractmethod
    def entity_terminated(self, entity_id: str) -> None:
        """Scala 116: an entity has terminated; an id no longer tracked is a no-op."""

    @property
    @abstractmethod
    def scheduled_interval(self) -> float | None:
        """Scala 122: how often to call ``interval_passed``, if at all."""

    @abstractmethod
    def interval_passed(self) -> PassivateEntities:
        """Scala 128: the scheduled interval has passed."""


class DisabledEntityPassivationStrategy(EntityPassivationStrategy):
    """Scala 135-144: automatic passivation is disabled."""

    def limit_updated(self, new_limit: int) -> PassivateEntities:
        return []

    def shards_updated(self, active_shards: int) -> PassivateEntities:
        return []

    def entity_touched(self, entity_id: str) -> PassivateEntities:
        return []

    def entity_terminated(self, entity_id: str) -> None:
        pass

    @property
    def scheduled_interval(self) -> float | None:
        return None

    def interval_passed(self) -> PassivateEntities:
        return []


@dataclass(frozen=True)
class IdleCheck:
    """Scala 150."""

    timeout: float
    interval: float


class IdleEntityPassivationStrategy(EntityPassivationStrategy):
    """Scala 157-178: passivates entities that have not received a message for the timeout."""

    def __init__(self, idle_check: IdleCheck, clock: Clock) -> None:
        self._idle_check = idle_check
        self._recency_list = RecencyList[str](clock)

    @property
    def scheduled_interval(self) -> float | None:
        return self._idle_check.interval

    def limit_updated(self, new_limit: int) -> PassivateEntities:
        return []

    def shards_updated(self, active_shards: int) -> PassivateEntities:
        return []

    def entity_touched(self, entity_id: str) -> PassivateEntities:
        self._recency_list.update(entity_id)
        return []

    def entity_terminated(self, entity_id: str) -> None:
        self._recency_list.remove(entity_id)

    def interval_passed(self) -> PassivateEntities:
        return self._recency_list.remove_least_recent_outside(self._idle_check.timeout)


class LimitBasedEntityPassivationStrategy(EntityPassivationStrategy):
    """Scala 185-205: the per-region limit is divided evenly among the region's active
    shards."""

    def __init__(self, initial_limit: int) -> None:
        self._active_shards = 1
        self._per_region_limit = initial_limit
        self._per_shard_limit = initial_limit

    def limit_updated(self, new_limit: int) -> PassivateEntities:
        self._per_region_limit = new_limit
        self._per_shard_limit = self._per_region_limit // self._active_shards
        return self._passivate_entities_on_limit_update()

    def shards_updated(self, active_shards: int) -> PassivateEntities:
        self._active_shards = active_shards
        self._per_shard_limit = self._per_region_limit // self._active_shards
        return self._passivate_entities_on_limit_update()

    @abstractmethod
    def _passivate_entities_on_limit_update(self) -> PassivateEntities:
        """Scala 204."""


class LeastRecentlyUsedEntityPassivationStrategy(LimitBasedEntityPassivationStrategy):
    """Scala 216-237: passivates the least recently used entities over the per-shard limit."""

    def __init__(self, initial_limit: int, idle_check: IdleCheck | None, clock: Clock) -> None:
        super().__init__(initial_limit)
        self._idle_check = idle_check
        self.active = LeastRecentlyUsedReplacementPolicy(initial_limit, clock)

    def _passivate_entities_on_limit_update(self) -> PassivateEntities:
        return self.active.update_limit(self._per_shard_limit)

    def entity_touched(self, entity_id: str) -> PassivateEntities:
        return self.active.update(entity_id)

    def entity_terminated(self, entity_id: str) -> None:
        self.active.remove(entity_id)

    @property
    def scheduled_interval(self) -> float | None:
        return None if self._idle_check is None else self._idle_check.interval

    def interval_passed(self) -> PassivateEntities:
        if self._idle_check is None:
            return []
        return self.active.remove_idle(self._idle_check.timeout)


class SegmentedLeastRecentlyUsedEntityPassivationStrategy(LimitBasedEntityPassivationStrategy):
    """Scala 253-275: least recently used, over segmented levels of the given proportions."""

    def __init__(
        self,
        initial_limit: int,
        proportions: Sequence[float],
        idle_check: IdleCheck | None,
        clock: Clock,
    ) -> None:
        super().__init__(initial_limit)
        self._idle_check = idle_check
        self.active = SegmentedLeastRecentlyUsedReplacementPolicy(
            initial_limit, proportions, idle_check is not None, clock
        )

    def _passivate_entities_on_limit_update(self) -> PassivateEntities:
        return self.active.update_limit(self._per_shard_limit)

    def entity_touched(self, entity_id: str) -> PassivateEntities:
        return self.active.update(entity_id)

    def entity_terminated(self, entity_id: str) -> None:
        self.active.remove(entity_id)

    @property
    def scheduled_interval(self) -> float | None:
        return None if self._idle_check is None else self._idle_check.interval

    def interval_passed(self) -> PassivateEntities:
        if self._idle_check is None:
            return []
        return self.active.remove_idle(self._idle_check.timeout)


class MostRecentlyUsedEntityPassivationStrategy(LimitBasedEntityPassivationStrategy):
    """Scala 287-308: passivates the most recently used entities over the per-shard limit."""

    def __init__(self, initial_limit: int, idle_check: IdleCheck | None, clock: Clock) -> None:
        super().__init__(initial_limit)
        self._idle_check = idle_check
        self.active = MostRecentlyUsedReplacementPolicy(initial_limit, clock)

    def _passivate_entities_on_limit_update(self) -> PassivateEntities:
        return self.active.update_limit(self._per_shard_limit)

    def entity_touched(self, entity_id: str) -> PassivateEntities:
        return self.active.update(entity_id)

    def entity_terminated(self, entity_id: str) -> None:
        self.active.remove(entity_id)

    @property
    def scheduled_interval(self) -> float | None:
        return None if self._idle_check is None else self._idle_check.interval

    def interval_passed(self) -> PassivateEntities:
        if self._idle_check is None:
            return []
        return self.active.remove_idle(self._idle_check.timeout)


class LeastFrequentlyUsedEntityPassivationStrategy(LimitBasedEntityPassivationStrategy):
    """Scala 321-343: passivates the least frequently used entities over the per-shard limit,
    optionally with dynamic aging."""

    def __init__(
        self,
        initial_limit: int,
        dynamic_aging: bool,
        idle_check: IdleCheck | None,
        clock: Clock,
    ) -> None:
        super().__init__(initial_limit)
        self._idle_check = idle_check
        self.active = LeastFrequentlyUsedReplacementPolicy(
            initial_limit, dynamic_aging, idle_check is not None, clock
        )

    def _passivate_entities_on_limit_update(self) -> PassivateEntities:
        return self.active.update_limit(self._per_shard_limit)

    def entity_touched(self, entity_id: str) -> PassivateEntities:
        return self.active.update(entity_id)

    def entity_terminated(self, entity_id: str) -> None:
        self.active.remove(entity_id)

    @property
    def scheduled_interval(self) -> float | None:
        return None if self._idle_check is None else self._idle_check.interval

    def interval_passed(self) -> PassivateEntities:
        if self._idle_check is None:
            return []
        return self.active.remove_idle(self._idle_check.timeout)


class ActiveEntities(ABC):
    """Scala 372-420: active entity tracking, with a replacement policy."""

    @staticmethod
    def create(strategy: PassivationStrategy, idle_enabled: bool, clock: Clock) -> ActiveEntities:
        """Scala 349-364 (``ActiveEntities.apply``), for the composite strategy's areas."""
        match strategy:
            case LeastRecentlyUsedPassivationStrategy(_, segmented, _):
                if not segmented:
                    return LeastRecentlyUsedReplacementPolicy(0, clock)
                return SegmentedLeastRecentlyUsedReplacementPolicy(
                    0, segmented, idle_enabled, clock
                )
            case MostRecentlyUsedPassivationStrategy():
                return MostRecentlyUsedReplacementPolicy(0, clock)
            case LeastFrequentlyUsedPassivationStrategy(_, dynamic_aging, _):
                return LeastFrequentlyUsedReplacementPolicy(0, dynamic_aging, idle_enabled, clock)
            case _:
                return NoActiveEntities()

    @property
    @abstractmethod
    def size(self) -> int:
        """Scala 379."""

    @abstractmethod
    def is_active(self, entity_id: str) -> bool:
        """Scala 386."""

    @abstractmethod
    def update_limit(self, new_limit: int) -> PassivateEntities:
        """Scala 393: the per-shard limit has been updated."""

    @abstractmethod
    def update(self, entity_id: str) -> PassivateEntities:
        """Scala 400: an entity has been touched."""

    @abstractmethod
    def select(self) -> str | None:
        """Scala 406: the entity the policy would passivate next."""

    @abstractmethod
    def remove(self, entity_id: str) -> None:
        """Scala 412: an id no longer tracked is a no-op."""

    @abstractmethod
    def remove_idle(self, timeout: float) -> PassivateEntities:
        """Scala 419: remove the entities not active for the timeout."""


class NoActiveEntities(ActiveEntities):
    """Scala 428-438: no area (the composite strategy without a window)."""

    @property
    def size(self) -> int:
        return 0

    def is_active(self, entity_id: str) -> bool:
        return False

    def update_limit(self, new_limit: int) -> PassivateEntities:
        return []

    def update(self, entity_id: str) -> PassivateEntities:
        return [entity_id]

    def select(self) -> str | None:
        return None

    def remove(self, entity_id: str) -> None:
        pass

    def remove_idle(self, timeout: float) -> PassivateEntities:
        return []


class LeastRecentlyUsedReplacementPolicy(ActiveEntities):
    """Scala 449-479."""

    def __init__(self, initial_limit: int, clock: Clock) -> None:
        self._limit = initial_limit
        self._recency_list = RecencyList[str](clock)

    @property
    def size(self) -> int:
        return self._recency_list.size

    def is_active(self, entity_id: str) -> bool:
        return self._recency_list.contains(entity_id)

    def update_limit(self, new_limit: int) -> PassivateEntities:
        self._limit = new_limit
        return self._remove_excess()

    def update(self, entity_id: str) -> PassivateEntities:
        self._recency_list.update(entity_id)
        return self._remove_excess()

    def select(self) -> str | None:
        return self._recency_list.least_recent()

    def remove(self, entity_id: str) -> None:
        self._recency_list.remove(entity_id)

    def remove_idle(self, timeout: float) -> PassivateEntities:
        return self._recency_list.remove_least_recent_outside(timeout)

    def _remove_excess(self) -> PassivateEntities:
        excess = self._recency_list.size - self._limit
        return self._recency_list.remove_least_recent(excess) if excess > 0 else []


class SegmentedLeastRecentlyUsedReplacementPolicy(ActiveEntities):
    """Scala 495-538. Without idle passivation the list has no clock, and ``remove_idle``
    raises, as Scala's does."""

    def __init__(
        self, initial_limit: int, proportions: Sequence[float], idle_enabled: bool, clock: Clock
    ) -> None:
        self._limit = initial_limit
        self._proportions = tuple(proportions)
        self._segmented_recency_list = SegmentedRecencyList[str](
            self._segment_limits(), clock if idle_enabled else None
        )

    def _segment_limits(self) -> list[int]:
        """Scala 505-509: the first segment takes the leftover, so the total is the limit."""
        higher_limits = [int(p * self._limit) for p in self._proportions[1:]]
        return [self._limit - sum(higher_limits), *higher_limits]

    @property
    def size(self) -> int:
        return self._segmented_recency_list.size

    def is_active(self, entity_id: str) -> bool:
        return self._segmented_recency_list.contains(entity_id)

    def update_limit(self, new_limit: int) -> PassivateEntities:
        self._limit = new_limit
        self._segmented_recency_list.update_limits(self._segment_limits())
        return self._remove_excess()

    def update(self, entity_id: str) -> PassivateEntities:
        self._segmented_recency_list.update(entity_id)
        return self._remove_excess()

    def select(self) -> str | None:
        return self._segmented_recency_list.least_recent()

    def remove(self, entity_id: str) -> None:
        self._segmented_recency_list.remove(entity_id)

    def remove_idle(self, timeout: float) -> PassivateEntities:
        return self._segmented_recency_list.remove_overall_least_recent_outside(timeout)

    def _remove_excess(self) -> PassivateEntities:
        return self._segmented_recency_list.remove_least_recent_over_limit()


class MostRecentlyUsedReplacementPolicy(ActiveEntities):
    """Scala 549-579."""

    def __init__(self, initial_limit: int, clock: Clock) -> None:
        self._limit = initial_limit
        self._recency_list = RecencyList[str](clock)

    @property
    def size(self) -> int:
        return self._recency_list.size

    def is_active(self, entity_id: str) -> bool:
        return self._recency_list.contains(entity_id)

    def update_limit(self, new_limit: int) -> PassivateEntities:
        self._limit = new_limit
        return self._remove_excess()

    def update(self, entity_id: str) -> PassivateEntities:
        self._recency_list.update(entity_id)
        # remove the most recent entity before the just touched entity
        return self._remove_excess(skip=1)

    def select(self) -> str | None:
        return self._recency_list.most_recent()

    def remove(self, entity_id: str) -> None:
        self._recency_list.remove(entity_id)

    def remove_idle(self, timeout: float) -> PassivateEntities:
        return self._recency_list.remove_least_recent_outside(timeout)

    def _remove_excess(self, skip: int = 0) -> PassivateEntities:
        excess = self._recency_list.size - self._limit
        return self._recency_list.remove_most_recent(excess, skip) if excess > 0 else []


class LeastFrequentlyUsedReplacementPolicy(ActiveEntities):
    """Scala 592-637. Without idle passivation the list has no clock, and ``remove_idle``
    raises, as Scala's does."""

    def __init__(
        self, initial_limit: int, dynamic_aging: bool, idle_enabled: bool, clock: Clock
    ) -> None:
        self._limit = initial_limit
        self._frequency_list = FrequencyList[str](dynamic_aging, clock if idle_enabled else None)

    @property
    def size(self) -> int:
        return self._frequency_list.size

    def is_active(self, entity_id: str) -> bool:
        return self._frequency_list.contains(entity_id)

    def update_limit(self, new_limit: int) -> PassivateEntities:
        self._limit = new_limit
        return self._remove_excess()

    def update(self, entity_id: str) -> PassivateEntities:
        # first remove excess entities so that dynamic aging is updated
        # and the adjusted age is applied to any new entities on update
        # adjust the expected size by 1 if this is a newly activated entity
        adjustment = 0 if self._frequency_list.contains(entity_id) else 1
        passivated = self._remove_excess(adjustment)
        self._frequency_list.update(entity_id)
        return passivated

    def select(self) -> str | None:
        return self._frequency_list.least_frequent()

    def remove(self, entity_id: str) -> None:
        self._frequency_list.remove(entity_id)

    def remove_idle(self, timeout: float) -> PassivateEntities:
        return self._frequency_list.remove_overall_least_recent_outside(timeout)

    def _remove_excess(self, adjustment: int = 0) -> PassivateEntities:
        excess = self._frequency_list.size - self._limit + adjustment
        return self._frequency_list.remove_least_frequent(excess) if excess > 0 else []
