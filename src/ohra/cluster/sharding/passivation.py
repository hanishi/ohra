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
from ohra.util.frequency_sketch import FastFrequencySketch, FrequencySketch
from ohra.util.recency_list import RecencyList
from ohra.util.segmented_recency_list import SegmentedRecencyList

from .passivation_settings import (
    CompositePassivationStrategy,
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
        """Scala 40-81 (``EntityPassivationStrategy.apply``)."""
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
            case CompositePassivationStrategy() as composite:
                idle_enabled = composite.idle is not None
                main = ActiveEntities.create(composite.main_strategy, idle_enabled, clock)
                if isinstance(main, NoActiveEntities):
                    return DisabledEntityPassivationStrategy()
                window = ActiveEntities.create(composite.window_strategy, idle_enabled, clock)
                no_window = isinstance(window, NoActiveEntities)
                return CompositeEntityPassivationStrategy(
                    composite.limit,
                    window,
                    0.0 if no_window else composite.initial_window_proportion,
                    0.0 if no_window else composite.minimum_window_proportion,
                    0.0 if no_window else composite.maximum_window_proportion,
                    NoAdmissionOptimizer()
                    if no_window
                    else AdmissionOptimizer.create(composite.limit, composite.window_optimizer),
                    AdmissionFilter.create(composite.limit, composite.admission_filter),
                    main,
                    _idle_check(composite.idle),
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


class CompositeEntityPassivationStrategy(LimitBasedEntityPassivationStrategy):
    """Scala 639-760: an admission window in front of the main area, an admission filter for
    entering the main area, and an optimizer for the window's proportion (TinyLFU, and the
    adaptive W-TinyLFU of "Adaptive Software Cache Management")."""

    def __init__(
        self,
        initial_limit: int,
        window: ActiveEntities,
        initial_window_proportion: float,
        minimum_window_proportion: float,
        maximum_window_proportion: float,
        window_optimizer: AdmissionOptimizer,
        admission_filter: AdmissionFilter,
        main: ActiveEntities,
        idle_check: IdleCheck | None,
    ) -> None:
        super().__init__(initial_limit)
        self._window = window
        self._minimum_window_proportion = minimum_window_proportion
        self._maximum_window_proportion = maximum_window_proportion
        self._window_optimizer = window_optimizer
        self._admission_filter = admission_filter
        self._main = main
        self._idle_check = idle_check
        self._window_proportion = initial_window_proportion
        self._window_limit = 0
        self._main_limit = 0
        # set initial limits based on initial window proportion
        self._calculate_limits()
        self._window.update_limit(self._window_limit)
        self._main.update_limit(self._main_limit)

    def _calculate_limits(self) -> None:
        """Scala 682-685."""
        self._window_limit = int(self._window_proportion * self._per_shard_limit)
        self._main_limit = self._per_shard_limit - self._window_limit

    def entity_touched(self, entity_id: str) -> PassivateEntities:
        self._admission_filter.update(entity_id)
        if self._window.is_active(entity_id):
            self._window_optimizer.record_active()
            passivated = self._window.update(entity_id)
        elif self._main.is_active(entity_id):
            self._window_optimizer.record_active()
            passivated = self._main.update(entity_id)
        else:
            self._window_optimizer.record_passive()
            passivated = self._maybe_admit_to_main(self._window.update(entity_id))
        self._adapt_window()
        return passivated

    def entity_terminated(self, entity_id: str) -> None:
        self._window.remove(entity_id)
        self._main.remove(entity_id)

    def _passivate_entities_on_limit_update(self) -> PassivateEntities:
        self._calculate_limits()
        self._window_optimizer.update_limit(self._per_shard_limit)
        self._admission_filter.update_capacity(self._per_shard_limit)
        admitted = self._maybe_admit_to_main(self._window.update_limit(self._window_limit))
        return admitted + self._main.update_limit(self._main_limit)

    @property
    def scheduled_interval(self) -> float | None:
        return None if self._idle_check is None else self._idle_check.interval

    def interval_passed(self) -> PassivateEntities:
        if self._idle_check is None:
            return []
        timeout = self._idle_check.timeout
        return self._window.remove_idle(timeout) + self._main.remove_idle(timeout)

    def _maybe_admit_to_main(self, candidates: PassivateEntities) -> PassivateEntities:
        """Scala 726-742: each candidate the window drops enters the main area if the filter
        admits it over the entity the main area would drop, and is passivated otherwise."""
        passivated: PassivateEntities = []
        for candidate in candidates:
            if self._main.size >= self._main_limit:
                selected = self._main.select()
                if selected is not None and not self._admission_filter.admit(candidate, selected):
                    passivated.append(candidate)
                else:
                    passivated += self._main.update(candidate)
            else:
                passivated += self._main.update(candidate)
        return passivated

    def _adapt_window(self) -> None:
        """Scala 744-759: entities move between the window and the main area, and the updates
        that move them passivate nothing, so what they return is ignored."""
        adjustment = self._window_optimizer.calculate_adjustment()
        if adjustment != 0.0:
            self._window_proportion = max(
                self._minimum_window_proportion,
                min(self._maximum_window_proportion, self._window_proportion + adjustment),
            )
            self._calculate_limits()
            if adjustment > 0.0:  # increase window limit
                self._window.update_limit(self._window_limit)
                for entity_id in self._main.update_limit(self._main_limit):
                    self._window.update(entity_id)
            else:  # decrease window limit
                self._main.update_limit(self._main_limit)
                for entity_id in self._window.update_limit(self._window_limit):
                    self._main.update(entity_id)


class AdmissionOptimizer(ABC):
    """Scala 787-812: an optimizer for the size of the composite strategy's admission
    window."""

    @staticmethod
    def create(
        initial_limit: int,
        optimizer: CompositePassivationStrategy.HillClimbingAdmissionOptimizer
        | CompositePassivationStrategy.NoAdmissionOptimizer,
    ) -> AdmissionOptimizer:
        """Scala 766-779 (``AdmissionOptimizer.apply``)."""
        match optimizer:
            case CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
                adjust_multiplier, initial_step, restart_threshold, step_decay
            ):
                return HillClimbingAdmissionOptimizer(
                    initial_limit, adjust_multiplier, initial_step, restart_threshold, step_decay
                )
            case _:
                return NoAdmissionOptimizer()

    @abstractmethod
    def record_active(self) -> None:
        """Scala 792: an entity was accessed that is already active."""

    @abstractmethod
    def record_passive(self) -> None:
        """Scala 797: an entity was accessed that was passive (needed to be activated)."""

    @abstractmethod
    def update_limit(self, new_limit: int) -> None:
        """Scala 803: the per-shard limit has been updated."""

    @abstractmethod
    def calculate_adjustment(self) -> float:
        """Scala 811: the adjustment to the window's proportion, positive to grow it, negative
        to shrink it, and 0.0 for none."""


class NoAdmissionOptimizer(AdmissionOptimizer):
    """Scala 820-825: the window's proportion is not optimized."""

    def record_active(self) -> None:
        pass

    def record_passive(self) -> None:
        pass

    def update_limit(self, new_limit: int) -> None:
        pass

    def calculate_adjustment(self) -> float:
        return 0.0


class HillClimbingAdmissionOptimizer(AdmissionOptimizer):
    """Scala 833-870: optimizes the window with a simple hill-climbing algorithm."""

    def __init__(
        self,
        initial_limit: int,
        adjust_multiplier: float,
        initial_step: float,
        restart_threshold: float,
        step_decay: float,
    ) -> None:
        self._adjust_multiplier = adjust_multiplier
        self._initial_step = initial_step
        self._restart_threshold = restart_threshold
        self._step_decay = step_decay
        self._adjust_size = adjust_multiplier * initial_limit
        self._accesses = 0
        self._active_accesses = 0
        self._previous_active_rate = 0.0
        self._next_step = -initial_step  # start in decreasing direction

    def record_active(self) -> None:
        self._accesses += 1
        self._active_accesses += 1

    def record_passive(self) -> None:
        self._accesses += 1

    def update_limit(self, new_limit: int) -> None:
        self._adjust_size = self._adjust_multiplier * new_limit

    def calculate_adjustment(self) -> float:
        if self._accesses < self._adjust_size:
            return 0.0
        active_rate = self._active_accesses / self._accesses
        delta = active_rate - self._previous_active_rate
        adjustment = self._next_step if delta >= 0 else -self._next_step
        direction = 1 if adjustment >= 0 else -1
        restart = abs(delta) >= self._restart_threshold
        self._next_step = (
            self._initial_step * direction if restart else adjustment * self._step_decay
        )
        self._previous_active_rate = active_rate
        self._accesses = 0
        self._active_accesses = 0
        return adjustment


class AdmissionFilter(ABC):
    """Scala 893-917: the filter for admitting entities to the composite strategy's main
    area."""

    @staticmethod
    def create(
        initial_capacity: int,
        filter: CompositePassivationStrategy.FrequencySketchAdmissionFilter
        | CompositePassivationStrategy.AlwaysAdmissionFilter,
    ) -> AdmissionFilter:
        """Scala 876-885 (``AdmissionFilter.apply``)."""
        match filter:
            case CompositePassivationStrategy.FrequencySketchAdmissionFilter(
                width_multiplier, reset_multiplier, depth, counter_bits
            ):
                return FrequencySketchAdmissionFilter.create(
                    initial_capacity, width_multiplier, reset_multiplier, depth, counter_bits
                )
            case _:
                return AlwaysAdmissionFilter()

    @abstractmethod
    def update_capacity(self, new_capacity: int) -> None:
        """Scala 899: the capacity, the per-shard limit, has been updated."""

    @abstractmethod
    def update(self, entity_id: str) -> None:
        """Scala 905: an entity has been accessed."""

    @abstractmethod
    def admit(self, candidate: str, selected: str) -> bool:
        """Scala 916: whether the candidate the window dropped enters the main area in place of
        the entity the main area selected. Whichever is not admitted or retained is
        passivated."""


class AlwaysAdmissionFilter(AdmissionFilter):
    """Scala 925-929: every candidate is admitted to the main area."""

    def update_capacity(self, new_capacity: int) -> None:
        pass

    def update(self, entity_id: str) -> None:
        pass

    def admit(self, candidate: str, selected: str) -> bool:
        return True


class FrequencySketchAdmissionFilter(AdmissionFilter):
    """Scala 957-976: admits a candidate more frequent than the selected entity. The sketch is
    recreated, so its counts reset, whenever the capacity changes."""

    @staticmethod
    def create(  # pyright: ignore[reportIncompatibleMethodOverride]
        initial_capacity: int,
        width_multiplier: int,
        reset_multiplier: float,
        depth: int,
        counter_bits: int,
    ) -> AdmissionFilter:
        """Scala 937-949 (``FrequencySketchAdmissionFilter.apply``): the fast sketch for a depth
        and counter size of 4. Scala's companion objects are unrelated, so this ``create``
        does not override ``AdmissionFilter.create``."""
        if depth == 4 and counter_bits == 4:
            return FastFrequencySketchAdmissionFilter(
                initial_capacity, width_multiplier, reset_multiplier
            )
        return FrequencySketchAdmissionFilter(
            initial_capacity, width_multiplier, reset_multiplier, depth, counter_bits
        )

    def __init__(
        self,
        initial_capacity: int,
        width_multiplier: int,
        reset_multiplier: float,
        depth: int,
        counter_bits: int,
    ) -> None:
        self._width_multiplier = width_multiplier
        self._reset_multiplier = reset_multiplier
        self._depth = depth
        self._counter_bits = counter_bits
        self._frequency_sketch = self._create_sketch(initial_capacity)

    def _create_sketch(self, capacity: int) -> FrequencySketch:
        return FrequencySketch.create(
            capacity,
            self._width_multiplier,
            self._reset_multiplier,
            self._depth,
            self._counter_bits,
        )

    def update_capacity(self, new_capacity: int) -> None:
        self._frequency_sketch = self._create_sketch(new_capacity)

    def update(self, entity_id: str) -> None:
        self._frequency_sketch.increment(entity_id)

    def admit(self, candidate: str, selected: str) -> bool:
        sketch = self._frequency_sketch
        return sketch.frequency(candidate) > sketch.frequency(selected)


class FastFrequencySketchAdmissionFilter(AdmissionFilter):
    """Scala 984-1001: ``FrequencySketchAdmissionFilter`` over the fast sketch (a depth of 4
    and 4-bit counters)."""

    def __init__(
        self, initial_capacity: int, width_multiplier: int, reset_multiplier: float
    ) -> None:
        self._width_multiplier = width_multiplier
        self._reset_multiplier = reset_multiplier
        self._frequency_sketch = self._create_sketch(initial_capacity)

    def _create_sketch(self, capacity: int) -> FastFrequencySketch:
        return FastFrequencySketch.create(capacity, self._width_multiplier, self._reset_multiplier)

    def update_capacity(self, new_capacity: int) -> None:
        self._frequency_sketch = self._create_sketch(new_capacity)

    def update(self, entity_id: str) -> None:
        self._frequency_sketch.increment(entity_id)

    def admit(self, candidate: str, selected: str) -> bool:
        sketch = self._frequency_sketch
        return sketch.frequency(candidate) > sketch.frequency(selected)
