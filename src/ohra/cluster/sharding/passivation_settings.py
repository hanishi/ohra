"""Passivation strategy settings and the strategies they resolve to.

Apache Pekko's ``ClusterShardingSettings.PassivationStrategySettings`` (typed
``ClusterShardingSettings.scala`` 177-600, classic 144-557) and the internal strategies resolved
from them (classic 563-780), with the ``reference.conf`` defaults (33-206). Durations are seconds.
Pekko reads these from HOCON; ohra builds them with the ``with_*`` methods, so the named
strategies of ``reference.conf`` are the constants ``default_idle_strategy`` and
``default_strategy``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import ClassVar


@dataclass(frozen=True)
class PassivationStrategySettings:
    """Typed 177-236. ``with_*`` return a changed copy."""

    idle_entity_settings: IdleSettings | None = None
    active_entity_limit: int | None = None
    replacement_policy_settings: PolicySettings | None = None
    admission_settings: AdmissionSettings | None = None

    defaults: ClassVar[PassivationStrategySettings]
    disabled: ClassVar[PassivationStrategySettings]
    default_idle_strategy: ClassVar[PassivationStrategySettings]
    """reference.conf ``default-idle-strategy``: idle after 120 s."""
    default_strategy: ClassVar[PassivationStrategySettings]
    """reference.conf ``default-strategy``: 100 000 active entities, a segmented LRU (0.2, 0.8)
    behind an LRU window with a hill-climbing optimizer and a frequency-sketch filter."""

    def with_idle_entity_passivation(
        self, timeout: float | IdleSettings, interval: float | None = None
    ) -> PassivationStrategySettings:
        """Typed 191-215: an ``IdleSettings`` is used as is, a timeout starts from the defaults."""
        if isinstance(timeout, PassivationStrategySettings.IdleSettings):
            idle = timeout
        else:
            idle = PassivationStrategySettings.IdleSettings.defaults.with_timeout(timeout)
            if interval is not None:
                idle = idle.with_interval(interval)
        return replace(self, idle_entity_settings=idle)

    def with_active_entity_limit(self, limit: int) -> PassivationStrategySettings:
        return replace(self, active_entity_limit=limit)

    def with_replacement_policy(self, settings: PolicySettings) -> PassivationStrategySettings:
        return replace(self, replacement_policy_settings=settings)

    def with_least_recently_used_replacement(self) -> PassivationStrategySettings:
        return self.with_replacement_policy(self.LeastRecentlyUsedSettings.defaults)

    def with_most_recently_used_replacement(self) -> PassivationStrategySettings:
        return self.with_replacement_policy(self.MostRecentlyUsedSettings.defaults)

    def with_least_frequently_used_replacement(self) -> PassivationStrategySettings:
        return self.with_replacement_policy(self.LeastFrequentlyUsedSettings.defaults)

    def with_admission(self, settings: AdmissionSettings) -> PassivationStrategySettings:
        return replace(self, admission_settings=settings)

    @dataclass(frozen=True)
    class IdleSettings:
        """Typed 258-300. ``interval`` None is Pekko's ``default``: half the timeout."""

        timeout: float = 120.0
        interval: float | None = None

        defaults: ClassVar[PassivationStrategySettings.IdleSettings]

        def with_timeout(self, timeout: float) -> PassivationStrategySettings.IdleSettings:
            return replace(self, timeout=timeout)

        def with_interval(self, interval: float) -> PassivationStrategySettings.IdleSettings:
            return replace(self, interval=interval)

    @dataclass(frozen=True)
    class LeastRecentlyUsedSettings:
        """Typed 318-381."""

        segmented_settings: (
            PassivationStrategySettings.LeastRecentlyUsedSettings.SegmentedSettings | None
        ) = None

        defaults: ClassVar[PassivationStrategySettings.LeastRecentlyUsedSettings]

        @dataclass(frozen=True)
        class SegmentedSettings:
            """Typed 340-362. Proportions are stored as a tuple, so settings stay hashable."""

            levels: int
            proportions: tuple[float, ...] = ()

            def __post_init__(self) -> None:
                object.__setattr__(self, "proportions", tuple(self.proportions))

            def with_levels(
                self, levels: int
            ) -> PassivationStrategySettings.LeastRecentlyUsedSettings.SegmentedSettings:
                return replace(self, levels=levels)

            def with_proportions(
                self, proportions: Sequence[float]
            ) -> PassivationStrategySettings.LeastRecentlyUsedSettings.SegmentedSettings:
                return replace(self, proportions=tuple(proportions))

        def with_segmented(
            self, levels_or_proportions: int | Sequence[float]
        ) -> PassivationStrategySettings.LeastRecentlyUsedSettings:
            """Typed 366-372: a number of levels (equal segments), or the segments' proportions
            (one level each)."""
            segmented = PassivationStrategySettings.LeastRecentlyUsedSettings.SegmentedSettings
            if isinstance(levels_or_proportions, int):
                return replace(self, segmented_settings=segmented(levels_or_proportions, ()))
            p = tuple(levels_or_proportions)
            return replace(self, segmented_settings=segmented(len(p), p))

    @dataclass(frozen=True)
    class MostRecentlyUsedSettings:
        """Typed 391-401."""

        defaults: ClassVar[PassivationStrategySettings.MostRecentlyUsedSettings]

    @dataclass(frozen=True)
    class LeastFrequentlyUsedSettings:
        """Typed 410-430."""

        dynamic_aging: bool = False

        defaults: ClassVar[PassivationStrategySettings.LeastFrequentlyUsedSettings]

        def with_dynamic_aging(
            self, enabled: bool = True
        ) -> PassivationStrategySettings.LeastFrequentlyUsedSettings:
            return replace(self, dynamic_aging=enabled)

    type PolicySettings = (
        LeastRecentlyUsedSettings | MostRecentlyUsedSettings | LeastFrequentlyUsedSettings
    )

    @dataclass(frozen=True)
    class AdmissionSettings:
        """Typed 436-600: a frequency filter and an admission window in front of the main
        policy."""

        filter: PassivationStrategySettings.AdmissionSettings.FilterSettings | None = None
        window: PassivationStrategySettings.AdmissionSettings.WindowSettings | None = None

        defaults: ClassVar[PassivationStrategySettings.AdmissionSettings]

        @dataclass(frozen=True)
        class FrequencySketchSettings:
            depth: int = 4
            counter_bits: int = 4
            width_multiplier: int = 4
            reset_multiplier: float = 10.0

            defaults: ClassVar[
                PassivationStrategySettings.AdmissionSettings.FrequencySketchSettings
            ]

            def with_depth(
                self, depth: int
            ) -> PassivationStrategySettings.AdmissionSettings.FrequencySketchSettings:
                return replace(self, depth=depth)

            def with_counter_bits(
                self, bits: int
            ) -> PassivationStrategySettings.AdmissionSettings.FrequencySketchSettings:
                return replace(self, counter_bits=bits)

            def with_width_multiplier(
                self, multiplier: int
            ) -> PassivationStrategySettings.AdmissionSettings.FrequencySketchSettings:
                return replace(self, width_multiplier=multiplier)

            def with_reset_multiplier(
                self, multiplier: float
            ) -> PassivationStrategySettings.AdmissionSettings.FrequencySketchSettings:
                return replace(self, reset_multiplier=multiplier)

        @dataclass(frozen=True)
        class HillClimbingSettings:
            adjust_multiplier: float = 10.0
            initial_step: float = 0.0625
            restart_threshold: float = 0.05
            step_decay: float = 0.98

            defaults: ClassVar[PassivationStrategySettings.AdmissionSettings.HillClimbingSettings]

            def with_adjust_multiplier(
                self, multiplier: float
            ) -> PassivationStrategySettings.AdmissionSettings.HillClimbingSettings:
                return replace(self, adjust_multiplier=multiplier)

            def with_initial_step(
                self, step: float
            ) -> PassivationStrategySettings.AdmissionSettings.HillClimbingSettings:
                return replace(self, initial_step=step)

            def with_restart_threshold(
                self, threshold: float
            ) -> PassivationStrategySettings.AdmissionSettings.HillClimbingSettings:
                return replace(self, restart_threshold=threshold)

            def with_step_decay(
                self, decay: float
            ) -> PassivationStrategySettings.AdmissionSettings.HillClimbingSettings:
                return replace(self, step_decay=decay)

        type FilterSettings = FrequencySketchSettings
        type OptimizerSettings = HillClimbingSettings

        @dataclass(frozen=True)
        class WindowSettings:
            initial_proportion: float = 0.01
            minimum_proportion: float = 0.01
            maximum_proportion: float = 1.0
            optimizer: PassivationStrategySettings.AdmissionSettings.OptimizerSettings | None = (
                None
            )
            policy: PassivationStrategySettings.PolicySettings | None = None

            defaults: ClassVar[PassivationStrategySettings.AdmissionSettings.WindowSettings]

            def with_initial_proportion(
                self, proportion: float
            ) -> PassivationStrategySettings.AdmissionSettings.WindowSettings:
                return replace(self, initial_proportion=proportion)

            def with_minimum_proportion(
                self, proportion: float
            ) -> PassivationStrategySettings.AdmissionSettings.WindowSettings:
                return replace(self, minimum_proportion=proportion)

            def with_maximum_proportion(
                self, proportion: float
            ) -> PassivationStrategySettings.AdmissionSettings.WindowSettings:
                return replace(self, maximum_proportion=proportion)

            def with_optimizer(
                self, settings: PassivationStrategySettings.AdmissionSettings.OptimizerSettings
            ) -> PassivationStrategySettings.AdmissionSettings.WindowSettings:
                return replace(self, optimizer=settings)

            def with_policy(
                self, settings: PassivationStrategySettings.PolicySettings
            ) -> PassivationStrategySettings.AdmissionSettings.WindowSettings:
                return replace(self, policy=settings)

        def with_filter(
            self, settings: PassivationStrategySettings.AdmissionSettings.FilterSettings
        ) -> PassivationStrategySettings.AdmissionSettings:
            return replace(self, filter=settings)

        def with_window(
            self, settings: PassivationStrategySettings.AdmissionSettings.WindowSettings
        ) -> PassivationStrategySettings.AdmissionSettings:
            return replace(self, window=settings)


_S = PassivationStrategySettings
_S.defaults = _S()
_S.disabled = _S.defaults
_S.IdleSettings.defaults = _S.IdleSettings()
_S.LeastRecentlyUsedSettings.defaults = _S.LeastRecentlyUsedSettings()
_S.MostRecentlyUsedSettings.defaults = _S.MostRecentlyUsedSettings()
_S.LeastFrequentlyUsedSettings.defaults = _S.LeastFrequentlyUsedSettings()
_S.AdmissionSettings.defaults = _S.AdmissionSettings()
_S.AdmissionSettings.FrequencySketchSettings.defaults = (
    _S.AdmissionSettings.FrequencySketchSettings()
)
_S.AdmissionSettings.HillClimbingSettings.defaults = _S.AdmissionSettings.HillClimbingSettings()
_S.AdmissionSettings.WindowSettings.defaults = _S.AdmissionSettings.WindowSettings()
_S.default_idle_strategy = _S.defaults.with_idle_entity_passivation(120.0)
_S.default_strategy = (
    _S.defaults.with_active_entity_limit(100_000)
    .with_admission(
        _S.AdmissionSettings.defaults.with_window(
            _S.AdmissionSettings.WindowSettings.defaults.with_policy(
                _S.LeastRecentlyUsedSettings.defaults
            ).with_optimizer(_S.AdmissionSettings.HillClimbingSettings.defaults)
        ).with_filter(_S.AdmissionSettings.FrequencySketchSettings.defaults)
    )
    .with_replacement_policy(_S.LeastRecentlyUsedSettings.defaults.with_segmented((0.2, 0.8)))
)


# --- the resolved strategies (classic 563-740) ---------------------------------------------------


@dataclass(frozen=True)
class NoPassivationStrategy:
    """Classic 566: entities are never passivated automatically."""


@dataclass(frozen=True)
class IdlePassivationStrategy:
    """Classic 568-582."""

    timeout: float
    interval: float

    @staticmethod
    def create(settings: PassivationStrategySettings.IdleSettings) -> IdlePassivationStrategy:
        interval = settings.timeout / 2 if settings.interval is None else settings.interval
        return IdlePassivationStrategy(settings.timeout, interval)


@dataclass(frozen=True)
class LeastRecentlyUsedPassivationStrategy:
    """Classic 584-615. ``segmented`` is empty for a plain LRU."""

    limit: int
    segmented: tuple[float, ...]
    idle: IdlePassivationStrategy | None

    @staticmethod
    def create(
        settings: PassivationStrategySettings.LeastRecentlyUsedSettings,
        limit: int,
        idle: IdlePassivationStrategy | None,
    ) -> LeastRecentlyUsedPassivationStrategy:
        segmented = settings.segmented_settings
        if segmented is None or segmented.levels < 2:
            return LeastRecentlyUsedPassivationStrategy(limit, (), idle)
        proportions = segmented.proportions or (1.0 / segmented.levels,) * segmented.levels
        return LeastRecentlyUsedPassivationStrategy(limit, proportions, idle)


@dataclass(frozen=True)
class MostRecentlyUsedPassivationStrategy:
    """Classic 617-620."""

    limit: int
    idle: IdlePassivationStrategy | None


@dataclass(frozen=True)
class LeastFrequentlyUsedPassivationStrategy:
    """Classic 622-640."""

    limit: int
    dynamic_aging: bool
    idle: IdlePassivationStrategy | None

    @staticmethod
    def create(
        settings: PassivationStrategySettings.LeastFrequentlyUsedSettings,
        limit: int,
        idle: IdlePassivationStrategy | None,
    ) -> LeastFrequentlyUsedPassivationStrategy:
        return LeastFrequentlyUsedPassivationStrategy(limit, settings.dynamic_aging, idle)


@dataclass(frozen=True)
class CompositePassivationStrategy:
    """Classic 642-740: a window policy and a main policy behind an admission filter."""

    limit: int
    main_strategy: PassivationStrategy
    window_strategy: PassivationStrategy
    initial_window_proportion: float
    minimum_window_proportion: float
    maximum_window_proportion: float
    window_optimizer: CompositePassivationStrategy.AdmissionOptimizer
    admission_filter: CompositePassivationStrategy.AdmissionFilter
    idle: IdlePassivationStrategy | None

    @dataclass(frozen=True)
    class AlwaysAdmissionFilter:
        pass

    @dataclass(frozen=True)
    class FrequencySketchAdmissionFilter:
        width_multiplier: int
        reset_multiplier: float
        depth: int
        counter_bits: int

    type AdmissionFilter = AlwaysAdmissionFilter | FrequencySketchAdmissionFilter

    @dataclass(frozen=True)
    class NoAdmissionOptimizer:
        pass

    @dataclass(frozen=True)
    class HillClimbingAdmissionOptimizer:
        adjust_multiplier: float
        initial_step: float
        restart_threshold: float
        step_decay: float

    type AdmissionOptimizer = NoAdmissionOptimizer | HillClimbingAdmissionOptimizer

    @staticmethod
    def create_filter(
        settings: PassivationStrategySettings.AdmissionSettings.FilterSettings | None,
    ) -> CompositePassivationStrategy.AdmissionFilter:
        """Classic 647-657 (``AdmissionFilter.apply``)."""
        c = CompositePassivationStrategy
        if settings is None:
            return c.AlwaysAdmissionFilter()
        return c.FrequencySketchAdmissionFilter(
            width_multiplier=settings.width_multiplier,
            reset_multiplier=settings.reset_multiplier,
            depth=settings.depth,
            counter_bits=settings.counter_bits,
        )

    @staticmethod
    def create_optimizer(
        settings: PassivationStrategySettings.AdmissionSettings.OptimizerSettings | None,
    ) -> CompositePassivationStrategy.AdmissionOptimizer:
        """Classic 673-686 (``AdmissionOptimizer.apply``)."""
        c = CompositePassivationStrategy
        if settings is None:
            return c.NoAdmissionOptimizer()
        return c.HillClimbingAdmissionOptimizer(
            adjust_multiplier=settings.adjust_multiplier,
            initial_step=settings.initial_step,
            restart_threshold=settings.restart_threshold,
            step_decay=settings.step_decay,
        )

    @staticmethod
    def create(
        limit: int,
        main_settings: PassivationStrategySettings.PolicySettings | None,
        admission_settings: PassivationStrategySettings.AdmissionSettings,
        idle: IdlePassivationStrategy | None,
    ) -> CompositePassivationStrategy:
        """Classic 688-723."""
        c = CompositePassivationStrategy
        window = admission_settings.window
        return CompositePassivationStrategy(
            limit,
            _policy_strategy(main_settings, 0, None),
            _policy_strategy(window.policy if window else None, 0, None),
            window.initial_proportion if window else 0.0,
            window.minimum_proportion if window else 0.0,
            window.maximum_proportion if window else 0.0,
            c.create_optimizer(window.optimizer if window else None),
            c.create_filter(admission_settings.filter),
            idle,
        )


type PassivationStrategy = (
    NoPassivationStrategy
    | IdlePassivationStrategy
    | LeastRecentlyUsedPassivationStrategy
    | MostRecentlyUsedPassivationStrategy
    | LeastFrequentlyUsedPassivationStrategy
    | CompositePassivationStrategy
)


def _policy_strategy(
    policy: PassivationStrategySettings.PolicySettings | None,
    limit: int,
    idle: IdlePassivationStrategy | None,
) -> PassivationStrategy:
    """Classic 765-780 (``PassivationStrategy.apply`` of a policy)."""
    s = PassivationStrategySettings
    match policy:
        case s.LeastRecentlyUsedSettings():
            return LeastRecentlyUsedPassivationStrategy.create(policy, limit, idle)
        case s.MostRecentlyUsedSettings():
            return MostRecentlyUsedPassivationStrategy(limit, idle)
        case s.LeastFrequentlyUsedSettings():
            return LeastFrequentlyUsedPassivationStrategy.create(policy, limit, idle)
        case None:
            return idle if idle is not None else NoPassivationStrategy()


def passivation_strategy(settings: PassivationStrategySettings) -> PassivationStrategy:
    """Classic 747-763: the strategy the settings resolve to. A timeout of zero or less turns
    idle passivation off. The ``remember_entities`` branch (749) waits for remembered entities."""
    idle_settings = settings.idle_entity_settings
    idle = (
        IdlePassivationStrategy.create(idle_settings)
        if idle_settings is not None and idle_settings.timeout > 0
        else None
    )
    limit = settings.active_entity_limit
    if limit is None:
        return idle if idle is not None else NoPassivationStrategy()
    if settings.admission_settings is not None:
        return CompositePassivationStrategy.create(
            limit, settings.replacement_policy_settings, settings.admission_settings, idle
        )
    return _policy_strategy(settings.replacement_policy_settings, limit, idle)
