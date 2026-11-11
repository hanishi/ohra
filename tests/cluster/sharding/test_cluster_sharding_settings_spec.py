"""Ported from Apache Pekko's ClusterShardingSettingsSpec (cluster-sharding/src/test/scala/org/
apache/pekko/cluster/sharding/ClusterShardingSettingsSpec.scala). The cases, in the Scala order,
each asserting ``settings.passivation_strategy`` (durations in seconds):

- default passivation strategy, idle after 120 s (41)
- default idle timeout, via config (47) and via factory method (59)
- default idle timeout and interval, via config (68) and via factory method (83)
- new default strategy (92), its limit (118) and its idle timeout (147), via config
- LRU, via config (176) and via factory method (195)
- segmented LRU, via config (207); 4-level segmented LRU, via config (232); segmented LRU, via
  factory method (254)
- LRU with idle timeout, via config (266) and via factory method (282)
- MRU, via config (295) and via factory method (311); with idle timeout, via config (322) and via
  factory method (337)
- LFU, via config (349) and via factory method (366); with idle timeout, via config (378) and via
  factory method (394); with dynamic aging, via config (407) and via factory method (429)
- admission window policy (443), window proportion (469) and window optimizer (498), via config
- admission, via config (531); admission parameters tuned, via config (576)
- admission, via factory method (634); admission settings tuned, via factory method (668)
- admission and idle timeout, via config (714) and via factory method (758)
- idle timeout of zero disables passivation, via config (805) and via factory method (811)
- disabled, via factory method (818)

Differences from the Scala:

- ohra has no HOCON, so each "(via config)" case builds the same settings from the builders. A case
  that sets ``default-idle-strategy.*`` starts from ``default_idle_strategy``, one that sets
  ``strategy = default-strategy`` from ``default_strategy``, and one that names a custom strategy
  from ``defaults`` (``segmented.levels = 4`` is ``with_segmented(4)``). ``settings("")`` is
  ``ClusterShardingSettings()``.
- The two remember-entities cases (793, 799) are left to the sub-project that adds
  ``remember_entities``.
- ``interval = 42.millis`` of the second idle case is 0.042 s.

Beyond the Scala: proportions given as a list are stored as a tuple, a negative idle timeout and
fewer than two segment levels resolve as Pekko's, and the settings are exported.
"""

import ohra
import ohra.cluster.sharding as sharding
from ohra.cluster.sharding import ClusterShardingSettings
from ohra.cluster.sharding.passivation_settings import (
    CompositePassivationStrategy as Composite,
    IdlePassivationStrategy,
    LeastFrequentlyUsedPassivationStrategy,
    LeastRecentlyUsedPassivationStrategy,
    MostRecentlyUsedPassivationStrategy,
    NoPassivationStrategy,
    PassivationStrategySettings as S,
)

Window = S.AdmissionSettings.WindowSettings
Hill = S.AdmissionSettings.HillClimbingSettings
Sketch = S.AdmissionSettings.FrequencySketchSettings
Lru = S.LeastRecentlyUsedSettings
Lfu = S.LeastFrequentlyUsedSettings

MIN = 60.0


def resolved(strategy_settings: S) -> object:
    return (
        ClusterShardingSettings().with_passivation_strategy(strategy_settings).passivation_strategy
    )


def idle(timeout: float, interval: float) -> IdlePassivationStrategy:
    return IdlePassivationStrategy(timeout, interval)


def composite(
    limit: int,
    *,
    main: object,
    window_proportions: tuple[float, float, float] = (0.01, 0.01, 1.0),
    optimizer: object = Composite.NoAdmissionOptimizer(),
    admission_filter: object = Composite.AlwaysAdmissionFilter(),
    idle_strategy: IdlePassivationStrategy | None = None,
) -> Composite:
    return Composite(
        limit,
        main,  # type: ignore[arg-type]
        LeastRecentlyUsedPassivationStrategy(0, (), None),
        *window_proportions,
        optimizer,  # type: ignore[arg-type]
        admission_filter,  # type: ignore[arg-type]
        idle_strategy,
    )


HILL = Composite.HillClimbingAdmissionOptimizer(10.0, 0.0625, 0.05, 0.98)
SKETCH = Composite.FrequencySketchAdmissionFilter(4, 10.0, 4, 4)
SLRU = LeastRecentlyUsedPassivationStrategy(0, (0.2, 0.8), None)
LFU0 = LeastFrequentlyUsedPassivationStrategy(0, False, None)
ADMISSION = S.AdmissionSettings.defaults.with_window(
    Window.defaults.with_policy(Lru.defaults).with_optimizer(Hill.defaults)
).with_filter(Sketch.defaults)
SLRU_SETTINGS = Lru.defaults.with_segmented((0.2, 0.8))


def test_default_passivation_strategy_is_idle_after_120_seconds() -> None:
    assert ClusterShardingSettings().passivation_strategy == idle(120.0, 60.0)


def test_default_idle_timeout_via_config() -> None:
    s = S.default_idle_strategy.with_idle_entity_passivation(3 * MIN)
    assert resolved(s) == idle(180.0, 90.0)


def test_default_idle_timeout_via_factory_method() -> None:
    assert resolved(S.defaults.with_idle_entity_passivation(42.0)) == idle(42.0, 21.0)


def test_default_idle_timeout_and_interval_via_config() -> None:
    s = S.default_idle_strategy.with_idle_entity_passivation(42.0, 0.042)
    assert resolved(s) == idle(42.0, 0.042)


def test_default_idle_timeout_and_interval_via_factory_method() -> None:
    assert resolved(S.defaults.with_idle_entity_passivation(42.0, 0.042)) == idle(42.0, 0.042)


def test_new_default_strategy_via_config() -> None:
    assert resolved(S.default_strategy) == composite(
        100_000, main=SLRU, optimizer=HILL, admission_filter=SKETCH
    )


def test_new_default_strategy_limit_via_config() -> None:
    s = S.default_strategy.with_active_entity_limit(1_000_000)
    assert resolved(s) == composite(1_000_000, main=SLRU, optimizer=HILL, admission_filter=SKETCH)


def test_new_default_strategy_with_idle_timeout_via_config() -> None:
    s = S.default_strategy.with_idle_entity_passivation(30 * MIN)
    assert resolved(s) == composite(
        100_000,
        main=SLRU,
        optimizer=HILL,
        admission_filter=SKETCH,
        idle_strategy=idle(1800.0, 900.0),
    )


def test_lru_via_config() -> None:
    s = S.defaults.with_active_entity_limit(1_000_000).with_replacement_policy(Lru.defaults)
    assert resolved(s) == LeastRecentlyUsedPassivationStrategy(1_000_000, (), None)


def test_lru_via_factory_method() -> None:
    s = S.defaults.with_active_entity_limit(42_000).with_least_recently_used_replacement()
    assert resolved(s) == LeastRecentlyUsedPassivationStrategy(42_000, (), None)


def test_segmented_lru_via_config() -> None:
    s = S.defaults.with_active_entity_limit(1_000_000).with_replacement_policy(
        Lru.defaults.with_segmented(Lru.SegmentedSettings(2, (0.2, 0.8)).proportions)
    )
    assert resolved(s) == LeastRecentlyUsedPassivationStrategy(1_000_000, (0.2, 0.8), None)


def test_four_level_segmented_lru_via_config() -> None:
    s = S.defaults.with_active_entity_limit(1_000_000).with_replacement_policy(
        Lru.defaults.with_segmented(4)
    )
    assert resolved(s) == LeastRecentlyUsedPassivationStrategy(
        1_000_000, (0.25, 0.25, 0.25, 0.25), None
    )


def test_segmented_lru_via_factory_method() -> None:
    s = S.defaults.with_active_entity_limit(42_000).with_replacement_policy(
        Lru.defaults.with_segmented([0.4, 0.3, 0.2, 0.1])
    )
    assert resolved(s) == LeastRecentlyUsedPassivationStrategy(42_000, (0.4, 0.3, 0.2, 0.1), None)


def test_lru_with_idle_timeout_via_config() -> None:
    s = (
        S.defaults.with_active_entity_limit(1_000_000)
        .with_replacement_policy(Lru.defaults)
        .with_idle_entity_passivation(30 * MIN)
    )
    assert resolved(s) == LeastRecentlyUsedPassivationStrategy(1_000_000, (), idle(1800.0, 900.0))


def test_lru_with_idle_timeout_via_factory_method() -> None:
    s = (
        S.defaults.with_active_entity_limit(42_000)
        .with_least_recently_used_replacement()
        .with_idle_entity_passivation(42 * MIN)
    )
    assert resolved(s) == LeastRecentlyUsedPassivationStrategy(42_000, (), idle(2520.0, 1260.0))


def test_mru_via_config() -> None:
    s = S.defaults.with_active_entity_limit(1_000_000).with_replacement_policy(
        S.MostRecentlyUsedSettings.defaults
    )
    assert resolved(s) == MostRecentlyUsedPassivationStrategy(1_000_000, None)


def test_mru_via_factory_method() -> None:
    s = S.defaults.with_active_entity_limit(42_000).with_most_recently_used_replacement()
    assert resolved(s) == MostRecentlyUsedPassivationStrategy(42_000, None)


def test_mru_with_idle_timeout_via_config() -> None:
    s = (
        S.defaults.with_active_entity_limit(1_000_000)
        .with_replacement_policy(S.MostRecentlyUsedSettings.defaults)
        .with_idle_entity_passivation(30 * MIN)
    )
    assert resolved(s) == MostRecentlyUsedPassivationStrategy(1_000_000, idle(1800.0, 900.0))


def test_mru_with_idle_timeout_via_factory_method() -> None:
    s = (
        S.defaults.with_active_entity_limit(42_000)
        .with_most_recently_used_replacement()
        .with_idle_entity_passivation(42 * MIN)
    )
    assert resolved(s) == MostRecentlyUsedPassivationStrategy(42_000, idle(2520.0, 1260.0))


def test_lfu_via_config() -> None:
    s = S.defaults.with_active_entity_limit(1_000_000).with_replacement_policy(Lfu.defaults)
    assert resolved(s) == LeastFrequentlyUsedPassivationStrategy(1_000_000, False, None)


def test_lfu_via_factory_method() -> None:
    s = S.defaults.with_active_entity_limit(42_000).with_least_frequently_used_replacement()
    assert resolved(s) == LeastFrequentlyUsedPassivationStrategy(42_000, False, None)


def test_lfu_with_idle_timeout_via_config() -> None:
    s = (
        S.defaults.with_active_entity_limit(1_000_000)
        .with_replacement_policy(Lfu.defaults)
        .with_idle_entity_passivation(30 * MIN)
    )
    assert resolved(s) == LeastFrequentlyUsedPassivationStrategy(
        1_000_000, False, idle(1800.0, 900.0)
    )


def test_lfu_with_idle_timeout_via_factory_method() -> None:
    s = (
        S.defaults.with_active_entity_limit(42_000)
        .with_least_frequently_used_replacement()
        .with_idle_entity_passivation(42 * MIN)
    )
    assert resolved(s) == LeastFrequentlyUsedPassivationStrategy(
        42_000, False, idle(2520.0, 1260.0)
    )


def test_lfu_with_dynamic_aging_via_config() -> None:
    s = S.defaults.with_active_entity_limit(1000).with_replacement_policy(Lfu(dynamic_aging=True))
    assert resolved(s) == LeastFrequentlyUsedPassivationStrategy(1000, True, None)


def test_lfu_with_dynamic_aging_via_factory_method() -> None:
    s = S.defaults.with_active_entity_limit(42_000).with_replacement_policy(
        Lfu.defaults.with_dynamic_aging()
    )
    assert resolved(s) == LeastFrequentlyUsedPassivationStrategy(42_000, True, None)


def test_admission_window_policy_via_config() -> None:
    s = (
        S.defaults.with_active_entity_limit(1_000_000)
        .with_admission(
            S.AdmissionSettings.defaults.with_window(Window.defaults.with_policy(Lru.defaults))
        )
        .with_replacement_policy(Lfu.defaults)
    )
    assert resolved(s) == composite(1_000_000, main=LFU0)


def test_admission_window_proportion_via_config() -> None:
    s = (
        S.defaults.with_active_entity_limit(1_000_000)
        .with_admission(
            S.AdmissionSettings.defaults.with_window(
                Window.defaults.with_policy(Lru.defaults).with_initial_proportion(0.1)
            )
        )
        .with_replacement_policy(Lfu.defaults)
    )
    assert resolved(s) == composite(1_000_000, main=LFU0, window_proportions=(0.1, 0.01, 1.0))


def test_admission_window_optimizer_via_config() -> None:
    s = (
        S.defaults.with_active_entity_limit(1_000_000)
        .with_admission(
            S.AdmissionSettings.defaults.with_window(
                Window.defaults.with_policy(Lru.defaults).with_optimizer(Hill.defaults)
            )
        )
        .with_replacement_policy(Lfu.defaults)
    )
    assert resolved(s) == composite(1_000_000, main=LFU0, optimizer=HILL)


def test_admission_via_config() -> None:
    s = S.default_strategy.with_active_entity_limit(1_000_000)
    assert resolved(s) == composite(1_000_000, main=SLRU, optimizer=HILL, admission_filter=SKETCH)


def test_admission_parameters_tuned_via_config() -> None:
    window = (
        Window.defaults.with_policy(Lru.defaults)
        .with_initial_proportion(0.1)
        .with_minimum_proportion(0.05)
        .with_maximum_proportion(0.50)
        .with_optimizer(Hill(5.0, 0.05, 0.025, 0.95))
    )
    s = (
        S.defaults.with_active_entity_limit(1_000_000)
        .with_admission(
            S.AdmissionSettings.defaults.with_window(window).with_filter(Sketch(3, 8, 2, 50.0))
        )
        .with_replacement_policy(SLRU_SETTINGS)
    )
    assert resolved(s) == composite(
        1_000_000,
        main=SLRU,
        window_proportions=(0.1, 0.05, 0.5),
        optimizer=Composite.HillClimbingAdmissionOptimizer(5.0, 0.05, 0.025, 0.95),
        admission_filter=Composite.FrequencySketchAdmissionFilter(2, 50.0, 3, 8),
    )


def test_admission_via_factory_method() -> None:
    s = (
        S.defaults.with_active_entity_limit(42_000)
        .with_admission(ADMISSION)
        .with_replacement_policy(SLRU_SETTINGS)
    )
    assert resolved(s) == composite(42_000, main=SLRU, optimizer=HILL, admission_filter=SKETCH)


def test_admission_settings_tuned_via_factory_method() -> None:
    window = (
        Window.defaults.with_policy(Lru.defaults)
        .with_initial_proportion(0.1)
        .with_minimum_proportion(0.05)
        .with_maximum_proportion(0.5)
        .with_optimizer(
            Hill.defaults.with_adjust_multiplier(5)
            .with_initial_step(0.05)
            .with_restart_threshold(0.025)
            .with_step_decay(0.95)
        )
    )
    sketch = (
        Sketch.defaults.with_depth(3)
        .with_counter_bits(8)
        .with_width_multiplier(2)
        .with_reset_multiplier(50)
    )
    s = (
        S.defaults.with_active_entity_limit(42_000)
        .with_admission(S.AdmissionSettings.defaults.with_window(window).with_filter(sketch))
        .with_replacement_policy(SLRU_SETTINGS)
    )
    assert resolved(s) == composite(
        42_000,
        main=SLRU,
        window_proportions=(0.1, 0.05, 0.5),
        optimizer=Composite.HillClimbingAdmissionOptimizer(5.0, 0.05, 0.025, 0.95),
        admission_filter=Composite.FrequencySketchAdmissionFilter(2, 50.0, 3, 8),
    )


def test_admission_and_idle_timeout_via_config() -> None:
    s = (
        S.defaults.with_active_entity_limit(1_000_000)
        .with_admission(ADMISSION)
        .with_replacement_policy(SLRU_SETTINGS)
        .with_idle_entity_passivation(30 * MIN)
    )
    assert resolved(s) == composite(
        1_000_000,
        main=SLRU,
        optimizer=HILL,
        admission_filter=SKETCH,
        idle_strategy=idle(1800.0, 900.0),
    )


def test_admission_and_idle_timeout_via_factory_method() -> None:
    s = (
        S.defaults.with_active_entity_limit(42_000)
        .with_admission(ADMISSION)
        .with_replacement_policy(SLRU_SETTINGS)
        .with_idle_entity_passivation(42 * MIN)
    )
    assert resolved(s) == composite(
        42_000,
        main=SLRU,
        optimizer=HILL,
        admission_filter=SKETCH,
        idle_strategy=idle(2520.0, 1260.0),
    )


def test_idle_timeout_of_zero_disables_passivation_via_config() -> None:
    s = S.default_idle_strategy.with_idle_entity_passivation(0.0)
    assert resolved(s) == NoPassivationStrategy()


def test_idle_timeout_of_zero_disables_passivation_via_factory_method() -> None:
    assert resolved(S.defaults.with_idle_entity_passivation(0.0)) == NoPassivationStrategy()


def test_passivation_is_disabled_via_factory_method() -> None:
    s = ClusterShardingSettings().with_no_passivation_strategy()
    assert s.passivation_strategy == NoPassivationStrategy()


def test_proportions_given_as_a_list_are_stored_as_a_tuple() -> None:
    segmented = Lru.defaults.with_segmented([0.2, 0.8]).segmented_settings
    assert segmented == Lru.SegmentedSettings(2, (0.2, 0.8))
    assert Lru.SegmentedSettings(2).with_proportions([0.2, 0.8]).proportions == (0.2, 0.8)
    hash(ClusterShardingSettings().with_passivation_strategy(S.default_strategy))


def test_a_negative_idle_timeout_disables_idle_passivation() -> None:
    assert resolved(S.defaults.with_idle_entity_passivation(-1.0)) == NoPassivationStrategy()


def test_one_segment_level_is_plain_lru() -> None:
    s = S.defaults.with_active_entity_limit(10).with_replacement_policy(
        Lru.defaults.with_segmented(1)
    )
    assert resolved(s) == LeastRecentlyUsedPassivationStrategy(10, (), None)


def test_passivation_strategy_settings_are_exported() -> None:
    assert ohra.PassivationStrategySettings is sharding.PassivationStrategySettings
