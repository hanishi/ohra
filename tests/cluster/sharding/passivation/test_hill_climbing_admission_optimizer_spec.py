"""Ported from Apache Pekko's HillClimbingAdmissionOptimizerSpec (cluster-sharding/src/test/scala/
org/apache/pekko/cluster/sharding/passivation/HillClimbingAdmissionOptimizerSpec.scala). The
cases:

- start in decreasing direction (33)
- only adjust every 'adjust size' accesses (39)
- decay each step when active rate is under restart threshold (48)
- restart when active rate is over restart threshold (58)
- change direction when active rate drops (72)

``create`` keeps Scala's defaults (initial limit 5, adjust multiplier 2.0, initial step 0.01,
restart threshold 0.05, step decay 0.98), and the adjustments are compared with ``==``, as
Scala's ``shouldBe``.

Then tests of ohra's own, that the optimizer computes as Scala's doubles do: with no accesses and
an adjust size of 0 the active rate is NaN (Scala's ``0.0 / 0``), and an adjust size of NaN
never adjusts (Scala's ``accesses >= adjustSize`` is false).

Differences from the Scala: none.
"""

import math

from ohra.cluster.sharding.passivation import HillClimbingAdmissionOptimizer


def create(
    initial_limit: int = 5,
    adjust_multiplier: float = 2.0,
    initial_step: float = 0.01,
    restart_threshold: float = 0.05,
    step_decay: float = 0.98,
) -> HillClimbingAdmissionOptimizer:
    return HillClimbingAdmissionOptimizer(
        initial_limit, adjust_multiplier, initial_step, restart_threshold, step_decay
    )


def test_start_in_decreasing_direction() -> None:
    optimizer = create(initial_step=0.123)
    for _ in range(10):
        optimizer.record_passive()
    assert optimizer.calculate_adjustment() == -0.123


def test_only_adjust_every_adjust_size_accesses() -> None:
    optimizer = create()
    for i in range(1, 101):
        optimizer.record_passive()
        if i % 10 == 0:
            assert optimizer.calculate_adjustment() != 0.0
        else:
            assert optimizer.calculate_adjustment() == 0.0


def test_decay_each_step_when_active_rate_is_under_restart_threshold() -> None:
    step = 0.1
    decay = 0.5
    optimizer = create(initial_step=step, step_decay=decay)
    for i in range(1, 101):
        optimizer.record_passive()
        if i % 10 == 0:
            assert optimizer.calculate_adjustment() == decay ** (i / 10 - 1) * -step


def test_restart_when_active_rate_is_over_restart_threshold() -> None:
    step = 0.1
    decay = 0.5
    optimizer = create(initial_step=step, step_decay=decay)
    for i in range(1, 101):
        # increase (and maintain) active rate after 40 accesses to trigger restart at 60 then
        # decay again
        if i > 40:
            optimizer.record_active()
        else:
            optimizer.record_passive()
        if i % 10 == 0:
            shift = 1 if i < 60 else 6
            assert optimizer.calculate_adjustment() == decay ** (i / 10 - shift) * -step


def test_change_direction_when_active_rate_drops() -> None:
    optimizer = create()
    for i in range(1, 501):
        # decrease active rate every 50 accesses to switch direction
        active_rate = max(1, 10 - (i // 50))
        if i % 10 < active_rate:
            optimizer.record_active()
        else:
            optimizer.record_passive()
        if i % 10 == 0:
            # at 10-50 should be negative direction, at 60-100 should be positive direction, and
            # so on
            adjustment = optimizer.calculate_adjustment()
            if ((i - 1) // 50) % 2 == 0:
                assert adjustment < 0.0
            else:
                assert adjustment > 0.0


def test_no_accesses_at_an_adjust_size_of_0_give_an_active_rate_of_nan() -> None:
    optimizer = create(initial_limit=0)
    # NaN active rate: delta >= 0 is false, so the step flips, and no restart
    assert optimizer.calculate_adjustment() == 0.01
    optimizer.record_passive()
    # the previous rate is NaN, so delta is NaN again and the decayed step flips back
    assert optimizer.calculate_adjustment() == -(0.01 * 0.98)
    optimizer.record_passive()
    # active rate 0.0 after 0.0: delta 0.0 keeps the direction
    assert optimizer.calculate_adjustment() == -(0.01 * 0.98) * 0.98


def test_an_adjust_size_of_nan_never_adjusts() -> None:
    optimizer = create(adjust_multiplier=math.nan, initial_step=0.1)
    for _ in range(10):
        optimizer.record_passive()
        assert optimizer.calculate_adjustment() == 0.0
