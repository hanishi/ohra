"""Ported from Pekko's ``TimeoutSettingsSpec.scala`` (coordination/src/test).

Cases, with their Scala lines: default heartbeat interval (29), minimum of 5 s (37), overriding
(45), interval not at or above half the timeout (53).

Difference: ``TimeoutSettings.create(...)``'s arguments stand for the config, and ``None`` for
``heartbeat-interval=""``. The ohra-only tests cover the defaults, the ``with_*`` copies and the
repr of ``LeaseSettings``.
"""

import pytest

from ohra.coordination.lease import LeaseSettings, TimeoutSettings


def test_default_heartbeat_interval_to_heartbeat_timeout_over_10() -> None:
    assert TimeoutSettings.create(100.0, None, 5.0).heartbeat_interval == 10.0


def test_have_a_min_of_5s_for_heartbeat_interval() -> None:
    assert TimeoutSettings.create(40.0, None, 5.0).heartbeat_interval == 5.0


def test_allow_overriding_of_heartbeat_interval() -> None:
    assert TimeoutSettings.create(100.0, 20.0, 5.0).heartbeat_interval == 20.0


def test_not_allow_interval_to_be_greater_or_equal_to_half_the_interval() -> None:
    with pytest.raises(
        ValueError, match="^heartbeat-interval must be less than half heartbeat-timeout$"
    ):
        TimeoutSettings.create(100.0, 50.0, 5.0)


def test_defaults_are_reference_conf() -> None:
    assert TimeoutSettings() == TimeoutSettings(12.0, 120.0, 5.0)
    assert TimeoutSettings.create() == TimeoutSettings()


def test_with_copies_change_one_field() -> None:
    base = TimeoutSettings()
    assert base.with_heartbeat_interval(1.0) == TimeoutSettings(1.0, 120.0, 5.0)
    assert base.with_heartbeat_timeout(2.0) == TimeoutSettings(12.0, 2.0, 5.0)
    assert base.with_operation_timeout(3.0) == TimeoutSettings(12.0, 120.0, 3.0)
    settings = LeaseSettings("a", "o", base, {})
    assert settings.with_timeout_settings(base.with_operation_timeout(3.0)).timeout_settings == (
        TimeoutSettings(12.0, 120.0, 3.0)
    )


def test_lease_settings_repr_leaves_out_the_config() -> None:
    assert "secret" not in repr(LeaseSettings("a", "o", TimeoutSettings(), {"k": "secret"}))
