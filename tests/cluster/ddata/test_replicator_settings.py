"""Ported from Apache Pekko's ReplicatorSettingsSpec."""

import pytest

from ohra.cluster.core import ClusterSettings
from ohra.cluster.ddata.settings import ReplicatorSettings


def test_have_the_default_replicator_name() -> None:
    assert ReplicatorSettings.name_for(None, None) == "ddataReplicator"  # type: ignore[arg-type]


def test_have_the_prefixed_replicator_name() -> None:
    assert ReplicatorSettings.name_for(None, "other") == "otherDdataReplicator"  # type: ignore[arg-type]


def test_expire_keys_after_inactivity() -> None:
    s = ReplicatorSettings(expire_keys_after_inactivity={"key-1": 600.0, "cache-*": 120.0})
    assert len(s.expire_keys_after_inactivity) == 2
    assert s.expire_keys_after_inactivity["key-1"] == 600.0
    assert s.expire_keys_after_inactivity["cache-*"] == 120.0


def test_have_empty_expiry_keys_by_default() -> None:
    assert len(ReplicatorSettings().expire_keys_after_inactivity) == 0


def test_cluster_settings_carry_replicator_settings() -> None:
    assert ClusterSettings().distributed_data == ReplicatorSettings()


@pytest.mark.parametrize(
    "kw", ["gossip_interval", "notify_subscribers_interval", "max_delta_elements"]
)
def test_reject_non_positive(kw: str) -> None:
    with pytest.raises(ValueError):
        ReplicatorSettings(**{kw: 0})
