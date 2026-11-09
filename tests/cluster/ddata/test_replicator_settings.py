"""Ported from Apache Pekko's ReplicatorSettingsSpec."""

import pytest

from ohra.cluster.core import ClusterSettings
from ohra.cluster.ddata.settings import ReplicatorSettings
from ohra.testkit import ActorTestKit

from ...util import free_port
from ..test_cluster import cluster_node


async def test_have_the_default_replicator_name() -> None:
    async with ActorTestKit() as kit:
        assert ReplicatorSettings.name_for(kit.system, None) == "ddataReplicator"


async def test_have_the_prefixed_replicator_name() -> None:
    async with ActorTestKit() as kit:
        assert ReplicatorSettings.name_for(kit.system, "other") == "otherDdataReplicator"


async def test_have_the_configured_replicator_name() -> None:
    port = free_port()
    settings = ReplicatorSettings(name="fooReplicator")
    async with cluster_node("ddata", port, [port], distributed_data=settings) as system:
        assert ReplicatorSettings.name_for(system, None) == "fooReplicator"
        assert ReplicatorSettings.name_for(system, "other") == "otherFooReplicator"


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


@pytest.mark.parametrize("expiry", [{"key-1": -1.0}, {"cache-*": -0.5}])
def test_reject_negative_expiry(expiry: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        ReplicatorSettings(expire_keys_after_inactivity=expiry)


def test_zero_expiry_means_never() -> None:
    assert ReplicatorSettings(expire_keys_after_inactivity={"key-1": 0.0})


@pytest.mark.parametrize("size", [0, -1])
def test_reject_non_positive_log_data_size_exceeding(size: int) -> None:
    with pytest.raises(ValueError):
        ReplicatorSettings(log_data_size_exceeding=size)


def test_log_data_size_exceeding_can_be_off() -> None:
    assert ReplicatorSettings(log_data_size_exceeding=None).log_data_size_exceeding is None


def test_have_the_pruning_defaults() -> None:
    s = ReplicatorSettings()
    assert s.pruning_interval == 120.0
    assert s.max_pruning_dissemination == 300.0
    assert s.pruning_marker_time_to_live == 21600.0


@pytest.mark.parametrize("kw", ["max_pruning_dissemination", "pruning_marker_time_to_live"])
def test_reject_negative_pruning_durations(kw: str) -> None:
    with pytest.raises(ValueError):
        ReplicatorSettings(**{kw: -1.0})
