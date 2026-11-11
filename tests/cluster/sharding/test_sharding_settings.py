"""ClusterShardingSettings' coordinator-state settings, and the Replicator sharding spawns
(ClusterSharding.scala 730-815, reference.conf coordinator-state and distributed-data)."""

import logging

import pytest

from ohra.cluster import ClusterSettings
from ohra.cluster.ddata import Replicator, ReplicatorSettings
from ohra.cluster.sharding import (
    ClusterSharding,
    ClusterShardingSettings,
    Entity,
    EntityTypeKey,
)
from ohra.behavior import Behaviors

from ...util import eventually, free_port
from ..test_cluster import all_up, cluster_node


def test_defaults_are_pekkos():
    s = ClusterShardingSettings()
    assert s.waiting_for_state_timeout == 2.0
    assert s.updating_state_timeout == 5.0
    assert s.coordinator_state_read_majority_plus == 5
    assert s.coordinator_state_write_majority_plus == 3
    assert s.majority_min_cap == 5
    assert s.distributed_data is None


def test_read_consistency_is_majority_plus():
    assert ClusterShardingSettings().state_read_consistency() == Replicator.ReadMajorityPlus(
        2.0, 5, 5
    )


def test_write_consistency_is_majority_plus():
    assert ClusterShardingSettings().state_write_consistency() == Replicator.WriteMajorityPlus(
        5.0, 3, 5
    )


def test_all_maps_to_read_all_and_write_all():
    s = ClusterShardingSettings(
        coordinator_state_read_majority_plus="all", coordinator_state_write_majority_plus="all"
    )
    assert s.state_read_consistency() == Replicator.ReadAll(2.0)
    assert s.state_write_consistency() == Replicator.WriteAll(5.0)


@pytest.mark.parametrize(
    "bad",
    [
        {"waiting_for_state_timeout": 0},
        {"updating_state_timeout": -1},
        {"coordinator_state_read_majority_plus": -1},
        {"coordinator_state_write_majority_plus": -1},
        {"majority_min_cap": -1},
    ],
)
def test_invalid_values_raise(bad: dict[str, int]):
    with pytest.raises(ValueError):
        ClusterShardingSettings(**bad)  # pyright: ignore[reportArgumentType]


def test_replicator_settings_default_to_the_clusters_with_pekkos_overrides():
    cluster = ClusterSettings(distributed_data=ReplicatorSettings(gossip_interval=0.3))
    eff = ClusterShardingSettings().effective_replicator_settings(cluster)
    assert eff.gossip_interval == 0.3
    assert eff.prefer_oldest is True
    assert eff.max_delta_elements == 5
    assert eff.durable_keys == frozenset()


def test_durable_keys_are_forced_empty():
    s = ClusterShardingSettings(distributed_data=ReplicatorSettings(durable_keys=frozenset({"*"})))
    assert s.effective_replicator_settings(ClusterSettings()).durable_keys == frozenset()


A = EntityTypeKey[str]("A")
B = EntityTypeKey[str]("B")


def entity(key: EntityTypeKey[str], settings: ClusterShardingSettings | None = None):
    return Entity(key, lambda _ctx: Behaviors.ignore).with_settings(
        settings or ClusterShardingSettings(number_of_shards=2)
    )


async def test_the_first_init_spawns_the_sharding_replicator():
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        sharding = ClusterSharding(system)
        sharding.init(entity(A))
        sharding.init(entity(B))
        assert sharding._replicator is not None  # pyright: ignore[reportPrivateUsage]
        root = f"ohra://c@127.0.0.1:{port}"
        found = system.resolve(root + "/system/sharding/replicator")
        assert found is sharding._replicator  # pyright: ignore[reportPrivateUsage]
        parent = sharding._parent  # pyright: ignore[reportPrivateUsage]
        assert parent is not None
        assert list(parent.children).count("replicator") == 1


async def test_a_later_init_with_other_replicator_settings_warns(
    caplog: pytest.LogCaptureFixture,
):
    port = free_port()
    async with cluster_node("c", port, [port]) as system:
        await eventually(lambda: all_up(system, 1), timeout=5)
        sharding = ClusterSharding(system)
        sharding.init(entity(A))
        other = ClusterShardingSettings(
            number_of_shards=2, distributed_data=ReplicatorSettings(gossip_interval=0.7)
        )
        with caplog.at_level(logging.WARNING):
            sharding.init(entity(B, other))
        assert any(r.levelno == logging.WARNING and "B" in r.getMessage() for r in caplog.records)
