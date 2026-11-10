"""ClusterReceptionistSettings, after Pekko's ClusterReceptionistSettings.scala and the
pekko.cluster.typed.receptionist block of reference.conf."""

from dataclasses import replace

import pytest

from ohra import ClusterReceptionistSettings, ClusterSettings, Replicator, ReplicatorSettings


def test_defaults():
    s = ClusterReceptionistSettings()
    assert s.write_consistency == Replicator.WriteLocal()
    assert (s.pruning_interval, s.prune_removed_older_than, s.distributed_key_count) == (
        3.0,
        60.0,
        5,
    )
    assert s.replicator_settings is None
    assert ClusterSettings().receptionist == s


@pytest.mark.parametrize(
    "field",
    [{"pruning_interval": 0}, {"prune_removed_older_than": -1}, {"distributed_key_count": 0}],
)
def test_invalid_values_are_rejected(field: dict[str, float]):
    with pytest.raises(ValueError):
        ClusterReceptionistSettings(**field)  # pyright: ignore[reportArgumentType]


def test_the_replicator_settings_are_the_clusters_without_durable_keys():
    base = ReplicatorSettings(durable_keys=frozenset({"*"}), gossip_interval=0.3)
    got = ClusterReceptionistSettings().effective_replicator_settings(
        ClusterSettings(distributed_data=base)
    )
    assert got == replace(base, durable_keys=frozenset())


def test_explicit_replicator_settings_win():
    mine = ReplicatorSettings(name="mine", durable_keys=frozenset({"a"}))
    got = ClusterReceptionistSettings(replicator_settings=mine).effective_replicator_settings(
        ClusterSettings(distributed_data=ReplicatorSettings(gossip_interval=0.3))
    )
    assert got == replace(mine, durable_keys=frozenset())
