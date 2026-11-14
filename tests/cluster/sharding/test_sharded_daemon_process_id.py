"""ShardedDaemonProcessIdSpec and ShardedDaemonProcessStateSpec from Apache Pekko's
cluster-sharding-typed, plus the settings and the state on the wire."""

from __future__ import annotations

import json

import pytest

from ohra.cluster.sharding.sharded_daemon_process import (
    DecodedId,
    ShardedDaemonProcessIdExtractor,
    ShardedDaemonProcessSettings,
    ShardedDaemonProcessState,
    ShardedDaemonProcessStateKey,
    all_shards_for,
    decode_entity_id,
    sorted_identities_for,
)
from ohra.remote.serialization import Serialization


def _never():
    raise AssertionError("resolver called")


def _round_trip(v: object) -> object:
    data = Serialization.encode(v, lambda r: r.path)
    return Serialization.decode(json.loads(json.dumps(data)), lambda _p: _never())


def test_decode_single_int_in_id_as_id_for_non_rescaling():
    assert decode_entity_id("1", 8) == DecodedId(0, 8, 1)


def test_decode_composed_id_as_id_for_rescaling():
    assert decode_entity_id("1|4|3", 8) == DecodedId(1, 4, 3)


def test_decode_rejects_a_two_part_id():
    with pytest.raises(ValueError, match="Unexpected id for sharded daemon process: '1\\|2'"):
        decode_entity_id("1|2", 8)


def test_encode_initial_revision_with_old_scheme():
    assert DecodedId(0, 8, 4).encode_entity_id() == "4"


def test_encode_non_0_revisions_with_new_scheme():
    assert DecodedId(1, 8, 4).encode_entity_id() == "1|8|4"


def test_spread_out_workers_over_shards_initial_revision():
    assert len(all_shards_for(0, 8)) == 8


def test_spread_out_workers_over_shards_later_revision():
    assert len(all_shards_for(1, 8)) == 8


def test_extract_new_id_as_shard_in_message_extractor():
    assert ShardedDaemonProcessIdExtractor[str]().shard_id("1|8|4") == "4"


def test_extract_old_id_as_shard_in_message_extractor():
    assert ShardedDaemonProcessIdExtractor[str]().shard_id("1") == "1"


def test_sorted_identities_sort_as_strings():
    assert sorted_identities_for(0, 12)[:4] == ["0", "1", "10", "11"]


def test_merge_highest_revision():
    a = ShardedDaemonProcessState(1, 2, True, 0.0)
    b = ShardedDaemonProcessState(2, 3, False, 0.0)
    assert a.merge(b) is b
    assert b.merge(a) is b


def test_merge_same_revision_completion():
    started = ShardedDaemonProcessState(1, 2, False, 0.0)
    done = started.complete_scaling()
    assert started.merge(done) is done
    assert done.merge(started) is done


def test_the_state_crosses_the_wire():
    state = ShardedDaemonProcessState.initial_state(4).start_scaling_to(6)
    key = ShardedDaemonProcessStateKey("sdp")
    assert _round_trip(state) == state
    assert _round_trip(key) == key
    assert Serialization.encode(state, lambda r: r.path)["$type"] == "ShardedDaemonProcess.State"


def test_the_settings_defaults_are_pekkos():
    s = ShardedDaemonProcessSettings()
    assert (
        s.keep_alive_interval,
        s.sharding_settings,
        s.role,
        s.keep_alive_from_number_of_nodes,
        s.keep_alive_throttle_interval,
    ) == (10.0, None, None, 3, 0.1)


def test_an_empty_role_means_none():
    assert ShardedDaemonProcessSettings().with_role("").role is None
