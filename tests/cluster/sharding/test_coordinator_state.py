"""Not a Pekko spec: Pekko has no spec of its own for ShardCoordinator.State
(ShardCoordinator.scala ~455-540). These tests cover its checks, plus what ohra adds: the
state travels as the value of an LWWRegister, and each region's entry records its node (D5)."""

import json
from typing import Any
from dataclasses import replace

import pytest

from ohra.cluster.ddata.lww_register import LWWRegister
from ohra.cluster.sharding.coordinator import (
    CoordinatorState,
    ShardHomeAllocated,
    ShardHomeDeallocated,
    ShardRegionProxyRegistered,
    ShardRegionProxyTerminated,
    ShardRegionRegistered,
    ShardRegionTerminated,
)
from ohra.ref import ActorRef
from ohra.remote import UniqueAddress
from ohra.remote.serialization import Serialization

n1 = UniqueAddress("h1", 2551, 1, "sys")
n2 = UniqueAddress("h2", 2552, 2, "sys")
A, B = "/user/region-a", "/user/region-b"


def _never(*_a: object) -> object:
    raise AssertionError("resolver called")


def _two_regions() -> CoordinatorState:
    s = CoordinatorState.EMPTY
    s = s.updated(ShardRegionRegistered(A, n1)).updated(ShardRegionRegistered(B, n2))
    for shard, region in (("s1", A), ("s2", A), ("s3", B)):
        s = s.updated(ShardHomeAllocated(shard, region))
    return s


def test_registered_records_the_regions_node():
    s = CoordinatorState.EMPTY.updated(ShardRegionRegistered(A, n1))
    assert s.regions == {A: ()}
    assert s.region_nodes == {A: n1}


def test_terminated_drops_the_regions_node():
    s = _two_regions().updated(ShardRegionTerminated(A))
    assert s.region_nodes == {B: n2}
    assert s.shards == {"s3": B}


def test_the_state_round_trips_on_the_wire():
    reg = LWWRegister._create(n1, _two_regions(), lambda _t, _v: 1)
    wire = json.loads(json.dumps(Serialization.encode(reg, lambda r: r.path)))
    back = Serialization.decode(wire, _never)
    assert back == reg
    assert isinstance(back, LWWRegister)
    assert back.value.region_nodes == {A: n1, B: n2}


def test_updated_keeps_pekkos_checks():
    s = _two_regions()
    with pytest.raises(ValueError):
        s.updated(ShardRegionRegistered(A, n1))
    with pytest.raises(ValueError):
        s.updated(ShardRegionTerminated("/user/none"))
    with pytest.raises(ValueError):
        s.updated(ShardHomeAllocated("s1", B))
    with pytest.raises(ValueError):
        s.updated(ShardHomeAllocated("s9", "/user/none"))
    with pytest.raises(ValueError):
        s.updated(ShardHomeDeallocated("s9"))


def _remember() -> CoordinatorState:
    return _two_regions().with_remember_entities(True)


def test_with_remember_entities_off_clears_the_unallocated_shards():
    s = replace(_remember(), unallocated_shards=frozenset({"1"}))
    assert s.with_remember_entities(False).unallocated_shards == frozenset()
    assert not s.with_remember_entities(False).remember_entities
    assert s.with_remember_entities(True).unallocated_shards == frozenset({"1"})


def test_a_terminated_regions_shards_become_unallocated_with_remember_entities():
    after = _remember().updated(ShardRegionTerminated(A))
    assert after.unallocated_shards == frozenset({"s1", "s2"})
    assert after.shards == {"s3": B}
    assert after.remember_entities


def test_an_allocated_shard_is_no_longer_unallocated():
    s = replace(_remember(), unallocated_shards=frozenset({"s9"}))
    after = s.updated(ShardHomeAllocated("s9", B))
    assert after.unallocated_shards == frozenset()
    assert after.remember_entities


def test_a_deallocated_shard_becomes_unallocated_with_remember_entities():
    after = _remember().updated(ShardHomeDeallocated("s3"))
    assert after.unallocated_shards == frozenset({"s3"})
    assert after.remember_entities


def test_without_remember_entities_nothing_becomes_unallocated():
    s = _two_regions()
    assert s.updated(ShardHomeDeallocated("s3")).unallocated_shards == frozenset()
    assert s.updated(ShardRegionTerminated(A)).unallocated_shards == frozenset()
    s = s.updated(ShardHomeDeallocated("s3")).updated(ShardHomeAllocated("s3", A))
    assert s.unallocated_shards == frozenset()


def test_all_shards_includes_the_unallocated_ones():
    s = replace(_two_regions(), unallocated_shards=frozenset({"s9"}))
    assert s.all_shards == frozenset({"s1", "s2", "s3", "s9"})


def test_is_empty_ignores_the_unallocated_shards():
    assert replace(CoordinatorState.EMPTY, unallocated_shards=frozenset({"1"})).is_empty
    assert not _two_regions().is_empty


def test_the_remember_fields_survive_serialization():
    state = replace(_remember(), unallocated_shards=frozenset({"1", "2"}))
    reg = LWWRegister._create(n1, state, lambda _t, _v: 1)
    wire = json.loads(json.dumps(Serialization.encode(reg, lambda r: r.path)))
    back = Serialization.decode(wire, _never)
    assert back == reg
    assert isinstance(back, LWWRegister)
    assert back.value.unallocated_shards == frozenset({"1", "2"})
    assert back.value.remember_entities


# --- region proxies (ShardCoordinator.scala 466-500) ---------------------------------------------

P = "/user/proxy"


def test_a_registered_proxy_makes_the_state_non_empty():
    s = CoordinatorState.EMPTY.updated(ShardRegionProxyRegistered(P, n1))
    assert s.region_proxies == {P: n1}
    assert not s.is_empty
    assert s.regions == {}


def test_a_proxy_registered_twice_is_refused_with_pekkos_text():
    s = CoordinatorState.EMPTY.updated(ShardRegionProxyRegistered(P, n1))
    with pytest.raises(ValueError, match=f"Region proxy {P} already registered"):
        s.updated(ShardRegionProxyRegistered(P, n1))


def test_an_unknown_terminated_proxy_is_refused_with_pekkos_text():
    with pytest.raises(ValueError, match=f"Terminated region proxy {P} not registered"):
        CoordinatorState.EMPTY.updated(ShardRegionProxyTerminated(P))


def test_a_terminated_proxy_leaves_the_regions_and_shards_alone():
    s = _two_regions().updated(ShardRegionProxyRegistered(P, n1))
    after = s.updated(ShardRegionProxyTerminated(P))
    assert after.region_proxies == {}
    assert after == _two_regions()


def test_a_state_with_proxies_round_trips_on_the_wire():
    state = _two_regions().updated(ShardRegionProxyRegistered(P, n2))
    reg = LWWRegister._create(n1, state, lambda _t, _v: 1)  # pyright: ignore[reportPrivateUsage]
    wire = json.loads(json.dumps(Serialization.encode(reg, lambda r: r.path)))
    back = Serialization.decode(wire, _no_resolve)
    assert back == reg


def _no_resolve(_path: str) -> ActorRef[Any]:
    raise AssertionError("resolver called")
