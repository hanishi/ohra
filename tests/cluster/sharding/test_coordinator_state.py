"""Not a Pekko spec: Pekko has no spec of its own for ShardCoordinator.State
(ShardCoordinator.scala ~455-540). These tests cover its checks, plus what ohra adds: the
state travels as the value of an LWWRegister, and each region's entry records its node (D5)."""

import json

import pytest

from ohra.cluster.ddata.lww_register import LWWRegister
from ohra.cluster.sharding.coordinator import (
    CoordinatorState,
    ShardHomeAllocated,
    ShardHomeDeallocated,
    ShardRegionRegistered,
    ShardRegionTerminated,
)
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
