"""The pure parts of cluster sharding: LeastShardAllocationStrategy and the coordinator's state, as
LeastShardAllocationStrategy.scala and ShardCoordinator.State."""

from __future__ import annotations

import pytest

from pekko_py.cluster.core import CurrentClusterState
from pekko_py.cluster.gossip import Member, MemberStatus
from pekko_py.cluster.sharding import (
    CoordinatorState,
    LeastShardAllocationStrategy,
    ShardHomeAllocated,
    ShardHomeDeallocated,
    ShardRegionRegistered,
    ShardRegionTerminated,
)
from pekko_py.remote import Address, UniqueAddress

A = UniqueAddress("127.0.0.1", 1, 11, "c")
B = UniqueAddress("127.0.0.1", 2, 22, "c")
C = UniqueAddress("127.0.0.1", 3, 33, "c")
RA, RB, RC = (f"pekko://c@127.0.0.1:{p}/system/sharding/T" for p in (1, 2, 3))


def member(node: UniqueAddress, up_number: int, status: MemberStatus = "up") -> Member:
    return Member(node, up_number, status)


MEMBERS = {RA: member(A, 1), RB: member(B, 2), RC: member(C, 3)}


def cluster_state(
    members: tuple[Member, ...] = tuple(MEMBERS.values()),
    unreachable: frozenset[Member] = frozenset(),
) -> CurrentClusterState:
    return CurrentClusterState(members, unreachable, frozenset(), Address("c", "127.0.0.1", 1))


def shards(*ids: int) -> tuple[str, ...]:
    return tuple(str(i) for i in ids)


def test_allocate_picks_the_region_with_fewest_shards_oldest_on_ties():
    strategy = LeastShardAllocationStrategy(absolute_limit=0, relative_limit=0.1)
    assert (
        strategy.allocate_shard("9", {RA: shards(1, 2), RB: shards(3), RC: shards(4, 5)}, MEMBERS)
        == RB
    )
    assert (
        strategy.allocate_shard("9", {RA: shards(1), RB: shards(3), RC: shards()}, MEMBERS) == RC
    )
    # the oldest on a tie
    assert strategy.allocate_shard("9", {RA: (), RB: (), RC: ()}, MEMBERS) == RA
    assert strategy.allocate_shard("9", {}, MEMBERS) is None


def test_rebalance_phase_one_moves_shards_from_regions_above_optimal():
    strategy = LeastShardAllocationStrategy(absolute_limit=0, relative_limit=0.1)
    # 6 shards, optimal 2 per region
    allocations = {RA: shards(1, 2, 3, 4, 5), RB: shards(6), RC: ()}
    moved = strategy.rebalance(allocations, MEMBERS, frozenset(), cluster_state())
    # limit(6) = max(1, int(0.1 * 6)) = 1
    assert len(moved) == 1 and moved <= {"1", "2", "3", "4", "5"}


def test_rebalance_phase_two_fills_regions_below_optimal():
    strategy = LeastShardAllocationStrategy(absolute_limit=0, relative_limit=1.0)
    # 4 shards, optimal 2: phase one has nothing
    allocations = {RA: shards(1, 2), RB: shards(3, 4), RC: ()}
    moved = strategy.rebalance(allocations, MEMBERS, frozenset(), cluster_state())
    # countBelowOptimal = (optimal - 1) - 0 = 1: one shard from a full region
    assert moved == {"1"} or moved == {"3"}


def test_rebalance_limits():
    allocations = {RA: shards(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), RB: (), RC: ()}
    assert (
        len(
            LeastShardAllocationStrategy(absolute_limit=2, relative_limit=1.0).rebalance(
                allocations, MEMBERS, frozenset(), cluster_state()
            )
        )
        == 2
    )
    assert (
        len(
            LeastShardAllocationStrategy(absolute_limit=0, relative_limit=0.5).rebalance(
                allocations, MEMBERS, frozenset(), cluster_state()
            )
        )
        == 5
    )
    # down to optimal 4 = 10 // 3 + 1
    assert (
        len(
            LeastShardAllocationStrategy(absolute_limit=0, relative_limit=1.0).rebalance(
                allocations, MEMBERS, frozenset(), cluster_state()
            )
        )
        == 6
    )


def test_rebalance_is_skipped_while_in_progress_or_not_a_good_time():
    strategy = LeastShardAllocationStrategy(absolute_limit=0, relative_limit=1.0)
    allocations = {RA: shards(1, 2, 3, 4), RB: (), RC: ()}
    assert (
        strategy.rebalance(allocations, MEMBERS, frozenset({"1"}), cluster_state()) == frozenset()
    )
    assert (
        strategy.rebalance(
            allocations, MEMBERS, frozenset(), cluster_state(unreachable=frozenset({member(C, 3)}))
        )
        == frozenset()
    )
    joining = (member(A, 1), member(B, 2), Member(C, 2**31 - 1, "joining"))
    assert (
        strategy.rebalance(allocations, MEMBERS, frozenset(), cluster_state(members=joining))
        == frozenset()
    )
    assert strategy.rebalance(allocations, MEMBERS, frozenset(), cluster_state()) != frozenset()


def test_coordinator_state_events():
    s = CoordinatorState().updated(ShardRegionRegistered(RA)).updated(ShardRegionRegistered(RB))
    s = s.updated(ShardHomeAllocated("1", RA)).updated(ShardHomeAllocated("2", RB))
    assert s.shards == {"1": RA, "2": RB} and s.regions == {RA: ("1",), RB: ("2",)}
    s = s.updated(ShardHomeDeallocated("1"))
    assert s.shards == {"2": RB} and s.regions[RA] == ()
    s = s.updated(ShardRegionTerminated(RB))
    assert s.shards == {} and RB not in s.regions
    with pytest.raises(ValueError):
        s.updated(ShardRegionRegistered(RA))  # already registered
    with pytest.raises(ValueError):
        s.updated(ShardHomeAllocated("3", RC))  # unknown region
    with pytest.raises(ValueError):
        s.updated(ShardHomeDeallocated("3"))  # unknown shard
    with pytest.raises(ValueError):
        s.updated(ShardRegionTerminated(RC))
