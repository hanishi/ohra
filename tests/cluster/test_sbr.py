"""The split-brain resolver: settings, Pekko's DowningStrategy and its four strategies, and the resolver driven by
a manual clock, as Pekko's SplitBrainResolverSpec."""
from __future__ import annotations

import logging
from typing import Any

import pytest

from pekko_py.cluster.core import ClusterSettings
from pekko_py.cluster.gossip import UP_NUMBER_UNSET, Member
from pekko_py.cluster.reachability import Reachability, Record
from pekko_py.cluster.sbr import (
    DownAll,
    DownAllDecision,
    DownIndirectlyConnected,
    DowningStrategy,
    DownReachable,
    DownUnreachable,
    KeepMajority,
    KeepOldest,
    SplitBrainResolverSettings,
    StaticQuorum,
    _DoubleDecision,  # pyright: ignore[reportPrivateUsage]
    _DowningStrategy,  # pyright: ignore[reportPrivateUsage]
)
from pekko_py.remote import UniqueAddress

# --- fixtures (TestAddresses.scala and StrategySetup) ------------------------------------------------------------


def member(host: str, up_number: int) -> Member:
    return Member(UniqueAddress(host, 7355, 0, "sys"), up_number, "up")


A, B, C, D, E, F, G = (member(h, n) for h, n in zip("abcdefg", (5, 4, 3, 2, 1, 5, 6), strict=True))  # E is the oldest


def joining(m: Member) -> Member:
    return Member(m.unique_address, UP_NUMBER_UNSET, "joining")


def leaving(m: Member) -> Member:
    return m.with_status("leaving")


def exiting(m: Member) -> Member:
    return leaving(m).with_status("exiting")


def downed(m: Member) -> Member:
    return m.with_status("down")


def nodes(*ms: Member) -> frozenset[UniqueAddress]:
    return frozenset(m.unique_address for m in ms)


def reachability(pairs: list[tuple[Member, Member]]) -> Reachability:
    return Reachability(
        tuple(Record(f.unique_address, t.unique_address, "unreachable", 1) for f, t in pairs),
        tuple((f.unique_address, 1) for f, _ in pairs),
    )


def strategy(
    config: DowningStrategy, side: set[Member], *others: set[Member], indirectly: list[tuple[Member, Member]] | None = None
) -> _DowningStrategy:
    """StrategySetup.strategy: this side sees every other side as unreachable, observed by its lowest member."""
    s = _DowningStrategy(A.unique_address, config)
    for m in side.union(*others):
        s.add(m)
    other: set[Member] = set().union(*others)
    head = min(side)
    pairs = list(dict.fromkeys((indirectly or []) + [(head, o) for o in sorted(other)]))
    s.set_reachability(reachability(pairs))
    for _, to in pairs:
        s.add_unreachable(to)
    s.set_seen_by(frozenset(m.address for m in side))
    return s


# --- settings ----------------------------------------------------------------------------------------------------


def test_settings_defaults_and_validation():
    s = SplitBrainResolverSettings()
    assert s.strategy == KeepMajority() and s.stable_after == 20.0 and s.down_all_when_unstable is None
    assert s.effective_down_all_when_unstable == 15.0 and s.tick_interval == 1.0
    assert SplitBrainResolverSettings(stable_after=2.0).effective_down_all_when_unstable == 4.0
    assert SplitBrainResolverSettings(stable_after=0.1).tick_interval == 0.025
    assert SplitBrainResolverSettings(down_all_when_unstable=0).effective_down_all_when_unstable == 0
    with pytest.raises(ValueError):
        SplitBrainResolverSettings(stable_after=0)
    with pytest.raises(ValueError):
        SplitBrainResolverSettings(down_all_when_unstable=-1)
    with pytest.raises(ValueError):
        StaticQuorum(0)


def test_effective_down_removal_margin():
    assert ClusterSettings().effective_down_removal_margin == 0.0
    assert ClusterSettings(downing=SplitBrainResolverSettings(stable_after=7.0)).effective_down_removal_margin == 7.0
    assert ClusterSettings(downing=SplitBrainResolverSettings(stable_after=7.0), down_removal_margin=3.0).effective_down_removal_margin == 3.0
    with pytest.raises(ValueError):
        ClusterSettings(down_removal_margin=-1)


# --- the DowningStrategy base ------------------------------------------------------------------------------------


def test_add_and_remove_members_with_address_order():
    s = _DowningStrategy(A.unique_address, KeepMajority())
    for m in (C, A, B):
        s.add(m)
    assert s.all_members == [A, B, C]
    s.add(joining(D))
    assert s.all_members == [A, B, C, joining(D)]
    s.add(D)
    assert s.all_members == [A, B, C, D] and s.all_members[3].status == "up"
    s.remove(B)
    assert s.all_members == [A, C, D]


def test_add_and_remove_members_with_age_order():
    s = _DowningStrategy(A.unique_address, KeepOldest())
    for m in (A, C, E, joining(B)):
        s.add(m)
    assert s.all_members == [E, C, A, joining(B)]
    s.add(B)
    assert s.all_members == [E, C, B, A]  # up numbers E=1, C=3, B=4, A=5


def test_collect_and_filter_members():
    s = _DowningStrategy(A.unique_address, KeepMajority())
    for m in (A, joining(B), C, leaving(D), exiting(E), downed(F)):
        s.add(m)
    s.add_unreachable(C)
    assert s.members() == [A, C, leaving(D)]
    assert s.members(including_possibly_up=True) == [A, joining(B), C, leaving(D)]
    assert s.members(excluding_possibly_exiting=True) == [A, C]
    assert s.joining == [joining(B)]
    assert s.reachable_members() == [A, leaving(D)] and s.unreachable_members() == [C]
    s.add_unreachable(A)
    assert s.reachable_members() == [leaving(D)] and s.unreachable_members() == [A, C]
    assert s.unreachable == nodes(A, C) and s.is_unreachable(A)
    s.add_reachable(A)
    assert s.unreachable == nodes(C)
    s.remove(C)
    assert s.unreachable == frozenset()


def test_set_reachability_keeps_only_unreachable_records_between_members():
    s = _DowningStrategy(A.unique_address, KeepMajority())
    for m in (A, B):
        s.add(m)
    r = Reachability(
        (
            Record(A.unique_address, B.unique_address, "unreachable", 1),
            Record(A.unique_address, C.unique_address, "unreachable", 2),
            Record(B.unique_address, A.unique_address, "reachable", 1),
        ),
        ((A.unique_address, 2), (B.unique_address, 1)),
    )
    s.set_reachability(r)
    assert [(x.observer, x.subject) for x in s.reachability.records] == [(A.unique_address, B.unique_address)]


def test_is_all_unreachable_down_or_exiting():
    s = _DowningStrategy(A.unique_address, KeepMajority())
    for m in (A, B, downed(C)):
        s.add(m)
    assert s.is_all_unreachable_down_or_exiting
    s.add_unreachable(downed(C))
    assert s.is_all_unreachable_down_or_exiting
    s.add_unreachable(B)
    assert not s.is_all_unreachable_down_or_exiting


def test_indirectly_connected_from_cycle_and_from_seen_by():
    s = strategy(KeepMajority(), {A, B, C}, indirectly=[(A, B), (B, A)])
    assert s.indirectly_connected == nodes(A, B) and s.has_indirectly_connected
    s = strategy(KeepMajority(), {A, B, C}, {D})  # D unreachable, has not seen the gossip
    assert s.indirectly_connected == frozenset()
    s.set_seen_by(frozenset(m.address for m in (A, B, C, D)))  # D has seen the current gossip: indirectly connected
    assert s.indirectly_connected == nodes(A, D) and s.unreachable_but_not_indirectly_connected == frozenset()


def test_nodes_to_down_skips_down_and_exiting_and_includes_joining():
    s = strategy(KeepMajority(), {A, joining(B), exiting(C), downed(D)}, {E})
    assert s.nodes_to_down(DownAllDecision()) == nodes(A, B, E)
    assert s.nodes_to_down(DownUnreachable()) == nodes(E)
    assert s.nodes_to_down(DownReachable()) == nodes(A, B)


def test_filter_records_keeps_versions():
    r = reachability([(A, B), (B, C)])
    kept = r.filter_records(lambda x: x.observer == A.unique_address)
    assert len(kept.records) == 1 and kept.versions == r.versions



# --- the strategies (SplitBrainResolverSpec scenarios) -----------------------------------------------------------


def downing(config: DowningStrategy, members: set[Member], *sides: set[Member], indirectly: list[tuple[Member, Member]] | None = None) -> None:
    """assertDowning: every non-empty side reaches the same nodes_to_down."""
    for side in sides:
        if side:
            others = [o for o in sides if o is not side]
            assert strategy(config, side, *others, indirectly=indirectly).nodes_to_down() == nodes(*members)


# StaticQuorum


def test_static_quorum_downs_unreachable_when_enough_reachable():
    downing(StaticQuorum(3), {B, D}, {A, C, E}, {B, D})


def test_static_quorum_downs_reachable_when_not_enough():
    assert strategy(StaticQuorum(3), {A, B}, {C, D}).decide() == DownReachable()
    assert strategy(StaticQuorum(3), {C, D}, {A, B}).decide() == DownReachable()


def test_static_quorum_downs_all_when_too_many_members():
    downing(StaticQuorum(3), {A, B, C, D, E, F}, {A, B, C}, {D, E, F})


def test_static_quorum_does_not_count_joining():
    assert strategy(StaticQuorum(3), {A, B, joining(C)}, {D, E}).decide() == DownReachable()
    assert strategy(StaticQuorum(3), {A, B, C}, {joining(D), E}).decide() == DownUnreachable()


def test_static_quorum_does_not_count_leaving_and_exiting():
    assert strategy(StaticQuorum(3), {A, B, leaving(C)}, {D, E}).decide() == DownUnreachable()
    assert strategy(StaticQuorum(3), {A, B, exiting(C)}, {D, E}).decide() == DownReachable()


# KeepMajority


def test_keep_majority_downs_minority():
    downing(KeepMajority(), {D, E}, {A, B, C}, {D, E})


def test_keep_majority_keeps_lowest_address_when_equal():
    downing(KeepMajority(), {C, D}, {A, B}, {C, D})


def test_keep_majority_two_node_cluster():
    downing(KeepMajority(), {B}, {A}, {B})


def test_keep_majority_does_not_count_joining_but_downs_it():
    downing(KeepMajority(), {joining(A), C}, {B, D}, {joining(A), C})


def test_keep_majority_downs_each_part_when_split_in_three():
    s1, s2, s3 = {A, B}, {C, D}, {E}
    assert strategy(KeepMajority(), s1, s2, s3).nodes_to_down() == nodes(A, B)
    assert strategy(KeepMajority(), s2, s1, s3).nodes_to_down() == nodes(C, D)
    assert strategy(KeepMajority(), s3, s1, s2).nodes_to_down() == nodes(E)


def test_keep_majority_membership_change_edge_case():
    s1 = strategy(KeepMajority(), {A, B, F, G}, {C, D, E})
    assert s1.decide() == DownUnreachable() and s1.nodes_to_down() == nodes(C, D, E)
    s2 = strategy(KeepMajority(), {C, D, E}, {A, B, joining(F), joining(G)})
    assert s2.decide() == DownAllDecision() and s2.nodes_to_down() == nodes(A, B, C, D, E, F, G)


def test_keep_majority_membership_change_edge_case_equal_size():
    assert strategy(KeepMajority(), {A, B, F}, {C, D, E}).decide() == DownUnreachable()
    assert strategy(KeepMajority(), {C, D, E}, {A, B, joining(F)}).decide() == DownAllDecision()


def test_keep_majority_safe_edge_case():
    assert strategy(KeepMajority(), {A, B}, {C, D, E, joining(F)}).decide() == DownReachable()
    assert strategy(KeepMajority(), {C, D, E, F}, {A, B}).decide() == DownUnreachable()


def test_keep_majority_leaving_edge_case():
    s1 = strategy(KeepMajority(), {leaving(A), B, joining(E)}, {C, D})
    assert s1.decide() == DownAllDecision()
    s2 = strategy(KeepMajority(), {C, D}, {exiting(A), B})
    assert s2.decide() == DownUnreachable() and s2.nodes_to_down() == nodes(B)


def test_keep_majority_downs_indirectly_connected():
    downing(KeepMajority(), {A, B}, {A, B, C}, indirectly=[(A, B), (B, A)])


def test_keep_majority_indirectly_connected_with_clean_partition():
    s1 = strategy(KeepMajority(), {A, B, C}, {D, E}, indirectly=[(B, C), (C, B)])
    assert s1.decide() == DownIndirectlyConnected() and s1.nodes_to_down() == nodes(B, C, D, E)
    s2 = strategy(KeepMajority(), {D, E}, {A, B, C})
    assert s2.nodes_to_down() == nodes(D, E)


def test_keep_majority_indirectly_connected_detected_before_partition():
    assert strategy(KeepMajority(), {A, B, C}, {D, E, F, G}).nodes_to_down() == nodes(A, B, C)
    assert strategy(KeepMajority(), {A, B, C}, {D, E, F, G}, indirectly=[(D, E), (E, D)]).nodes_to_down() == nodes(A, B, C, D, E)
    assert strategy(KeepMajority(), {D, E, F, G}, {A, B, C}, indirectly=[(D, E), (E, D)]).nodes_to_down() == nodes(A, B, C, D, E)


def test_keep_majority_double_indirectly_connected_raises():
    s1 = strategy(KeepMajority(), {A, B, C}, {D, E, F, G}, indirectly=[(D, E), (G, F)])
    with pytest.raises(_DoubleDecision):
        s1.nodes_to_down()
    assert strategy(KeepMajority(), {D, E, F, G}, {A, B, C}, indirectly=[(D, E), (G, F)]).nodes_to_down() == nodes(A, B, C, D, E, F, G)


# KeepOldest (E is the oldest)


def test_keep_oldest_keeps_partition_with_oldest():
    downing(KeepOldest(), {B, C, D}, {A, E}, {B, C, D})


def test_keep_oldest_unless_alone():
    downing(KeepOldest(), {E}, {E}, {A, B, C, D})


def test_keep_oldest_in_two_node_cluster():
    downing(KeepOldest(), {A}, {B}, {A})


def test_keep_oldest_even_when_alone_when_down_if_alone_false():
    downing(KeepOldest(down_if_alone=False), {A, B, C, D}, {E}, {A, B, C, D})


def test_keep_oldest_leaving_edge_cases():
    assert strategy(KeepOldest(), {leaving(D), E}, {A, B, C}).decide() == DownAllDecision()
    assert strategy(KeepOldest(), {A, B, C}, {leaving(D), E}).decide() == DownReachable()
    assert strategy(KeepOldest(), {E}, {A, B, leaving(C)}).decide() == DownReachable()
    assert strategy(KeepOldest(), {A, B, leaving(C)}, {E}).decide() == DownUnreachable()
    assert strategy(KeepOldest(), {E}, {A, leaving(B), leaving(C), leaving(D)}).decide() == DownReachable()
    assert strategy(KeepOldest(), {A, leaving(B), leaving(C), leaving(D)}, {E}).decide() == DownAllDecision()


def test_keep_oldest_exiting_alone_edge_case():
    assert strategy(KeepOldest(), {D, exiting(E)}, {A, B, C}).decide() == DownReachable()
    assert strategy(KeepOldest(), {A, B, C}, {D, leaving(E)}).decide() == DownReachable()
    assert strategy(KeepOldest(), {A}, {exiting(B)}).decide() == DownUnreachable()
    assert strategy(KeepOldest(), {leaving(B)}, {A}).decide() == DownAllDecision()


def test_keep_oldest_joining_edge_case():
    assert strategy(KeepOldest(), {joining(A), E}, {B, C, D}).decide() == DownReachable()
    assert strategy(KeepOldest(), {B, C, D}, {joining(A), E}).decide() == DownAllDecision()
    assert strategy(KeepOldest(down_if_alone=False), {joining(A), E}, {B, C, D}).decide() == DownUnreachable()
    assert strategy(KeepOldest(down_if_alone=False), {B, C, D}, {joining(A), E}).decide() == DownReachable()


def test_keep_oldest_downs_indirectly_connected():
    downing(KeepOldest(), {A, B}, {A, B, C}, indirectly=[(A, B), (B, A)])


# DownAll


def test_down_all_downs_everything():
    downing(DownAll(), {A, B, C, D}, {A, B}, {C, D})
