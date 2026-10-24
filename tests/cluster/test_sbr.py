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
    DowningStrategy,
    DownReachable,
    DownUnreachable,
    KeepMajority,
    KeepOldest,
    SplitBrainResolverSettings,
    StaticQuorum,
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

