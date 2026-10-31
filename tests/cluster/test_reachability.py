"""Reachability: per-observer, versioned observations, as Reachability.scala."""

from __future__ import annotations

from ohra.cluster.reachability import Reachability
from ohra.remote import UniqueAddress

A = UniqueAddress("127.0.0.1", 1, 11, "s")
B = UniqueAddress("127.0.0.1", 2, 22, "s")
C = UniqueAddress("127.0.0.1", 3, 33, "s")


def test_unreachable_then_reachable_again_prunes_the_observer_row():
    r = Reachability.empty().unreachable(A, B)
    assert r.status(A, B) == "unreachable"
    assert r.status(B) == "unreachable"
    assert not r.is_reachable(B) and not r.is_all_reachable
    assert r.all_unreachable_from(A) == frozenset({B})
    back = r.reachable(A, B)
    assert back.is_all_reachable and back.records == ()
    assert back.version_of(A) == 2  # the version still moved


def test_terminated_wins_over_unreachable_and_is_final():
    r = Reachability.empty().unreachable(A, B).terminated(C, B)
    assert r.status(B) == "terminated"
    assert r.terminated(C, B) is r  # no change
    assert r.reachable(C, B).status(C, B) == "terminated"  # a terminated record is never undone
    assert r.all_unreachable_or_terminated == frozenset({B})


def test_merge_keeps_the_rows_of_the_higher_observer_version():
    r1 = Reachability.empty().unreachable(A, B)  # A's version 1
    r2 = r1.unreachable(A, C)  # A's version 2
    merged = r1.merge(frozenset({A, B, C}), r2)
    assert merged.status(A, C) == "unreachable" and merged.status(A, B) == "unreachable"
    assert merged.version_of(A) == 2
    assert r2.merge(frozenset({A, B, C}), r1).version_of(A) == 2
    only_ab = r2.merge(frozenset({A, B}), r1)  # C left the cluster: its records and versions drop
    assert only_ab.status(A, C) == "reachable"


def test_remove_drops_a_node_as_observer_and_subject():
    r = Reachability.empty().unreachable(A, B).unreachable(B, C)
    gone = r.remove([B])
    assert gone.records == () and gone.version_of(B) == 0
    assert r.remove_observers(frozenset({A})).status(B, C) == "unreachable"
    assert r.all_observers == frozenset({A, B})


def test_equal_tables_compare_equal_in_any_record_order():
    """As Pekko's equals, by observer rows and versions: merge builds records in a frozenset's
    order, which differs between processes, and an order change must not count as a change."""
    r = Reachability.empty().unreachable(A, C).unreachable(B, C)
    swapped = Reachability(r.records[::-1], r.versions[::-1])
    assert r == swapped and hash(r) == hash(swapped)
    assert r != r.unreachable(A, B)
