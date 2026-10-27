"""The pure parts of the cluster singleton: settings arithmetic and the oldest-changed tracker, as
ClusterSingletonManager.scala's retry setup and OldestChangedBuffer."""

from __future__ import annotations

import pytest

from ohra.cluster.gossip import UP_NUMBER_UNSET, Member, MemberStatus
from ohra.cluster.singleton import (
    ClusterSingletonSettings,
    InitialOldestState,
    OldestChanged,
    OldestChangedTracker,
)
from ohra.remote import UniqueAddress

A = UniqueAddress("127.0.0.1", 1, 11, "c")
B = UniqueAddress("127.0.0.1", 2, 22, "c")
C = UniqueAddress("127.0.0.1", 3, 33, "c")
D = UniqueAddress("127.0.0.1", 4, 44, "c")


def member(node: UniqueAddress, up_number: int, status: MemberStatus = "up") -> Member:
    return Member(node, up_number, status)


def test_retry_arithmetic_follows_pekko():
    s = ClusterSingletonSettings()
    assert (s.max_hand_over_retries, s.max_take_over_retries) == (15, 12)
    s = ClusterSingletonSettings(removal_margin=30.0, hand_over_retry_interval=1.0)
    assert (s.max_hand_over_retries, s.max_take_over_retries) == (33, 30)
    s = ClusterSingletonSettings(min_number_of_hand_over_retries=1)
    assert (s.max_hand_over_retries, s.max_take_over_retries) == (3, 1)
    s = ClusterSingletonSettings(
        min_number_of_hand_over_retries=1, removal_margin=0.3, hand_over_retry_interval=0.1
    )
    # whole milliseconds, as Pekko: 300 // 100 is 3
    assert (s.max_hand_over_retries, s.max_take_over_retries) == (6, 3)


def test_settings_are_validated():
    for bad in (
        dict(buffer_size=10001),
        dict(min_number_of_hand_over_retries=0),
        dict(hand_over_retry_interval=0.0),
        dict(removal_margin=-1.0),
    ):
        with pytest.raises(ValueError):
            ClusterSingletonSettings(**bad)  # pyright: ignore[reportArgumentType]
    assert ClusterSingletonSettings(buffer_size=0).buffer_size == 0


def test_initial_state_lists_the_oldest_up_to_self():
    t = OldestChangedTracker(B)
    t.handle_initial(
        [member(A, 1), member(B, 2), member(C, 3), member(D, UP_NUMBER_UNSET, "joining")]
    )
    assert t.next_change() == InitialOldestState((A, B), True)
    assert t.next_change() is None
    # the joining member has no age yet
    assert [m.unique_address for m in t.members_by_age] == [A, B, C]
    unsafe = OldestChangedTracker(B)
    unsafe.handle_initial([member(A, 1, "leaving"), member(B, 2), member(C, 3)])
    assert unsafe.next_change() == InitialOldestState((A, B), False)


def test_changes_are_queued_and_delivered_one_at_a_time():
    t = OldestChangedTracker(C)
    t.handle_initial([member(A, 1), member(B, 2), member(C, 3)])
    assert isinstance(t.next_change(), InitialOldestState)
    t.remove(member(A, 1))
    t.remove(member(B, 2))
    t.add(member(D, 4))  # younger than the oldest: no change
    assert t.next_change() == OldestChanged(B)
    assert t.next_change() == OldestChanged(C)
    assert t.next_change() is None
    t.remove(member(C, 3))
    t.remove(member(D, 4))
    assert t.next_change() == OldestChanged(D)
    assert t.next_change() == OldestChanged(None)


def test_self_exiting_removes_self_from_the_order():
    t = OldestChangedTracker(A)
    t.handle_initial([member(A, 1), member(B, 2)])
    t.next_change()
    t.self_exiting()
    assert t.next_change() == OldestChanged(B)
    assert [m.unique_address for m in t.members_by_age] == [B]


def test_retry_counts_use_the_resolved_removal_margin():
    s = ClusterSingletonSettings(hand_over_retry_interval=1.0)
    assert s.max_hand_over_retries_for(20.0) == 23 and s.max_take_over_retries_for(20.0) == 20
    assert s.max_hand_over_retries_for(0.0) == 15
