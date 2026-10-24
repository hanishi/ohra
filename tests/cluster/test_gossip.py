"""Members, Gossip merge, convergence and leader selection, as Gossip.scala and MembershipState.scala."""
from __future__ import annotations

import pytest

from pekko_py.cluster.gossip import (
    Gossip,
    GossipTargetSelector,
    Member,
    MembershipState,
    highest_priority_of,
    vclock_name,
)
from pekko_py.remote import Address, UniqueAddress

A = UniqueAddress("127.0.0.1", 1, 11, "s")
B = UniqueAddress("127.0.0.1", 2, 22, "s")
C = UniqueAddress("127.0.0.1", 3, 33, "s")


def up(node: UniqueAddress, n: int) -> Member:
    return Member(node, 0, "joining").copy_up(n)


def test_address_ordering_and_parsing():
    assert Address.parse("pekko://s@10.0.0.1:2552") == Address("s", "10.0.0.1", 2552)
    assert Address("s", "10.0.0.1", 2552).root == "pekko://s@10.0.0.1:2552"
    assert A.address == Address("s", "127.0.0.1", 1)
    assert sorted([up(C, 1), up(A, 2), up(B, 3)]) == [up(A, 2), up(B, 3), up(C, 1)]


def test_member_transitions_follow_the_table():
    m = Member(A, 0, "joining")
    assert m.with_status("up").status == "up"
    with pytest.raises(ValueError):
        m.with_status("up").with_status("joining")
    assert up(A, 1).is_older_than(up(B, 2))
    assert not up(B, 2).is_older_than(up(A, 1))
    assert up(A, 1).is_older_than(up(B, 1))  # same age: lower address is older
    assert Member(A, 0, "joining") == up(A, 7)  # equality by unique address only


def test_highest_priority_of():
    assert highest_priority_of(up(A, 1), up(A, 1).with_status("down")).status == "down"
    assert highest_priority_of(Member(A, 0, "joining"), up(A, 1)).status == "up"
    assert highest_priority_of(up(A, 1).with_status("leaving"), up(A, 1).with_status("leaving").with_status("exiting")).status == "exiting"
    assert highest_priority_of(up(A, 2), up(A, 1)).up_number == 1  # same status keeps the older


def test_gossip_merge_is_union_with_highest_priority():
    g1 = Gossip.empty().with_member(up(A, 1)).with_member(Member(B, 0, "joining")).increment(vclock_name(A))
    g2 = Gossip.empty().with_member(up(A, 1)).with_member(up(B, 2)).with_member(Member(C, 0, "joining"))
    g2 = g2.increment(vclock_name(B)).seen(B)
    assert g1.version.compare(g2.version) == "concurrent"
    merged = g1.merge(g2)
    assert [m.status for m in merged.members] == ["up", "up", "joining"]
    assert merged.seen_by == ()  # nobody has seen the merged gossip yet
    assert merged.version.versions == {vclock_name(A): 1, vclock_name(B): 1}


def test_tombstoned_or_down_members_present_on_one_side_only_are_dropped():
    g1 = Gossip.empty().with_member(up(A, 1)).with_member(up(B, 2))
    g2 = g1.remove(B, removal_timestamp=1.0)  # B removed on one side: tombstone
    assert g1.merge(g2).has_member(B) is False and g2.merge(g1).has_member(B) is False
    g3 = Gossip.empty().with_member(up(A, 1)).with_member(up(C, 3).with_status("down"))
    assert g3.merge(Gossip.empty().with_member(up(A, 1))).has_member(C) is False  # Down on one side only: dropped
    assert g3.merge(g3).member(C).status == "down"  # on both sides: kept


def test_mark_as_down_and_seen_digest():
    g = Gossip.empty().with_member(up(A, 1)).with_member(up(B, 2)).seen(A).seen(B)
    downed = g.mark_as_down(g.member(B))
    assert downed.member(B).status == "down" and downed.seen_by == (A,)
    assert g.seen_digest != downed.seen_digest and g.seen_digest == g.only_seen(A).seen(B).seen_digest


def test_convergence_rules():
    base = Gossip.empty().with_member(up(A, 1)).with_member(up(B, 2))
    assert MembershipState(base.seen(A), A).convergence(frozenset()) is False  # B has not seen it
    assert MembershipState(base.seen(A).seen(B), A).convergence(frozenset()) is True
    unreachable_b = base.seen(A).seen(B).with_reachability(base.reachability.unreachable(A, B))
    assert MembershipState(unreachable_b, A).convergence(frozenset()) is False  # unreachable Up member
    assert MembershipState(unreachable_b.mark_as_down(unreachable_b.member(B)), A).convergence(frozenset()) is True
    with_joiner = base.with_member(Member(C, 0, "joining")).seen(A).seen(B)
    assert MembershipState(with_joiner, A).convergence(frozenset()) is True  # a Joining member need not have seen it
    # ... unless nobody is Up or Leaving yet: then Joining and WeaklyUp members must have seen it too (firstMemberInDc)
    only_joiners = Gossip.empty().with_member(Member(A, 0, "joining")).with_member(Member(B, 0, "weakly_up")).seen(A)
    assert MembershipState(only_joiners, A).convergence(frozenset()) is False
    assert MembershipState(only_joiners.seen(B), A).convergence(frozenset()) is True


def test_leader_is_first_reachable_member_with_a_leader_status():
    g = Gossip.empty().with_member(Member(A, 0, "joining")).with_member(up(B, 1)).with_member(up(C, 2))
    assert MembershipState(g, B).leader == B  # A is only Joining
    assert MembershipState(g.with_reachability(g.reachability.unreachable(C, B)), C).leader == C
    assert MembershipState(g.mark_as_down(g.member(B)), C).leader == C
    assert MembershipState(Gossip.empty().with_member(Member(A, 0, "joining")), A).leader == A  # nobody Up yet: lowest
    assert MembershipState(g, B).youngest_member == up(C, 2)


def test_target_selector_prefers_nodes_that_have_not_seen_the_gossip():
    g = Gossip.empty().with_member(up(A, 1)).with_member(up(B, 2)).with_member(up(C, 3)).seen(A).seen(B)
    state = MembershipState(g, A)
    selector = GossipTargetSelector(gossip_different_view_probability=1.0)
    assert selector.gossip_target(state) == C
    assert set(selector.random_nodes_for_full_gossip(state, 5)) == {B, C}
    assert state.valid_node_for_gossip(A) is False
