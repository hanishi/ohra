"""Role leader events, as Pekko's ClusterDomainEventSpec (role leader changes)."""

from __future__ import annotations

from ohra.cluster.core import RoleLeaderChanged, diff_role_leader
from ohra.cluster.gossip import Gossip, Member, MembershipState, MemberStatus
from ohra.remote import UniqueAddress

SELF = UniqueAddress("127.0.0.1", 99, 17, "s")


def member(name: int, status: MemberStatus, roles: set[str]) -> Member:
    node = UniqueAddress("127.0.0.1", name, name, "s")
    return Member(node, 0, status, frozenset({"dc-default", *roles}))


A_UP = member(1, "up", {"AA", "AB"})
B_UP = member(2, "up", {"AB", "BB"})
C_UP = member(3, "up", set())
D_LEAVING = member(4, "leaving", {"DD", "DE"})
D_EXITING = member(4, "exiting", {"DD", "DE"})
E_JOINING = member(5, "joining", {"EE", "DE"})


def state(*members: Member) -> MembershipState:
    return MembershipState(Gossip(members=tuple(sorted(members))), SELF)


def test_role_leader_changes_are_produced():
    g0 = state()
    g1 = state(A_UP, B_UP, C_UP, D_LEAVING, E_JOINING)
    g2 = state(B_UP, C_UP, D_EXITING, E_JOINING)
    a, b, d, e = (m.unique_address.address for m in (A_UP, B_UP, D_LEAVING, E_JOINING))
    assert set(diff_role_leader(g0, g1)) == {
        RoleLeaderChanged("dc-default", a),
        RoleLeaderChanged("AA", a),
        RoleLeaderChanged("AB", a),
        RoleLeaderChanged("BB", b),
        RoleLeaderChanged("DD", d),
        RoleLeaderChanged("DE", d),
        RoleLeaderChanged("EE", e),
    }
    assert set(diff_role_leader(g1, g2)) == {
        RoleLeaderChanged("dc-default", b),
        RoleLeaderChanged("AA", None),
        RoleLeaderChanged("AB", b),
        RoleLeaderChanged("DE", e),
    }
