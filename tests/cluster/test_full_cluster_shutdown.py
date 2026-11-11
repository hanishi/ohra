"""Full cluster shutdown: the PreparingForShutdown and ReadyForShutdown statuses and their events.

These are ohra's own unit tests; Pekko has none for the statuses (its coverage is the multi-jvm
ClusterShutdownSpec).
"""

from __future__ import annotations

from dataclasses import replace

import pytest

import ohra
import ohra.cluster
import ohra.cluster.core as core
from ohra.cluster.core import (
    ClusterDomainEvent,
    MemberEvent,
    MemberPreparingForShutdown,
    MemberReadyForShutdown,
    MemberRemoved,
    diff_member_events,
    publish_diff,
)
from ohra.cluster.gossip import Gossip, Member, MembershipState
from ohra.remote import UniqueAddress

A = UniqueAddress("127.0.0.1", 1, 11, "s")
B = UniqueAddress("127.0.0.1", 2, 22, "s")


def up(node: UniqueAddress, n: int) -> Member:
    return Member(node, 0, "joining").copy_up(n)


def state(*members: Member) -> MembershipState:
    g = Gossip.empty()
    for m in members:
        g = g.with_member(m)
    return MembershipState(g, A)


def test_diff_publishes_preparing_then_ready():
    s0 = state(up(A, 1), up(B, 2))
    s1 = state(up(A, 1).with_status("preparing_for_shutdown"), up(B, 2))
    events = diff_member_events(s0, s1)
    assert events == [MemberPreparingForShutdown(s1.latest_gossip.member(A))]
    assert events[0].member.status == "preparing_for_shutdown"
    s2 = state(
        up(A, 1).with_status("preparing_for_shutdown").with_status("ready_for_shutdown"), up(B, 2)
    )
    events = diff_member_events(s1, s2)
    assert events == [MemberReadyForShutdown(s2.latest_gossip.member(A))]
    assert events[0].member.status == "ready_for_shutdown"


def test_initial_state_as_events_names_shutdown_members():
    s = state(
        replace(up(A, 1), status="preparing_for_shutdown"),
        replace(up(B, 2), status="ready_for_shutdown"),
    )
    published: list[ClusterDomainEvent] = []
    publish_diff(MembershipState(Gossip.empty(), A), s, published.append)
    members = [e for e in published if isinstance(e, MemberEvent)]
    assert [type(e) for e in members] == [MemberPreparingForShutdown, MemberReadyForShutdown]
    assert [e.member.unique_address for e in members] == [A, B]


def test_removed_keeps_a_shutdown_previous_status():
    old = state(replace(up(A, 1), status="ready_for_shutdown"), up(B, 2))
    new = state(up(B, 2))
    assert diff_member_events(old, new) == [
        MemberRemoved(replace(up(A, 1), status="removed"), "ready_for_shutdown")
    ]
    assert diff_member_events(old, new)[0].previous_status == "ready_for_shutdown"


@pytest.mark.parametrize(
    "name",
    [
        "MemberPreparingForShutdown",
        "MemberReadyForShutdown",
        pytest.param("PrepareForFullClusterShutdown", marks=pytest.mark.xfail(strict=True)),
    ],
)
def test_the_events_are_member_events_from_every_package(name: str):
    assert getattr(ohra, name) is getattr(ohra.cluster, name) is getattr(core, name)
    if name != "PrepareForFullClusterShutdown":
        assert issubclass(getattr(core, name), MemberEvent)
