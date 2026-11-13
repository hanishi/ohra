"""Full cluster shutdown: the PreparingForShutdown and ReadyForShutdown statuses and their events.

These are ohra's own unit tests; Pekko has none for the statuses (its coverage is the multi-jvm
ClusterShutdownSpec).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from contextlib import AsyncExitStack
from dataclasses import replace

import pytest

import ohra
import ohra.cluster
import ohra.cluster.core as core
from ohra.cluster.core import (
    Cluster,
    ClusterDomainEvent,
    Down,
    Join,
    PrepareForFullClusterShutdown,
    Subscribe,
    MemberEvent,
    MemberPreparingForShutdown,
    MemberReadyForShutdown,
    MemberRemoved,
    diff_member_events,
    publish_diff,
)
from ohra.cluster.gossip import Gossip, Member, MembershipState
from ohra.remote import Address, UniqueAddress

from ..util import eventually, free_port
from .test_cluster import FAST, all_up, cluster_node, crash, probe, unreachable_ports, view

DC = frozenset({"dc-default"})

A = UniqueAddress("127.0.0.1", 1, 11, "s")
B = UniqueAddress("127.0.0.1", 2, 22, "s")


def up(node: UniqueAddress, n: int) -> Member:
    return Member(node, 0, "joining", DC).copy_up(n)


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
        "PrepareForFullClusterShutdown",
    ],
)
def test_the_events_are_member_events_from_every_package(name: str):
    assert getattr(ohra, name) is getattr(ohra.cluster, name) is getattr(core, name)
    if name != "PrepareForFullClusterShutdown":
        assert issubclass(getattr(core, name), MemberEvent)


# --- the daemon on real nodes (Pekko's ClusterShutdownSpec, multi-jvm) ---------------------------

# "It does not happen" checks wait ten leader ticks, as `expect_no_message` would: no condition
# can show that something never happens.
TICKS = 10 * FAST["leader_actions_interval"]


def statuses(system: Any) -> list[str]:
    return [m.status for m in Cluster(system).state.members]


def all_ready(system: Any, n: int) -> bool:
    return statuses(system) == ["ready_for_shutdown"] * n


async def start(stack: AsyncExitStack, n: int, ports: list[int]) -> list[Any]:
    nodes = [await stack.enter_async_context(cluster_node("c", p, [ports[0]])) for p in ports[:n]]
    await eventually(lambda: all(all_up(x, n) for x in nodes), timeout=5)
    return nodes


async def test_prepare_moves_every_up_member_through_preparing_to_ready():
    ports = sorted(free_port() for _ in range(3))
    async with AsyncExitStack() as stack:
        a, b, c = await start(stack, 3, ports)
        events: list[ClusterDomainEvent] = []
        for kind in (MemberPreparingForShutdown, MemberReadyForShutdown):
            Cluster(c).subscriptions.tell(Subscribe(probe(events), kind))
        Cluster(a).prepare_for_full_cluster_shutdown()
        Cluster(b).manager.tell(PrepareForFullClusterShutdown())
        await eventually(lambda: all(all_ready(n, 3) for n in (a, b, c)), timeout=5)
        for port in ports:
            seen = [
                type(e)
                for e in events
                if isinstance(e, MemberEvent) and e.member.unique_address.port == port
            ]
            assert seen == [MemberPreparingForShutdown, MemberReadyForShutdown]
        late: list[ClusterDomainEvent] = []
        Cluster(c).subscriptions.tell(Subscribe(probe(late), MemberReadyForShutdown))
        await eventually(lambda: len(late) == 3)
        assert {
            e.member.unique_address.port for e in late if isinstance(e, MemberReadyForShutdown)
        } == set(ports)


async def test_a_join_through_another_member_stays_joining_while_the_leader_prepares():
    ports = sorted(free_port() for _ in range(4))
    async with AsyncExitStack() as stack:
        a, b, c = await start(stack, 3, ports)
        Cluster(a).prepare_for_full_cluster_shutdown()
        await eventually(lambda: all(all_ready(n, 3) for n in (a, b, c)), timeout=5)
        d = await stack.enter_async_context(cluster_node("c", ports[3], [ports[1]]))

        def d_joining(system: Any) -> bool:
            return any(
                m.unique_address.port == ports[3] and m.status == "joining"
                for m in Cluster(system).state.members
            )

        await eventually(lambda: d_joining(a) and Cluster(d).self_member.status == "joining")
        await asyncio.sleep(TICKS)
        assert d_joining(a)
        for n in (a, b, c):
            others = [
                m.status for m in Cluster(n).state.members if m.unique_address.port != ports[3]
            ]
            assert others == ["ready_for_shutdown"] * 3


async def test_an_up_member_that_sees_a_preparing_member_prepares_itself(
    caplog: pytest.LogCaptureFixture,
):
    ports = sorted(free_port() for _ in range(4))
    async with AsyncExitStack() as stack:
        a, b, c = await start(stack, 3, ports)
        Cluster(c).prepare_for_full_cluster_shutdown()
        await eventually(lambda: all(all_ready(n, 3) for n in (a, b, c)), timeout=5)
        with caplog.at_level(logging.DEBUG):
            d = await stack.enter_async_context(cluster_node("c", ports[3], [ports[0]]))
            await eventually(lambda: all(all_ready(n, 4) for n in (a, b, c, d)), timeout=5)
        # a, b and c were Ready before d started, so only d, still Up, can detect the shutdown.
        assert any("Detected full cluster shutdown" in r.getMessage() for r in caplog.records)


async def test_prepare_before_joining_is_ignored():
    port = free_port()
    async with cluster_node("c", port, []) as a:
        cluster = Cluster(a)
        cluster.prepare_for_full_cluster_shutdown()
        cluster.manager.tell(Join(cluster.self_address))
        await eventually(lambda: cluster.self_member.status == "up", timeout=3)
        await asyncio.sleep(TICKS)
        assert cluster.self_member.status == "up"


async def test_an_unreachable_member_holds_the_others_in_preparing_until_it_is_downed():
    ports = sorted(free_port() for _ in range(3))
    async with AsyncExitStack() as stack:
        a, b, c = await start(stack, 3, ports)
        await crash(c)
        await eventually(lambda: ports[2] in unreachable_ports(a), timeout=5)
        Cluster(a).prepare_for_full_cluster_shutdown()
        await eventually(lambda: statuses(a) == ["preparing_for_shutdown"] * 3, timeout=5)
        await asyncio.sleep(TICKS)
        assert "ready_for_shutdown" not in statuses(a)
        Cluster(a).manager.tell(Down(Address("c", "127.0.0.1", ports[2])))
        await eventually(lambda: all_ready(a, 2) and all_ready(b, 2), timeout=5)
        assert len(view(a)) == len(view(b)) == 2


async def test_a_join_through_the_preparing_node_is_ignored(caplog: pytest.LogCaptureFixture):
    ports = sorted(free_port() for _ in range(3))
    async with AsyncExitStack() as stack:
        a, b = await start(stack, 2, ports)
        Cluster(a).prepare_for_full_cluster_shutdown()
        await eventually(lambda: all(all_ready(n, 2) for n in (a, b)), timeout=5)
        with caplog.at_level(logging.INFO):
            d = await stack.enter_async_context(cluster_node("c", ports[2], [ports[0]]))
            d_unique = Cluster(d).self_unique_address
            await eventually(
                lambda: any(
                    f"Ignoring join request from [{d_unique}]" in r.getMessage()
                    for r in caplog.records
                ),
                timeout=5,
            )
        assert len(Cluster(a).state.members) == 2
        assert Cluster(d).self_member.status == "removed"
