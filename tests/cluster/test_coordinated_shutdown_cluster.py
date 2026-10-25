"""CoordinatedShutdown across nodes: terminate() leaves gracefully, a Leave from elsewhere runs the user's tasks,
Down and a not-yet-joined node still stop through the phases."""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack

import pytest

from pekko_py import (
    ActorSystemTerminateReason,
    ClusterDomainEvent,
    ClusterDowningReason,
    ClusterLeavingReason,
    CoordinatedShutdown,
    Down,
    Leave,
    MemberEvent,
    Subscribe,
    UnreachableMember,
)
from pekko_py.cluster.core import Cluster
from pekko_py.remote import Address

from ..util import eventually, free_port
from .test_cluster import all_up, cluster_node, probe


def two_ports() -> tuple[int, int]:
    p1, p2 = sorted((free_port(), free_port()))
    return p1, p2


async def test_terminate_leaves_gracefully():
    p1, p2 = two_ports()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        events: list[ClusterDomainEvent] = []
        Cluster(a).subscriptions.tell(Subscribe(probe(events), ClusterDomainEvent))
        b.terminate()
        await asyncio.wait_for(b.when_terminated(), timeout=5)
        await eventually(lambda: all_up(a, 1), timeout=5)
        names = [type(e).__name__ for e in events if isinstance(e, MemberEvent) and e.member.unique_address.port == p2]
        assert names == ["MemberUp", "MemberLeft", "MemberExited", "MemberRemoved"]
        assert not any(isinstance(e, UnreachableMember) for e in events)
        assert CoordinatedShutdown(b).shutdown_reason == ActorSystemTerminateReason()


async def test_a_leave_from_another_node_runs_user_tasks():
    p1, p2 = two_ports()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        ran: list[str] = []

        async def task() -> None:
            ran.append("stopped")

        CoordinatedShutdown(b).add_task("service-stop", "t", task)
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", p2)))
        await asyncio.wait_for(b.when_terminated(), timeout=5)
        assert ran == ["stopped"] and CoordinatedShutdown(b).shutdown_reason == ClusterLeavingReason()


async def test_terminate_during_a_leave_started_run_is_one_run():
    p1, p2 = two_ports()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        ran: list[str] = []

        async def task() -> None:
            ran.append("x")

        CoordinatedShutdown(b).add_task("service-stop", "t", task)
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", p2)))
        await eventually(lambda: CoordinatedShutdown(b).shutdown_reason is not None, timeout=5)
        b.terminate()
        await asyncio.wait_for(b.when_terminated(), timeout=5)
        assert ran == ["x"]


async def test_terminate_before_joining_stops_promptly():
    p = free_port()
    began = asyncio.get_running_loop().time()
    async with cluster_node("c", p, [p + 1]):  # a seed that never answers: the node stays uninitialized
        await asyncio.sleep(0.1)
    assert asyncio.get_running_loop().time() - began < 2.0  # no cluster phase waited for its timeout


async def test_downed_node_terminates_through_the_phases():
    p1, p2, p3 = sorted((free_port(), free_port(), free_port()))
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        ran: list[str] = []

        async def task() -> None:
            ran.append("stopped")

        CoordinatedShutdown(nodes[2]).add_task("service-stop", "t", task)
        Cluster(nodes[0]).manager.tell(Down(Address("c", "127.0.0.1", p3)))
        await asyncio.wait_for(nodes[2].when_terminated(), timeout=5)
        assert ran == ["stopped"] and CoordinatedShutdown(nodes[2]).shutdown_reason == ClusterDowningReason()
        await eventually(lambda: all_up(nodes[0], 2) and all_up(nodes[1], 2), timeout=5)


_unused = pytest  # keep the fixture import for later tests
