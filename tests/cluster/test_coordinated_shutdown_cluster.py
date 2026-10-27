"""CoordinatedShutdown across nodes: terminate() leaves gracefully, a Leave from elsewhere runs the
user's tasks, Down and a not-yet-joined node still stop through the phases."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack

from ohra import (
    ActorSystem,
    ActorSystemTerminateReason,
    Behavior,
    Behaviors,
    ClusterSettings,
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
from ohra.cluster.core import Cluster
from ohra.patterns import ask
from ohra.remote import Address

from ..util import eventually, free_port
from .test_cluster import FAST, FAST_SHUTDOWN, all_up, cluster_node, probe
from .test_cluster_sharding import Count, hosted, ids_hosted_by, ref
from .test_cluster_sharding import node as sharding_node
from .test_cluster_sharding import start as start_sharding
from .test_singleton import Hit, Log, never_two_at_once, starts, stops
from .test_singleton import node as singleton_node
from .test_singleton import start as start_singleton


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
        names = [
            type(e).__name__
            for e in events
            if isinstance(e, MemberEvent) and e.member.unique_address.port == p2
        ]
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
        assert (
            ran == ["stopped"] and CoordinatedShutdown(b).shutdown_reason == ClusterLeavingReason()
        )


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
    # a seed that never answers: the node stays uninitialized
    async with cluster_node("c", p, [p + 1]):
        await asyncio.sleep(0.1)
    # no cluster phase waited for its timeout
    assert asyncio.get_running_loop().time() - began < 2.0


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
        assert (
            ran == ["stopped"]
            and CoordinatedShutdown(nodes[2]).shutdown_reason == ClusterDowningReason()
        )
        await eventually(lambda: all_up(nodes[0], 2) and all_up(nodes[1], 2), timeout=5)


# --- singleton and sharding through the phases (Task 3) ------------------------------------------


async def test_singleton_hands_over_on_terminate():
    p1, p2, p3 = sorted((free_port(), free_port(), free_port()))
    async with AsyncExitStack() as stack:
        a, b, c = [await stack.enter_async_context(singleton_node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        log: Log = []
        proxies = [
            start_singleton(n, log, p) for n, p in zip((a, b, c), (p1, p2, p3), strict=True)
        ]
        assert await ask(proxies[2], Hit, timeout=3) == 1
        a.terminate()
        await asyncio.wait_for(a.when_terminated(), timeout=5)
        await eventually(lambda: starts(log) == [p1, p2], timeout=5)
        assert stops(log) == [p1] and never_two_at_once(log)
        await eventually(lambda: all_up(c, 2), timeout=5)
        assert await ask(proxies[2], Hit, timeout=3) == 1


async def test_shards_hand_off_before_the_node_leaves():
    p1, p2, p3 = sorted((free_port(), free_port(), free_port()))
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(sharding_node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start_sharding(n) for n in nodes]
        where = await ids_hosted_by(regions, nodes, 12)
        on_c = [e for e, i in where.items() if i == 2]
        assert on_c
        hosted_at_leave: list[frozenset[str]] = []

        # runs in cluster-leave: after the region phase, before the member leaves
        async def record() -> None:
            try:
                hosted_at_leave.append(await asyncio.wait_for(hosted(regions[2]), 0.5))
            # the region stopped itself once everything was handed off, as Pekko's does
            except TimeoutError:
                hosted_at_leave.append(frozenset())

        CoordinatedShutdown(nodes[2]).add_task("cluster-leave", "record", record)
        nodes[2].terminate()
        await asyncio.wait_for(nodes[2].when_terminated(), timeout=8)
        # every entity had been handed off before the leave began
        assert hosted_at_leave == [frozenset()]
        await eventually(lambda: all_up(nodes[0], 2) and all_up(nodes[1], 2), timeout=5)
        for e in on_c:
            # a fresh entity elsewhere (state is in memory)
            assert await ask(ref(nodes[0], e), lambda r: Count(1, r), timeout=5) == 1


async def test_a_gracefully_left_region_is_forgotten_without_the_removal_margin():
    """Pekko delays a region's termination by the removal margin only for a crashed node
    (addressTerminated); a node that left gracefully has stopped, so the coordinator must not make
    the next leaver wait for the margin."""
    p1, p2 = sorted((free_port(), free_port()))
    async with AsyncExitStack() as stack:
        a, b = [
            await stack.enter_async_context(sharding_node(p, [p1], down_removal_margin=3.0))
            for p in (p1, p2)
        ]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        regions = [start_sharding(n) for n in (a, b)]
        await ids_hosted_by(regions, [a, b], 12)
        b.terminate()
        await asyncio.wait_for(b.when_terminated(), timeout=5)
        await eventually(lambda: all_up(a, 1), timeout=5)
        began = asyncio.get_running_loop().time()
        a.terminate()
        await asyncio.wait_for(a.when_terminated(), timeout=5)
        # ~1 s: the coordinator singleton, alone and exiting, waits one hand-over retry before
        # stopping, as Pekko's; with b's region kept for the margin it would be 3 s more
        assert asyncio.get_running_loop().time() - began < 2.5


async def test_a_guardian_that_stops_itself_leaves_gracefully():
    """Pekko Typed's guardian wrapper turns the guardian's stop into terminate(), so the phases
    run."""
    p1, p2 = two_ports()
    async with AsyncExitStack() as stack:
        a = await stack.enter_async_context(cluster_node("c", p1, [p1]))
        guardian: Behavior[str] = Behaviors.receive_message(lambda _m: Behaviors.stopped())
        seeds = (Address("c", "127.0.0.1", p1),)
        b = await stack.enter_async_context(
            ActorSystem(
                guardian,
                "c",
                address=("127.0.0.1", p2),
                cluster=ClusterSettings(seed_nodes=seeds, **FAST),
                shutdown=FAST_SHUTDOWN,
            )
        )
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        events: list[ClusterDomainEvent] = []
        Cluster(a).subscriptions.tell(Subscribe(probe(events), ClusterDomainEvent))
        b.tell("stop")
        await asyncio.wait_for(b.when_terminated(), timeout=5)
        await eventually(lambda: all_up(a, 1), timeout=5)
        names = [
            type(e).__name__
            for e in events
            if isinstance(e, MemberEvent) and e.member.unique_address.port == p2
        ]
        assert names == ["MemberUp", "MemberLeft", "MemberExited", "MemberRemoved"]
        assert not any(isinstance(e, UnreachableMember) for e in events)
        assert CoordinatedShutdown(b).shutdown_reason == ActorSystemTerminateReason()
