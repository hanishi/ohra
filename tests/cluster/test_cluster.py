"""Cluster membership over remoting, as the Pekko docs describe it: seed nodes, joining,
convergence, a leader, leaving, and events for subscribers. All nodes run in this process at
millisecond settings."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
import logging
from dataclasses import dataclass
from typing import Any, cast

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterLeavingReason,
    CoordinatedShutdown,
    CoordinatedShutdownSettings,
    ManagementSettings,
)
from ohra.cluster.core import (
    Cluster,
    ClusterDomainEvent,
    ClusterSettings,
    CurrentClusterState,
    Down,
    GetCurrentState,
    Join,
    LeaderChanged,
    Leave,
    MemberEvent,
    MemberRemoved,
    MemberUp,
    MemberWeaklyUp,
    ReachabilityEvent,
    is_gossip_speedup_needed,
    ReachableMember,
    Subscribe,
    UnreachableMember,
)
from ohra.patterns import ask
from ohra.ref import FunctionRef
from ohra.cluster.gossip import Gossip, Member
from ohra.remote import Address, UniqueAddress
from ohra.remote.serialization import Json

from ..util import eventually, free_port

FAST: dict[str, Any] = dict(
    gossip_interval=0.02,
    leader_actions_interval=0.02,
    unreachable_nodes_reaper_interval=0.02,
    seed_node_timeout=0.1,
    periodic_tasks_initial_delay=0.0,
    heartbeat_interval=0.02,
    acceptable_heartbeat_pause=0.2,
    allow_weakly_up_members=None,
)


FAST_SHUTDOWN = CoordinatedShutdownSettings(
    default_phase_timeout=1.0,
    phase_timeouts={
        "cluster-sharding-shutdown-region": 1.0,
        "cluster-exiting": 0.3,
        "actor-system-terminate": 1.0,
    },
)
"""A leaving node whose cluster cannot converge (an undowned unreachable member) waits out
cluster-exiting, as in Pekko; 0.3 s instead of ten keeps the crash-style tests' teardown short.
Nodes that hand off use their own helpers."""


def cluster_node(
    name: str,
    port: int,
    seeds: list[int],
    shutdown: CoordinatedShutdownSettings = FAST_SHUTDOWN,
    management: ManagementSettings | None = None,
    **settings: Any,
) -> ActorSystem[Any]:
    """A node whose guardian ignores messages; the cluster lives under /system. Management binds an
    ephemeral port."""
    seed_nodes = tuple(Address(name, "127.0.0.1", p) for p in seeds)
    return ActorSystem(
        Behaviors.ignore,
        name,
        address=("127.0.0.1", port),
        cluster=ClusterSettings(seed_nodes=seed_nodes, **{**FAST, **settings}),
        shutdown=shutdown,
        management=management or ManagementSettings(hostname="127.0.0.1", port=0),
    )


def view(system: ActorSystem[Any]) -> tuple[tuple[str, int, str, int], ...]:
    """What this node believes: (host, port, status, up_number) per member."""
    return tuple(
        (m.unique_address.host, m.unique_address.port, m.status, m.up_number)
        for m in Cluster(system).state.members
    )


def all_up(system: ActorSystem[Any], n: int) -> bool:
    members = Cluster(system).state.members
    return len(members) == n and all(m.status == "up" for m in members)


def probe(events: list[ClusterDomainEvent]) -> FunctionRef[ClusterDomainEvent]:
    return FunctionRef("probe", events.append)


async def test_first_seed_bootstraps_alone():
    port = free_port()
    async with cluster_node("c", port, [port]) as a:
        cluster = Cluster(a)
        await eventually(lambda: cluster.self_member.status == "up", timeout=3)
        assert cluster.state.leader == Address("c", "127.0.0.1", port)
        assert cluster.state.members == (cluster.self_member,)
        assert cluster.self_member.up_number == 1


async def test_three_nodes_join_and_reach_up():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        assert sorted(m.up_number for m in Cluster(nodes[0]).state.members) == [1, 2, 3]
        assert view(nodes[0]) == view(nodes[1]) == view(nodes[2])


async def test_leader_is_lowest_address():
    ports = sorted([free_port(), free_port(), free_port()])
    # the seed is not the lowest address, so the leader is not simply "the first node"
    seed = ports[-1]
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(cluster_node("c", p, [seed]))
            for p in (seed, *ports[:-1])
        ]
        events: list[ClusterDomainEvent] = []
        Cluster(nodes[0]).subscriptions.tell(Subscribe(probe(events), LeaderChanged))
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        lowest = Address("c", "127.0.0.1", ports[0])
        assert all(Cluster(n).state.leader == lowest for n in nodes)
        changes = [e for e in events if isinstance(e, LeaderChanged)]
        assert changes and changes[-1].leader == lowest


async def test_concurrent_joins_converge():
    seed, *others = free_port(), free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        first = await stack.enter_async_context(cluster_node("c", seed, [seed]))
        rest = await asyncio.gather(
            *(stack.enter_async_context(cluster_node("c", p, [seed])) for p in others)
        )
        nodes = [first, *rest]
        await eventually(lambda: all(all_up(n, 4) for n in nodes), timeout=5)
        assert len({view(n) for n in nodes}) == 1


async def test_leave_moves_through_exiting_to_removed():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c = [
            await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        events: list[ClusterDomainEvent] = []
        Cluster(a).subscriptions.tell(Subscribe(probe(events), MemberEvent))
        b_address = Address("c", "127.0.0.1", p2)
        Cluster(a).manager.tell(Leave(b_address))
        await asyncio.wait_for(b.when_terminated(), timeout=5)
        await eventually(lambda: all_up(a, 2) and all_up(c, 2), timeout=5)
        about_b = [
            type(e).__name__
            for e in events
            if isinstance(e, MemberEvent) and e.member.address == b_address
        ]
        # the current state arrives first as events, then every change of b's status, in order
        assert about_b == ["MemberUp", "MemberLeft", "MemberExited", "MemberRemoved"]
        removed = next(e for e in events if isinstance(e, MemberRemoved))
        assert removed.previous_status == "exiting"
        others = [
            e for e in events if isinstance(e, MemberEvent) and e.member.address != b_address
        ]
        assert all(isinstance(e, MemberUp) for e in others)  # nobody else changed
        assert Cluster(b).is_terminated


async def test_subscribers_get_initial_state_as_events():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        events: list[ClusterDomainEvent] = []
        Cluster(b).subscriptions.tell(Subscribe(probe(events), ClusterDomainEvent))
        await eventually(lambda: len(events) >= 3, timeout=3)
        ups = sorted(e.member.unique_address.port for e in events if isinstance(e, MemberUp))
        assert ups == sorted([p1, p2])
        assert [e.leader for e in events if isinstance(e, LeaderChanged)] == [
            Address("c", "127.0.0.1", min(p1, p2))
        ]


async def test_get_current_state_snapshot():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        state = await ask(Cluster(b).subscriptions, GetCurrentState, timeout=1)
        assert isinstance(state, CurrentClusterState)
        assert sorted(m.unique_address.port for m in state.members) == sorted([p1, p2])
        assert state.unreachable == frozenset()
        assert state.leader == Address("c", "127.0.0.1", min(p1, p2))
        assert state == Cluster(b).state


async def test_join_wrong_system_name_is_ignored():
    p1, p2 = free_port(), free_port()
    async with cluster_node("x", p1, [p1]) as x, cluster_node("y", p2, []) as y:
        await eventually(lambda: all_up(x, 1), timeout=3)
        Cluster(y).manager.tell(Join(Address("x", "127.0.0.1", p1)))
        await asyncio.sleep(0.2)
        assert Cluster(y).self_member.status == "removed"  # never joined anything
        assert Cluster(y).state.members == ()
        assert all_up(x, 1)


# --- failure detection, downing, incarnations (Task 4) -------------------------------------------


def unreachable_ports(system: ActorSystem[Any]) -> set[int]:
    return {m.unique_address.port for m in Cluster(system).state.unreachable}


async def crash(system: ActorSystem[Any]) -> None:
    """Tear a node down without leaving: the others only find out through missing heartbeats."""
    system._final_terminate()  # pyright: ignore[reportPrivateUsage]  # a crash does not run the coordinated shutdown
    await system.when_terminated()


async def test_torn_down_node_becomes_unreachable():
    p1, p2, p3, p4 = free_port(), free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c = [
            await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        events: list[ClusterDomainEvent] = []
        Cluster(a).subscriptions.tell(Subscribe(probe(events), ReachabilityEvent))
        await crash(b)
        await eventually(
            lambda: unreachable_ports(a) == {p2} and unreachable_ports(c) == {p2}, timeout=5
        )
        assert [e for e in events if isinstance(e, UnreachableMember)][
            0
        ].member.unique_address.port == p2
        # an unreachable Up member blocks convergence: a new node joins but the leader cannot move
        # it to Up
        d = await stack.enter_async_context(cluster_node("c", p4, [p1]))
        await eventually(
            lambda: any(m.unique_address.port == p4 for m in Cluster(a).state.members), timeout=5
        )
        await asyncio.sleep(0.3)
        assert Cluster(d).self_member.status == "joining"
        assert (
            next(m for m in Cluster(a).state.members if m.unique_address.port == p4).status
            == "joining"
        )


async def test_down_removes_an_unreachable_member():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c = [
            await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        events: list[ClusterDomainEvent] = []
        Cluster(c).subscriptions.tell(Subscribe(probe(events), MemberRemoved))
        await crash(b)
        await eventually(lambda: unreachable_ports(a) == {p2}, timeout=5)
        Cluster(a).manager.tell(Down(Address("c", "127.0.0.1", p2)))
        await eventually(lambda: all_up(a, 2) and all_up(c, 2), timeout=5)
        removed = [e for e in events if isinstance(e, MemberRemoved)]
        assert [(e.member.unique_address.port, e.previous_status) for e in removed] == [
            (p2, "down")
        ]
        assert unreachable_ports(a) == set() and unreachable_ports(c) == set()


async def test_downed_node_terminates_itself():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c = [
            await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        Cluster(a).manager.tell(Down(Address("c", "127.0.0.1", p2)))
        await asyncio.wait_for(b.when_terminated(), timeout=5)
        await eventually(lambda: all_up(a, 2) and all_up(c, 2), timeout=5)


async def test_new_incarnation_replaces_the_old():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a = await stack.enter_async_context(
            cluster_node("c", p1, [p1], retry_unsuccessful_join_after=0.1)
        )
        b = await stack.enter_async_context(
            cluster_node("c", p2, [p1], retry_unsuccessful_join_after=0.1)
        )
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        old_uid = Cluster(b).self_unique_address.uid
        events: list[ClusterDomainEvent] = []
        Cluster(a).subscriptions.tell(Subscribe(probe(events), MemberRemoved))
        await crash(b)
        b2 = await stack.enter_async_context(
            cluster_node("c", p2, [p1], retry_unsuccessful_join_after=0.1)
        )
        await eventually(lambda: all_up(a, 2) and all_up(b2, 2), timeout=8)
        uids = {m.unique_address.port: m.unique_address.uid for m in Cluster(a).state.members}
        assert uids[p2] == Cluster(b2).self_unique_address.uid != old_uid
        removed = [e for e in events if isinstance(e, MemberRemoved)]
        assert [(e.member.unique_address.uid, e.previous_status) for e in removed] == [
            (old_uid, "down")
        ]


async def test_weakly_up_when_convergence_is_blocked():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [
            await stack.enter_async_context(
                cluster_node("c", p, [p1], allow_weakly_up_members=0.05)
            )
            for p in (p1, p2)
        ]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        events: list[ClusterDomainEvent] = []
        Cluster(a).subscriptions.tell(Subscribe(probe(events), MemberWeaklyUp))
        await crash(b)
        await eventually(lambda: unreachable_ports(a) == {p2}, timeout=5)
        c = await stack.enter_async_context(
            cluster_node("c", p3, [p1], allow_weakly_up_members=0.05)
        )
        await eventually(lambda: Cluster(c).self_member.status == "weakly_up", timeout=5)
        assert [e.member.unique_address.port for e in events if isinstance(e, MemberWeaklyUp)] == [
            p3
        ]
        Cluster(a).manager.tell(Down(Address("c", "127.0.0.1", p2)))
        await eventually(lambda: all_up(a, 2) and all_up(c, 2), timeout=5)


async def test_reachable_again_clears_unreachable():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        events: list[ClusterDomainEvent] = []
        Cluster(a).subscriptions.tell(Subscribe(probe(events), ReachabilityEvent))
        # b stops answering heartbeats: its remoting drops every Heartbeat frame for a while
        remoting = b.remoting
        assert remoting is not None
        deliver = remoting._handle  # pyright: ignore[reportPrivateUsage]

        def dropping_heartbeats(frame: Json) -> None:
            message = frame.get("message") if isinstance(frame, dict) else None
            if isinstance(message, dict) and message.get("$type") == "Heartbeat":
                return
            deliver(frame)

        remoting._handle = dropping_heartbeats  # pyright: ignore[reportPrivateUsage]
        await eventually(lambda: unreachable_ports(a) == {p2}, timeout=5)
        remoting._handle = deliver  # pyright: ignore[reportPrivateUsage]
        await eventually(lambda: unreachable_ports(a) == set(), timeout=5)
        assert [type(e).__name__ for e in events] == ["UnreachableMember", "ReachableMember"]
        assert all(
            isinstance(e, UnreachableMember | ReachableMember)
            and e.member.unique_address.port == p2
            for e in events
        )
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)


# --- from the final review -----------------------------------------------------------------------


async def test_leader_changes_when_the_leader_leaves():
    ports = sorted([free_port(), free_port(), free_port()])
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(cluster_node("c", p, [ports[0]])) for p in ports]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        leader, successor = (
            Address("c", "127.0.0.1", ports[0]),
            Address("c", "127.0.0.1", ports[1]),
        )
        assert all(Cluster(n).state.leader == leader for n in nodes)
        Cluster(nodes[0]).manager.tell(Leave(leader))
        await asyncio.wait_for(nodes[0].when_terminated(), timeout=5)
        await eventually(
            lambda: all(all_up(n, 2) and Cluster(n).state.leader == successor for n in nodes[1:]),
            timeout=5,
        )


async def test_concurrent_changes_on_different_nodes_merge():
    """Leave on one node and Down on another in the same instant: two gossips neither of which
    precedes the other. The merge keeps both changes, and the leader acts on both."""
    p1, p2, p3, p4 = free_port(), free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c, d = [
            await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2, p3, p4)
        ]
        await eventually(lambda: all(all_up(n, 4) for n in (a, b, c, d)), timeout=5)
        events: list[ClusterDomainEvent] = []
        Cluster(a).subscriptions.tell(Subscribe(probe(events), MemberRemoved))
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", p2)))
        Cluster(d).manager.tell(Down(Address("c", "127.0.0.1", p3)))
        await asyncio.wait_for(asyncio.gather(b.when_terminated(), c.when_terminated()), timeout=5)
        await eventually(lambda: all_up(a, 2) and all_up(d, 2), timeout=5)
        removed = sorted(
            (e.member.unique_address.port, e.previous_status)
            for e in events
            if isinstance(e, MemberRemoved)
        )
        assert removed == sorted([(p2, "exiting"), (p3, "down")])


async def test_when_terminated_completes_when_the_guardian_stops_before_the_system_is_entered():
    system = ActorSystem(
        Behaviors.setup(lambda _ctx: Behaviors.stopped()),
        "c",
        address=("127.0.0.1", free_port()),
        cluster=ClusterSettings(),
    )
    await asyncio.wait_for(system.when_terminated(), timeout=2)


async def test_cluster_settings_without_an_address_is_rejected_before_anything_starts():
    with pytest.raises(ValueError):
        ActorSystem(Behaviors.ignore, "c", cluster=ClusterSettings())
    # the conftest fails this test if the guardian was started and leaked


def test_gossip_speedup_uses_integer_division_as_pekko():
    nodes = [UniqueAddress("127.0.0.1", p, p, "c") for p in (1, 2, 3)]
    gossip = Gossip.empty()
    for i, n in enumerate(nodes, start=1):
        gossip = gossip.with_member(Member(n, i, "up"))
    # 1 seen of 3: 1 < 3 // 2 is false
    assert is_gossip_speedup_needed(gossip.seen(nodes[0])) is False
    assert is_gossip_speedup_needed(gossip) is True
    assert (
        is_gossip_speedup_needed(
            gossip.seen(nodes[0]).seen(nodes[1]).mark_as_down(gossip.member(nodes[2]))
        )
        is True
    )


# --- deferred minors from the final review -------------------------------------------------------


@dataclass(frozen=True)
class SpawnSubscriber:
    reply_to: ActorRef[ActorRef[object]]


def spawning_guardian(
    ctx: ActorContext[SpawnSubscriber], msg: SpawnSubscriber
) -> Behavior[SpawnSubscriber]:
    def subscriber(event: object) -> Behavior[object]:
        return Behaviors.stopped() if event == "stop" else Behaviors.same

    msg.reply_to.tell(ctx.spawn(Behaviors.receive_message(subscriber), "subscriber"))
    return Behaviors.same


async def test_a_stopped_local_subscriber_is_forgotten(caplog: pytest.LogCaptureFixture):
    p1, p2 = free_port(), free_port()
    seeds = (Address("c", "127.0.0.1", p1),)
    a = ActorSystem(
        Behaviors.receive(spawning_guardian),
        "c",
        address=("127.0.0.1", p1),
        cluster=ClusterSettings(seed_nodes=seeds, **FAST),
    )
    async with a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        subscriber = await ask(a, SpawnSubscriber, timeout=1)
        Cluster(a).subscriptions.tell(Subscribe(subscriber, ClusterDomainEvent))
        subscriber.tell("stop")
        await eventually(
            lambda: a.resolve(subscriber.path) is None if False else _gone(a, subscriber.path),
            timeout=2,
        )
        with caplog.at_level(logging.DEBUG, logger="ohra.dead_letters"):
            async with cluster_node("c", p2, [p1]) as b:
                await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        assert not [r for r in caplog.records if subscriber.path in r.getMessage()]


def _gone(system: ActorSystem[Any], path: str) -> bool:
    try:
        system.resolve(path)
    except LookupError:
        return True
    return False


async def test_a_failing_cluster_daemon_shuts_the_system_down():
    port = free_port()
    async with cluster_node("c", port, [port]) as a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        # a reply ref that is not a ref makes the daemon raise; Pekko's ClusterCoreSupervisor then
        # shuts the cluster down
        Cluster(a).subscriptions.tell(GetCurrentState(cast(Any, "not a ref")))
        await asyncio.wait_for(a.when_terminated(), timeout=3)
        assert Cluster(a).is_terminated


def test_cluster_settings_are_validated():
    for bad in (
        dict(heartbeat_interval=0.0),
        dict(gossip_interval=-1.0),
        dict(monitored_by_nr_of_members=0),
        dict(min_nr_of_members=0),
        dict(failure_detector_threshold=0.0),
        dict(gossip_different_view_probability=1.5),
        dict(allow_weakly_up_members=-1.0),
    ):
        with pytest.raises(ValueError):
            ClusterSettings(**cast(Any, bad))
    assert ClusterSettings(
        periodic_tasks_initial_delay=0.0,
        acceptable_heartbeat_pause=0.0,
        allow_weakly_up_members=None,
    )


async def test_tearing_down_in_forward_order_leaks_nothing():
    p1, p2, p3 = free_port(), free_port(), free_port()
    nodes = [cluster_node("c", p, [p1]) for p in (p1, p2, p3)]
    for n in nodes:
        await n.__aenter__()
    await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
    for n in nodes:  # the seed and leader goes first
        await n.__aexit__(None, None, None)
    assert all(n.is_terminated for n in nodes)


# --- the exiting phase (cluster singleton plan, Task 1) ------------------------------------------


async def test_exiting_tasks_run_before_the_node_exits():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        done: list[str] = []

        async def task() -> None:
            await asyncio.sleep(0.1)
            done.append("done")

        CoordinatedShutdown(b).add_task("cluster-exiting", "t", task)
        events: list[ClusterDomainEvent] = []
        Cluster(a).subscriptions.tell(Subscribe(probe(events), MemberEvent))
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", p2)))
        await asyncio.wait_for(b.when_terminated(), timeout=5)
        assert done == ["done"]  # the task finished before the node went away
        assert CoordinatedShutdown(b).shutdown_reason == ClusterLeavingReason()
        await eventually(lambda: all_up(a, 1), timeout=5)
        names = [
            type(e).__name__
            for e in events
            if isinstance(e, MemberEvent) and e.member.unique_address.port == p2
        ]
        assert names.index("MemberExited") < names.index("MemberRemoved")


async def test_a_slow_exiting_task_is_bounded_by_exiting_timeout():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        short = CoordinatedShutdownSettings(phase_timeouts={"cluster-exiting": 0.2})
        a = await stack.enter_async_context(cluster_node("c", p1, [p1], shutdown=short))
        b = await stack.enter_async_context(cluster_node("c", p2, [p1], shutdown=short))
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)

        async def slow() -> None:
            await asyncio.sleep(5)

        CoordinatedShutdown(b).add_task("cluster-exiting", "slow", slow)
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", p2)))
        await asyncio.wait_for(b.when_terminated(), timeout=2)


async def test_a_failing_exiting_task_does_not_block_the_exit():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)

        async def failing() -> None:
            raise RuntimeError("boom")

        CoordinatedShutdown(b).add_task("cluster-exiting", "failing", failing)
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", p2)))
        await asyncio.wait_for(b.when_terminated(), timeout=2)


def test_publish_diff_emits_seen_and_reachability_changes_last():
    from ohra.cluster.core import (
        ReachabilityChanged,
        SeenChanged,
        diff_reachability,
        diff_seen,
        publish_diff,
    )
    from ohra.cluster.gossip import MembershipState
    from ohra.cluster.reachability import Reachability

    a, b = UniqueAddress("127.0.0.1", 1, 1, "c"), UniqueAddress("127.0.0.1", 2, 2, "c")
    g = Gossip.empty().with_member(Member(a, 1, "up")).with_member(Member(b, 2, "up"))
    old = MembershipState(g.seen(a), a)
    new = MembershipState(
        g.seen(a).seen(b).with_reachability(Reachability.empty().unreachable(a, b)), a
    )
    events: list[ClusterDomainEvent] = []
    publish_diff(old, new, events.append)
    assert isinstance(events[-2], SeenChanged) and events[-2].seen_by == frozenset(
        {a.address, b.address}
    )
    assert isinstance(events[-1], ReachabilityChanged) and events[
        -1
    ].reachability.all_unreachable == frozenset({b})
    assert diff_seen(new, new) == [] and diff_reachability(new, new) == []
    # same seen set, same convergence: nothing
    assert diff_seen(old, MembershipState(g.seen(a), a)) == []
