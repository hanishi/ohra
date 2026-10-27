"""The split-brain resolver across nodes: crashes removed without a manual Down, partitions
resolved by keep-majority, a crashed resolver taking its node down. Millisecond settings; every
test under the leak check."""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import AsyncExitStack
from typing import Any, cast

import pytest

from pekko_py import ActorSystem, AskTimeoutError
from pekko_py.cluster.core import Cluster, MemberRemoved, Subscribe
from pekko_py.patterns import ask
from pekko_py.ref import FunctionRef
from pekko_py.cluster.sbr import SplitBrainResolverSettings, _Resolver  # pyright: ignore[reportPrivateUsage]
from pekko_py.remote.remoting import read_frame

from ..util import eventually, free_port
from .test_cluster import all_up, cluster_node
from .test_cluster_sharding import Count, hosted, ids_hosted_by, ref
from .test_cluster_sharding import node as sharding_node
from .test_cluster_sharding import start as start_sharding
from .test_singleton import Hit, Log, starts
from .test_singleton import node as singleton_node
from .test_singleton import start as start_singleton

SBR: dict[str, Any] = dict(downing=SplitBrainResolverSettings(stable_after=0.3))


async def crash(system: ActorSystem[Any]) -> None:
    assert system.remoting is not None
    await system.remoting.close()
    system._final_terminate()  # pyright: ignore[reportPrivateUsage]  # a crash does not run the coordinated shutdown
    await system.when_terminated()


def partition(system: ActorSystem[Any], blocked_ports: set[int]) -> None:
    """Drop every inbound frame from the given peers at this node; each connection announces its
    peer in its hello."""
    remoting = system.remoting
    assert remoting is not None

    async def read_loop(reader: asyncio.StreamReader) -> None:
        peer: int | None = None
        with contextlib.suppress(ValueError, OSError):
            while (frame := await read_frame(reader)) is not None:
                if isinstance(frame, dict) and frame.get("kind") == "hello":
                    peer = cast(dict[str, Any], frame["address"]).get("port")
                if peer not in blocked_ports:
                    remoting._handle(frame)  # pyright: ignore[reportPrivateUsage]

    remoting.read_loop = read_loop  # pyright: ignore[reportAttributeAccessIssue]
    # Connections already open keep reading through the original loop: close ours to the blocked
    # peers, so every link to them is re-established through the patched one (each link is one
    # side's outbound association).
    for (_host, port), association in list(remoting._associations.items()):  # pyright: ignore[reportPrivateUsage]
        if port in blocked_ports:
            association.task.cancel()


def three_ports() -> tuple[int, int, int]:
    p1, p2, p3 = sorted((free_port(), free_port(), free_port()))
    return p1, p2, p3


async def test_crashed_node_is_removed_without_manual_down():
    p1, p2, p3 = three_ports()
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(cluster_node("c", p, [p1], **SBR))
            for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        await crash(nodes[2])
        await eventually(
            lambda: all(len(Cluster(n).state.members) == 2 for n in nodes[:2]), timeout=5
        )


async def test_without_downing_a_crashed_node_stays_unreachable():
    p1, p2 = sorted((free_port(), free_port()))
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(cluster_node("c", p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all(all_up(n, 2) for n in nodes), timeout=5)
        await crash(nodes[1])
        await eventually(lambda: len(Cluster(nodes[0]).state.unreachable) == 1, timeout=5)
        await asyncio.sleep(1.0)
        assert len(Cluster(nodes[0]).state.members) == 2


async def test_partition_keeps_the_majority_and_the_minority_downs_itself():
    p1, p2, p3 = three_ports()
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(cluster_node("c", p, [p1], **SBR))
            for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        partition(nodes[2], {p1, p2})
        partition(nodes[0], {p3})
        partition(nodes[1], {p3})
        await eventually(
            lambda: all(len(Cluster(n).state.members) == 2 for n in nodes[:2]), timeout=5
        )
        # downed itself and shut down
        await eventually(lambda: Cluster(nodes[2]).is_terminated, timeout=5)


async def test_partition_isolating_the_leader():
    p1, p2, p3 = three_ports()
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(cluster_node("c", p, [p1], **SBR))
            for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        partition(nodes[0], {p2, p3})
        partition(nodes[1], {p1})
        partition(nodes[2], {p1})
        await eventually(
            lambda: all(len(Cluster(n).state.members) == 2 for n in nodes[1:]), timeout=5
        )
        # the isolated leader downed itself
        await eventually(lambda: Cluster(nodes[0]).is_terminated, timeout=5)


async def test_resolver_failure_fails_the_daemon(monkeypatch: pytest.MonkeyPatch):
    def boom(self: Any) -> None:
        raise RuntimeError("resolver crashed")

    monkeypatch.setattr(_Resolver, "tick", boom)
    p = free_port()
    async with cluster_node("c", p, [p], **SBR) as node:
        await eventually(lambda: Cluster(node).is_terminated, timeout=5)


# --- the down-removal margin: singletons and shards of a downed node start elsewhere only after it

MARGIN: dict[str, Any] = dict(
    downing=SplitBrainResolverSettings(stable_after=0.3), down_removal_margin=1.0
)


def removal_times(system: ActorSystem[Any]) -> list[float]:
    times: list[float] = []
    Cluster(system).subscriptions.tell(
        Subscribe(
            FunctionRef("removed", lambda _e: times.append(asyncio.get_running_loop().time())),
            MemberRemoved,
        )
    )
    return times


async def test_singleton_moves_after_crash_only_after_the_removal_margin():
    p1, p2, p3 = three_ports()
    async with AsyncExitStack() as stack:
        a, b, c = [
            await stack.enter_async_context(singleton_node(p, [p1], **MARGIN))
            for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        log: Log = []
        proxies = [
            start_singleton(n, log, p) for n, p in zip((a, b, c), (p1, p2, p3), strict=True)
        ]
        assert await ask(proxies[2], Hit, timeout=3) == 1
        removed_at = removal_times(b)
        await crash(a)
        # downed by the resolver, removed, then the margin
        await eventually(lambda: starts(log) == [p1, p2], timeout=8)
        start_at = next(t for kind, port, t in log if kind == "start" and port == p2)
        # not before the margin (within timer tolerance)
        assert removed_at and start_at - removed_at[0] >= 0.9
        await eventually(lambda: all_up(c, 2), timeout=5)
        assert await ask(proxies[2], Hit, timeout=3) == 1


async def test_shards_of_a_crashed_region_move_only_after_the_removal_margin():
    p1, p2, p3 = three_ports()
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(sharding_node(p, [p1], **MARGIN)) for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start_sharding(n) for n in nodes]
        where = await ids_hosted_by(regions, nodes, 12)
        on_c = next(e for e, i in where.items() if i == 2)
        removed_at = removal_times(nodes[0])
        await crash(nodes[2])
        # downed by the resolver and removed by the leader
        await eventually(lambda: bool(removed_at), timeout=5)
        # nothing answers during the margin; the >= 0.9 below is what pins it to the margin
        with pytest.raises(AskTimeoutError):
            await ask(ref(nodes[0], on_c), lambda r: Count(0, r), timeout=0.5)
        # a fresh entity elsewhere
        assert await ask(ref(nodes[0], on_c), lambda r: Count(1, r), timeout=8) == 1
        assert asyncio.get_running_loop().time() - removed_at[0] >= 0.9


async def test_a_node_restarted_on_its_port_inside_the_margin_hosts_no_shard_twice():
    """Pekko keys the delayed termination by incarnation: the restarted node must not inherit the
    dead one's shards and then lose them to a second allocation when the margin ends."""
    p1, p2, p3 = three_ports()
    async with AsyncExitStack() as stack:
        nodes = [
            await stack.enter_async_context(sharding_node(p, [p1], **MARGIN)) for p in (p1, p2, p3)
        ]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        regions = [start_sharding(n) for n in nodes]
        where = await ids_hosted_by(regions, nodes, 12)
        on_c = [e for e, i in where.items() if i == 2]
        assert on_c
        removed_at = removal_times(nodes[0])
        await crash(nodes[2])
        await eventually(lambda: bool(removed_at), timeout=5)
        # restarted inside the 1 s margin
        c2 = await stack.enter_async_context(sharding_node(p3, [p1], **MARGIN))
        await eventually(lambda: all_up(c2, 3) and all_up(nodes[0], 3), timeout=5)
        region_c2 = start_sharding(c2)
        await asyncio.sleep(1.5)  # past the margin
        e = on_c[0]
        # one fresh entity
        assert [
            await ask(ref(nodes[0], e), lambda r: Count(1, r), timeout=5) for _ in range(3)
        ] == [1, 2, 3]
        hosts = [r for r in (regions[0], regions[1], region_c2) if e in await hosted(r)]
        assert len(hosts) == 1, f"{e} hosted by {len(hosts)} regions"
        for i in range(40):  # the restarted region stays registered: fresh ids reach it
            await ask(ref(nodes[0], f"fresh{i}"), lambda r: Count(1, r), timeout=5)
            if await hosted(region_c2):
                break
        else:
            raise AssertionError("the restarted node's region never hosted anything")
