"""The split-brain resolver across nodes: crashes removed without a manual Down, partitions resolved by keep-majority,
a crashed resolver taking its node down. Millisecond settings; every test under the leak check."""
from __future__ import annotations

import asyncio
import contextlib
from contextlib import AsyncExitStack
from typing import Any, cast

import pytest

from pekko_py import ActorSystem
from pekko_py.cluster.core import Cluster
from pekko_py.cluster.sbr import SplitBrainResolverSettings, _Resolver  # pyright: ignore[reportPrivateUsage]
from pekko_py.remote.remoting import read_frame

from ..util import eventually, free_port
from .test_cluster import all_up, cluster_node

SBR: dict[str, Any] = dict(downing=SplitBrainResolverSettings(stable_after=0.3))


async def crash(system: ActorSystem[Any]) -> None:
    assert system.remoting is not None
    await system.remoting.close()
    system.terminate()
    await system.when_terminated()


def partition(system: ActorSystem[Any], blocked_ports: set[int]) -> None:
    """Drop every inbound frame from the given peers at this node; each connection announces its peer in its hello."""
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
    # Connections already open keep reading through the original loop: close ours to the blocked peers, so every
    # link to them is re-established through the patched one (each link is one side's outbound association).
    for (_host, port), association in list(remoting._associations.items()):  # pyright: ignore[reportPrivateUsage]
        if port in blocked_ports:
            association.task.cancel()


def three_ports() -> tuple[int, int, int]:
    p1, p2, p3 = sorted((free_port(), free_port(), free_port()))
    return p1, p2, p3


async def test_crashed_node_is_removed_without_manual_down():
    p1, p2, p3 = three_ports()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(cluster_node("c", p, [p1], **SBR)) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        await crash(nodes[2])
        await eventually(lambda: all(len(Cluster(n).state.members) == 2 for n in nodes[:2]), timeout=5)


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
        nodes = [await stack.enter_async_context(cluster_node("c", p, [p1], **SBR)) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        partition(nodes[2], {p1, p2})
        partition(nodes[0], {p3})
        partition(nodes[1], {p3})
        await eventually(lambda: all(len(Cluster(n).state.members) == 2 for n in nodes[:2]), timeout=5)
        await eventually(lambda: Cluster(nodes[2]).is_terminated, timeout=5)  # downed itself and shut down


async def test_partition_isolating_the_leader():
    p1, p2, p3 = three_ports()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(cluster_node("c", p, [p1], **SBR)) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        partition(nodes[0], {p2, p3})
        partition(nodes[1], {p1})
        partition(nodes[2], {p1})
        await eventually(lambda: all(len(Cluster(n).state.members) == 2 for n in nodes[1:]), timeout=5)
        await eventually(lambda: Cluster(nodes[0]).is_terminated, timeout=5)  # the isolated leader downed itself


async def test_resolver_failure_fails_the_daemon(monkeypatch: pytest.MonkeyPatch):
    def boom(self: Any) -> None:
        raise RuntimeError("resolver crashed")

    monkeypatch.setattr(_Resolver, "tick", boom)
    p = free_port()
    async with cluster_node("c", p, [p], **SBR) as node:
        await eventually(lambda: Cluster(node).is_terminated, timeout=5)
