"""Cluster singleton across nodes: one instance on the oldest member, hand-over when it leaves,
proxies on every node, as ClusterSingletonManager.scala and ClusterSingletonProxy.scala."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any

import pytest

from pekko_py import (
    CoordinatedShutdownSettings,
    ActorContext,
    ActorRef,
    ActorSystem,
    AskTimeoutError,
    Behavior,
    Behaviors,
    ClusterSingleton,
    ClusterSingletonSettings,
    PostStop,
    Signal,
    SingletonActor,
    ask,
    remotable,
)
from pekko_py.cluster.core import Cluster, Down, Leave, MemberRemoved, Subscribe
from pekko_py.ref import FunctionRef
from pekko_py.remote import Address
from pekko_py.remote.serialization import Json

from ..util import eventually, free_port
from .test_cluster import all_up, cluster_node, unreachable_ports

FAST_SINGLETON = ClusterSingletonSettings(
    hand_over_retry_interval=0.05,
    min_number_of_hand_over_retries=5,
    singleton_identification_interval=0.02,
)

type Log = list[tuple[str, int, float]]


@remotable
@dataclass(frozen=True)
class Hit:
    reply_to: ActorRef[int]


@remotable
@dataclass(frozen=True)
class Crash: ...


def counter(log: Log, port: int) -> Behavior[Hit | Crash]:
    """The singleton under test: counts hits, records its start and stop with the hosting port."""

    def setup(ctx: ActorContext[Hit | Crash]) -> Behavior[Hit | Crash]:
        loop = asyncio.get_running_loop()
        log.append(("start", port, loop.time()))
        count = 0

        def on_message(_ctx: ActorContext[Hit | Crash], msg: Hit | Crash) -> Behavior[Hit | Crash]:
            nonlocal count
            match msg:
                case Hit(reply_to):
                    count += 1
                    reply_to.tell(count)
                case Crash():
                    raise RuntimeError("crash")
            return Behaviors.same

        def on_signal(_ctx: ActorContext[Hit | Crash], sig: Signal) -> Behavior[Hit | Crash]:
            if sig is PostStop:
                log.append(("stop", port, loop.time()))
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    return Behaviors.setup(setup)


def node(
    port: int, seeds: list[int], exiting_timeout: float = 1.0, **settings: Any
) -> ActorSystem[Any]:
    """A node whose hand-off phases (region shutdown and cluster-exiting) are bounded by
    ``exiting_timeout``."""
    phases = {
        "cluster-sharding-shutdown-region": exiting_timeout,
        "cluster-exiting": exiting_timeout,
    }
    return cluster_node(
        "c", port, seeds, shutdown=CoordinatedShutdownSettings(phase_timeouts=phases), **settings
    )


def start(
    system: ActorSystem[Any],
    log: Log,
    port: int,
    settings: ClusterSingletonSettings = FAST_SINGLETON,
) -> ActorRef[Hit | Crash]:
    return ClusterSingleton(system).init(
        SingletonActor(counter(log, port), "counter").with_settings(settings)
    )


def starts(log: Log) -> list[int]:
    return [port for kind, port, _ in log if kind == "start"]


def stops(log: Log) -> list[int]:
    return [port for kind, port, _ in log if kind == "stop"]


def never_two_at_once(log: Log) -> bool:
    running = 0
    for kind, _, _ in sorted(log, key=lambda e: e[2]):
        running += 1 if kind == "start" else -1
        if running > 1:
            return False
    return True


async def crash(system: ActorSystem[Any]) -> None:
    assert system.remoting is not None
    await system.remoting.close()
    system._final_terminate()  # pyright: ignore[reportPrivateUsage]  # a crash does not run the coordinated shutdown
    await system.when_terminated()


async def test_singleton_runs_on_the_oldest_and_proxies_reach_it():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in nodes), timeout=5)
        log: Log = []
        proxies = [start(n, log, p) for n, p in zip(nodes, (p1, p2, p3), strict=True)]
        assert await ask(proxies[2], Hit, timeout=3) == 1
        assert await ask(proxies[1], Hit, timeout=3) == 2
        assert starts(log) == [p1]


async def test_messages_before_identification_are_buffered():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        log: Log = []
        start(a, log, p1)
        proxy = start(b, log, p2)
        replies: list[int] = []
        # held here: a temp reply path lives as long as its ref
        probes = [FunctionRef("probe", replies.append) for _ in range(3)]
        for p in probes:
            proxy.tell(Hit(p))  # before the proxy has found the instance
        assert await ask(proxy, Hit, timeout=3) == 4
        assert sorted(replies) == [1, 2, 3]


async def test_hand_over_when_the_oldest_leaves():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        log: Log = []
        proxies = [start(n, log, p) for n, p in zip((a, b, c), (p1, p2, p3), strict=True)]
        assert await ask(proxies[2], Hit, timeout=3) == 1
        Cluster(b).manager.tell(Leave(Address("c", "127.0.0.1", p1)))
        await asyncio.wait_for(a.when_terminated(), timeout=5)
        await eventually(lambda: starts(log) == [p1, p2], timeout=5)
        assert stops(log) == [p1] and never_two_at_once(log)
        assert await ask(proxies[2], Hit, timeout=3) == 1  # a fresh instance


async def test_proxies_on_the_host_and_elsewhere_follow_the_hand_over():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        log: Log = []
        proxies = [start(n, log, p) for n, p in zip((a, b, c), (p1, p2, p3), strict=True)]
        assert await ask(proxies[0], Hit, timeout=3) == 1  # the proxy on the host itself
        assert await ask(proxies[1], Hit, timeout=3) == 2  # the same instance from elsewhere
        Cluster(c).manager.tell(Leave(Address("c", "127.0.0.1", p1)))
        await asyncio.wait_for(a.when_terminated(), timeout=5)
        await eventually(lambda: starts(log) == [p1, p2], timeout=5)
        assert await ask(proxies[1], Hit, timeout=3) == 1  # the new host's own proxy
        assert await ask(proxies[2], Hit, timeout=3) == 2  # and a proxy elsewhere, same instance


async def test_crashed_oldest_is_replaced_only_after_removal():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        log: Log = []
        proxies = [start(n, log, p) for n, p in zip((a, b, c), (p1, p2, p3), strict=True)]
        assert await ask(proxies[2], Hit, timeout=3) == 1
        removed_at: list[float] = []
        Cluster(b).subscriptions.tell(
            Subscribe(
                FunctionRef(
                    "removed", lambda _e: removed_at.append(asyncio.get_running_loop().time())
                ),
                MemberRemoved,
            )
        )
        await crash(a)
        await eventually(lambda: unreachable_ports(b) == {p1}, timeout=5)
        await asyncio.sleep(0.3)
        assert starts(log) == [p1]  # unreachable but not removed: nobody takes over
        Cluster(b).manager.tell(Down(Address("c", "127.0.0.1", p1)))
        await eventually(lambda: starts(log) == [p1, p2], timeout=5)
        start_at = next(t for kind, port, t in log if kind == "start" and port == p2)
        assert removed_at and removed_at[0] <= start_at
        # until c has seen the removal its proxy still points at the dead instance, as Pekko's does
        # until its remote DeathWatch fires on removal; a message sent in that window is lost on
        # both
        await eventually(lambda: all_up(c, 2), timeout=5)
        assert await ask(proxies[2], Hit, timeout=3) == 1


async def test_younger_node_leaving_does_not_move_it():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        log: Log = []
        proxies = [start(n, log, p) for n, p in zip((a, b, c), (p1, p2, p3), strict=True)]
        assert await ask(proxies[1], Hit, timeout=3) == 1
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", p3)))
        await asyncio.wait_for(c.when_terminated(), timeout=5)
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        assert starts(log) == [p1] and stops(log) == []
        assert await ask(proxies[1], Hit, timeout=3) == 2


async def test_two_leaves_in_a_row_keep_one_instance():
    p1, p2, p3, p4 = free_port(), free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        nodes = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3, p4)]
        await eventually(lambda: all(all_up(n, 4) for n in nodes), timeout=5)
        log: Log = []
        proxies = [start(n, log, p) for n, p in zip(nodes, (p1, p2, p3, p4), strict=True)]
        assert await ask(proxies[3], Hit, timeout=3) == 1
        Cluster(nodes[3]).manager.tell(Leave(Address("c", "127.0.0.1", p1)))
        Cluster(nodes[3]).manager.tell(Leave(Address("c", "127.0.0.1", p2)))
        await asyncio.wait_for(
            asyncio.gather(nodes[0].when_terminated(), nodes[1].when_terminated()), timeout=8
        )
        await eventually(
            lambda: starts(log)[-1:] == [p3] and len(starts(log)) - len(stops(log)) == 1, timeout=5
        )
        assert never_two_at_once(log)
        assert await ask(proxies[3], Hit, timeout=3) == 1


async def test_a_failed_instance_is_not_restarted_and_proxies_buffer():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        log: Log = []
        proxy_a, proxy_b = start(a, log, p1), start(b, log, p2)
        assert await ask(proxy_b, Hit, timeout=3) == 1
        proxy_a.tell(Crash())
        await eventually(lambda: stops(log) == [p1], timeout=3)
        with pytest.raises(AskTimeoutError):
            await ask(proxy_b, Hit, timeout=0.3)  # buffered: nobody is there to answer
        assert starts(log) == [p1]
        # past one identification interval: b's proxy has noticed the instance is gone
        await asyncio.sleep(0.1)
        # buffered now, not sent into the void
        pending = asyncio.ensure_future(ask(proxy_b, Hit, timeout=5))
        # a hands over nothing; b starts a fresh instance
        Cluster(b).manager.tell(Leave(Address("c", "127.0.0.1", p1)))
        assert await pending == 1
        assert starts(log) == [p1, p2]


async def test_buffer_size_drops_the_oldest_message():
    port = free_port()
    async with node(port, [port]) as a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        log: Log = []
        proxy = start(
            a,
            log,
            port,
            ClusterSingletonSettings(singleton_identification_interval=0.02, buffer_size=1),
        )
        received: list[str] = []
        for name in ("first", "second", "third"):
            proxy.tell(Hit(FunctionRef(name, lambda _n, name=name: received.append(name))))
        await eventually(lambda: received == ["third"], timeout=3)
        await asyncio.sleep(0.1)
        assert received == ["third"]


async def test_a_leaver_whose_successor_never_answers_gives_up_after_the_take_over_retries():
    """As Pekko's WasOldest with selfExited: after max_take_over_retries the leaver stops its
    instance and exits, well before the exiting-phase timeout."""
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a = await stack.enter_async_context(node(p1, [p1], exiting_timeout=2.0))
        b = await stack.enter_async_context(node(p2, [p1], exiting_timeout=2.0))
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        log: Log = []
        start(a, log, p1)
        start(b, log, p2)
        await eventually(lambda: starts(log) == [p1], timeout=3)
        remoting = a.remoting
        assert remoting is not None
        deliver = remoting._handle  # pyright: ignore[reportPrivateUsage]

        def deaf_to_hand_over(frame: Json) -> None:  # a never hears b's hand-over requests
            message = frame.get("message") if isinstance(frame, dict) else None
            if isinstance(message, dict) and message.get("$type") in (
                "TakeOverFromMe",
                "HandOverToMe",
            ):
                return
            deliver(frame)

        remoting._handle = deaf_to_hand_over  # pyright: ignore[reportPrivateUsage]
        began = asyncio.get_running_loop().time()
        Cluster(b).manager.tell(Leave(Address("c", "127.0.0.1", p1)))
        await asyncio.wait_for(a.when_terminated(), timeout=3)
        # the retries ran out, not the exiting timeout
        assert asyncio.get_running_loop().time() - began < 2.0
        assert stops(log) == [p1]


async def test_a_stuck_manager_restarts_and_takes_over():
    """b's hand-over requests never reach a, so b runs out of retries while a is still a member and
    fails with ClusterSingletonManagerIsStuck; its manager restarts, as under Pekko's system
    guardian, and takes over once a is gone."""
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a = await stack.enter_async_context(node(p1, [p1], exiting_timeout=0.5))
        b = await stack.enter_async_context(node(p2, [p1], exiting_timeout=0.5))
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        log: Log = []
        start(a, log, p1)
        proxy_b = start(b, log, p2)
        await eventually(lambda: starts(log) == [p1], timeout=3)
        remoting = a.remoting
        assert remoting is not None
        deliver = remoting._handle  # pyright: ignore[reportPrivateUsage]
        remoting._handle = lambda frame: (
            None
            if (
                isinstance(frame, dict)
                and isinstance(frame.get("message"), dict)
                and frame["message"].get("$type") == "HandOverToMe"
            )
            else deliver(frame)
        )  # pyright: ignore[reportPrivateUsage]
        Cluster(b).manager.tell(Leave(Address("c", "127.0.0.1", p1)))
        await asyncio.wait_for(a.when_terminated(), timeout=5)
        await eventually(lambda: starts(log) == [p1, p2], timeout=5)
        assert await ask(proxy_b, Hit, timeout=3) == 1


async def test_a_single_node_leave_exits_promptly():
    port = free_port()
    async with node(port, [port], exiting_timeout=5.0) as a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        log: Log = []
        proxy = start(a, log, port)
        assert await ask(proxy, Hit, timeout=3) == 1
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", port)))
        # it removes itself from the age order: no 5 s wait
        await asyncio.wait_for(a.when_terminated(), timeout=2)
        assert stops(log) == [port]


async def test_init_twice_returns_the_same_proxy():
    port = free_port()
    async with node(port, [port]) as a:
        log: Log = []
        assert start(a, log, port) is start(a, log, port)


async def test_init_without_a_cluster_raises():
    async with ActorSystem(Behaviors.ignore, "plain") as system:
        with pytest.raises(RuntimeError):
            ClusterSingleton(system)
