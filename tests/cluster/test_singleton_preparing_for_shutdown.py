"""The singleton manager while the cluster prepares for full shutdown: the guards and cases
ClusterSingletonManager.scala gives its preparingForFullShutdown flag.

These are ohra's own tests. Pekko's only spec for it is the multi-jvm
ClusterSingletonManagerPreparingForShutdownSpec, ported in
test_cluster_singleton_manager_preparing_for_shutdown_spec.py. Where a test injects an event, it
tells the manager the event directly, with the self member replaced to the status: that is the
only way to reach these states on demand.

The send-only guards each have a test that fails when that guard alone is removed: the guard's
message would reach a probe, or arrive over the wire at the peer."""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from contextlib import AsyncExitStack, contextmanager
from dataclasses import dataclass, replace
from typing import Any

import pytest

from ohra import ActorRef, ActorSystem, AskTimeoutError, Behaviors, ClusterSingleton, ask
from ohra import SingletonActor
from ohra.cluster.core import (
    Cluster,
    Leave,
    MemberDowned,
    MemberPreparingForShutdown,
    MemberReadyForShutdown,
)
from ohra.cluster.singleton import (
    HandOverDone,
    HandOverInProgress,
    HandOverToMe,
    IdentifySingleton,
    InitialOldestState,
    OldestChanged,
    SingletonIdentity,
    TakeOverFromMe,
    _HandOverRetry,  # pyright: ignore[reportPrivateUsage]
    _TakeOverRetry,  # pyright: ignore[reportPrivateUsage]
)
from ohra.remote.serialization import Json

from ..util import eventually, free_port
from .receptionist.test_cluster_receptionist_spec import probe
from .test_cluster import all_up
from .test_singleton import FAST_SINGLETON, Hit, Log, node, start, starts


@dataclass(frozen=True)
class Ignored: ...


def stubborn() -> SingletonActor[Any]:
    """A singleton that ignores its stop message, so its manager waits in HandingOver or
    Stopping."""
    return (
        SingletonActor(Behaviors.ignore, "counter")
        .with_stop_message(Ignored())
        .with_settings(FAST_SINGLETON)
    )


def manager(system: ActorSystem[Any]) -> ActorRef[Any]:
    return system.resolve(f"{Cluster(system).self_address.root}/system/singletonManagercounter")


def preparing(system: ActorSystem[Any]) -> MemberPreparingForShutdown:
    return MemberPreparingForShutdown(
        replace(Cluster(system).self_member, status="preparing_for_shutdown")
    )


async def await_oldest_with_instance(system: ActorSystem[Any]) -> None:
    """Until the manager is Oldest with its instance: before, it answers IdentifySingleton with
    None. Each attempt has its own probe, so a late answer cannot reach a later check."""

    async def identified() -> None:
        p = probe(system)
        manager(system).tell(IdentifySingleton(p.ref, 0))
        identity = await p.expect_message_type(SingletonIdentity, 0.2)
        assert identity.instance is not None

    await probe(system).await_assert(identified, max=3)


async def await_younger(system: ActorSystem[Any]) -> None:
    """Until the manager is Younger: before, in Start, it drops HandOverToMe. Each attempt has its
    own probe, so a late HandOverDone cannot reach a later check."""

    async def confirmed() -> None:
        p = probe(system)
        manager(system).tell(HandOverToMe(p.ref))
        await p.expect_message_type(HandOverDone, 0.2)

    await probe(system).await_assert(confirmed, max=3)


@contextmanager
def arrivals(system: ActorSystem[Any]) -> Generator[list[str]]:
    """The type of every message that reaches ``system`` over the wire while the block runs."""
    remoting = system.remoting
    assert remoting is not None
    deliver = remoting._handle  # pyright: ignore[reportPrivateUsage]
    seen: list[str] = []

    def record(frame: Json) -> None:
        message = frame.get("message") if isinstance(frame, dict) else None
        if isinstance(message, dict):
            seen.append(str(message.get("$type")))
        deliver(frame)

    remoting._handle = record  # pyright: ignore[reportPrivateUsage]
    try:
        yield seen
    finally:
        remoting._handle = deliver  # pyright: ignore[reportPrivateUsage]


async def test_a_younger_manager_stops_confirming_hand_overs_once_preparing():
    pa, pb = sorted(free_port() for _ in range(2))
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [pa])) for p in (pa, pb)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        log: Log = []
        start(a, log, pa)
        start(b, log, pb)
        await await_younger(b)

        p = probe(b)
        manager(b).tell(preparing(b))
        manager(b).tell(HandOverToMe(p.ref))
        await p.expect_no_message(0.3)


async def test_a_handing_over_manager_gives_up_on_self_preparing():
    port = free_port()
    async with node(port, [port]) as a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        ClusterSingleton(a).init(stubborn())
        await await_oldest_with_instance(a)
        p = probe(a)

        manager(a).tell(HandOverToMe(p.ref))
        await p.expect_message_type(HandOverInProgress, 3)
        manager(a).tell(preparing(a))
        await p.expect_terminated(manager(a), 3)


async def test_a_stopping_manager_does_not_wait_for_the_instance_on_self_ready():
    port = free_port()
    async with node(port, [port]) as a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        ClusterSingleton(a).init(stubborn())
        await await_oldest_with_instance(a)
        p = probe(a)

        self_member = Cluster(a).self_member
        manager(a).tell(MemberDowned(replace(self_member, status="down")))
        manager(a).tell(MemberReadyForShutdown(replace(self_member, status="ready_for_shutdown")))
        await p.expect_terminated(manager(a), 3)


async def test_a_singleton_started_after_the_node_is_ready_never_starts_its_instance():
    """Passes without the manager's flag too: it pins Task 2's behavior that a replayed state
    without MemberUp for self publishes no SelfUp, so the manager stays in Start, as Pekko's
    registerOnMemberUp never fires."""
    port = free_port()
    async with node(port, [port]) as a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        Cluster(a).prepare_for_full_cluster_shutdown()
        await eventually(lambda: Cluster(a).self_member.status == "ready_for_shutdown", timeout=3)
        log: Log = []
        proxy = start(a, log, port)
        # it does not happen: an instance would answer within a few milliseconds
        with pytest.raises(AskTimeoutError):
            await ask(proxy, Hit, timeout=0.5)
        assert starts(log) == []


async def test_the_new_oldest_does_not_start_the_singleton_while_preparing():
    ports = sorted(free_port() for _ in range(3))
    async with AsyncExitStack() as stack:
        a, b, c = [await stack.enter_async_context(node(p, [ports[0]])) for p in ports]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        log: Log = []
        proxies = [start(n, log, p) for n, p in zip((a, b, c), ports, strict=True)]
        assert await ask(proxies[1], Hit, timeout=3) == 1
        assert starts(log) == [ports[0]]

        Cluster(a).prepare_for_full_cluster_shutdown()
        await eventually(
            lambda: all(Cluster(n).self_member.status == "ready_for_shutdown" for n in (a, b, c)),
            timeout=5,
        )
        Cluster(a).manager.tell(Leave(Cluster(a).self_address))
        await eventually(
            lambda: all(len(Cluster(n).state.members) == 2 for n in (b, c)), timeout=5
        )
        # it does not happen: without the flag the new oldest would start the instance after the
        # hand-over, or once its retries ran out
        wait = FAST_SINGLETON.max_hand_over_retries * FAST_SINGLETON.hand_over_retry_interval
        with pytest.raises(AskTimeoutError):
            await ask(proxies[2], Hit, timeout=wait + 0.5)
        assert starts(log) == [ports[0]]


async def test_a_preparing_younger_that_becomes_oldest_asks_nobody_to_hand_over():
    """Scala 680-684, the HandOverToMe on OldestChanged to self, and 803-810, the one on each
    HandOverRetry."""
    pa, pb = sorted(free_port() for _ in range(2))
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [pa])) for p in (pa, pb)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        log: Log = []
        start(a, log, pa)
        start(b, log, pb)
        await await_younger(b)

        with arrivals(a) as seen:
            manager(b).tell(preparing(b))
            # BecomingOldest, with a as the previous oldest, still a member
            manager(b).tell(OldestChanged(Cluster(b).self_unique_address))
            manager(b).tell(_HandOverRetry(1))  # as its timer would, without waiting on it
            # it does not happen: six hand-over retry intervals
            await asyncio.sleep(0.3)
        assert "HandOverToMe" not in seen


async def test_a_preparing_becoming_oldest_does_not_answer_take_over_requests():
    """Scala 785-799: neither the request from no known previous oldest nor the one from the
    previous oldest gets HandOverToMe."""
    port = free_port()
    async with node(port, [port]) as a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        Cluster(a).prepare_for_full_cluster_shutdown()
        await eventually(lambda: Cluster(a).self_member.status == "ready_for_shutdown", timeout=3)
        log: Log = []
        start(a, log, port)
        # the manager stays in Start (no SelfUp once Ready), so the test moves it on itself; the
        # injected event sets the flag before anything else reaches the manager
        manager(a).tell(preparing(a))
        self_address = Cluster(a).self_unique_address
        manager(a).tell(InitialOldestState((self_address,), safe_to_be_oldest=False))

        p = probe(a)
        manager(a).tell(TakeOverFromMe(p.ref))  # no previous oldest: p's node becomes it
        await p.expect_no_message(0.3)
        manager(a).tell(TakeOverFromMe(p.ref))  # now from the previous oldest
        await p.expect_no_message(0.3)


async def test_a_preparing_oldest_that_is_no_longer_oldest_asks_nobody_to_take_over():
    """Scala 916-940, the TakeOverFromMe on OldestChanged, and 1000-1010, the one on each
    TakeOverRetry."""
    pa, pb = sorted(free_port() for _ in range(2))
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [pa])) for p in (pa, pb)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        log: Log = []
        start(a, log, pa)
        await await_oldest_with_instance(a)

        with arrivals(b) as seen:
            manager(a).tell(preparing(a))
            manager(a).tell(OldestChanged(Cluster(b).self_unique_address))  # WasOldest
            manager(a).tell(_TakeOverRetry(1))
            # it does not happen: six hand-over retry intervals
            await asyncio.sleep(0.3)
        assert "TakeOverFromMe" not in seen


async def test_a_preparing_handing_over_manager_does_not_answer_a_retry():
    """Scala 1066-1071. HandingOver does not handle member events, so the flag is set in
    Oldest."""
    port = free_port()
    async with node(port, [port]) as a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        ClusterSingleton(a).init(stubborn())
        await await_oldest_with_instance(a)
        manager(a).tell(preparing(a))  # Oldest sets the flag and stays

        p = probe(a)
        manager(a).tell(HandOverToMe(p.ref))
        await p.expect_message_type(HandOverInProgress, 3)  # gotoHandingOver's, not guarded
        manager(a).tell(HandOverToMe(p.ref))  # the retry
        await p.expect_no_message(0.3)
