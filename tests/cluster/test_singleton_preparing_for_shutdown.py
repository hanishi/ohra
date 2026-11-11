"""The singleton manager while the cluster prepares for full shutdown: the guards and cases
ClusterSingletonManager.scala gives its preparingForFullShutdown flag.

These are ohra's own tests. Pekko's only spec for it is the multi-jvm
ClusterSingletonManagerPreparingForShutdownSpec, ported in
test_cluster_singleton_manager_preparing_for_shutdown_spec.py. Where a test injects an event, it
tells the manager the event directly, with the self member replaced to the status: that is the
only way to reach these states on demand."""

from __future__ import annotations

from contextlib import AsyncExitStack
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
    SingletonIdentity,
)
from ohra.remote import Address
from ohra.testkit import TestProbe

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


async def await_oldest_with_instance(system: ActorSystem[Any], p: TestProbe[Any]) -> None:
    """Until the manager is Oldest with its instance: before, it answers IdentifySingleton with
    None."""

    async def identified() -> None:
        manager(system).tell(IdentifySingleton(p.ref, 0))
        identity = await p.expect_message_type(SingletonIdentity, 0.2)
        assert identity.instance is not None

    await p.await_assert(identified, max=3)


async def test_a_younger_manager_stops_confirming_hand_overs_once_preparing():
    pa, pb = sorted(free_port() for _ in range(2))
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [pa])) for p in (pa, pb)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        log: Log = []
        start(a, log, pa)
        start(b, log, pb)
        p = probe(b)

        async def confirmed() -> None:
            # dropped in Start, before the manager is Younger
            manager(b).tell(HandOverToMe(p.ref))
            await p.expect_message_type(HandOverDone, 0.2)

        await p.await_assert(confirmed, max=3)

        self_member = Cluster(b).self_member
        manager(b).tell(
            MemberPreparingForShutdown(replace(self_member, status="preparing_for_shutdown"))
        )
        manager(b).tell(HandOverToMe(p.ref))
        await p.expect_no_message(0.3)


async def test_a_handing_over_manager_gives_up_on_self_preparing():
    port = free_port()
    async with node(port, [port]) as a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        ClusterSingleton(a).init(stubborn())
        p = probe(a)
        await await_oldest_with_instance(a, p)

        manager(a).tell(HandOverToMe(p.ref))
        await p.expect_message_type(HandOverInProgress, 3)
        self_member = Cluster(a).self_member
        manager(a).tell(
            MemberPreparingForShutdown(replace(self_member, status="preparing_for_shutdown"))
        )
        await p.expect_terminated(manager(a), 3)


async def test_a_stopping_manager_does_not_wait_for_the_instance_on_self_ready():
    port = free_port()
    async with node(port, [port]) as a:
        await eventually(lambda: all_up(a, 1), timeout=3)
        ClusterSingleton(a).init(stubborn())
        p = probe(a)
        await await_oldest_with_instance(a, p)

        self_member = Cluster(a).self_member
        manager(a).tell(MemberDowned(replace(self_member, status="down")))
        manager(a).tell(MemberReadyForShutdown(replace(self_member, status="ready_for_shutdown")))
        await p.expect_terminated(manager(a), 3)


async def test_a_singleton_started_after_the_node_is_ready_never_starts_its_instance():
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
        Cluster(a).manager.tell(Leave(Address("c", "127.0.0.1", ports[0])))
        await eventually(
            lambda: all(len(Cluster(n).state.members) == 2 for n in (b, c)), timeout=5
        )
        # it does not happen: without the flag the new oldest would start the instance after the
        # hand-over, or once its retries ran out
        wait = FAST_SINGLETON.max_hand_over_retries * FAST_SINGLETON.hand_over_retry_interval
        with pytest.raises(AskTimeoutError):
            await ask(proxies[2], Hit, timeout=wait + 0.5)
        assert starts(log) == [ports[0]]
