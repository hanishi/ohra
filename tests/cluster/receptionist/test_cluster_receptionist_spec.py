"""Ported from Apache Pekko's ClusterReceptionistSpec (cluster-typed/src/test/scala/org/apache/
pekko/cluster/typed/internal/receptionist/ClusterReceptionistSpec.scala), the receptionist on
several cluster nodes in one process. One test per case, named after Pekko's, with the Scala line:

- eventually_replicate_registrations_to_the_other_side (97)
- handle_registrations_before_joining (135)
- remove_registrations_when_node_dies (165), remove_registrations_when_node_leaves (169), both
  through testNodeRemoval (172)
- not_remove_registrations_when_self_is_shutdown (231)
- work_with_services_registered_before_node_joins_cluster (280)
- handle_a_new_incarnation_of_the_same_node_well (338)
- handle_a_new_incarnation_of_the_same_node_that_is_no_longer_part_of_same_cluster (436)
- not_lose_removals_on_concurrent_updates_to_same_key (543)
- not_conflict_with_the_cluster_client_receptionist_default_name (614)
- handle_unregistration_and_re_registration_of_services (623)
- handle_unregistration_per_key_not_per_actor (681)
- handle_concurrent_unregistration_and_registration_on_different_nodes (734)
- notify_subscribers_when_registering_and_joining_simultaneously (793)
- never_use_durable_store (856)
- test_a_joining_node_sees_all_500_registrations_of_one_key is not in Pekko's spec.

``ClusterReceptionistSpec.config`` (Scala 49-76) maps to ohra as follows:

- pekko.cluster.typed.receptionist.pruning-interval = 1 s: ``ClusterReceptionistSettings(
  pruning_interval=1.0)``, Pekko's value.
- pekko.cluster.failure-detector.acceptable-heartbeat-pause = 3s: ``ClusterSettings(
  acceptable_heartbeat_pause=3.0)``, Pekko's value (20 s in the new-cluster case, as Pekko). The
  other cluster intervals are ``FAST``'s, as the other cluster tests shorten them.
- pekko.cluster.distributed-data.durable.keys = ["*"]: ``ReplicatorSettings(durable_keys=
  frozenset({"*"}))``, in every system; the receptionist forces it empty.
- pekko.cluster.distributed-data gossip-interval (Pekko's default 2 s): 0.1 s, as
  tests/cluster/test_receptionist_cluster.py; notify-subscribers-interval stays Pekko's 0.5 s,
  because a shorter one lets the second Listing of a node removal (NodeRemoved, then the leader's
  Update coming back as Changed) land between the Listing the case waits for and the next
  Registered. The case that sets the receptionist's own gossip-interval = 500ms keeps it.
- delta-crdt.enabled = false (case 543): ``ReplicatorSettings(delta_crdt_enabled=False)``.
- receptionist.write-consistency = all (cases 436, 793): ``Replicator.WriteAll(5.0)``, the 5 s of
  Pekko's ``writeTimeout``.
- receptionist.distributed-data.gossip-interval = 500ms (793):
  ``ClusterReceptionistSettings(replicator_settings=ReplicatorSettings(gossip_interval=0.5))``.
- loglevel, jmx.multi-mbeans-in-same-jvm, the classic and Artery ports (0, i.e. a free port: each
  system takes ``free_port()``), remote.retry-gate-closed-for = 1 s: no ohra counterpart.

Differences from the Scala:

- ``system.close()`` in the node dies and the new incarnation cases (165, 338) is ``crash``: the
  transport is closed and the system terminated, so nothing the node says on its way down gets
  out.
- The new incarnation (338) joins with ``seed_nodes`` and ``retry_unsuccessful_join_after=1.0``,
  which is what Pekko's ``JoinSeedNodes`` plus ``retry-unsuccessful-join-after = 1s`` do.
- ohra's actor paths carry no uid, so the ``ref.path.uid should === service3Uid`` assertion (418)
  has no counterpart; the path assertion stays.
- ``Listing`` equality is the dataclass's, as the Scala case class's: key, both instance sets and
  ``services_were_added_or_removed``, which ``Listing(key, set)`` sets to true. A ``Listing`` of
  remote refs is compared by their paths.
- The CBOR serialization and ``LogCapturing`` have no counterpart.
- ``Behaviors.empty`` is ``Behaviors.ignore``.
"""

from __future__ import annotations

import asyncio
import random
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from ohra import ActorRef, ActorSystem, Behavior, Behaviors, remotable
from ohra.cluster.core import Cluster, ClusterSettings, Down, Join, Leave
from ohra.cluster.ddata import Replicator, ReplicatorSettings
from ohra.cluster.receptionist import ClusterReceptionistSettings
from ohra.receptionist import (
    Deregister,
    Deregistered,
    Find,
    Listing,
    Receptionist,
    Register,
    Registered,
    ServiceKey,
    Subscribe,
)
from ohra.remote import Address
from ohra.testkit import FishingOutcome, FishingOutcomes, TestProbe
from ohra.testkit.settings import TestKitSettings

from ...util import free_port
from ..test_cluster import FAST, FAST_SHUTDOWN

GOSSIP = 0.1
NOTIFY = 0.5


@remotable
@dataclass(frozen=True)
class SpecPong: ...


@remotable
@dataclass(frozen=True)
class SpecPing:
    respond_to: ActorRef[SpecPong]


@remotable
@dataclass(frozen=True)
class SpecPerish: ...


type PingProtocol = SpecPing | SpecPerish


@Behaviors.receive_message
def ping_pong_behavior(msg: PingProtocol) -> Behavior[PingProtocol]:
    match msg:
        case SpecPing(respond_to):
            respond_to.tell(SpecPong())
            return Behaviors.same
        case SpecPerish():
            return Behaviors.stopped()


PING_KEY = ServiceKey[PingProtocol]("pingy")
ANOTHER_KEY = ServiceKey[PingProtocol]("pingy-2")


@dataclass
class Host:
    """What a TestProbe needs of a test kit."""

    system: ActorSystem[Any]
    settings: TestKitSettings = field(default_factory=TestKitSettings)


def probe(system: ActorSystem[Any]) -> TestProbe[Any]:
    return TestProbe(Host(system))


def new_system(
    port: int,
    *,
    seeds: tuple[int, ...] = (),
    pause: float = 3.0,
    pruning_interval: float = 1.0,
    write_consistency: Replicator.WriteConsistency | None = None,
    replicator_settings: ReplicatorSettings | None = None,
    ddata: ReplicatorSettings | None = None,
    retry_join: float | None = None,
) -> ActorSystem[Any]:
    """ClusterReceptionistSpec.config on a node at port; ``seeds`` makes it join by itself."""
    consistency: dict[str, Any] = (
        {} if write_consistency is None else {"write_consistency": write_consistency}
    )
    receptionist = ClusterReceptionistSettings(
        pruning_interval=pruning_interval, replicator_settings=replicator_settings, **consistency
    )
    extra: dict[str, Any] = (
        {} if retry_join is None else {"retry_unsuccessful_join_after": retry_join}
    )
    fast: dict[str, Any] = {**FAST, "acceptable_heartbeat_pause": pause}
    cluster = ClusterSettings(
        seed_nodes=tuple(Address("c", "127.0.0.1", p) for p in seeds),
        **fast,
        **extra,
        distributed_data=ddata
        or ReplicatorSettings(
            gossip_interval=GOSSIP,
            notify_subscribers_interval=NOTIFY,
            durable_keys=frozenset({"*"}),
        ),
        receptionist=receptionist,
    )
    return ActorSystem(
        Behaviors.ignore, "c", address=("127.0.0.1", port), cluster=cluster, shutdown=FAST_SHUTDOWN
    )


def join(system: ActorSystem[Any], seed: ActorSystem[Any]) -> None:
    Cluster(system).manager.tell(Join(Cluster(seed).self_address))


def up_count(system: ActorSystem[Any]) -> int:
    return sum(m.status == "up" for m in Cluster(system).state.members)


def spawn(
    system: ActorSystem[Any], behavior: Behavior[Any], name: str | None = None
) -> ActorRef[Any]:
    return system._root.spawn_child(behavior, name, None).ref  # pyright: ignore[reportPrivateUsage]


def receptionist(system: ActorSystem[Any]) -> ActorRef[Any]:
    return Receptionist(system).ref


async def crash(system: ActorSystem[Any]) -> None:
    """Pekko's ``system.close()``: the node vanishes, nothing it says on the way down gets out."""
    assert system.remoting is not None
    await system.remoting.close()
    system.terminate()
    await system.when_terminated()


def listing(key: ServiceKey[Any], *refs: ActorRef[Any]) -> Listing[Any]:
    """Pekko's ``Listing(key, Set(refs))``."""
    return Listing(key, frozenset(refs), frozenset(refs), True)


def as_listing(msg: Any) -> Listing[Any]:
    assert isinstance(msg, Listing), f"Unexpected: {msg!r}"
    return cast(Listing[Any], msg)


def instances(msg: Any, key: ServiceKey[Any]) -> frozenset[ActorRef[Any]]:
    """``serviceInstances(key)`` of a Listing; a Listing of another key is an error."""
    found = as_listing(msg)
    assert found.key == key, f"Wrong key [{key}] used, must use listing key [{found.key}]"
    return found.service_instances


def paths(refs: frozenset[ActorRef[Any]]) -> set[str]:
    return {r.path for r in refs}


async def cluster_of_two(
    stack: AsyncExitStack, **settings: Any
) -> tuple[ActorSystem[Any], ActorSystem[Any]]:
    """system1 joins itself, system2 joins system1, and both see two Up members (the awaitAssert
    of the Scala)."""
    system1 = await stack.enter_async_context(new_system(free_port(), **settings))
    system2 = await stack.enter_async_context(new_system(free_port(), **settings))
    join(system1, system1)
    join(system2, system1)
    await probe(system1).await_assert(lambda: _assert_eq(up_count(system1), 2), 10)
    return system1, system2


def _assert_eq(actual: Any, expected: Any) -> None:
    assert actual == expected, f"{actual!r} did not equal {expected!r}"


# --- 97 --------------------------------------------------------------------------------------


async def test_eventually_replicate_registrations_to_the_other_side():
    async with AsyncExitStack() as stack:
        system1, system2 = await cluster_of_two(stack)
        reg_probe1, reg_probe2 = probe(system1), probe(system2)

        receptionist(system2).tell(Subscribe(PING_KEY, reg_probe2.ref))
        await reg_probe2.expect_message(listing(PING_KEY))

        service = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service))

        remote_service_refs = instances(await reg_probe2.expect_message_type(Listing), PING_KEY)
        (the_ref,) = remote_service_refs
        the_ref.tell(SpecPing(reg_probe2.ref))
        await reg_probe2.expect_message(SpecPong())

        service.tell(SpecPerish())
        await reg_probe2.expect_message(listing(PING_KEY))


# --- 135 -------------------------------------------------------------------------------------


async def test_handle_registrations_before_joining():
    async with AsyncExitStack() as stack:
        system1 = await stack.enter_async_context(new_system(free_port()))
        system2 = await stack.enter_async_context(new_system(free_port()))
        reg_probe1, reg_probe2 = probe(system1), probe(system2)
        service = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service))
        receptionist(system2).tell(Subscribe(PING_KEY, reg_probe2.ref))
        await reg_probe2.expect_message(listing(PING_KEY))

        join(system1, system1)
        join(system2, system1)

        remote_service_refs = instances(
            await reg_probe2.expect_message_type(Listing, 10), PING_KEY
        )
        assert next(iter(remote_service_refs)).path.startswith(Cluster(system1).self_address.root)


# --- 165, 169 --------------------------------------------------------------------------------


async def node_removal(down: bool) -> None:
    async with AsyncExitStack() as stack:
        system1, system2 = await cluster_of_two(stack)
        reg_probe1, reg_probe2 = probe(system1), probe(system2)

        receptionist(system1).tell(Subscribe(PING_KEY, reg_probe1.ref))
        await reg_probe1.expect_message(listing(PING_KEY))

        service1 = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service1, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service1))

        await reg_probe1.expect_message(listing(PING_KEY, service1))

        service2 = spawn(system2, ping_pong_behavior)
        receptionist(system2).tell(Register(PING_KEY, service2, reg_probe2.ref))
        await reg_probe2.expect_message(Registered(PING_KEY, service2))

        service_refs2 = instances(await reg_probe1.expect_message_type(Listing), PING_KEY)
        assert len(service_refs2) == 2

        if down:
            # abrupt termination
            await crash(system2)
            Cluster(system1).manager.tell(Down(Cluster(system2).self_address))
        else:
            Cluster(system1).manager.tell(Leave(Cluster(system2).self_address))

        await reg_probe1.await_assert(
            # we will also potentially get an update that the service was unreachable before the
            # expected one
            lambda: reg_probe1.expect_message(listing(PING_KEY, service1), 10),
            10,
        )

        # register another after removal
        service1b = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service1b, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service1b))
        await reg_probe1.expect_message(listing(PING_KEY, service1, service1b))


async def test_remove_registrations_when_node_dies():
    await node_removal(down=True)


async def test_remove_registrations_when_node_leaves():
    await node_removal(down=False)


# --- 231 -------------------------------------------------------------------------------------


async def test_not_remove_registrations_when_self_is_shutdown():
    async with AsyncExitStack() as stack:
        system1, system2 = await cluster_of_two(stack)
        reg_probe1, reg_probe2 = probe(system1), probe(system2)

        receptionist(system2).tell(Subscribe(PING_KEY, reg_probe2.ref))
        await reg_probe2.expect_message(listing(PING_KEY))

        service1 = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service1, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service1))

        assert len(instances(await reg_probe2.expect_message_type(Listing), PING_KEY)) == 1

        service2 = spawn(system2, ping_pong_behavior)
        receptionist(system2).tell(Register(PING_KEY, service2, reg_probe2.ref))
        await reg_probe2.expect_message(Registered(PING_KEY, service2))

        assert len(instances(await reg_probe2.expect_message_type(Listing), PING_KEY)) == 2

        Cluster(system1).shutdown()

        await reg_probe2.expect_no_message(3)

        Cluster(system2).manager.tell(Down(Cluster(system1).self_address))
        # service1 removed
        await reg_probe2.await_assert(
            # we will also potentially get an update that the service was unreachable before the
            # expected one
            lambda: reg_probe2.expect_message(listing(PING_KEY, service2), 10),
            10,
        )


# --- 280 -------------------------------------------------------------------------------------


async def test_work_with_services_registered_before_node_joins_cluster():
    async with AsyncExitStack() as stack:
        system1 = await stack.enter_async_context(new_system(free_port()))
        system2 = await stack.enter_async_context(new_system(free_port()))
        join(system1, system1)

        reg_probe1, reg_probe2 = probe(system1), probe(system2)

        receptionist(system1).tell(Subscribe(PING_KEY, reg_probe1.ref))
        await reg_probe1.expect_message(listing(PING_KEY))

        service2 = spawn(system2, ping_pong_behavior)
        receptionist(system2).tell(Register(PING_KEY, service2, reg_probe2.ref))
        await reg_probe2.expect_message(Registered(PING_KEY, service2))

        reply2: TestProbe[Any] = probe(system2)

        # awaitAssert because it is not immediately included in the registry (round trip to ddata)
        async def sees_service2() -> None:
            receptionist(system2).tell(Find(PING_KEY, reply2.ref))
            assert instances(await reply2.receive_message(), PING_KEY) == frozenset({service2})

        await reply2.await_assert(sees_service2)

        # and it shouldn't be removed (wait longer than pruning-interval)
        await asyncio.sleep(2)
        receptionist(system2).tell(Find(PING_KEY, reply2.ref))
        assert instances(await reply2.receive_message(), PING_KEY) == frozenset({service2})

        # then we join the cluster
        join(system2, system1)
        await reg_probe1.await_assert(lambda: _assert_eq(up_count(system1), 2), 10)

        # and the subscriber on node1 should see the service
        remote_service_refs = instances(await reg_probe1.expect_message_type(Listing), PING_KEY)
        next(iter(remote_service_refs)).tell(SpecPing(reg_probe1.ref))
        await reg_probe1.expect_message(SpecPong())

        # abrupt termination
        system2.terminate()
        await asyncio.wait_for(system2.when_terminated(), 10)
        Cluster(system1).manager.tell(Down(Cluster(system2).self_address))
        await reg_probe1.await_assert(
            # we will also potentially get an update that the service was unreachable before the
            # expected one
            lambda: reg_probe1.expect_message(listing(PING_KEY), 10),
            10,
        )


# --- 338 -------------------------------------------------------------------------------------


async def test_handle_a_new_incarnation_of_the_same_node_well():
    async with AsyncExitStack() as stack:
        system1, system2 = await cluster_of_two(stack)
        reg_probe1, reg_probe2 = probe(system1), probe(system2)

        receptionist(system1).tell(Subscribe(PING_KEY, reg_probe1.ref))
        await reg_probe1.expect_message(listing(PING_KEY))

        service2 = spawn(system2, ping_pong_behavior, "instance")
        receptionist(system2).tell(Register(PING_KEY, service2, reg_probe2.ref))
        await reg_probe2.expect_message(Registered(PING_KEY, service2))

        # make sure we saw the first incarnation on node1
        remote_service_refs = instances(await reg_probe1.expect_message_type(Listing), PING_KEY)
        next(iter(remote_service_refs)).tell(SpecPing(reg_probe1.ref))
        await reg_probe1.expect_message(SpecPong())

        # abrupt termination but then a node with the same host:port comes online quickly
        port2 = Cluster(system2).self_address.port
        await crash(system2)

        # seed_nodes: the join is retried when the existing member is removed
        system3 = await stack.enter_async_context(
            new_system(port2, seeds=(Cluster(system1).self_address.port,), retry_join=1.0)
        )
        reg_probe3 = probe(system3)

        # and registers the same service key
        service3 = spawn(system3, ping_pong_behavior, "instance")
        receptionist(system3).tell(Register(PING_KEY, service3, reg_probe3.ref))
        await reg_probe3.expect_message(Registered(PING_KEY, service3))

        # make sure it joined fine and node1 has upped it
        node3 = Cluster(system3).self_unique_address

        def node3_is_up() -> None:
            state = Cluster(system1).state
            assert any(
                m.unique_address == node3 and m.status == "up" and m not in state.unreachable
                for m in state.members
            )

        await reg_probe1.await_assert(node3_is_up, 10)

        # we should get either empty message and then updated with the new incarnation actor
        # or just updated with the new service directly
        def fisher(msg: Any) -> FishingOutcome:
            if isinstance(msg, Listing) and len(as_listing(msg).service_instances) == 1:
                return FishingOutcomes.complete()
            if isinstance(msg, Listing):
                return FishingOutcomes.continue_and_ignore()
            return FishingOutcomes.fail(f"Unexpected: {msg!r}")

        msg = (await reg_probe1.fish_for_message(20, fisher))[-1]
        entries = instances(msg, PING_KEY)
        assert len(entries) == 1
        (ref,) = entries
        assert ref.path == f"{Cluster(system3).self_address.root}/user/instance"

        ref.tell(SpecPing(reg_probe1.ref))
        await reg_probe1.expect_message(SpecPong())


# --- 436 -------------------------------------------------------------------------------------


async def test_handle_a_new_incarnation_of_the_same_node_that_is_no_longer_part_of_same_cluster():
    # reproducer of issue #26284
    async with AsyncExitStack() as stack:
        system1, system2 = await cluster_of_two(
            stack, pause=20.0, write_consistency=Replicator.WriteAll(5.0)
        )
        reg_probe1, reg_probe2 = probe(system1), probe(system2)
        reply1: TestProbe[Any] = probe(system1)

        receptionist(system1).tell(Subscribe(PING_KEY, reg_probe1.ref))
        await reg_probe1.expect_message(listing(PING_KEY))

        service1 = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service1, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service1))
        await reg_probe1.expect_message(listing(PING_KEY, service1))

        service2 = spawn(system2, ping_pong_behavior, "instance")
        receptionist(system2).tell(Register(PING_KEY, service2, reg_probe2.ref))
        await reg_probe2.expect_message(Registered(PING_KEY, service2))

        # make sure we saw the first incarnation on node1
        assert len(instances(await reg_probe1.expect_message_type(Listing), PING_KEY)) == 2

        # abrupt termination but then a node with the same host:port comes online quickly
        port2 = Cluster(system2).self_address.port
        system2.terminate()
        await asyncio.wait_for(system2.when_terminated(), 10)

        system3 = await stack.enter_async_context(
            new_system(port2, pause=20.0, write_consistency=Replicator.WriteAll(5.0))
        )
        reg_probe3 = probe(system3)
        # joining itself, i.e. not same cluster
        join(system3, system3)
        await reg_probe3.await_assert(lambda: _assert_eq(up_count(system3), 1))

        # register another
        await asyncio.sleep(2)
        service1b = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service1b, reg_probe1.ref))

        service1c = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service1c, reg_probe1.ref))

        receptionist(system3).tell(Subscribe(PING_KEY, reg_probe3.ref))
        # shouldn't get anything from the other cluster
        await reg_probe3.expect_message(listing(PING_KEY))

        # and registers the same service key
        service3 = spawn(system3, ping_pong_behavior, "instance")
        receptionist(system3).tell(Register(PING_KEY, service3, reg_probe3.ref))
        await reg_probe3.expect_message(Registered(PING_KEY, service3))

        # shouldn't get anything from the other cluster
        await reg_probe3.expect_message(listing(PING_KEY, service3))

        await reply1.expect_no_message(1)
        receptionist(system1).tell(Find(PING_KEY, reply1.ref))
        assert {service1, service1b, service1c} <= instances(
            await reply1.receive_message(), PING_KEY
        )

        await reply1.expect_no_message(1)
        receptionist(system1).tell(Find(PING_KEY, reply1.ref))
        assert {service1, service1b, service1c} <= instances(
            await reply1.receive_message(), PING_KEY
        )


# --- 543 -------------------------------------------------------------------------------------


async def test_not_lose_removals_on_concurrent_updates_to_same_key():
    # disable delta propagation so we can have repeatable concurrent writes without delta reaching
    # between nodes already
    ddata = ReplicatorSettings(
        gossip_interval=GOSSIP,
        notify_subscribers_interval=NOTIFY,
        delta_crdt_enabled=False,
        durable_keys=frozenset({"*"}),
    )
    the_key = ServiceKey[Any]("whatever")
    async with AsyncExitStack() as stack:
        system1 = await stack.enter_async_context(new_system(free_port(), ddata=ddata))
        system2 = await stack.enter_async_context(new_system(free_port(), ddata=ddata))
        join(system1, system1)
        join(system2, system1)

        reg_probe1, reg_probe2 = probe(system1), probe(system2)

        await reg_probe1.await_assert(lambda: _assert_eq(up_count(system1), 2))

        # one actor on each node up front
        @Behaviors.receive_message
        def stoppable(msg: Any) -> Behavior[Any]:
            return Behaviors.stopped() if msg == "stop" else Behaviors.same

        actor1 = spawn(system1, stoppable, "actor1")
        actor2 = spawn(system2, Behaviors.ignore, "actor2")

        receptionist(system1).tell(Register(the_key, actor1))
        receptionist(system1).tell(Subscribe(the_key, reg_probe1.ref))
        await reg_probe1.await_assert(
            lambda: reg_probe1.expect_message(listing(the_key, actor1)), 5
        )

        receptionist(system2).tell(Subscribe(the_key, reg_probe2.ref))

        def has_actors(msg: Any) -> FishingOutcome:
            if isinstance(msg, Listing) and as_listing(msg).service_instances:
                return FishingOutcomes.complete()
            return FishingOutcomes.continue_()

        await reg_probe2.fish_for_message(10, has_actors)

        # TheKey -> Set(actor1) seen by both nodes, now,
        # remove on node1 and add on node2 (hopefully) concurrently
        receptionist(system2).tell(Register(the_key, actor2, reg_probe2.ref))
        actor1.tell("stop")
        await reg_probe2.expect_message(Registered(the_key, actor2))

        # we should now, eventually, see the removal on both nodes
        def one_actor(msg: Any) -> FishingOutcome:
            if isinstance(msg, Listing) and len(as_listing(msg).service_instances) == 1:
                return FishingOutcomes.complete()
            return FishingOutcomes.continue_()

        await reg_probe1.fish_for_message(10, one_actor)
        await reg_probe2.fish_for_message(10, one_actor)


# --- 614 -------------------------------------------------------------------------------------


async def test_not_conflict_with_the_cluster_client_receptionist_default_name():
    async with new_system(free_port()) as system:
        # systemActorOf(Behaviors.ignore, "receptionist")
        ref = system._system_root.spawn_child(Behaviors.ignore, "receptionist", None).ref  # pyright: ignore[reportPrivateUsage]
        assert ref.path.endswith("/system/receptionist")
        assert receptionist(system).path.endswith("/system/clusterReceptionist")


# --- 623 -------------------------------------------------------------------------------------


async def test_handle_unregistration_and_re_registration_of_services():
    async with AsyncExitStack() as stack:
        system1, system2 = await cluster_of_two(stack)
        reg_probe1, reg_probe2 = probe(system1), probe(system2)

        receptionist(system2).tell(Subscribe(PING_KEY, reg_probe2.ref))
        await reg_probe2.expect_message(listing(PING_KEY))

        # register and verify seen on remote side
        service1 = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service1, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service1))
        assert len(instances(await reg_probe2.expect_message_type(Listing), PING_KEY)) == 1

        # another service for the same key
        service2 = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service2, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service2))
        assert len(instances(await reg_probe2.expect_message_type(Listing), PING_KEY)) == 2

        # unregister service1 and verify
        receptionist(system1).tell(Deregister(PING_KEY, service1, reg_probe1.ref))
        await reg_probe1.expect_message(Deregistered(PING_KEY, service1))
        assert len(instances(await reg_probe2.expect_message_type(Listing), PING_KEY)) == 1

        # re-register and verify
        receptionist(system1).tell(Register(PING_KEY, service1, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service1))
        assert len(instances(await reg_probe2.expect_message_type(Listing), PING_KEY)) == 2

        # cover a race between termination and unregistration as well (should lead to only one
        # update)
        receptionist(system1).tell(Deregister(PING_KEY, service1, reg_probe1.ref))
        service1.tell(SpecPerish())

        assert len(instances(await reg_probe2.expect_message_type(Listing), PING_KEY)) == 1
        await reg_probe2.expect_no_message(1)

        Cluster(system1).shutdown()
        Cluster(system2).manager.tell(Down(Cluster(system1).self_address))


# --- 681 -------------------------------------------------------------------------------------


async def test_handle_unregistration_per_key_not_per_actor():
    async with AsyncExitStack() as stack:
        system1, system2 = await cluster_of_two(stack)
        reg_probe1, reg_probe2 = probe(system1), probe(system2)

        receptionist(system2).tell(Subscribe(PING_KEY, reg_probe2.ref))
        await reg_probe2.expect_message(listing(PING_KEY))
        receptionist(system2).tell(Subscribe(ANOTHER_KEY, reg_probe2.ref))
        await reg_probe2.expect_message(listing(ANOTHER_KEY))

        # register same actor for two service keys and verify seen on remote side
        service1 = spawn(system1, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service1, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service1))
        assert len(instances(await reg_probe2.expect_message_type(Listing), PING_KEY)) == 1
        receptionist(system1).tell(Register(ANOTHER_KEY, service1, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(ANOTHER_KEY, service1))
        assert len(instances(await reg_probe2.expect_message_type(Listing), ANOTHER_KEY)) == 1

        # unregister service1 for one of the keys and verify
        receptionist(system1).tell(Deregister(PING_KEY, service1, reg_probe1.ref))
        await reg_probe1.expect_message(Deregistered(PING_KEY, service1))
        assert len(instances(await reg_probe2.expect_message_type(Listing), PING_KEY)) == 0
        receptionist(system2).tell(Find(ANOTHER_KEY, reg_probe2.ref))
        assert len(instances(await reg_probe2.expect_message_type(Listing), ANOTHER_KEY)) == 1

        receptionist(system1).tell(Find(PING_KEY, reg_probe1.ref))
        assert len(instances(await reg_probe1.expect_message_type(Listing), PING_KEY)) == 0
        receptionist(system1).tell(Find(ANOTHER_KEY, reg_probe1.ref))
        assert len(instances(await reg_probe1.expect_message_type(Listing), ANOTHER_KEY)) == 1

        Cluster(system1).shutdown()
        Cluster(system2).manager.tell(Down(Cluster(system1).self_address))


# --- 734 -------------------------------------------------------------------------------------


async def test_handle_concurrent_unregistration_and_registration_on_different_nodes():
    # this covers the fact that with ddata a removal can be lost
    async with AsyncExitStack() as stack:
        system1, system2 = await cluster_of_two(stack)
        reg_probe1, reg_probe2 = probe(system1), probe(system2)

        receptionist(system2).tell(Subscribe(PING_KEY, reg_probe2.ref))
        await reg_probe2.expect_message(listing(PING_KEY))
        receptionist(system1).tell(Subscribe(PING_KEY, reg_probe1.ref))
        await reg_probe1.expect_message(listing(PING_KEY))

        # register an actor on one side and verify seen on both
        service1 = spawn(system1, ping_pong_behavior)
        service2 = spawn(system2, ping_pong_behavior)
        receptionist(system1).tell(Register(PING_KEY, service1, reg_probe1.ref))
        await reg_probe1.expect_message(Registered(PING_KEY, service1))
        await reg_probe1.expect_message(listing(PING_KEY, service1))
        assert len(instances(await reg_probe2.expect_message_type(Listing), PING_KEY)) == 1

        # then concurrently register on one node and unregister on the other node for the same key
        # (ofc racy)
        receptionist(system1).tell(Deregister(PING_KEY, service1, reg_probe1.ref))
        receptionist(system2).tell(Register(PING_KEY, service2, reg_probe2.ref))
        await reg_probe1.expect_message(Deregistered(PING_KEY, service1))
        await reg_probe2.expect_message(Registered(PING_KEY, service2))

        def on_node2(msg: Any) -> FishingOutcome:
            actors = instances(msg, PING_KEY)
            if actors == frozenset({service2}):
                return FishingOutcomes.complete()
            if len(actors) == 2:  # we may see both actors before we see the removal
                return FishingOutcomes.continue_and_ignore()
            return FishingOutcomes.fail(f"Unexpected: {msg!r}")

        await reg_probe2.fish_for_message(3, on_node2)

        def on_node1(msg: Any) -> FishingOutcome:
            actors = instances(msg, PING_KEY)
            if len(actors) == 1:
                return FishingOutcomes.complete()
            if not actors:
                return FishingOutcomes.continue_and_ignore()
            return FishingOutcomes.fail(f"Unexpected: {msg!r}")

        await reg_probe1.fish_for_message(3, on_node1)

        Cluster(system1).shutdown()
        Cluster(system2).manager.tell(Down(Cluster(system1).self_address))


# --- 793 -------------------------------------------------------------------------------------


async def test_notify_subscribers_when_registering_and_joining_simultaneously():
    # failing test reproducer for issue #28792
    # It's possible that the registry entry from the ddata update arrives before MemberJoined.
    number_of_nodes = 6  # use 9 or more to stress it more
    async with AsyncExitStack() as stack:
        # quick dissemination to increase the chance of the race condition; the RemoveTick cleanup
        # runs often to exercise that scenario
        systems = [
            await stack.enter_async_context(
                new_system(
                    free_port(),
                    write_consistency=Replicator.WriteAll(5.0),
                    pruning_interval=0.05,
                    replicator_settings=ReplicatorSettings(
                        gossip_interval=0.5, notify_subscribers_interval=NOTIFY
                    ),
                )
            )
            for _ in range(number_of_nodes)
        ]
        probes = [probe(s) for s in systems]
        for s, p in zip(systems, probes, strict=True):
            receptionist(s).tell(Subscribe(PING_KEY, p.ref))

        # join 3 first
        for s in systems[:3]:
            join(s, systems[0])
            receptionist(s).tell(Register(PING_KEY, spawn(s, ping_pong_behavior)))
        # wait until all those are Up
        for s, p in zip(systems[:3], probes, strict=False):
            await p.await_assert(lambda s=s: _assert_eq(up_count(s), 3), 10)

        # then join the rest randomly to the first 3
        # important to not join all to first to be able to reproduce the problem
        for s in systems[3:]:
            join(s, systems[random.randrange(3)])
            ref = spawn(s, ping_pong_behavior)
            await asyncio.sleep(0.1)  # increase chance of the race condition
            receptionist(s).tell(Register(PING_KEY, ref))

        for s, p in zip(systems, probes, strict=True):
            await p.await_assert(lambda s=s: _assert_eq(up_count(s), number_of_nodes), 30)

        # eventually, all should be included in the Listing
        def all_included(msg: Any) -> FishingOutcome:
            if (
                isinstance(msg, Listing)
                and len(as_listing(msg).service_instances) == number_of_nodes
            ):
                return FishingOutcomes.complete()
            return FishingOutcomes.continue_()

        for i, p in enumerate(probes):
            await p.fish_for_message(10, all_included, str(i))

        # faster to terminate all at the same time
        for s in systems:
            s.terminate()


# --- 856 -------------------------------------------------------------------------------------


async def test_never_use_durable_store():
    async with new_system(free_port()) as system:
        reg_probe = probe(system)
        service = spawn(system, ping_pong_behavior)
        receptionist(system).tell(Register(PING_KEY, service, reg_probe.ref))
        await reg_probe.expect_message(Registered(PING_KEY, service))

        replicator_path = receptionist(system).path + "/replicator"

        # double check that the replicator is running where we expect it to
        await reg_probe.await_assert(lambda: system.resolve(replicator_path))

        # and that it does not have a durable store child
        with pytest.raises(LookupError):
            system.resolve(replicator_path + "/durableStore")


# --- not in Pekko's spec ---------------------------------------------------------------------


async def test_a_joining_node_sees_all_500_registrations_of_one_key():
    count = 500
    async with AsyncExitStack() as stack:
        system1 = await stack.enter_async_context(new_system(free_port()))
        system2 = await stack.enter_async_context(new_system(free_port()))
        join(system1, system1)
        services = [spawn(system1, ping_pong_behavior) for _ in range(count)]
        for service in services:
            receptionist(system1).tell(Register(PING_KEY, service))

        reg_probe2 = probe(system2)
        receptionist(system2).tell(Subscribe(PING_KEY, reg_probe2.ref))
        join(system2, system1)

        def all_there(msg: Any) -> FishingOutcome:
            if isinstance(msg, Listing) and len(as_listing(msg).service_instances) == count:
                return FishingOutcomes.complete()
            return FishingOutcomes.continue_and_ignore()

        found = (await reg_probe2.fish_for_message(20, all_there))[-1]
        assert paths(found.service_instances) == {s.path for s in services}
