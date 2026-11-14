"""Ported from Apache Pekko's scaladsl ShardedDaemonProcessSpec (cluster-sharding-typed/src/test/
scala/org/apache/pekko/cluster/sharding/typed/scaladsl/ShardedDaemonProcessSpec.scala), "The
ShardedDaemonProcess" (79-117), and the multi-jvm ShardedDaemonProcessSpec (cluster-sharding-typed/
src/multi-jvm/scala/org/apache/pekko/cluster/sharding/typed/ShardedDaemonProcessSpec.scala), "init
actor set".

Differences from the Scala:

- The nodes run in one process, on one event loop, each on its own port. The one-node specs join
  themselves through their own seed, for "have a single node cluster running first".
- The config is settings: ``SDP_SETTINGS`` is the spec's ``keep-alive-interval = 1s`` and
  ``keep-alive-throttle-interval = 20ms`` with ``FAST_SHARDING``, which shortens the sharding
  intervals as the multi-jvm spec's ``retry-interval = 0.2s`` does.
- ``MyActor`` is ``my_actor``, and its ``Started`` and ``Stop`` are ``SdpStarted`` and ``SdpStop``.
- ``ProcessActor`` tells ``SdpEvent`` to the snitch it is given, the ref a ``Receptionist.Find``
  on its node returned, as ohra has no ``Routers.group``. ``SdpEvent`` names the node's port and
  the worker's ref too.
- The tests after those are ohra's own: ``init`` twice, the workers and the coordinator moving
  off a leaving node (the multi-jvm spec's FIXME), the sharding settings the daemon forces, two
  daemon processes on one system, a role on a mixed cluster, and rescaling through
  ``init_with_context``'s ref, which Pekko has no spec for. ``rs_worker`` reports ``RsEvent`` with
  its context.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from typing import Any, cast

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ChangeNumberOfProcesses,
    GetNumberOfProcesses,
    ShardedDaemonProcess,
    ShardedDaemonProcessCommand,
    ShardedDaemonProcessContext,
    ShardedDaemonProcessSettings,
    ask,
    ask_with_status,
    remotable,
)
from ohra.cluster.core import Cluster, Leave
from ohra.cluster.ddata import ReplicatorSettings
from ohra.cluster.sharding.passivation_settings import NoPassivationStrategy
from ohra.cluster.sharding.sharded_daemon_process import (
    _sharding_settings,  # pyright: ignore[reportPrivateUsage]
)
from ohra.patterns import DONE
from ohra.receptionist import Find, Listing, Receptionist, Register, ServiceKey
from ohra.testkit import FishingOutcome, FishingOutcomes, TestProbe
from ohra.testkit.settings import TestKitSettings

from ...util import eventually, free_port
from ..test_cluster import all_up
from ..test_cluster_sharding import FAST_SHARDING, node

SDP_SETTINGS = ShardedDaemonProcessSettings(
    keep_alive_interval=1.0, keep_alive_throttle_interval=0.02
).with_sharding_settings(FAST_SHARDING)

# the receptionist's registrations reach the other nodes by gossip, 2 s by default
FAST_DDATA = ReplicatorSettings(gossip_interval=0.1, notify_subscribers_interval=0.05)


@remotable
@dataclass(frozen=True)
class SdpStarted:
    id: int
    self_ref: ActorRef[Any]


@remotable
@dataclass(frozen=True)
class SdpStop:
    pass


@remotable
@dataclass(frozen=True)
class SdpEvent:
    id: int
    event: str
    port: int
    self_ref: ActorRef[Any]


@remotable
@dataclass(frozen=True)
class RsEvent:
    process_number: int
    total_processes: int
    revision: int
    port: int
    event: str
    name: str


SNITCH = ServiceKey[SdpEvent]("snitch")


@dataclass
class Host:
    """What a TestProbe needs of a test kit."""

    system: ActorSystem[Any]
    settings: TestKitSettings = field(default_factory=TestKitSettings)


def my_actor(n: int, probe: ActorRef[Any]) -> Behavior[SdpStop]:
    """``MyActor``."""

    @Behaviors.setup
    def setup(ctx: ActorContext[SdpStop]) -> Behavior[SdpStop]:
        probe.tell(SdpStarted(n, ctx.self))

        @Behaviors.receive_message
        def receive(_msg: SdpStop) -> Behavior[SdpStop]:
            return Behaviors.stopped()

        return receive

    return setup


def process_actor(n: int, snitch: ActorRef[SdpEvent], port: int) -> Behavior[SdpStop]:
    """``ProcessActor``."""

    @Behaviors.setup
    def setup(ctx: ActorContext[SdpStop]) -> Behavior[SdpStop]:
        snitch.tell(SdpEvent(n, "Started", port, ctx.self))

        @Behaviors.receive_message
        def receive(_msg: SdpStop) -> Behavior[SdpStop]:
            snitch.tell(SdpEvent(n, "Stopped", port, ctx.self))
            return Behaviors.stopped()

        return receive

    return setup


def rs_worker(sdp: ShardedDaemonProcessContext, snitch: ActorRef[Any]) -> Behavior[SdpStop]:
    """A worker that reports its context when it starts and when it is told to stop."""

    @Behaviors.setup
    def setup(ctx: ActorContext[SdpStop]) -> Behavior[SdpStop]:
        port = port_of(ctx.system)

        def event(name: str) -> RsEvent:
            n, total, revision = sdp.process_number, sdp.total_processes, sdp.revision
            return RsEvent(n, total, revision, port, name, sdp.name)

        snitch.tell(event("Started"))

        @Behaviors.receive_message
        def receive(_msg: SdpStop) -> Behavior[SdpStop]:
            snitch.tell(event("Stopped"))
            return Behaviors.stopped()

        return receive

    return setup


def port_of(system: ActorSystem[Any]) -> int:
    return Cluster(system).self_address.port


async def single_node(stack: AsyncExitStack) -> ActorSystem[Any]:
    port = free_port()
    system = await stack.enter_async_context(node(port, [port]))
    await eventually(lambda: all_up(system, 1), timeout=5)
    return system


async def three_nodes(stack: AsyncExitStack) -> list[ActorSystem[Any]]:
    ports = [free_port() for _ in range(3)]
    systems = [
        await stack.enter_async_context(node(p, [ports[0]], distributed_data=FAST_DDATA))
        for p in ports
    ]
    await eventually(lambda: all(all_up(s, 3) for s in systems), timeout=10)
    return systems


async def snitch_on(systems: list[ActorSystem[Any]], host: ActorSystem[Any]) -> TestProbe[Any]:
    """The probe registered under ``SNITCH`` on ``host``, once every node's receptionist sees it
    (the multi-jvm spec's "form cluster")."""
    probe = TestProbe[Any](Host(host))
    Receptionist(host).ref.tell(Register(SNITCH, probe.ref))

    async def seen_everywhere() -> None:
        for system in systems:
            assert len((await find_snitch(system)).service_instances) == 1

    await probe.await_assert(seen_everywhere, max=5.0, interval=0.05)
    return probe


async def find_snitch(system: ActorSystem[Any]) -> Listing[SdpEvent]:
    timeout = TestKitSettings().single_expect_default_dilated
    return await ask(Receptionist(system).ref, lambda r: Find(SNITCH, r), timeout=timeout)


async def init_processes(
    system: ActorSystem[Any],
    name: str,
    n: int,
    settings: ShardedDaemonProcessSettings,
    stop_message: SdpStop | None = None,
) -> None:
    [snitch] = (await find_snitch(system)).service_instances
    port = port_of(system)
    ShardedDaemonProcess(system).init(
        name, n, lambda i: process_actor(i, snitch, port), settings, stop_message
    )


async def init_rs(
    system: ActorSystem[Any],
    name: str,
    n: int,
    settings: ShardedDaemonProcessSettings = SDP_SETTINGS,
) -> ActorRef[ShardedDaemonProcessCommand]:
    [snitch] = (await find_snitch(system)).service_instances
    return ShardedDaemonProcess(system).init_with_context(
        name, n, lambda c: rs_worker(c, snitch), settings, SdpStop()
    )


def rescaled(old: int, new: int) -> tuple[set[int], set[int], Callable[[RsEvent], FishingOutcome]]:
    """A fishing function that completes once the workers ``0`` to ``old - 1`` of revision 0 have
    stopped and ``0`` to ``new - 1`` of revision 1 have started, and the two sets it fills.
    Anything else of revision 0 is ignored: the throttle may have held an old id."""
    stopped: set[int] = set()
    started: set[int] = set()

    def fish(e: RsEvent) -> FishingOutcome:
        if e.revision == 0 and e.event == "Stopped":
            stopped.add(e.process_number)
        elif e.revision == 1 and e.event == "Started":
            assert e.total_processes == new
            assert e.process_number < new
            started.add(e.process_number)
        if stopped == set(range(old)) and started == set(range(new)):
            return FishingOutcomes.complete()
        return FishingOutcomes.continue_()

    return stopped, started, fish


# --- "The ShardedDaemonProcess" (79-117) ---------------------------------------------------------


async def test_start_n_actors_with_unique_ids():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))
        ShardedDaemonProcess(system).init("a", 5, lambda n: my_actor(n, probe.ref), SDP_SETTINGS)

        started = await probe.receive_messages(5)
        assert len({s.id for s in started}) == 5
        await probe.expect_no_message()


async def test_restart_actors_if_they_stop():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))
        ShardedDaemonProcess(system).init(
            "stop", 2, lambda n: my_actor(n, probe.ref), SDP_SETTINGS
        )

        started = [await probe.expect_message_type(SdpStarted) for _ in range(2)]
        for s in started:
            s.self_ref.tell(SdpStop())

        # periodic ping every 1s makes it restart
        restarted = [await probe.expect_message_type(SdpStarted, max=3.0) for _ in range(2)]
        assert {s.id for s in restarted} == {0, 1}


async def test_not_run_if_the_role_does_not_match_node_role():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))
        ShardedDaemonProcess(system).init(
            "roles", 3, lambda n: my_actor(n, probe.ref), SDP_SETTINGS.with_role("workers")
        )

        await probe.expect_no_message(0.5)


# --- the multi-jvm "init actor set" --------------------------------------------------------------


async def test_init_actor_set():
    async with AsyncExitStack() as stack:
        systems = await three_nodes(stack)
        probe = await snitch_on(systems, systems[0])

        for system in systems:
            await init_processes(system, "the-fearless", 4, SDP_SETTINGS)

        events: list[SdpEvent] = await probe.receive_messages(4, max=5.0)
        assert all(e.event == "Started" for e in events)
        assert len({e.id for e in events}) == 4


# --- ohra's own ----------------------------------------------------------------------------------


async def test_init_twice_with_one_name_starts_one_set():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))
        for _ in range(2):
            ShardedDaemonProcess(system).init(
                "twice", 2, lambda n: my_actor(n, probe.ref), SDP_SETTINGS
            )

        started = await probe.receive_messages(2)
        assert {s.id for s in started} == {0, 1}
        await probe.expect_no_message()


async def test_workers_and_coordinator_move_when_the_oldest_node_leaves():
    # no rebalancing, so the workers leave the first node only because it leaves
    settings = SDP_SETTINGS.with_sharding_settings(replace(FAST_SHARDING, rebalance_interval=60.0))
    async with AsyncExitStack() as stack:
        systems = await three_nodes(stack)
        first = systems[0]
        probe = await snitch_on(systems, systems[2])

        for system in systems:
            await init_processes(system, "movers", 4, settings, SdpStop())
        events = [await probe.expect_message_type(SdpEvent, max=5.0) for _ in range(4)]
        assert {(e.id, e.event) for e in events} == {(n, "Started") for n in range(4)}
        first_port = port_of(first)
        on_first = {e.id for e in events if e.port == first_port}
        assert on_first  # or nothing has to move
        stopped: set[int] = set()
        on_remaining = {e.id: e for e in events if e.port != first_port}

        def until_all_moved(e: SdpEvent) -> FishingOutcome:
            if e.event == "Stopped" and e.port == first_port:
                stopped.add(e.id)
            elif e.event == "Started" and e.port != first_port:
                on_remaining[e.id] = e
            if stopped == on_first and on_remaining.keys() == set(range(4)):
                return FishingOutcomes.complete()
            return FishingOutcomes.continue_()

        Cluster(first).manager.tell(Leave(Cluster(first).self_address))
        await probe.fish_for_message(10.0, until_all_moved)

        # the old coordinator may have restarted the workers before it handed over; once the
        # first node is gone, a worker that stops is started again only by a coordinator on a
        # remaining node
        await asyncio.wait_for(first.when_terminated(), 10.0)
        worker = on_remaining[min(on_first)]
        worker.self_ref.tell(SdpStop())

        def restarted(e: SdpEvent) -> FishingOutcome:
            if e.id == worker.id and e.event == "Started" and e.port != first_port:
                return FishingOutcomes.complete()
            return FishingOutcomes.continue_and_ignore()

        # 10 s, as above: the moved coordinator may still be reading its state
        await probe.fish_for_message(10.0, restarted)


def test_init_refuses_no_instances():
    # ShardedDaemonProcess is a plain class: init fails before it touches the system
    with pytest.raises(ValueError, match="number_of_instances must be >= 1, got 0"):
        ShardedDaemonProcess(cast(ActorSystem[Any], None)).init(
            "none", 0, lambda n: Behaviors.empty
        )


async def test_two_daemon_processes_on_one_system():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))
        sdp = ShardedDaemonProcess(system)
        sdp.init(
            "one", 2, lambda n: rs_worker(ShardedDaemonProcessContext(n, 2, "one", 0), probe.ref)
        )
        sdp.init(
            "two", 3, lambda n: rs_worker(ShardedDaemonProcessContext(n, 3, "two", 0), probe.ref)
        )

        events: list[RsEvent] = await probe.receive_messages(5, max=10.0)
        assert {(e.name, e.process_number) for e in events} == {
            ("one", 0),
            ("one", 1),
            ("two", 0),
            ("two", 1),
            ("two", 2),
        }
        await probe.expect_no_message()


async def test_a_role_confines_the_workers_on_a_mixed_cluster():
    ports = [free_port() for _ in range(3)]
    async with AsyncExitStack() as stack:
        systems = [
            await stack.enter_async_context(
                node(
                    p,
                    [ports[0]],
                    roles=frozenset({"workers"}) if i == 1 else frozenset(),
                    distributed_data=FAST_DDATA,
                )
            )
            for i, p in enumerate(ports)
        ]
        await eventually(lambda: all(all_up(s, 3) for s in systems), timeout=10)
        probe = await snitch_on(systems, systems[0])

        refs = [await init_rs(s, "roles", 3, SDP_SETTINGS.with_role("workers")) for s in systems]

        events: list[RsEvent] = await probe.receive_messages(3, max=10.0)
        assert {e.process_number for e in events} == {0, 1, 2}
        assert {e.port for e in events} == {ports[1]}
        # the first node has no role, so only a proxy of the coordinator
        reply = await ask(refs[0], GetNumberOfProcesses, 3.0)
        assert reply.number_of_processes == 3


# --- rescaling -----------------------------------------------------------------------------------


async def test_rescaling_up_stops_the_old_set_and_starts_the_new():
    async with AsyncExitStack() as stack:
        systems = await three_nodes(stack)
        probe = await snitch_on(systems, systems[0])
        refs = [await init_rs(s, "grow", 4) for s in systems]
        ref = refs[0]
        events: list[RsEvent] = await probe.receive_messages(4, max=10.0)
        assert {(e.process_number, e.revision, e.event) for e in events} == {
            (n, 0, "Started") for n in range(4)
        }

        assert await ask_with_status(ref, lambda r: ChangeNumberOfProcesses(6, r), 10.0) == DONE

        stopped, started, fish = rescaled(4, 6)
        await probe.fish_for_message(10.0, fish)
        assert (stopped, started) == (set(range(4)), set(range(6)))
        reply = await ask(ref, GetNumberOfProcesses, 3.0)
        assert reply.number_of_processes == 6
        assert reply.revision == 1
        assert reply.rescale_in_progress is False


async def test_rescaling_down_stops_the_extra_workers():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))
        ref = ShardedDaemonProcess(system).init_with_context(
            "shrink", 4, lambda c: rs_worker(c, probe.ref), SDP_SETTINGS, SdpStop()
        )
        events: list[RsEvent] = await probe.receive_messages(4, max=10.0)
        assert {(e.process_number, e.revision) for e in events} == {(n, 0) for n in range(4)}

        assert await ask_with_status(ref, lambda r: ChangeNumberOfProcesses(2, r), 10.0) == DONE

        _, _, fish = rescaled(4, 2)
        await probe.fish_for_message(10.0, fish)
        # no process 2 or 3 starts again, of either revision
        await probe.expect_no_message(2 * SDP_SETTINGS.keep_alive_interval)


async def test_a_rescale_from_another_node_goes_through_the_proxy():
    async with AsyncExitStack() as stack:
        systems = await three_nodes(stack)
        probe = await snitch_on(systems, systems[0])
        refs = [await init_rs(s, "proxied", 2) for s in systems]
        await probe.receive_messages(2, max=10.0)

        # the coordinator runs on the oldest node, the first
        third = refs[2]
        assert await ask_with_status(third, lambda r: ChangeNumberOfProcesses(3, r), 10.0) == DONE

        reply = await ask(refs[0], GetNumberOfProcesses, 3.0)
        assert reply.number_of_processes == 3
        assert reply.revision == 1


# --- the settings --------------------------------------------------------------------------------


def test_the_sharding_settings_are_forced_for_the_daemon():
    base = replace(FAST_SHARDING, number_of_shards=1000, remember_entities=True, role="other")
    settings = _sharding_settings(SDP_SETTINGS.with_role("r").with_sharding_settings(base), 4)

    assert settings.number_of_shards == 4
    assert settings.remember_entities is False
    assert settings.role == "r"
    assert settings.passivation_strategy == NoPassivationStrategy()
    assert settings.retry_interval == FAST_SHARDING.retry_interval
