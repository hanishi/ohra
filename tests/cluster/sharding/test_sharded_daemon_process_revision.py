"""Pekko's ``verifyRevisionBeforeStarting`` (internal/ShardedDaemonProcessState.scala, 83-145),
which has no spec of its own: a worker of a rescalable daemon process asks Distributed Data for
the process set's state and starts only if its revision is the latest. Then ``initWithContext``
and the commands on the wire.

Each test runs on one node that joins itself. The state is written to the local replicator
first, and the worker is spawned by hand, as a shard would spawn it."""

from __future__ import annotations

import json
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ChangeNumberOfProcesses,
    GetNumberOfProcesses,
    NumberOfProcesses,
    ShardedDaemonProcess,
    ShardedDaemonProcessContext,
    UnhandledMessage,
)
from ohra.cluster.ddata import DistributedData
from ohra.cluster.ddata.replicator import Replicator
from ohra.cluster.sharding.sharded_daemon_process import (
    ShardedDaemonProcessState,
    ShardedDaemonProcessStateKey,
    verify_revision_before_starting,
)
from ohra.eventstream import EventStream
from ohra.remote.serialization import Serialization
from ohra.testkit import ActorTestKit, FishingOutcomes, TestProbe

from .test_sharded_daemon_process import SDP_SETTINGS, Host, single_node


@dataclass(frozen=True)
class RsCreated:
    context: ShardedDaemonProcessContext


@dataclass(frozen=True)
class RsSelf:
    ref: ActorRef[Any]


@dataclass(frozen=True)
class RsPing:
    n: int


def forwarder(probe: ActorRef[Any]) -> Behavior[Any]:
    @Behaviors.receive_message
    def receive(msg: Any) -> Behavior[Any]:
        probe.tell(msg)
        return Behaviors.same

    return receive


def factory(probe: ActorRef[Any]) -> Any:
    def create(c: ShardedDaemonProcessContext) -> Behavior[Any]:
        probe.tell(RsCreated(c))
        return forwarder(probe)

    return create


async def write_state(
    system: ActorSystem[Any], name: str, state: ShardedDaemonProcessState
) -> None:
    probe = TestProbe[Any](Host(system))
    DistributedData(system).replicator.tell(
        Replicator.Update(
            ShardedDaemonProcessStateKey(name),
            ShardedDaemonProcessState.initial_state(3),
            Replicator.WriteLocal(),
            probe.ref,
            lambda _s: state,
        )
    )
    await probe.expect_message_type(Replicator.UpdateSuccess)


def spawn_worker(
    system: ActorSystem[Any], behavior: Behavior[Any], name: str = "worker"
) -> ActorRef[Any]:
    return system._system_root.spawn_child(behavior, name, None).ref  # pyright: ignore[reportPrivateUsage]


RESCALED = ShardedDaemonProcessState.initial_state(3).start_scaling_to(4).complete_scaling()


async def test_a_worker_of_the_latest_revision_starts():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))
        await write_state(system, "latest", RESCALED)

        context = ShardedDaemonProcessContext(0, 4, "latest", 1)
        spawn_worker(system, verify_revision_before_starting(factory(probe.ref))(context))

        await probe.expect_message(RsCreated(context))


async def test_a_worker_of_an_old_revision_refuses_to_start():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))
        await write_state(system, "old", RESCALED)

        context = ShardedDaemonProcessContext(0, 3, "old", 0)
        worker = spawn_worker(system, verify_revision_before_starting(factory(probe.ref))(context))

        await probe.expect_terminated(worker)
        await probe.expect_no_message(0.3)


async def test_a_worker_at_revision_0_starts_without_state():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))

        context = ShardedDaemonProcessContext(1, 3, "nostate", 0)
        spawn_worker(system, verify_revision_before_starting(factory(probe.ref))(context))

        await probe.expect_message(RsCreated(context))


async def test_a_worker_above_revision_0_without_state_stops():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))

        context = ShardedDaemonProcessContext(1, 3, "nostate", 2)
        worker = spawn_worker(system, verify_revision_before_starting(factory(probe.ref))(context))

        await probe.expect_terminated(worker)
        await probe.expect_no_message(0.3)


async def test_a_worker_of_revision_minus_1_stops():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))

        context = ShardedDaemonProcessContext(0, 3, "upgrade", -1)
        worker = spawn_worker(system, verify_revision_before_starting(factory(probe.ref))(context))

        await probe.expect_terminated(worker)
        await probe.expect_no_message(0.3)


async def test_a_factory_returning_setup_runs_in_the_worker():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))

        def create(_c: ShardedDaemonProcessContext) -> Behavior[Any]:
            def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
                probe.ref.tell(RsSelf(ctx.self))
                return forwarder(probe.ref)

            return Behaviors.setup(setup)

        context = ShardedDaemonProcessContext(0, 3, "setup", 0)
        worker = spawn_worker(system, verify_revision_before_starting(create)(context))

        await probe.expect_message(RsSelf(worker))


async def test_messages_before_the_revision_check_are_unhandled():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))
        unhandled = TestProbe[UnhandledMessage](Host(system))
        system.event_stream.tell(EventStream.Subscribe(UnhandledMessage, unhandled.ref))

        context = ShardedDaemonProcessContext(0, 3, "early", 0)
        worker = spawn_worker(system, verify_revision_before_starting(factory(probe.ref))(context))
        # setup sends the Get first, so the ping is processed before the reply
        worker.tell(RsPing(1))

        await unhandled.fish_for_message(
            3.0,
            lambda u: (
                FishingOutcomes.complete()
                if u.message == RsPing(1)
                else FishingOutcomes.continue_and_ignore()
            ),
        )
        await probe.expect_message(RsCreated(context))
        worker.tell(RsPing(2))
        await probe.expect_message(RsPing(2))
        await probe.expect_no_message(0.3)


async def test_init_with_context_hands_the_factory_its_context():
    async with AsyncExitStack() as stack:
        system = await single_node(stack)
        probe = TestProbe[Any](Host(system))

        ref = ShardedDaemonProcess(system).init_with_context(
            "ctx", 3, factory(probe.ref), SDP_SETTINGS
        )

        assert isinstance(ref, ActorRef)
        created = [await probe.expect_message_type(RsCreated, max=10.0) for _ in range(3)]
        contexts = [c.context for c in created]
        assert all(c.total_processes == 3 and c.revision == 0 for c in contexts)
        assert all(c.name == "ctx" for c in contexts)
        assert {c.process_number for c in contexts} == {0, 1, 2}


async def test_the_commands_cross_the_wire():
    async with ActorTestKit() as kit:
        probe: TestProbe[Any] = kit.create_test_probe()
        reply_to = probe.ref

        for message, name in [
            (ChangeNumberOfProcesses(4, reply_to), "ShardedDaemonProcess.ChangeNumberOfProcesses"),
            (GetNumberOfProcesses(reply_to), "ShardedDaemonProcess.GetNumberOfProcesses"),
            (
                NumberOfProcesses(4, 1763000000.5, True, 2),
                "ShardedDaemonProcess.NumberOfProcesses",
            ),
        ]:
            data = Serialization.encode(message, lambda _r: "reply-to")
            assert isinstance(data, dict) and data["$type"] == name
            back = Serialization.decode(json.loads(json.dumps(data)), lambda _p: reply_to)
            assert back == message
