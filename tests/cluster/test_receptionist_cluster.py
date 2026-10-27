"""The Receptionist across cluster nodes: owner rows replicated between receptionists, pruned by
membership."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    RemoteRef,
    ask,
    remotable,
)
from ohra.cluster.core import Cluster, ClusterSettings, Down, Join, Leave
from ohra.receptionist import (
    Find,
    Listing,
    Receptionist,
    ReceptionistRow,
    Register,
    ServiceKey,
    Subscribe,
)
from ohra.remote.serialization import Json
from ohra.remote import Address

from ..util import eventually, free_port
from .test_cluster import FAST, FAST_SHUTDOWN, all_up, unreachable_ports


@remotable
@dataclass(frozen=True)
class SayHello:
    name: str
    reply_to: ActorRef[str]


GREETER = ServiceKey[SayHello]("greeter")


@dataclass(frozen=True)
class Spawn:
    behavior: Behavior[Any]
    name: str
    reply_to: ActorRef[ActorRef[Any]]


def spawner(ctx: ActorContext[Spawn], msg: Spawn) -> Behavior[Spawn]:
    msg.reply_to.tell(ctx.spawn(msg.behavior, msg.name))
    return Behaviors.same


def greeter(msg: SayHello | str) -> Behavior[SayHello | str]:
    if msg == "stop":
        return Behaviors.stopped()
    if isinstance(msg, SayHello):
        msg.reply_to.tell(f"hello {msg.name}")
    return Behaviors.same


def recorder(into: list[Any]) -> Behavior[Any]:
    def on_message(msg: Any) -> Behavior[Any]:
        into.append(msg)
        return Behaviors.same

    return Behaviors.receive_message(on_message)


def node(port: int, seeds: list[int], **settings: Any) -> ActorSystem[Any]:
    seed_nodes = tuple(Address("c", "127.0.0.1", p) for p in seeds)
    cfg = ClusterSettings(
        seed_nodes=seed_nodes, **{**FAST, "receptionist_resend_interval": 0.05, **settings}
    )
    return ActorSystem(
        Behaviors.receive(spawner),
        "c",
        address=("127.0.0.1", port),
        cluster=cfg,
        shutdown=FAST_SHUTDOWN,
    )


async def spawn(system: ActorSystem[Any], behavior: Behavior[Any], name: str) -> ActorRef[Any]:
    return await ask(system, lambda r: Spawn(behavior, name, r), timeout=1)


async def find(system: ActorSystem[Any]) -> Listing[SayHello]:
    return await ask(Receptionist(system).ref, lambda r: Find(GREETER, r), timeout=1)


async def instances_seen_on(system: ActorSystem[Any], n: int) -> bool:
    return len((await find(system)).service_instances) == n


async def crash(system: ActorSystem[Any]) -> None:
    """A node that vanishes mid-flight: nothing it would say on the way down gets out."""
    assert system.remoting is not None
    await system.remoting.close()
    system.terminate()
    await system.when_terminated()


async def eventually_async(cond: Any, timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not await cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


async def test_find_on_another_node_returns_a_working_remote_ref():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        svc = await spawn(a, Behaviors.receive_message(greeter), "svc")
        Receptionist(a).ref.tell(Register(GREETER, svc))
        await eventually_async(lambda: instances_seen_on(b, 1))
        listing = await find(b)
        (ref,) = listing.service_instances
        assert isinstance(ref, RemoteRef) and ref.path == svc.path
        assert listing.all_service_instances == listing.service_instances
        assert await ask(ref, lambda r: SayHello("b", r), timeout=1) == "hello b"


async def test_subscriber_on_another_node_sees_the_service_appear_and_leave():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        events: list[Listing[SayHello]] = []
        sub = await spawn(b, recorder(events), "sub")
        Receptionist(b).ref.tell(Subscribe(GREETER, sub))
        await eventually(lambda: events == [Listing(GREETER, frozenset(), frozenset(), True)])
        svc = await spawn(a, Behaviors.receive_message(greeter), "svc")
        Receptionist(a).ref.tell(Register(GREETER, svc))
        await eventually(
            lambda: len(events) == 2 and len(events[-1].service_instances) == 1, timeout=3
        )
        assert events[-1].services_were_added_or_removed
        svc.tell("stop")
        await eventually(
            lambda: len(events) == 3 and events[-1].service_instances == frozenset(), timeout=3
        )


async def test_registrations_on_two_nodes_both_reach_a_third():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b, c = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2, p3)]
        await eventually(lambda: all(all_up(n, 3) for n in (a, b, c)), timeout=5)
        svc_a = await spawn(a, Behaviors.receive_message(greeter), "svc")
        svc_b = await spawn(b, Behaviors.receive_message(greeter), "svc")
        Receptionist(a).ref.tell(Register(GREETER, svc_a))
        Receptionist(b).ref.tell(Register(GREETER, svc_b))
        await eventually_async(lambda: instances_seen_on(c, 2), timeout=2)
        assert {r.path for r in (await find(c)).service_instances} == {svc_a.path, svc_b.path}


async def test_a_row_from_a_not_yet_member_is_hidden_until_it_joins():
    p1, p2, p3 = free_port(), free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        # listening, but not joining anything yet
        c = await stack.enter_async_context(node(p3, []))
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        # b's receptionist exists and is empty
        assert (await find(b)).all_service_instances == frozenset()
        svc = await spawn(c, Behaviors.receive_message(greeter), "svc")
        row = ReceptionistRow(Cluster(c).self_unique_address, 5, ((GREETER.id, svc),))
        # over the wire, from c
        c.resolve(f"{Cluster(b).self_address.root}/system/receptionist").tell(row)
        await asyncio.sleep(0.2)
        assert (await find(b)).all_service_instances == frozenset()  # c is not a member: hidden
        Cluster(c).manager.tell(Join(Address("c", "127.0.0.1", p1)))
        await eventually_async(lambda: instances_seen_on(b, 1), timeout=5)
        (ref,) = (await find(b)).service_instances
        assert ref.path == svc.path


async def test_a_left_node_takes_its_services_with_it():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        svc = await spawn(a, Behaviors.receive_message(greeter), "svc")
        Receptionist(a).ref.tell(Register(GREETER, svc))
        events: list[Listing[SayHello]] = []
        sub = await spawn(b, recorder(events), "sub")
        Receptionist(b).ref.tell(Subscribe(GREETER, sub))
        await eventually(
            lambda: bool(events) and len(events[-1].service_instances) == 1, timeout=3
        )
        Cluster(b).manager.tell(Leave(Cluster(a).self_address))
        await asyncio.wait_for(a.when_terminated(), timeout=5)
        await eventually(lambda: events[-1].all_service_instances == frozenset(), timeout=5)
        assert events[-1].services_were_added_or_removed


async def test_a_crashed_node_keeps_services_in_all_instances_until_downed():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        svc = await spawn(a, Behaviors.receive_message(greeter), "svc")
        Receptionist(a).ref.tell(Register(GREETER, svc))
        events: list[Listing[SayHello]] = []
        sub = await spawn(b, recorder(events), "sub")
        Receptionist(b).ref.tell(Subscribe(GREETER, sub))
        await eventually(
            lambda: bool(events) and len(events[-1].service_instances) == 1, timeout=3
        )
        await crash(a)
        await eventually(lambda: unreachable_ports(b) == {p1}, timeout=5)
        await eventually(lambda: events[-1].service_instances == frozenset(), timeout=3)
        assert (
            len(events[-1].all_service_instances) == 1
            and not events[-1].services_were_added_or_removed
        )
        Cluster(b).manager.tell(Down(Address("c", "127.0.0.1", p1)))
        await eventually(lambda: events[-1].all_service_instances == frozenset(), timeout=5)
        assert events[-1].services_were_added_or_removed


async def test_a_restarted_node_replaces_its_old_row():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a = await stack.enter_async_context(node(p1, [p1], retry_unsuccessful_join_after=0.1))
        b = await stack.enter_async_context(node(p2, [p1], retry_unsuccessful_join_after=0.1))
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        Receptionist(b).ref.tell(
            Register(GREETER, await spawn(b, Behaviors.receive_message(greeter), "old"))
        )
        await eventually_async(lambda: instances_seen_on(a, 1))
        await crash(b)
        b2 = await stack.enter_async_context(node(p2, [p1], retry_unsuccessful_join_after=0.1))
        Receptionist(b2).ref.tell(
            Register(GREETER, await spawn(b2, Behaviors.receive_message(greeter), "new"))
        )
        await eventually(lambda: all_up(a, 2) and all_up(b2, 2), timeout=8)
        await eventually_async(lambda: instances_seen_on(a, 1), timeout=5)
        assert {r.path.rsplit("/", 1)[1] for r in (await find(a)).all_service_instances} == {"new"}


# --- from the final review -----------------------------------------------------------------------


def receptionist_path(system: ActorSystem[Any]) -> str:
    return f"{Cluster(system).self_address.root}/system/receptionist"


async def test_find_from_another_node_over_the_wire():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        svc = await spawn(a, Behaviors.receive_message(greeter), "svc")
        Receptionist(a).ref.tell(Register(GREETER, svc))
        listing: Listing[SayHello] = await ask(
            b.resolve(receptionist_path(a)), lambda r: Find(GREETER, r), timeout=2
        )
        assert listing.key == GREETER and {r.path for r in listing.service_instances} == {svc.path}


async def test_a_remote_subscriber_is_ignored_and_the_receptionist_survives():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        sub_on_b = await spawn(b, recorder([]), "sub")
        # a RemoteRef subscriber on a's side
        b.resolve(receptionist_path(a)).tell(Subscribe(GREETER, sub_on_b))
        await asyncio.sleep(0.1)
        assert (await find(a)).service_instances == frozenset()  # still answering


async def test_a_late_joiner_gets_existing_rows_within_one_exchange():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a = await stack.enter_async_context(node(p1, [p1], receptionist_resend_interval=10.0))
        await eventually(lambda: all_up(a, 1), timeout=3)
        Receptionist(a).ref.tell(
            Register(GREETER, await spawn(a, Behaviors.receive_message(greeter), "svc"))
        )
        b = await stack.enter_async_context(node(p2, [p1], receptionist_resend_interval=10.0))
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        # the member-event push, not a resend
        await eventually_async(lambda: instances_seen_on(b, 1), timeout=2)


async def test_a_service_comes_back_when_its_node_is_reachable_again():
    p1, p2 = free_port(), free_port()
    async with AsyncExitStack() as stack:
        a, b = [await stack.enter_async_context(node(p, [p1])) for p in (p1, p2)]
        await eventually(lambda: all_up(a, 2) and all_up(b, 2), timeout=5)
        Receptionist(a).ref.tell(
            Register(GREETER, await spawn(a, Behaviors.receive_message(greeter), "svc"))
        )
        events: list[Listing[SayHello]] = []
        Receptionist(b).ref.tell(Subscribe(GREETER, await spawn(b, recorder(events), "sub")))
        await eventually(
            lambda: bool(events) and len(events[-1].service_instances) == 1, timeout=3
        )
        remoting = a.remoting
        assert remoting is not None
        deliver = remoting._handle  # pyright: ignore[reportPrivateUsage]

        def dropping_heartbeats(frame: Json) -> None:  # a stops answering b's heartbeats
            message = frame.get("message") if isinstance(frame, dict) else None
            if isinstance(message, dict) and message.get("$type") == "Heartbeat":
                return
            deliver(frame)

        remoting._handle = dropping_heartbeats  # pyright: ignore[reportPrivateUsage]
        await eventually(lambda: events[-1].service_instances == frozenset(), timeout=5)
        assert (
            len(events[-1].all_service_instances) == 1
            and not events[-1].services_were_added_or_removed
        )
        remoting._handle = deliver  # pyright: ignore[reportPrivateUsage]
        await eventually(lambda: len(events[-1].service_instances) == 1, timeout=5)
        assert not events[-1].services_were_added_or_removed
