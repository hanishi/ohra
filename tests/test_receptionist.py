"""The Receptionist on one node, as Pekko's LocalReceptionist: register, find, subscribe, and the
watches."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import pytest

from ohra import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, ask
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
from ohra.ref import FunctionRef

from .util import eventually, free_port

GREETER = ServiceKey[str]("greeter")
OTHER = ServiceKey[str]("other")


@dataclass(frozen=True)
class Spawn:
    behavior: Behavior[Any]
    name: str
    reply_to: ActorRef[ActorRef[Any]]


def spawner(ctx: ActorContext[Spawn], msg: Spawn) -> Behavior[Spawn]:
    msg.reply_to.tell(ctx.spawn(msg.behavior, msg.name))
    return Behaviors.same


def recorder(into: list[Any]) -> Behavior[Any]:
    """Keeps what it receives; stops on "stop"."""

    def on_message(msg: Any) -> Behavior[Any]:
        if msg == "stop":
            return Behaviors.stopped()
        into.append(msg)
        return Behaviors.same

    return Behaviors.receive_message(on_message)


def gone(system: ActorSystem[Any], path: str) -> bool:
    try:
        system.resolve(path)
    except LookupError:
        return True
    return False


async def test_register_then_find_lists_the_service():
    async with ActorSystem(Behaviors.receive(spawner), "r") as system:
        svc = await ask(system, lambda r: Spawn(Behaviors.ignore, "svc", r), timeout=1)
        rec = Receptionist(system).ref
        assert await ask(rec, lambda r: Register(GREETER, svc, r), timeout=1) == Registered(
            GREETER, svc
        )
        listing = await ask(rec, lambda r: Find(GREETER, r), timeout=1)
        assert listing == Listing(GREETER, frozenset({svc}), frozenset({svc}), True)
        assert await ask(rec, lambda r: Find(OTHER, r), timeout=1) == Listing(
            OTHER, frozenset(), frozenset(), True
        )


async def test_subscribe_gets_current_listing_then_each_change():
    async with ActorSystem(Behaviors.receive(spawner), "r") as system:
        svc = await ask(system, lambda r: Spawn(Behaviors.ignore, "svc", r), timeout=1)
        other = await ask(system, lambda r: Spawn(Behaviors.ignore, "other", r), timeout=1)
        events: list[Listing[str]] = []
        sub = await ask(system, lambda r: Spawn(recorder(events), "sub", r), timeout=1)
        rec = Receptionist(system).ref
        rec.tell(Subscribe(GREETER, sub))
        await eventually(lambda: events == [Listing(GREETER, frozenset(), frozenset(), True)])
        rec.tell(Register(GREETER, svc))
        await eventually(lambda: len(events) == 2)
        assert events[1].service_instances == frozenset({svc})
        rec.tell(Register(OTHER, other))  # another key: not our business
        await asyncio.sleep(0.05)
        assert len(events) == 2
        rec.tell(Deregister(GREETER, svc))
        await eventually(lambda: len(events) == 3)
        assert events[2] == Listing(GREETER, frozenset(), frozenset(), True)


async def test_deregister_removes_one_binding():
    async with ActorSystem(Behaviors.receive(spawner), "r") as system:
        svc = await ask(system, lambda r: Spawn(Behaviors.ignore, "svc", r), timeout=1)
        rec = Receptionist(system).ref
        rec.tell(Register(GREETER, svc))
        rec.tell(Register(OTHER, svc))
        assert await ask(rec, lambda r: Deregister(GREETER, svc, r), timeout=1) == Deregistered(
            GREETER, svc
        )
        assert (
            await ask(rec, lambda r: Find(GREETER, r), timeout=1)
        ).service_instances == frozenset()
        assert (
            await ask(rec, lambda r: Find(OTHER, r), timeout=1)
        ).service_instances == frozenset({svc})


async def test_a_stopped_service_leaves_every_key():
    async with ActorSystem(Behaviors.receive(spawner), "r") as system:
        svc = await ask(system, lambda r: Spawn(recorder([]), "svc", r), timeout=1)
        greeter_events: list[Listing[str]] = []
        other_events: list[Listing[str]] = []
        sub1 = await ask(system, lambda r: Spawn(recorder(greeter_events), "sub1", r), timeout=1)
        sub2 = await ask(system, lambda r: Spawn(recorder(other_events), "sub2", r), timeout=1)
        rec = Receptionist(system).ref
        rec.tell(Register(GREETER, svc))
        rec.tell(Register(OTHER, svc))
        rec.tell(Subscribe(GREETER, sub1))
        rec.tell(Subscribe(OTHER, sub2))
        await eventually(
            lambda: (
                greeter_events[-1:] == [Listing(GREETER, frozenset({svc}), frozenset({svc}), True)]
            )
        )
        await eventually(
            lambda: other_events[-1:] == [Listing(OTHER, frozenset({svc}), frozenset({svc}), True)]
        )
        svc.tell("stop")
        await eventually(lambda: greeter_events[-1].service_instances == frozenset())
        await eventually(lambda: other_events[-1].service_instances == frozenset())


async def test_a_stopped_subscriber_is_dropped_from_every_key(caplog: pytest.LogCaptureFixture):
    async with ActorSystem(Behaviors.receive(spawner), "r") as system:
        svc = await ask(system, lambda r: Spawn(Behaviors.ignore, "svc", r), timeout=1)
        sub = await ask(system, lambda r: Spawn(recorder([]), "sub", r), timeout=1)
        rec = Receptionist(system).ref
        rec.tell(Subscribe(GREETER, sub))
        rec.tell(Subscribe(OTHER, sub))
        sub.tell("stop")
        await eventually(lambda: gone(system, sub.path))
        with caplog.at_level(logging.DEBUG, logger="ohra.dead_letters"):
            rec.tell(Register(GREETER, svc))
            rec.tell(Register(OTHER, svc))
            assert (
                await ask(rec, lambda r: Find(OTHER, r), timeout=1)
            ).service_instances == frozenset({svc})
        assert not [r for r in caplog.records if sub.path in r.getMessage()]


async def test_only_local_refs_can_register():
    async with ActorSystem(Behaviors.ignore, "r", address=("127.0.0.1", free_port())) as system:
        rec = Receptionist(system).ref
        rec.tell(Register(GREETER, FunctionRef("fn", lambda _: None)))
        rec.tell(Register(GREETER, system.resolve("ohra://x@127.0.0.1:1/user/greeter")))
        assert (
            await ask(rec, lambda r: Find(GREETER, r), timeout=1)
        ).service_instances == frozenset()


async def test_receptionist_is_one_per_system_and_lives_under_system():
    async with ActorSystem(Behaviors.ignore, "r") as system:
        receptionist = Receptionist(system)
        assert Receptionist(system) is receptionist
        assert receptionist.ref.path == "ohra://r/system/receptionist"
        assert system.resolve(receptionist.ref.path) is receptionist.ref


async def test_plain_system_still_stops_cleanly():
    async with ActorSystem(Behaviors.ignore, "r"):
        pass  # never touched the receptionist
    async with ActorSystem(Behaviors.ignore, "r") as system:
        Receptionist(system)
    assert system.is_terminated


async def test_receptionist_exists_from_the_start():
    async with ActorSystem(Behaviors.ignore, "r") as system:
        # created with the system, as Pekko's library extension
        assert system.receptionist is not None
        assert system.resolve("ohra://r/system/receptionist") is Receptionist(system).ref


async def test_a_subscription_through_a_message_adapter_ends_with_its_actor(
    caplog: pytest.LogCaptureFixture,
):
    def subscribing(ctx: ActorContext[Any]) -> Behavior[Any]:
        Receptionist(ctx.system).ref.tell(
            Subscribe(GREETER, ctx.message_adapter(lambda listing: ("listing", listing)))
        )
        return Behaviors.receive_message(
            lambda msg: Behaviors.stopped() if msg == "stop" else Behaviors.same
        )

    async with ActorSystem(Behaviors.receive(spawner), "r") as system:
        svc = await ask(system, lambda r: Spawn(Behaviors.ignore, "svc", r), timeout=1)
        actor = await ask(
            system, lambda r: Spawn(Behaviors.setup(subscribing), "subscriber", r), timeout=1
        )
        rec = Receptionist(system).ref
        await asyncio.sleep(0.05)
        actor.tell("stop")
        await eventually(lambda: gone(system, actor.path))
        with caplog.at_level(logging.DEBUG, logger="ohra.dead_letters"):
            rec.tell(Register(GREETER, svc))
            assert (
                await ask(rec, lambda r: Find(GREETER, r), timeout=1)
            ).service_instances == frozenset({svc})
        assert not [r for r in caplog.records if actor.path in r.getMessage()]
