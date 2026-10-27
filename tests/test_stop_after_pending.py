"""ctx.stop(child, after_pending=True): the stop queues behind the child's pending messages."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    MailboxOverflowError,
    MailboxSelector,
    Signal,
    Terminated,
)

from .util import eventually


@dataclass(frozen=True)
class Work:
    name: str


@dataclass(frozen=True)
class Quit:
    pass


type ChildCmd = Work | Quit


@dataclass(frozen=True)
class StopChild:
    pass


def child(log: list[str], gate: asyncio.Event) -> Behavior[ChildCmd]:
    async def on_message(msg: ChildCmd) -> Behavior[ChildCmd]:
        match msg:
            case Work(name):
                await gate.wait()
                log.append(name)
            case Quit():
                return Behaviors.stopped()
        return Behaviors.same

    return Behaviors.receive_message(on_message)


def parent(
    log: list[str],
    gate: asyncio.Event,
    holder: list[ActorRef[ChildCmd]],
    mailbox: MailboxSelector | None,
) -> Behavior[StopChild]:
    def setup(ctx: ActorContext[StopChild]) -> Behavior[StopChild]:
        ref = ctx.spawn(child(log, gate), "child", mailbox)
        ctx.watch(ref)
        holder.append(ref)

        def on_message(ctx: ActorContext[StopChild], _msg: StopChild) -> Behavior[StopChild]:
            ctx.stop(ref, after_pending=True)
            return Behaviors.same

        def on_signal(_ctx: ActorContext[StopChild], sig: Signal) -> Behavior[StopChild]:
            if isinstance(sig, Terminated):
                log.append("terminated")
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    return Behaviors.setup(setup)


async def test_stop_after_pending_runs_queued_messages_first():
    log: list[str] = []
    gate = asyncio.Event()
    holder: list[ActorRef[ChildCmd]] = []
    async with ActorSystem(parent(log, gate, holder, None), "s") as system:
        await eventually(lambda: bool(holder))
        c = holder[0]
        c.tell(Work("a"))
        c.tell(Work("b"))
        system.tell(StopChild())
        await asyncio.sleep(0.005)
        c.tell(Work("after"))  # sent after the stop: dropped with the stop
        gate.set()
        await eventually(lambda: "terminated" in log)
    assert log == ["a", "b", "terminated"]


async def test_stop_after_pending_survives_drop_old_overflow():
    log: list[str] = []
    gate = asyncio.Event()
    holder: list[ActorRef[ChildCmd]] = []
    async with ActorSystem(
        parent(log, gate, holder, MailboxSelector.bounded(2, overflow="drop_old")), "s"
    ) as system:
        await eventually(lambda: bool(holder))
        c = holder[0]
        c.tell(Work("busy"))  # taken by the handler, which then blocks on the gate
        await asyncio.sleep(0.005)
        c.tell(Work("a"))
        system.tell(StopChild())  # queue: [a, stop]
        await asyncio.sleep(0.005)
        c.tell(Work("b"))
        c.tell(Work("c"))  # overflow: the oldest real message goes, never the stop
        gate.set()
        await eventually(lambda: "terminated" in log)
    assert "terminated" in log
    assert "a" not in log  # evicted by drop_old


async def test_pending_stop_does_not_take_a_mailbox_slot():
    log: list[str] = []
    gate = asyncio.Event()
    holder: list[ActorRef[ChildCmd]] = []
    async with ActorSystem(
        parent(log, gate, holder, MailboxSelector.bounded(1, overflow="fail")), "s"
    ) as system:
        await eventually(lambda: bool(holder))
        c = holder[0]
        c.tell(Work("busy"))
        await asyncio.sleep(0.005)
        system.tell(StopChild())
        await asyncio.sleep(0.005)
        try:
            c.tell(Work("fits"))  # one slot, and the pending stop must not occupy it
            with pytest.raises(MailboxOverflowError):
                c.tell(Work("overflow"))
        finally:
            gate.set()
        await eventually(lambda: "terminated" in log)
    # "fits" was accepted but queued behind the stop, so it went with it
    assert log == ["busy", "terminated"]


async def test_child_stopping_on_its_own_does_not_dead_letter_the_pending_stop(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG, logger="ohra.dead_letters")
    log: list[str] = []
    gate = asyncio.Event()
    holder: list[ActorRef[ChildCmd]] = []
    async with ActorSystem(parent(log, gate, holder, None), "s") as system:
        await eventually(lambda: bool(holder))
        c = holder[0]
        c.tell(Work("busy"))
        await asyncio.sleep(0.005)
        c.tell(Quit())
        system.tell(StopChild())  # queued behind Quit, which stops the child first
        await asyncio.sleep(0.005)
        gate.set()
        await eventually(lambda: "terminated" in log)
    assert not [r.getMessage() for r in caplog.records if "object object" in r.getMessage()]
