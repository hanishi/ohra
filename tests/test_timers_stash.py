"""Timers and stash."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from pekko_py import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    StashBuffer,
    StashOverflowError,
    SupervisorStrategy,
    TimerScheduler,
)

from .util import eventually

# --- timers ----------------------------------------------------------------------


@dataclass(frozen=True)
class Tick:
    name: str


@dataclass(frozen=True)
class Start:
    name: str
    delay: float
    repeat: bool = False


@dataclass(frozen=True)
class Cancel:
    name: str


@dataclass(frozen=True)
class Busy:
    """Start a timer, let it fire into the mailbox while we are busy, then cancel it."""


@dataclass(frozen=True)
class Fail:
    pass


@dataclass(frozen=True)
class IsActive:
    name: str
    reply_to: ActorRef[bool]


type TimerCmd = Tick | Start | Cancel | Busy | Fail | IsActive


def ticker(ticks: list[str], setup_timers: tuple[str, ...] = ()) -> Behavior[TimerCmd]:
    def with_timers(timers: TimerScheduler[TimerCmd]) -> Behavior[TimerCmd]:
        for name in setup_timers:
            timers.start_timer_with_fixed_delay(name, Tick(name), 0.005)

        async def on_message(msg: TimerCmd) -> Behavior[TimerCmd]:
            match msg:
                case Tick(name):
                    ticks.append(name)
                case Start(name, delay, True):
                    timers.start_timer_with_fixed_delay(name, Tick(name), delay)
                case Start(name, delay, False):
                    timers.start_single_timer(name, Tick(name), delay)
                case Cancel(name):
                    timers.cancel(name)
                case Busy():
                    timers.start_single_timer("busy", Tick("busy"), 0)
                    await asyncio.sleep(0.02)  # the timer fires and Tick("busy") is queued
                    timers.cancel("busy")
                case Fail():
                    raise ValueError("boom")
                case IsActive(name, reply_to):
                    reply_to.tell(timers.is_timer_active(name))
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.with_timers(with_timers)


async def test_single_timer_fires_once():
    ticks: list[str] = []
    async with ActorSystem(ticker(ticks), "t") as system:
        system.tell(Start("a", 0.01))
        assert await system.ask(lambda r: IsActive("a", r), timeout=1) is True
        await eventually(lambda: ticks == ["a"])
        await asyncio.sleep(0.03)
        assert ticks == ["a"]
        assert await system.ask(lambda r: IsActive("a", r), timeout=1) is False


async def test_fixed_delay_timer_repeats_until_cancelled():
    ticks: list[str] = []
    async with ActorSystem(ticker(ticks), "t") as system:
        system.tell(Start("a", 0.005, repeat=True))
        await eventually(lambda: len(ticks) >= 3)
        system.tell(Cancel("a"))
        assert await system.ask(lambda r: IsActive("a", r), timeout=1) is False
        n = len(ticks)
        await asyncio.sleep(0.03)
        assert len(ticks) == n


async def test_restarting_a_key_cancels_the_previous_timer():
    ticks: list[str] = []
    async with ActorSystem(ticker(ticks), "t") as system:
        system.tell(Start("a", 0.01))
        system.tell(Start("a", 0.03))  # same key: the 10ms timer must not fire
        await asyncio.sleep(0.06)
        assert ticks == ["a"]


async def test_message_from_cancelled_timer_already_in_mailbox_is_dropped():
    ticks: list[str] = []
    async with ActorSystem(ticker(ticks), "t") as system:
        system.tell(Busy())
        system.tell(Start("after", 0))
        await eventually(lambda: ticks == ["after"])
        await asyncio.sleep(0.01)
        assert ticks == ["after"]


async def test_restart_cancels_timers_and_setup_recreates_its_own():
    ticks: list[str] = []
    b = Behaviors.supervise(ticker(ticks, setup_timers=("heartbeat",))).on_failure(
        SupervisorStrategy.restart()
    )
    async with ActorSystem(b, "t") as system:
        system.tell(Start("extra", 0.005, repeat=True))
        await eventually(lambda: "extra" in ticks and "heartbeat" in ticks)
        system.tell(Fail())
        assert await system.ask(lambda r: IsActive("extra", r), timeout=1) is False
        assert await system.ask(lambda r: IsActive("heartbeat", r), timeout=1) is True
        extra, hb = ticks.count("extra"), ticks.count("heartbeat")
        await eventually(lambda: ticks.count("heartbeat") > hb + 2)
        assert ticks.count("extra") == extra


async def test_timers_stop_with_the_actor():
    ticks: list[str] = []
    async with ActorSystem(ticker(ticks), "t") as system:
        system.tell(Start("a", 0.005, repeat=True))
        await eventually(lambda: len(ticks) >= 1)
    n = len(ticks)
    await asyncio.sleep(0.02)
    assert len(ticks) == n


# --- stash --------------------------------------------------------------------------


@dataclass(frozen=True)
class Authed:
    pass


@dataclass(frozen=True)
class Add:
    n: int


@dataclass(frozen=True)
class Get:
    reply_to: ActorRef[list[int]]


type StashCmd = Authed | Add | Get


def gate(capacity: int, seen: list[int]) -> Behavior[StashCmd]:
    def with_stash(buffer: StashBuffer[StashCmd]) -> Behavior[StashCmd]:
        def waiting(msg: StashCmd) -> Behavior[StashCmd]:
            match msg:
                case Authed():
                    return buffer.unstash_all(active)
                case _:
                    buffer.stash(msg)
                    return Behaviors.same

        async def on_active(msg: StashCmd) -> Behavior[StashCmd]:
            match msg:
                case Add(n):
                    await asyncio.sleep(0)  # an await inside the replay must not reorder anything
                    seen.append(n)
                case Get(reply_to):
                    reply_to.tell(list(seen))
                case Authed():
                    pass
            return Behaviors.same

        active = Behaviors.receive_message(on_active)
        return Behaviors.receive_message(waiting)

    return Behaviors.with_stash(capacity, with_stash)


async def test_stashed_messages_replay_in_order_before_new_ones():
    seen: list[int] = []
    async with ActorSystem(gate(1000, seen), "t") as system:
        for i in range(1000):
            system.tell(Add(i))
        system.tell(Authed())
        system.tell(Add(-1))  # arrives after Authed but must run after the replay
        assert await system.ask(Get, timeout=2) == [*range(1000), -1]


async def test_stash_overflow_fails_the_actor():
    seen: list[int] = []
    system = ActorSystem(gate(2, seen), "t")
    for i in range(3):
        system.tell(Add(i))
    await asyncio.wait_for(system.when_terminated(), 1)
    assert seen == []


async def test_stash_buffer_size_and_is_full():
    buffer: StashBuffer[int] = StashBuffer(2)
    assert buffer.is_empty and not buffer.is_full
    buffer.stash(1)
    buffer.stash(2)
    assert buffer.size == 2 and buffer.is_full
    try:
        buffer.stash(3)
    except StashOverflowError:
        pass
    else:
        raise AssertionError("expected StashOverflowError")


async def test_unstash_inside_setup_is_allowed():
    """Pekko lets a factory return buffer.unstashAll(...) directly."""
    seen: list[int] = []

    def with_stash(buffer: StashBuffer[StashCmd]) -> Behavior[StashCmd]:
        buffer.stash(Add(7))

        def on_message(msg: StashCmd) -> Behavior[StashCmd]:
            match msg:
                case Add(n):
                    seen.append(n)
                case Get(reply_to):
                    reply_to.tell(list(seen))
                case Authed():
                    pass
            return Behaviors.same

        return buffer.unstash_all(Behaviors.receive_message(on_message))

    async with ActorSystem(Behaviors.with_stash(10, with_stash), "t") as system:
        assert await system.ask(Get, timeout=1) == [7]
