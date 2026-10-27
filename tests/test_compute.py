"""Parallel compute with pipe_to_self(asyncio.to_thread(...)) and a stashing behavior."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    StashBuffer,
    Success,
    Try,
)

from .util import eventually


@dataclass(frozen=True)
class Compute:
    seconds: float
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class Done:
    result: Try[str]


@dataclass(frozen=True)
class Echo:
    text: str
    reply_to: ActorRef[str]


type Cmd = Compute | Done | Echo


def worker() -> Behavior[Cmd]:
    """One computation at a time, in a thread. Everything else waits in the stash."""

    def with_stash(buffer: StashBuffer[Cmd]) -> Behavior[Cmd]:
        def idle(msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Compute(seconds, reply_to):
                    return Behaviors.setup(lambda ctx: computing(ctx, seconds, reply_to))
                case Echo(text, reply_to):
                    reply_to.tell(text)
                case Done():
                    pass
            return Behaviors.same

        def computing(
            ctx: ActorContext[Cmd], seconds: float, reply_to: ActorRef[str]
        ) -> Behavior[Cmd]:
            ctx.pipe_to_self(asyncio.to_thread(blocking_work, seconds), Done)

            def on_message(msg: Cmd) -> Behavior[Cmd]:
                match msg:
                    case Done(result):
                        reply_to.tell(result.value if isinstance(result, Success) else "failed")
                        return buffer.unstash_all(Behaviors.receive_message(idle))
                    case _:
                        buffer.stash(msg)
                        return Behaviors.same

            return Behaviors.receive_message(on_message)

        return Behaviors.receive_message(idle)

    return Behaviors.with_stash(100, with_stash)


def blocking_work(seconds: float) -> str:
    time.sleep(seconds)  # releases the GIL, like numpy does
    return f"slept {seconds}"


def collector_of(replies: list[str]) -> Behavior[str]:
    def on_message(msg: str) -> Behavior[str]:
        replies.append(msg)
        return Behaviors.same

    return Behaviors.receive_message(on_message)


async def test_messages_during_compute_are_stashed_and_replayed_in_order():
    replies: list[str] = []
    collector = collector_of(replies)
    async with ActorSystem(collector, "sink") as sink, ActorSystem(worker(), "t") as system:
        system.tell(Compute(0.02, sink))
        system.tell(Echo("a", sink))
        system.tell(Echo("b", sink))
        await eventually(lambda: len(replies) == 3)
        assert replies == ["slept 0.02", "a", "b"]


async def test_event_loop_stays_responsive_during_parallel_compute():
    """Many actors blocking threads at once: wall time is parallel, and a ticker never stalls."""
    replies: list[str] = []
    collector = collector_of(replies)
    gaps: list[float] = []

    async def ticker() -> None:
        loop = asyncio.get_running_loop()
        last = loop.time()
        while True:
            await asyncio.sleep(0.001)
            now = loop.time()
            gaps.append(now - last)
            last = now

    def guardian(ctx: ActorContext[Compute]) -> Behavior[Compute]:
        workers = [ctx.spawn(worker(), f"w{i}") for i in range(8)]

        def on_message(msg: Compute) -> Behavior[Compute]:
            for w in workers:
                w.tell(msg)
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    tick = asyncio.create_task(ticker())
    try:
        async with (
            ActorSystem(collector, "sink") as sink,
            ActorSystem(Behaviors.setup(guardian), "t") as system,
        ):
            t0 = time.perf_counter()
            system.tell(Compute(0.1, sink))
            await eventually(lambda: len(replies) == 8)
            elapsed = time.perf_counter() - t0
    finally:
        tick.cancel()
        await asyncio.gather(tick, return_exceptions=True)
    assert elapsed < 0.4, elapsed  # 8 x 100ms sequential would be 800ms
    assert max(gaps) < 0.05, max(gaps)  # the loop kept ticking while threads were busy
