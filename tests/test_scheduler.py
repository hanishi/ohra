"""The system scheduler: Pekko's ``system.scheduler``."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    AskTimeoutError,
    Behavior,
    Behaviors,
    Cancellable,
    Scheduler,
    ask,
)
from ohra.scheduler import EventLoopScheduler


async def test_schedule_once_runs_after_the_delay():
    ran: list[int] = []
    EventLoopScheduler().schedule_once(0.02, lambda: ran.append(1))
    await asyncio.sleep(0.005)
    assert ran == []
    await asyncio.sleep(0.05)
    assert ran == [1]


async def test_cancel_before_it_runs():
    ran: list[int] = []
    c = EventLoopScheduler().schedule_once(0.02, lambda: ran.append(1))
    assert c.cancel() is True
    assert c.is_cancelled is True
    assert c.cancel() is False
    await asyncio.sleep(0.05)
    assert ran == []


async def test_fixed_delay_repeats_until_cancelled():
    ran: list[int] = []
    c = EventLoopScheduler().schedule_with_fixed_delay(0.01, 0.01, lambda: ran.append(1))
    await asyncio.sleep(0.06)
    c.cancel()
    n = len(ran)
    assert n >= 3
    await asyncio.sleep(0.03)
    assert len(ran) == n


async def test_fixed_rate_repeats_on_schedule():
    ran: list[int] = []
    c = EventLoopScheduler().schedule_at_fixed_rate(0.01, 0.01, lambda: ran.append(1))
    await asyncio.sleep(0.06)
    c.cancel()
    n = len(ran)
    assert n >= 3
    await asyncio.sleep(0.03)
    assert len(ran) == n


@dataclass
class _Handle:
    cancelled: bool = False

    def cancel(self) -> bool:
        was, self.cancelled = self.cancelled, True
        return not was

    @property
    def is_cancelled(self) -> bool:
        return self.cancelled


@dataclass
class FakeScheduler:
    """Records what it is asked to schedule and runs nothing until told to."""

    once: list[tuple[float, Callable[[], None]]] = field(
        default_factory=lambda: list[tuple[float, Callable[[], None]]]()
    )

    def schedule_once(self, delay: float, fn: Callable[[], None]) -> Cancellable:
        self.once.append((delay, fn))
        return _Handle()

    def schedule_with_fixed_delay(
        self, initial_delay: float, delay: float, fn: Callable[[], None]
    ) -> Cancellable:
        return _Handle()

    def schedule_at_fixed_rate(
        self, initial_delay: float, interval: float, fn: Callable[[], None]
    ) -> Cancellable:
        return _Handle()


async def test_system_scheduler_is_used_by_timers():
    fake = FakeScheduler()
    scheduler: Scheduler = fake

    def guardian() -> Behavior[str]:
        def setup(ctx: ActorContext[str]) -> Behavior[str]:
            ctx.timers.start_single_timer("k", "tick", 5.0)
            return Behaviors.ignore

        return Behaviors.setup(setup)

    async with ActorSystem(guardian(), "sched", scheduler=scheduler) as system:
        assert system.scheduler is fake
        for _ in range(10):
            await asyncio.sleep(0)
        assert [d for d, _ in fake.once] == [5.0]


async def test_ask_timeout_uses_the_given_scheduler():
    fake = FakeScheduler()

    @dataclass(frozen=True)
    class Ping:
        reply_to: ActorRef[int]

    async with ActorSystem(Behaviors.ignore, "silent") as system:
        task = asyncio.create_task(ask(system, Ping, 0.01, scheduler=fake))
        await asyncio.sleep(0.05)
        assert not task.done()
        [(delay, fn)] = fake.once
        assert delay == 0.01
        fn()
        with pytest.raises(AskTimeoutError, match="timed out after 0.01s"):
            await task
