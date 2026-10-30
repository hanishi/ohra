"""Pekko's ManualTime and the explicitly triggered scheduler under it."""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ..cell import ActorCell
from ..scheduler import Cancellable
from ..system import ActorSystem

if TYPE_CHECKING:
    from .probe import TestProbe

_log = logging.getLogger("ohra.testkit")

# virtual times are rounded to this many digits, so float steps add up as Pekko's millis do
_DIGITS = 9


class _Item:
    __slots__ = ("interval", "fn", "cancelled")

    def __init__(self, interval: float | None, fn: Callable[[], None]) -> None:
        self.interval = interval
        self.fn = fn
        self.cancelled = False


class _ItemCancellable:
    __slots__ = ("_scheduler", "_item")

    def __init__(self, scheduler: ExplicitlyTriggeredScheduler, item: _Item) -> None:
        self._scheduler = scheduler
        self._item = item

    def cancel(self) -> bool:
        self._item.cancelled = True
        return self._scheduler._scheduled.pop(self._item, None) is not None  # pyright: ignore[reportPrivateUsage]

    @property
    def is_cancelled(self) -> bool:
        return self._item.cancelled


class ExplicitlyTriggeredScheduler:
    """A scheduler whose clock moves only when told: Pekko's ExplicitlyTriggeredScheduler.

    ``current_time`` starts at 0. A job with a delay of 0 or less runs at once; the others run
    when ``time_passes`` reaches their time, in time order.
    """

    def __init__(self) -> None:
        self._current_time = 0.0
        self._scheduled: dict[_Item, tuple[float, int]] = {}  # item -> (due time, order)
        self._order = itertools.count()

    @property
    def current_time(self) -> float:
        return self._current_time

    def schedule_once(self, delay: float, fn: Callable[[], None]) -> Cancellable:
        return self._schedule(delay, None, fn)

    def schedule_with_fixed_delay(
        self, initial_delay: float, delay: float, fn: Callable[[], None]
    ) -> Cancellable:
        return self._schedule(initial_delay, delay, fn)

    def schedule_at_fixed_rate(
        self, initial_delay: float, interval: float, fn: Callable[[], None]
    ) -> Cancellable:
        return self._schedule(initial_delay, interval, fn)

    def time_passes(self, amount: float) -> None:
        """Run every job due within ``amount`` seconds, then move the clock there. Unlike
        ``ManualTime.time_passes``, the actors do not get to run in between."""
        new_time = round(self._current_time + amount, _DIGITS)
        self._execute_tasks(new_time)
        self.advance_to(new_time)

    def next_due(self, run_to: float) -> _Item | None:
        """The first job due at or before ``run_to``."""
        due = [(t, item) for item, t in self._scheduled.items() if t[0] <= run_to]
        return min(due, key=lambda d: d[0])[1] if due else None

    def run(self, item: _Item) -> None:
        """Run a due job at its time; a repeating job that did not raise is due again."""
        time, _ = self._scheduled.pop(item)
        self._current_time = time
        try:
            item.fn()
        except Exception:
            _log.exception("scheduled job failed")
            return
        if item.interval is not None and not item.cancelled:
            self._put(item, round(time + item.interval, _DIGITS))

    def advance_to(self, t: float) -> None:
        self._current_time = t

    def _execute_tasks(self, run_to: float) -> None:
        while (item := self.next_due(run_to)) is not None:
            self.run(item)  # running it may have scheduled more

    def _put(self, item: _Item, time: float) -> None:
        self._scheduled[item] = (time, next(self._order))

    def _schedule(
        self, initial_delay: float, interval: float | None, fn: Callable[[], None]
    ) -> Cancellable:
        item = _Item(interval, fn)
        self._put(item, round(self._current_time + initial_delay, _DIGITS))
        if initial_delay <= 0:
            self._execute_tasks(self._current_time)
        return _ItemCancellable(self, item)


class ManualTime:
    """``ManualTime(testkit.system)`` on a test kit made with ``manual_time=True``: timers,
    backoff delays and asks on the system scheduler wait for ``time_passes``.

    Before each due job the loop settles: every actor's mailbox is empty and every other task
    waits on a future (a stream has reached its pull), or 100 ms of real time have passed.
    Amounts are not dilated.
    """

    def __init__(self, system: ActorSystem[Any]) -> None:
        if not isinstance(system.scheduler, ExplicitlyTriggeredScheduler):
            raise ValueError(
                "ActorSystem not configured with explicitly triggered scheduler, make sure to "
                "pass manual_time=True to ActorTestKit or "
                "scheduler=ExplicitlyTriggeredScheduler() to ActorSystem"
            )
        self.system = system
        self.scheduler: ExplicitlyTriggeredScheduler = system.scheduler

    async def time_passes(self, amount: float) -> None:
        """Move the clock ``amount`` seconds, running each job that comes due on the way."""
        scheduler = self.scheduler
        target = round(scheduler.current_time + amount, _DIGITS)
        while True:
            await self._settle()
            item = scheduler.next_due(target)
            if item is None:
                break
            scheduler.run(item)
        scheduler.advance_to(target)

    async def expect_no_message_for(self, duration: float, *probes: TestProbe[Any]) -> None:
        """Let ``duration`` pass; then none of ``probes`` may have a message."""
        await self.time_passes(duration)
        for p in probes:
            await p.expect_no_message(0)

    async def _settle(self) -> None:
        # Pekko sleeps 100 ms once, to let the dispatchers clear. Here the loop is settled when
        # every mailbox is empty and every other task waits on a future, so a stream's drive
        # has reached its pull (a tick's waiter, grouped_within's timer) before time moves.
        # ponytail: a task that never blocks (a busy loop of sleep(0)) never settles; the
        # 100 ms deadline is the fallback for it, as Pekko's fixed sleep is.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 0.1
        roots: list[ActorCell[Any]] = [self.system._root, self.system._system_root]  # pyright: ignore[reportPrivateUsage]
        me = asyncio.current_task()
        while True:
            await asyncio.sleep(0)
            settled = all(_idle(c) for c in roots) and all(
                _blocked(t) for t in asyncio.all_tasks() if t is not me
            )
            if settled or loop.time() >= deadline:
                return


def _idle(cell: ActorCell[Any]) -> bool:
    return cell.is_idle and all(_idle(c) for c in list(cell.children.values()))


def _blocked(task: asyncio.Task[Any]) -> bool:
    # ``_fut_waiter`` is CPython's private record of the future a task is suspended on; a task
    # that is runnable (or about to be woken) has none, or one that is already done
    waiter = getattr(task, "_fut_waiter", None)
    return waiter is not None and not waiter.done()
