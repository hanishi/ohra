"""The system scheduler: Pekko's typed ``Scheduler``, reached as ``system.scheduler``.

Actor timers, backoff restart delays and ask timeouts run on it, so a test can replace it
with ``ohra.testkit``'s explicitly triggered one.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Protocol


class Cancellable(Protocol):
    def cancel(self) -> bool:
        """Cancel it; True if this call cancelled it, False if it was already cancelled."""
        ...

    @property
    def is_cancelled(self) -> bool: ...


class Scheduler(Protocol):
    """Runs ``fn`` later. Durations are seconds."""

    def schedule_once(self, delay: float, fn: Callable[[], None]) -> Cancellable: ...

    def schedule_with_fixed_delay(
        self, initial_delay: float, delay: float, fn: Callable[[], None]
    ) -> Cancellable: ...

    def schedule_at_fixed_rate(
        self, initial_delay: float, interval: float, fn: Callable[[], None]
    ) -> Cancellable: ...


class _Scheduled:
    """A job on the event loop; holds its current handle so a repeating job stays cancellable."""

    __slots__ = ("handle", "cancelled")

    def __init__(self) -> None:
        self.handle: asyncio.TimerHandle | None = None
        self.cancelled = False

    def cancel(self) -> bool:
        if self.cancelled:
            return False
        self.cancelled = True
        if self.handle is not None:
            self.handle.cancel()
        return True

    @property
    def is_cancelled(self) -> bool:
        return self.cancelled


class EventLoopScheduler:
    """The default scheduler: ``call_later`` and ``call_at`` on the running loop."""

    def schedule_once(self, delay: float, fn: Callable[[], None]) -> Cancellable:
        job = _Scheduled()
        job.handle = asyncio.get_running_loop().call_later(delay, fn)
        return job

    def schedule_with_fixed_delay(
        self, initial_delay: float, delay: float, fn: Callable[[], None]
    ) -> Cancellable:
        loop = asyncio.get_running_loop()
        job = _Scheduled()

        def run() -> None:
            fn()
            if not job.cancelled:
                job.handle = loop.call_later(delay, run)

        job.handle = loop.call_later(initial_delay, run)
        return job

    def schedule_at_fixed_rate(
        self, initial_delay: float, interval: float, fn: Callable[[], None]
    ) -> Cancellable:
        loop = asyncio.get_running_loop()
        job = _Scheduled()
        start = loop.time() + initial_delay
        n = 0

        def run() -> None:
            nonlocal n
            fn()
            n += 1
            if not job.cancelled:
                job.handle = loop.call_at(start + n * interval, run)

        job.handle = loop.call_at(start, run)
        return job
