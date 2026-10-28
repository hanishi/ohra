"""Pekko's ActorTestKit: an actor system for one test, with actors spawned synchronously."""

from __future__ import annotations

import asyncio
import re
import sys
from types import TracebackType
from typing import Any, Self

from ..behavior import Behavior, Behaviors
from ..eventstream import DeadLetter, Dropped, EventStream, UnhandledMessage
from ..cell import _cell_of  # pyright: ignore[reportPrivateUsage]
from ..ref import ActorRef
from ..scheduler import Scheduler
from ..system import ActorSystem
from .manual_time import ExplicitlyTriggeredScheduler
from .probe import TestProbe
from .settings import TestKitSettings, seconds


class ActorTestKit:
    """``async with ActorTestKit() as testkit:`` starts an actor system and shuts it down on exit.

    Without a name the system is named after the calling function, as Pekko names it after the
    test. A shutdown that takes longer than ``system_shutdown_default`` raises RuntimeError and
    leaves the system running, unless the block itself raised, whose exception is never masked.
    With ``manual_time=True`` the system runs on an explicitly triggered scheduler, for
    ``ManualTime``.
    """

    __test__ = False  # not a pytest test class

    def __init__(
        self,
        name: str | None = None,
        settings: TestKitSettings | None = None,
        manual_time: bool = False,
    ) -> None:
        if name is None:
            name = sys._getframe(1).f_code.co_name  # pyright: ignore[reportPrivateUsage]
        self.name = re.sub(r"[^A-Za-z0-9_-]", "", name) or "ActorTestKit"
        self.settings = settings or TestKitSettings()
        self.timeout = self.settings.default_timeout_dilated
        """The default for asks: ``default_timeout``, dilated."""
        self._scheduler: Scheduler | None = ExplicitlyTriggeredScheduler() if manual_time else None
        self.system: ActorSystem[Any]
        self.scheduler: Scheduler

    async def __aenter__(self) -> Self:
        # Pekko's ActorTestKitGuardian only hosts children here: spawn and stop go to its cell
        self.system = ActorSystem(Behaviors.empty, self.name, scheduler=self._scheduler)
        self.scheduler = self.system.scheduler
        return self

    async def __aexit__(
        self, et: type[BaseException] | None, e: BaseException | None, tb: TracebackType | None
    ) -> None:
        # Pekko's shutdownTestKit
        self.system.terminate()
        d = self.settings.system_shutdown_default_dilated
        terminated = asyncio.ensure_future(self.system.when_terminated())
        done, _ = await asyncio.wait({terminated}, timeout=d)
        if not done:  # as Pekko, give up and leave the system running
            terminated.cancel()
            message = f"Failed to stop [{self.name}] within [{seconds(d)}]"
            if self.settings.throw_on_shutdown_timeout and et is None:
                raise RuntimeError(message)
            self.system.log.warning(message)

    def spawn[T](self, behavior: Behavior[T], name: str | None = None) -> ActorRef[T]:
        """Spawn a child of the test kit's guardian and return its ref at once."""
        return self.system._root.spawn_child(behavior, name, None).ref  # pyright: ignore[reportPrivateUsage]

    def create_test_probe[M](self, name: str | None = None) -> TestProbe[M]:  # pyright: ignore[reportInvalidTypeVarUse]
        """A probe under ``/system``; its message type comes from the annotation at the call
        site: ``probe: TestProbe[int] = testkit.create_test_probe()``."""
        return TestProbe[M](self, name)

    def create_dead_letter_probe(self) -> TestProbe[DeadLetter]:
        """A probe subscribed to dead letters. The subscription is in place for every dead letter
        after this call: both go through the event stream's mailbox, in order."""
        return self._subscribed_probe(DeadLetter)

    def create_unhandled_message_probe(self) -> TestProbe[UnhandledMessage]:
        """A probe subscribed to unhandled messages."""
        return self._subscribed_probe(UnhandledMessage)

    def create_dropped_message_probe(self) -> TestProbe[Dropped]:
        """A probe subscribed to dropped messages."""
        return self._subscribed_probe(Dropped)

    def _subscribed_probe[E](self, topic: type[E]) -> TestProbe[E]:
        probe = TestProbe[E](self)
        self.system.event_stream.tell(EventStream.Subscribe(topic, probe.ref))
        return probe

    async def stop(self, ref: ActorRef[Any], max: float | None = None) -> None:
        """Stop an actor spawned by ``spawn`` and wait until it has stopped."""
        max = self.timeout if max is None else max
        cell = _cell_of(ref)
        if not cell.done.done():  # Pekko watches it first, so a stopped actor answers at once
            self.system._root.stop_child(ref)  # pyright: ignore[reportPrivateUsage]
        try:
            await asyncio.wait_for(asyncio.shield(cell.done), max)
        except TimeoutError:
            raise AssertionError(
                f"timeout ({seconds(max)}) during stop() waiting for actor [{ref.path}] to stop"
            ) from None
