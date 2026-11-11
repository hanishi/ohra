"""Pekko's ``TestLease`` and ``TestLeaseExt`` (``coordination/src/test/.../TestLease.scala``
36-113): a lease whose acquire result and check-lease result a test controls."""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar

from ..coordination.lease import Lease, LeaseProvider, LeaseSettings
from ..system import ActorSystem
from .probe import TestProbe
from .settings import TestKitSettings

log = logging.getLogger("ohra.testkit.lease")

# ponytail: a TestLease holds its system, so an entry lives as long as the process; test-only
_test_lease_exts: weakref.WeakKeyDictionary[ActorSystem[Any], TestLeaseExt] = (
    weakref.WeakKeyDictionary()
)


class TestLeaseExt:
    """Pekko's ``TestLeaseExt`` (``TestLease.scala`` 36-55): ``TestLeaseExt(system)`` is the
    system's one instance, which finds the ``TestLease`` created under a name."""

    __test__ = False  # not a pytest test class

    def __new__(cls, system: ActorSystem[Any]) -> TestLeaseExt:
        ext = _test_lease_exts.get(system)
        if ext is None:
            ext = object.__new__(cls)
            ext._test_leases = {}
            _test_lease_exts[system] = ext
        return ext

    _test_leases: dict[str, TestLease]

    def get_test_lease(self, name: str) -> TestLease:
        """``getTestLease`` (``TestLease.scala`` 43). Pekko throws ``IllegalStateException``;
        this raises ``RuntimeError``, not ``AssertionError``, so ``await_assert`` retries it."""
        lease = self._test_leases.get(name)
        if lease is None:
            raise RuntimeError(
                f"Test lease {name} has not been set yet. "
                f"Current leases {sorted(self._test_leases)}"
            )
        return lease

    def set_test_lease(self, name: str, lease: TestLease) -> None:
        """``setTestLease`` (``TestLease.scala`` 52)."""
        self._test_leases[name] = lease


class _Host:
    """What a TestProbe needs of a test kit."""

    def __init__(self, system: ActorSystem[Any]) -> None:
        self.system = system
        self.settings = TestKitSettings()


class TestLease(Lease):
    """Pekko's ``TestLease`` (``TestLease.scala`` 66-113). ``acquire`` reports to ``probe`` and
    answers with the future set by ``set_next_acquire_result``, ``initial_promise`` at first."""

    __test__ = False  # not a pytest test class

    @dataclass(frozen=True)
    class AcquireReq:
        """``AcquireReq`` (``TestLease.scala`` 58)."""

        owner: str

    @dataclass(frozen=True)
    class ReleaseReq:
        """``ReleaseReq`` (``TestLease.scala`` 59)."""

        owner: str

    IMPLEMENTATION: ClassVar[str] = "test-lease"

    @staticmethod
    def register(system: ActorSystem[Any]) -> None:
        """Pekko's ``TestLease.config`` (``TestLease.scala`` 61-65), the ``test-lease`` block."""
        LeaseProvider(system).register(
            TestLease.IMPLEMENTATION, lambda settings: TestLease(settings, system)
        )

    def __init__(self, settings: LeaseSettings, system: ActorSystem[Any]) -> None:
        super().__init__(settings)
        self.probe: TestProbe[TestLease.AcquireReq | TestLease.ReleaseReq] = TestProbe(
            _Host(system)
        )
        self.initial_promise: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._next_acquire_result = self.initial_promise
        self._next_check_lease_result = False
        self._current_callback: Callable[[BaseException | None], None] = lambda _: None
        log.info("Creating lease %s", settings)
        TestLeaseExt(system).set_test_lease(settings.lease_name, self)

    def set_next_acquire_result(self, next: asyncio.Future[bool]) -> None:
        """``setNextAcquireResult`` (``TestLease.scala`` 83)."""
        self._next_acquire_result = next

    def set_next_check_lease_result(self, value: bool) -> None:
        """``setNextCheckLeaseResult`` (``TestLease.scala`` 86)."""
        self._next_check_lease_result = value

    def current_callback(self) -> Callable[[BaseException | None], None]:
        """``getCurrentCallback`` (``TestLease.scala`` 89)."""
        return self._current_callback

    async def acquire(
        self, lease_lost_callback: Callable[[BaseException | None], None] | None = None
    ) -> bool:
        """``acquire()`` and ``acquire(callback)`` (``TestLease.scala`` 92-96 and 108-111). The
        callback is set and ``AcquireReq`` sent when the coroutine first runs, not when
        ``acquire(cb)`` is called. The result is awaited through ``asyncio.shield``:
        ``pipe_to_self`` cancels the awaitable when the actor stops, and that must not cancel the
        test's promise."""
        if lease_lost_callback is not None:
            self._current_callback = lease_lost_callback
        log.info("acquire, current response %s", self._next_acquire_result)
        self.probe.ref.tell(TestLease.AcquireReq(self.settings.owner_name))
        return await asyncio.shield(self._next_acquire_result)

    async def release(self) -> bool:
        """``release`` (``TestLease.scala`` 98-101)."""
        self.probe.ref.tell(TestLease.ReleaseReq(self.settings.owner_name))
        return True

    def check_lease(self) -> bool:
        """``checkLease`` (``TestLease.scala`` 104)."""
        return self._next_check_lease_result
