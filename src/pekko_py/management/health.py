"""Pekko Management's health checks: startup, readiness and liveness, each a list of named async checks run with a
timeout; the first failing check names the reason, as HealthChecksImpl does."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from ..cluster.gossip import MemberStatus

_log = logging.getLogger("pekko_py.management")

type HealthCheck = Callable[[], Awaitable[bool]]


@dataclass(frozen=True)
class HealthCheckSettings:
    """Pekko's ``health-checks`` settings; ``ready_states`` is the cluster-membership readiness check's accepted statuses."""

    startup_path: str = "startup"
    readiness_path: str = "ready"
    liveness_path: str = "alive"
    check_timeout: float = 1.0
    ready_states: frozenset[MemberStatus] = frozenset({"up", "weakly_up"})

    def __post_init__(self) -> None:
        if self.check_timeout <= 0:
            raise ValueError("check_timeout must be > 0")


class HealthChecks:
    def __init__(self, settings: HealthCheckSettings) -> None:
        self.settings = settings
        self._startup: list[tuple[str, HealthCheck]] = []
        self._readiness: list[tuple[str, HealthCheck]] = []
        self._liveness: list[tuple[str, HealthCheck]] = []

    def add_startup_check(self, name: str, check: HealthCheck) -> None:
        self._startup.append((name, check))

    def add_readiness_check(self, name: str, check: HealthCheck) -> None:
        self._readiness.append((name, check))

    def add_liveness_check(self, name: str, check: HealthCheck) -> None:
        self._liveness.append((name, check))

    async def startup(self) -> str | None:
        return await self._run(self._startup, "startup")

    async def ready(self) -> str | None:
        return await self._run(self._readiness, "readiness")

    async def alive(self) -> str | None:
        return await self._run(self._liveness, "liveness")

    async def _run(self, checks: list[tuple[str, HealthCheck]], kind: str) -> str | None:
        """None when every check passes, else the first failing check's reason, in registration order."""
        results = await asyncio.gather(*(self._one(name, check) for name, check in checks))
        for reason in results:
            if reason is not None:
                _log.info("%s check failed: %s", kind, reason)
                return reason
        return None

    async def _one(self, name: str, check: HealthCheck) -> str | None:
        try:
            ok = await asyncio.wait_for(check(), self.settings.check_timeout)
        except TimeoutError:
            return f"Check [{name}] timed out after {self.settings.check_timeout} s"
        except Exception as e:
            return f"Check [{name}] failed: {e}"
        return None if ok else f"Check [{name}] not ok"
