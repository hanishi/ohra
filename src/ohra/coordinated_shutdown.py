"""Apache Pekko's CoordinatedShutdown: a fixed sequence of phases, each with a timeout, run by
``ActorSystem. terminate()``, by the cluster when this node leaves or is downed, and, opted in, on
a signal. Tasks are added to phases; a phase's tasks run concurrently, a failing task is logged and
the run continues (Pekko's ``recover = on``), a phase that outlives its timeout is logged and its
tasks are cancelled (Pekko lets them run on), and the last phase stops the system."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from .ref import ActorRef

if TYPE_CHECKING:
    from .system import ActorSystem

_log = logging.getLogger("ohra.coordinated_shutdown")

DEFAULT_PHASE_TIMEOUT = 5.0


@dataclass(frozen=True)
class Phase:
    name: str
    timeout: float


PHASES: tuple[Phase, ...] = (
    Phase("before-service-unbind", DEFAULT_PHASE_TIMEOUT),
    Phase("service-unbind", DEFAULT_PHASE_TIMEOUT),
    Phase("service-requests-done", DEFAULT_PHASE_TIMEOUT),
    Phase("service-stop", DEFAULT_PHASE_TIMEOUT),
    Phase("before-cluster-shutdown", DEFAULT_PHASE_TIMEOUT),
    Phase("cluster-sharding-shutdown-region", 10.0),
    Phase("cluster-leave", DEFAULT_PHASE_TIMEOUT),
    Phase("cluster-exiting", 10.0),
    Phase("cluster-exiting-done", DEFAULT_PHASE_TIMEOUT),
    Phase("cluster-shutdown", DEFAULT_PHASE_TIMEOUT),
    Phase("before-actor-system-terminate", DEFAULT_PHASE_TIMEOUT),
    Phase("actor-system-terminate", 10.0),
)
"""Pekko's phases in order, with reference.conf's timeouts; the three with their own timeout keep
it when ``default_phase_timeout`` changes, as in Pekko's config."""
PHASE_NAMES: frozenset[str] = frozenset(p.name for p in PHASES)
_OWN_TIMEOUT: frozenset[str] = frozenset(
    p.name for p in PHASES if p.timeout != DEFAULT_PHASE_TIMEOUT
)


@dataclass(frozen=True)
class CoordinatedShutdownSettings:
    """Pekko's ``coordinated-shutdown`` settings, in seconds. ``run_on_signals`` installs a handler
    per signal that runs the shutdown with ``JvmExitReason``; off by default, where Pekko's JVM
    shutdown hook is on. A handler owns its signal for the whole event loop: use it only when this
    system owns the process's signals."""

    default_phase_timeout: float = DEFAULT_PHASE_TIMEOUT
    phase_timeouts: Mapping[str, float] = field(default_factory=lambda: MappingProxyType({}))
    run_by_actor_system_terminate: bool = True
    run_on_signals: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if self.default_phase_timeout <= 0:
            raise ValueError("default_phase_timeout must be > 0")
        for phase, timeout in self.phase_timeouts.items():
            if phase not in PHASE_NAMES:
                raise ValueError(f"unknown phase {phase!r}; known phases: {sorted(PHASE_NAMES)}")
            if timeout <= 0:
                raise ValueError(f"timeout of phase {phase!r} must be > 0")


# --- reasons -------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Reason:
    """Why the shutdown runs. Subclass for your own; Pekko's are below."""


@dataclass(frozen=True)
class ActorSystemTerminateReason(Reason): ...


@dataclass(frozen=True)
class JvmExitReason(Reason):
    """A signal from ``run_on_signals``; Pekko's name for its JVM shutdown hook."""


@dataclass(frozen=True)
class ClusterDowningReason(Reason): ...


@dataclass(frozen=True)
class ClusterLeavingReason(Reason): ...


# --- the extension -------------------------------------------------------------------------------

type Task = Callable[[], Awaitable[None]]


class CoordinatedShutdown:
    """``CoordinatedShutdown(system)``: add tasks to phases with ``add_task``; ``run(reason)`` runs
    the phases once."""

    system: ActorSystem[Any]
    settings: CoordinatedShutdownSettings
    _tasks: dict[str, list[tuple[str, Task]]]
    _started_phases: set[str]
    _run: asyncio.Task[None] | None
    _reason: Reason | None

    def __new__(cls, system: ActorSystem[Any]) -> CoordinatedShutdown:
        return system.coordinated_shutdown

    @classmethod
    def _start(
        cls, system: ActorSystem[Any], settings: CoordinatedShutdownSettings
    ) -> CoordinatedShutdown:
        self = object.__new__(cls)
        self.system = system
        self.settings = settings
        self._tasks = {}
        self._started_phases = set()
        self._run = None
        self._reason = None
        self.add_task("actor-system-terminate", "terminate-system", self._terminate_system)
        return self

    # --- timeouts --------------------------------------------------------------------------------

    def timeout(self, phase: str) -> float:
        if phase not in PHASE_NAMES:
            raise ValueError(f"unknown phase {phase!r}; known phases: {sorted(PHASE_NAMES)}")
        if phase in self.settings.phase_timeouts:
            return self.settings.phase_timeouts[phase]
        if phase in _OWN_TIMEOUT:
            return next(p.timeout for p in PHASES if p.name == phase)
        return self.settings.default_phase_timeout

    @property
    def total_timeout(self) -> float:
        return sum(self.timeout(p.name) for p in PHASES)

    # --- tasks -----------------------------------------------------------------------------------

    def add_task(self, phase: str, name: str, task: Task) -> None:
        """Run ``task`` in ``phase``. Tasks in one phase run concurrently; the phase waits for them
        up to its timeout."""
        if phase not in PHASE_NAMES:
            raise ValueError(f"unknown phase {phase!r}; known phases: {sorted(PHASE_NAMES)}")
        if not name:
            raise ValueError("set a task name when adding tasks to the coordinated shutdown")
        if phase in self._started_phases:
            _log.debug("Task [%s] added to phase [%s] after it ran, ignoring", name, phase)
            return
        self._tasks.setdefault(phase, []).append((name, task))

    def add_actor_termination_task(
        self, phase: str, name: str, actor: ActorRef[Any], stop_msg: object | None = None
    ) -> None:
        """Tell ``actor`` ``stop_msg``, if given, and wait for it to stop."""
        from .cell import _cell_of  # pyright: ignore[reportPrivateUsage]

        cell = _cell_of(actor)

        async def task() -> None:
            if stop_msg is not None:
                actor.tell(stop_msg)
            await asyncio.shield(cell.done)

        self.add_task(phase, name, task)

    async def _terminate_system(self) -> None:
        self.system._final_terminate()  # pyright: ignore[reportPrivateUsage]
        await asyncio.shield(self.system._system_root.done)  # pyright: ignore[reportPrivateUsage]

    # --- running ---------------------------------------------------------------------------------

    @property
    def shutdown_reason(self) -> Reason | None:
        return self._reason

    def run(self, reason: Reason) -> asyncio.Future[None]:
        """Start the run with ``reason`` unless one is going; returns the future of the one run."""
        if self._run is None:
            self._reason = reason
            self._run = asyncio.get_running_loop().create_task(self._run_phases(reason))
        return self._run

    async def _run_phases(self, reason: Reason) -> None:
        _log.info("Running CoordinatedShutdown with reason [%s]", type(reason).__name__)
        for phase in PHASES:
            self._started_phases.add(phase.name)
            tasks = list(self._tasks.get(phase.name, ()))
            if not tasks:
                _log.debug("Performing phase [%s] with [0] tasks", phase.name)
                continue
            _log.debug(
                "Performing phase [%s] with [%d] tasks: [%s]",
                phase.name,
                len(tasks),
                ", ".join(n for n, _ in tasks),
            )
            timeout = self.timeout(phase.name)
            try:
                await asyncio.wait_for(
                    asyncio.gather(*(self._guarded(phase.name, n, t) for n, t in tasks)), timeout
                )
            except TimeoutError:
                _log.warning(
                    "Coordinated shutdown phase [%s] timed out after %s s", phase.name, timeout
                )

    @staticmethod
    async def _guarded(phase: str, name: str, task: Task) -> None:
        try:
            await task()
        except Exception as e:  # recover = on: a failed task is logged and the phase continues
            _log.warning("Task [%s] in phase [%s] failed: %r", name, phase, e)
