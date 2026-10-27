"""Supervisor strategies (org.apache.pekko.actor.typed.SupervisorStrategy)."""

from __future__ import annotations

from dataclasses import dataclass, replace


class SupervisorStrategy:
    __slots__ = ()

    @staticmethod
    def resume() -> Resume:
        return Resume()

    @staticmethod
    def stop() -> Stop:
        return Stop()

    @staticmethod
    def restart() -> Restart:
        return Restart()

    @staticmethod
    def restart_with_backoff(
        min_backoff: float, max_backoff: float, random_factor: float = 0.2
    ) -> Backoff:
        return Backoff(min_backoff, max_backoff, random_factor)


@dataclass(frozen=True, slots=True)
class Resume(SupervisorStrategy):
    """Keep the current behavior and its state; drop the failing message."""


@dataclass(frozen=True, slots=True)
class Stop(SupervisorStrategy):
    """Let the failure stop the actor (the default without supervision)."""


@dataclass(frozen=True, slots=True)
class Restart(SupervisorStrategy):
    """Re-run the supervised initial behavior. Children are stopped first.

    max_restarts < 0 means unlimited. ``within`` is the sliding window in seconds;
    None means the count never resets.
    """

    max_restarts: int = -1
    within: float | None = None
    stop_children: bool = True

    def with_limit(self, max_restarts: int, within: float) -> Restart:
        return replace(self, max_restarts=max_restarts, within=within)

    def with_stop_children(self, enabled: bool) -> Restart:
        return replace(self, stop_children=enabled)


@dataclass(frozen=True, slots=True)
class Backoff(SupervisorStrategy):
    """Restart after an exponentially growing delay; messages are stashed meanwhile."""

    min_backoff: float
    max_backoff: float
    random_factor: float = 0.2
    reset_after: float | None = None
    stash_capacity: int = 1000
    max_restarts: int = -1

    def with_reset_after(self, seconds: float) -> Backoff:
        return replace(self, reset_after=seconds)

    def with_stash_capacity(self, capacity: int) -> Backoff:
        return replace(self, stash_capacity=capacity)

    def with_max_restarts(self, n: int) -> Backoff:
        return replace(self, max_restarts=n)

    def delay(self, attempt: int, rnd: float) -> float:
        base = min(self.max_backoff, self.min_backoff * (2**attempt))
        return base * (1.0 + rnd * self.random_factor)
