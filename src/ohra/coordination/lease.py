"""Pekko's ``coordination`` module: the lease API (``org.apache.pekko.coordination.lease``).

Implementations are registered with ``LeaseProvider(system).register``, not loaded by class
name from the config (deviation D7).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace


class LeaseError(RuntimeError):
    """Pekko's ``LeaseException`` (``LeaseException.scala`` 16): the outcome of a lease call is
    unknown."""


class LeaseTimeoutError(LeaseError):
    """Pekko's ``LeaseTimeoutException`` (``LeaseException.scala`` 18): the lease resource did
    not answer in time."""


@dataclass(frozen=True)
class TimeoutSettings:
    """Pekko's ``TimeoutSettings`` (``TimeoutSettings.scala`` 35-94), in seconds. The constructor
    does not validate, as Pekko's class constructor does not; ``create`` does."""

    heartbeat_interval: float = 12.0
    heartbeat_timeout: float = 120.0
    operation_timeout: float = 5.0

    @staticmethod
    def create(
        heartbeat_timeout: float = 120.0,
        heartbeat_interval: float | None = 12.0,
        operation_timeout: float = 5.0,
    ) -> TimeoutSettings:
        """Pekko's ``TimeoutSettings.apply(config)`` (``TimeoutSettings.scala`` 21-31). ``None``
        stands for the empty ``heartbeat-interval`` and gives ``max(timeout / 10, 5)``."""
        if heartbeat_interval is None:
            heartbeat_interval = max(heartbeat_timeout / 10, 5.0)
        if not heartbeat_interval < heartbeat_timeout / 2:
            raise ValueError("heartbeat-interval must be less than half heartbeat-timeout")
        return TimeoutSettings(heartbeat_interval, heartbeat_timeout, operation_timeout)

    def with_heartbeat_interval(self, heartbeat_interval: float) -> TimeoutSettings:
        """``withHeartbeatInterval`` (``TimeoutSettings.scala`` 62)."""
        return replace(self, heartbeat_interval=heartbeat_interval)

    def with_heartbeat_timeout(self, heartbeat_timeout: float) -> TimeoutSettings:
        """``withHeartbeatTimeout`` (``TimeoutSettings.scala`` 65)."""
        return replace(self, heartbeat_timeout=heartbeat_timeout)

    def with_operation_timeout(self, operation_timeout: float) -> TimeoutSettings:
        """``withOperationTimeout`` (``TimeoutSettings.scala`` 68)."""
        return replace(self, operation_timeout=operation_timeout)


@dataclass(frozen=True)
class LeaseSettings:
    """Pekko's ``LeaseSettings`` (``LeaseSettings.scala`` 24-34). The repr leaves out the config,
    as Pekko's ``toString``."""

    lease_name: str
    owner_name: str
    timeout_settings: TimeoutSettings
    lease_config: Mapping[str, object] = field(repr=False)

    def with_timeout_settings(self, timeout_settings: TimeoutSettings) -> LeaseSettings:
        """``withTimeoutSettings`` (``LeaseSettings.scala`` 27)."""
        return replace(self, timeout_settings=timeout_settings)


@dataclass(frozen=True)
class LeaseUsageSettings:
    """Pekko's ``LeaseUsageSettings`` (``LeaseUsageSettings.scala`` 22-27): which lease
    implementation a feature uses, and how long it waits before retrying a failed acquire."""

    lease_implementation: str
    lease_retry_interval: float = 5.0


class Lease(ABC):
    """Pekko's ``scaladsl.Lease`` (``Lease.scala`` 21-63). One owner at a time holds the lease
    until it releases it; an implementation may also lose it, for example when it is partitioned
    from the lease resource for too long."""

    def __init__(self, settings: LeaseSettings) -> None:
        self.settings = settings

    @abstractmethod
    async def acquire(
        self, lease_lost_callback: Callable[[BaseException | None], None] | None = None
    ) -> bool:
        """Both of Pekko's ``acquire`` overloads (``Lease.scala`` 36 and 45). Returns ``True``
        when the lease is acquired and ``False`` when it surely is not, for example because
        another owner holds it. Raises ``LeaseError`` when the outcome is unknown, for example a
        timeout talking to the lease resource.

        ``lease_lost_callback`` is called if the lease is lost afterwards. An implementation
        must not call it before the acquire has completed."""

    @abstractmethod
    async def release(self) -> bool:
        """``release`` (``Lease.scala`` 50): release the lease so another owner can acquire it."""

    @abstractmethod
    def check_lease(self) -> bool:
        """``checkLease`` (``Lease.scala`` 62). ``True`` means the owner certainly holds the
        lease; ``False`` means it might not, and only ``acquire`` or ``release`` can say."""
