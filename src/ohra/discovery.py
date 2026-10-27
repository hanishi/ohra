"""Apache Pekko Discovery: look a service name up and get its addresses. The cluster bootstrap
finds its peers through it; `ConfigServiceDiscovery` is Pekko's config method,
`KubernetesApiServiceDiscovery` lives in `ohra.management.kubernetes`, and anything else is a
subclass with one method."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Lookup:
    """What to look up: a service name, optionally narrowed to a named port and a protocol."""

    service_name: str
    port_name: str | None = None
    protocol: str | None = None


@dataclass(frozen=True, order=False)
class ResolvedTarget:
    """One address of a service: a host (name or IP), an optional port, and the IP when it is
    known."""

    host: str
    port: int | None
    address: str | None = None

    def __lt__(self, other: ResolvedTarget) -> bool:
        return (self.host, self.port or 0) < (other.host, other.port or 0)


@dataclass(frozen=True)
class Resolved:
    service_name: str
    addresses: tuple[ResolvedTarget, ...]


class DiscoveryTimeoutError(Exception):
    """A lookup did not answer within ``resolve_timeout``."""


class ServiceDiscovery:
    """Subclass and implement ``lookup``; any exception other than a timeout means the lookup
    failed."""

    async def lookup(self, lookup: Lookup, resolve_timeout: float) -> Resolved:
        raise NotImplementedError


class ConfigServiceDiscovery(ServiceDiscovery):
    """Pekko's config-based discovery: a fixed map from service name to addresses."""

    def __init__(self, services: Mapping[str, Sequence[ResolvedTarget]]) -> None:
        self._services = {name: tuple(targets) for name, targets in services.items()}

    async def lookup(self, lookup: Lookup, resolve_timeout: float) -> Resolved:
        return Resolved(lookup.service_name, self._services.get(lookup.service_name, ()))
