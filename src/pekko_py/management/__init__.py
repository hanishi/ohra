"""Apache Pekko Management narrowed to the Kubernetes path: the management HTTP endpoint with health checks,
read-only cluster routes, cluster bootstrap over service discovery, and Kubernetes API discovery. Reached as
``PekkoManagement(system)``; nothing binds until ``await PekkoManagement(system).start()``."""
from __future__ import annotations

import asyncio
import logging
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..discovery import ServiceDiscovery
from .health import HealthChecks, HealthCheckSettings
from .http import Response, serve

if TYPE_CHECKING:
    from ..system import ActorSystem

_log = logging.getLogger("pekko_py.management")

type RouteHandler = Callable[[str], Awaitable[Response]]
"""Receives the remainder of the path for a prefix route, or the full path for an exact one."""


@dataclass(frozen=True)
class PekkoManagementSettings:
    """Pekko's ``pekko.management.http`` settings. ``hostname=None`` is this host's address; ``bind_*`` default to the
    hostname and port; ``port=0`` binds an ephemeral port. ``bootstrap`` needs ``discovery``."""

    hostname: str | None = None
    port: int = 7626
    bind_hostname: str | None = None
    bind_port: int | None = None
    base_path: str = ""
    health_checks: HealthCheckSettings = field(default_factory=HealthCheckSettings)
    bootstrap: object | None = None  # ClusterBootstrapSettings, typed in the bootstrap module
    discovery: ServiceDiscovery | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError("port must be in 0..65535")
        if self.bind_port is not None and not 0 <= self.bind_port <= 65535:
            raise ValueError("bind_port must be in 0..65535")
        if self.base_path and not self.base_path.startswith("/"):
            raise ValueError("base_path must start with '/'")
        if self.bootstrap is not None and self.discovery is None:
            raise ValueError("bootstrap needs a discovery: pass discovery=KubernetesApiServiceDiscovery() or another ServiceDiscovery")

    @property
    def effective_hostname(self) -> str:
        return self.hostname if self.hostname else socket.gethostbyname(socket.gethostname())

    @property
    def effective_bind_hostname(self) -> str:
        return self.bind_hostname if self.bind_hostname else self.effective_hostname

    @property
    def effective_bind_port(self) -> int:
        return self.bind_port if self.bind_port is not None else self.port


class PekkoManagement:
    """``PekkoManagement(system)``: ``health`` for the checks, ``start()`` to bind the endpoint, ``uri`` once bound."""

    system: ActorSystem[Any]
    settings: PekkoManagementSettings
    health: HealthChecks
    uri: str | None
    _server: asyncio.Server | None
    _routes: dict[str, RouteHandler]
    _prefixes: list[tuple[str, RouteHandler]]
    _starting: asyncio.Lock

    def __new__(cls, system: ActorSystem[Any]) -> PekkoManagement:
        return system.management

    @classmethod
    def _start(cls, system: ActorSystem[Any], settings: PekkoManagementSettings) -> PekkoManagement:
        self = object.__new__(cls)
        self.system = system
        self.settings = settings
        self.health = HealthChecks(settings.health_checks)
        self.uri = None
        self._server = None
        self._routes = {}
        self._prefixes = []
        self._starting = asyncio.Lock()
        hc = settings.health_checks
        self.add_route("/" + hc.startup_path, lambda _p: self._health_route(self.health.startup))
        self.add_route("/" + hc.readiness_path, lambda _p: self._health_route(self.health.ready))
        self.add_route("/" + hc.liveness_path, lambda _p: self._health_route(self.health.alive))
        if system.cluster is not None:
            cluster = system.cluster

            async def cluster_membership() -> bool:
                return cluster.self_member.status in hc.ready_states

            self.health.add_readiness_check("cluster-membership", cluster_membership)
            from .cluster_http import register_cluster_routes  # imports the cluster, which this module must not at import time

            register_cluster_routes(self, system)
        return self

    # --- routes ---------------------------------------------------------------------------------------------------

    def add_route(self, path: str, handler: RouteHandler) -> None:
        """A GET route under ``base_path``: exact, or a prefix when ``path`` ends with ``/`` (the handler gets the rest)."""
        if path.endswith("/"):
            self._prefixes.append((path, handler))
        else:
            self._routes[path] = handler

    async def _dispatch(self, _method: str, path: str) -> Response:
        base = self.settings.base_path
        if base:
            if not path.startswith(base + "/") and path != base:
                return Response(404, b"not found", "text/plain")
            path = path[len(base) :] or "/"
        exact = self._routes.get(path)
        if exact is not None:
            return await exact(path)
        for prefix, handler in self._prefixes:
            if path.startswith(prefix) and len(path) > len(prefix):
                return await handler(path[len(prefix) :])
        return Response(404, b"not found", "text/plain")

    @staticmethod
    async def _health_route(check: Callable[[], Awaitable[str | None]]) -> Response:
        reason = await check()
        if reason is None:
            return Response(200, b"OK", "text/plain")
        return Response(500, f"Not Healthy: {reason}".encode(), "text/plain")

    # --- lifecycle ------------------------------------------------------------------------------------------------

    async def start(self) -> str:
        """Bind the endpoint once and return its base URI; a second call returns the same URI."""
        async with self._starting:
            if self.uri is None:
                s = self.settings
                server = await serve(s.effective_bind_hostname, s.effective_bind_port, self._dispatch)
                port = server.sockets[0].getsockname()[1]
                self._server = server
                self.uri = f"http://{s.effective_hostname}:{port}{s.base_path}"
                _log.info("Bound Pekko Management (HTTP) endpoint to: %s:%s", s.effective_bind_hostname, port)
            return self.uri

    async def stop(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.close()
            await server.wait_closed()
