"""Pekko Management's cluster bootstrap: discover the service's members, probe their management endpoints for
seed nodes, join any cluster seen, and otherwise let the lowest address form a new one once the discovered set
has been stable. Ported from BootstrapCoordinator.scala, HttpContactPointBootstrap.scala and
LowestAddressJoinDecider.scala."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import socket
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..behavior import Behavior, Behaviors
from ..cluster.core import Cluster, Join, JoinSeedNodes
from ..context import ActorContext
from ..discovery import Lookup, Resolved, ResolvedTarget, ServiceDiscovery
from ..ref import ActorRef
from ..remote import Address
from ..result import Failure, Success, Try
from ..supervision import SupervisorStrategy
from .http import get

if TYPE_CHECKING:
    from ..system import ActorSystem

_log = logging.getLogger("pekko_py.management.bootstrap")


@dataclass(frozen=True)
class ClusterBootstrapSettings:
    """Pekko's ``pekko.management.cluster.bootstrap`` settings, in seconds. ``fallback_port=None`` is the management port."""

    service_name: str
    port_name: str | None = None
    protocol: str = "tcp"
    new_cluster_enabled: bool = True
    stable_margin: float = 5.0
    interval: float = 1.0
    exponential_backoff_random_factor: float = 0.2
    exponential_backoff_max: float = 15.0
    required_contact_point_nr: int = 2
    resolve_timeout: float = 3.0
    contact_with_all_contact_points: bool = True
    fallback_port: int | None = None
    filter_on_fallback_port: bool = True
    probing_failure_timeout: float = 3.0
    probe_interval: float = 1.0
    probe_interval_jitter: float = 0.2
    http_max_seed_nodes_to_expose: int = 5

    def __post_init__(self) -> None:
        for name in ("stable_margin", "interval", "exponential_backoff_max", "resolve_timeout", "probing_failure_timeout", "probe_interval"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        if self.required_contact_point_nr < 1 or self.http_max_seed_nodes_to_expose < 1:
            raise ValueError("required_contact_point_nr and http_max_seed_nodes_to_expose must be >= 1")
        if not 0 <= self.probe_interval_jitter < 1 or self.exponential_backoff_random_factor < 0:
            raise ValueError("probe_interval_jitter must be in [0, 1) and exponential_backoff_random_factor >= 0")
        if not self.service_name:
            raise ValueError("service_name must be set")


# --- the decider's inputs and outputs (JoinDecider.scala) ------------------------------------------------------------


@dataclass(frozen=True)
class SeedNodesObservation:
    observed_at: float
    contact_point: ResolvedTarget
    self_node: Address
    seed_nodes: frozenset[Address]


@dataclass(frozen=True)
class SeedNodesInformation:
    current_time: float
    contact_points_changed_at: float
    contact_points: frozenset[ResolvedTarget]
    seed_nodes_observations: frozenset[SeedNodesObservation]

    @property
    def has_seed_nodes(self) -> bool:
        return any(o.seed_nodes for o in self.seed_nodes_observations)

    @property
    def all_seed_nodes(self) -> frozenset[Address]:
        out: set[Address] = set()
        for o in self.seed_nodes_observations:
            out |= o.seed_nodes
        return frozenset(out)


@dataclass(frozen=True)
class KeepProbing: ...


@dataclass(frozen=True)
class JoinSelf: ...


@dataclass(frozen=True)
class JoinOtherSeedNodes:
    seed_nodes: frozenset[Address]


type JoinDecision = KeepProbing | JoinSelf | JoinOtherSeedNodes


# --- pure helpers (BootstrapCoordinator.selectHosts / backedOffInterval, SelfAwareJoinDecider.matchesSelf) ------------


def select_hosts(lookup: Lookup, fallback_port: int, filter_on_fallback_port: bool, contact_points: Sequence[ResolvedTarget]) -> list[ResolvedTarget]:
    """With a port name, or without filtering, every target; else per host keep the fallback port when several resolve."""
    if lookup.port_name is not None or not filter_on_fallback_port:
        return list(contact_points)
    by_host: dict[str, list[ResolvedTarget]] = {}
    for cp in contact_points:
        by_host.setdefault(cp.host, []).append(cp)
    out: list[ResolvedTarget] = []
    for targets in by_host.values():
        if len(targets) == 1 or not any(t.port is not None for t in targets):
            out.extend(targets)
        else:
            out.extend(t for t in targets if t.port == fallback_port)
    return out


def _resolve(host: str) -> str | None:
    try:
        return socket.gethostbyname(host.strip("[]"))
    except OSError:
        return None


def matches_self(target: ResolvedTarget, self_contact_point: tuple[str, int], self_ip: str | None = None) -> bool:
    """The target is this node's management endpoint: same host (by name, or by IP when the target carries one and the
    host is an IP literal or was resolved once by the caller) and, when the target has a port, the same port."""
    host, port = self_contact_point
    bare = host.replace("[", "").replace("]", "")
    host_matches = target.host in (host, bare) or target.address == bare or (self_ip is not None and target.address == self_ip)
    if target.port is None:
        return host_matches
    return host_matches and target.port == port


def backed_off_interval(restart_count: int, min_backoff: float, max_backoff: float, random_factor: float) -> float:
    rnd = 1.0 + random.random() * random_factor
    try:
        return min(max_backoff, min_backoff * (2**restart_count)) * rnd  # Pekko's: the random factor applies after the cap
    except OverflowError:
        return max_backoff


# --- the decider (LowestAddressJoinDecider.scala) ------------------------------------------------------------------


def _cp(cp: ResolvedTarget) -> str:
    return f"{cp.host}:{cp.port if cp.port is not None else 0}"


class LowestAddressJoinDecider:
    def __init__(self, settings: ClusterBootstrapSettings, self_contact_point: tuple[str, int], log: logging.Logger) -> None:
        self.settings = settings
        self.self_contact_point = self_contact_point
        self.log = log
        self.self_ip = _resolve(self_contact_point[0])  # once, not on every decide tick (DNS blocks the event loop)

    def decide(self, info: SeedNodesInformation) -> JoinDecision:
        s = self.settings
        points = ", ".join(sorted(_cp(c) for c in info.contact_points))
        if info.has_seed_nodes:
            seeds = frozenset(sorted(info.all_seed_nodes, key=lambda a: (a.host, a.port))[:5])
            return JoinOtherSeedNodes(seeds) if seeds else KeepProbing()
        if len(info.seed_nodes_observations) < s.required_contact_point_nr:
            self.log.info(
                "Discovered [%d] contact points, confirmed [%d], which is less than the required [%d], retrying",
                len(info.contact_points),
                len(info.seed_nodes_observations),
                s.required_contact_point_nr,
            )
            return KeepProbing()
        if info.current_time - info.contact_points_changed_at < s.stable_margin:
            self.log.debug("Contact points observations have changed more recently than the stable-margin [%s s], not joining myself. This process will be retried.", s.stable_margin)
            return KeepProbing()
        observed = {o.contact_point for o in info.seed_nodes_observations}
        missing = (info.contact_points - observed) if s.contact_with_all_contact_points else frozenset[ResolvedTarget]()
        if missing:
            self.log.info("Exceeded stable margins but missing seed node information from some contact points [%s] (out of [%s])", ", ".join(sorted(_cp(c) for c in missing)), points)
            return KeepProbing()
        lowest = min(observed) if observed else None
        if lowest is not None and self._can_join_self(lowest, info) and s.new_cluster_enabled:
            return JoinSelf()
        if s.new_cluster_enabled:
            self.log.info(
                "Exceeded stable margins without locating seed-nodes, however this node %s is NOT the lowest address out of the discovered endpoints in this deployment, "
                "thus NOT joining self. Expecting node [%s] (out of [%s]) to perform the self-join and initiate the cluster.",
                f"{self.self_contact_point[0]}:{self.self_contact_point[1]}",
                _cp(lowest) if lowest is not None else "",
                points,
            )
        else:
            self.log.warning(
                "Exceeded stable margins without locating seed-nodes, however this node %s is configured with new-cluster-enabled=off, thus NOT joining self. "
                "Expecting existing cluster or node [%s] (out of [%s]) to perform the self-join and initiate the cluster.",
                f"{self.self_contact_point[0]}:{self.self_contact_point[1]}",
                _cp(lowest) if lowest is not None else "",
                points,
            )
        return KeepProbing()

    def _can_join_self(self, target: ResolvedTarget, info: SeedNodesInformation) -> bool:
        if matches_self(target, self.self_contact_point, self.self_ip):
            return True
        if not any(matches_self(c, self.self_contact_point, self.self_ip) for c in info.contact_points):
            self.log.warning("Self contact point [%s:%s] not found in targets %s", self.self_contact_point[0], self.self_contact_point[1], sorted(_cp(c) for c in info.contact_points))
        return False


# --- the coordinator and its probes (BootstrapCoordinator.scala, HttpContactPointBootstrap.scala) -------------------


@dataclass(frozen=True)
class _DiscoverTick: ...


@dataclass(frozen=True)
class _DecideTick: ...


@dataclass(frozen=True)
class _Resolved:
    resolved: Resolved


@dataclass(frozen=True)
class _ResolveFailed:
    error: BaseException


@dataclass(frozen=True)
class _Observed:
    observation: SeedNodesObservation


@dataclass(frozen=True)
class _ProbingFailed:
    contact_point: ResolvedTarget


@dataclass(frozen=True)
class _ProbeTick: ...


@dataclass(frozen=True)
class _ProbeResult:
    result: Try[tuple[int, bytes]]


def _parse_address(s: str) -> Address:
    """``pekko://name@host:port`` back to an Address."""
    scheme, _, rest = s.partition("://")
    if scheme != "pekko" or "@" not in rest:
        raise ValueError(f"not a pekko address: {s!r}")
    name, _, authority = rest.partition("@")
    host, _, port = authority.rpartition(":")
    return Address(name, host.strip("[]"), int(port))


def seed_nodes_json(system: ActorSystem[Any], settings: ClusterBootstrapSettings) -> dict[str, object]:
    """HttpClusterBootstrapRoutes: reachable Up, WeaklyUp or Joining members, at most ``http_max_seed_nodes_to_expose``."""
    from .cluster_http import member_json

    cluster = Cluster(system)
    state = cluster.state
    members = [m for m in state.members if m not in state.unreachable and m.status in ("up", "weakly_up", "joining")]
    exposed = members[: settings.http_max_seed_nodes_to_expose]
    _log.info("Bootstrap request: Contact Point returning %d seed-nodes [%s]", len(exposed), ", ".join(m.address.root for m in exposed))
    return {"selfNode": cluster.self_address.root, "seedNodes": [member_json(m) for m in exposed]}


def _probe_behavior(settings: ClusterBootstrapSettings, contact_point: ResolvedTarget, url: str, parent: ActorRef[Any]) -> Behavior[Any]:
    """Probe one contact point for seed nodes until told to stop, or until it has failed for probing_failure_timeout."""

    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + settings.probing_failure_timeout
        ctx.self.tell(_ProbeTick())

        def schedule_next() -> None:
            jitter = 1.0 + random.random() * settings.probe_interval_jitter
            ctx.timers.start_single_timer("probe", _ProbeTick(), settings.probe_interval * jitter)

        def on_message(msg: Any) -> Behavior[Any]:
            nonlocal deadline
            match msg:
                case _ProbeTick():
                    _log.debug("Probing [%s] for seed nodes...", url)
                    ctx.pipe_to_self(get(url, timeout=settings.probing_failure_timeout), _ProbeResult)
                case _ProbeResult(Success((200, body))):
                    try:
                        doc = json.loads(body)
                        observation = SeedNodesObservation(
                            loop.time(),
                            contact_point,
                            _parse_address(doc["selfNode"]),
                            frozenset(_parse_address(m["node"]) for m in doc["seedNodes"]),
                        )
                    except (ValueError, KeyError, TypeError) as e:
                        return failed(e)
                    parent.tell(_Observed(observation))
                    deadline = loop.time() + settings.probing_failure_timeout
                    schedule_next()
                case _ProbeResult(Success((status, _))):
                    return failed(RuntimeError(f"HTTP {status}"))
                case _ProbeResult(Failure(e)):
                    return failed(e)
                case _:
                    pass
            return Behaviors.same

        def failed(cause: BaseException) -> Behavior[Any]:
            _log.warning("Probing [%s] failed due to: %s", url, cause)
            if loop.time() > deadline:
                _log.error("Overdue of probing-failure-timeout, stop probing, signaling that it's failed")
                parent.tell(_ProbingFailed(contact_point))
                return Behaviors.stopped()
            schedule_next()
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)


class _Coordinator:
    def __init__(
        self,
        ctx: ActorContext[Any],
        settings: ClusterBootstrapSettings,
        discovery: ServiceDiscovery,
        self_contact_point: tuple[str, int],
        base_path: str,
    ) -> None:
        self.ctx = ctx
        self.settings = settings
        self.discovery = discovery
        self.self_contact_point = self_contact_point
        self.base_path = base_path
        self.cluster = Cluster(ctx.system)
        self.loop = asyncio.get_running_loop()
        self.lookup = Lookup(settings.service_name, settings.port_name, settings.protocol)
        self.decider = LowestAddressJoinDecider(settings, self_contact_point, _log)
        self.last_contacts: tuple[float, frozenset[ResolvedTarget]] | None = None  # observed_at (when it last changed), points
        self.observations: dict[ResolvedTarget, SeedNodesObservation] = {}
        self.probes: dict[ResolvedTarget, ActorRef[Any]] = {}
        self.failures = 0
        _log.info("Locating service members. Using discovery [%s], join decider [LowestAddressJoinDecider]", type(discovery).__name__)
        self.schedule_discovery()
        ctx.timers.start_timer_with_fixed_delay("decide", _DecideTick(), settings.probe_interval)

    @property
    def fallback_port(self) -> int:
        return self.settings.fallback_port if self.settings.fallback_port is not None else self.self_contact_point[1]

    def schedule_discovery(self) -> None:
        s = self.settings
        delay = backed_off_interval(self.failures, s.interval, s.exponential_backoff_max, s.exponential_backoff_random_factor)
        self.ctx.timers.start_single_timer("discover", _DiscoverTick(), delay)

    def discover(self) -> None:
        _log.info("Looking up [%s]", self.lookup)

        def adapt(result: Try[Resolved]) -> Any:
            return _Resolved(result.value) if isinstance(result, Success) else _ResolveFailed(result.exception)

        self.ctx.pipe_to_self(self.discovery.lookup(self.lookup, self.settings.resolve_timeout), adapt)

    def on_resolved(self, resolved: Resolved) -> None:
        filtered = select_hosts(self.lookup, self.fallback_port, self.settings.filter_on_fallback_port, resolved.addresses)
        _log.info("Located service members based on: [%s]: [%s], filtered to [%s]", self.lookup, ", ".join(map(_cp, resolved.addresses)), ", ".join(map(_cp, filtered)))
        points = frozenset(filtered)
        now = self.loop.time()
        if self.last_contacts is None or self.last_contacts[1] != points:
            self.last_contacts = (now, points)
        for cp in [cp for cp in self.observations if cp not in points]:
            del self.observations[cp]
        for cp, ref in [(cp, ref) for cp, ref in self.probes.items() if cp not in points]:
            self.ctx.stop(ref)
            del self.probes[cp]
        for cp in points:
            self.ensure_probing(cp)
        self.failures = 0
        self.schedule_discovery()

    def ensure_probing(self, cp: ResolvedTarget) -> None:
        if cp in self.probes:
            return
        port = cp.port if cp.port is not None else self.fallback_port
        self_remoting = self.cluster.self_address
        if (cp.host == self_remoting.host or cp.address == self_remoting.host) and port == self_remoting.port:
            _log.warning(
                "Misconfiguration detected! Attempted to start probing a contact-point which address [%s:%s] matches our local remoting address [%s]. "
                "Avoiding probing this address. Consider double checking your service discovery and port configurations.",
                cp.host,
                port,
                self_remoting.root,
            )
            return
        url = f"http://{cp.host}:{port}{self.base_path}/bootstrap/seed-nodes"
        name = "contactPointProbe-" + f"{cp.host}-{port}".replace(".", "-").replace(":", "-")
        existing = self.ctx.child(name)  # Pekko's context.child: a dropped probe may still be stopping under that name
        self.probes[cp] = existing if existing is not None else self.ctx.spawn(_probe_behavior(self.settings, cp, url, self.ctx.self), name)

    def decide(self) -> Behavior[Any]:
        if self.last_contacts is None:
            return Behaviors.same
        now = self.loop.time()
        changed_at, points = self.last_contacts
        fresh = frozenset(o for o in self.observations.values() if now - o.observed_at <= self.settings.probing_failure_timeout)
        decision = self.decider.decide(SeedNodesInformation(now, changed_at, points, fresh))
        match decision:
            case JoinOtherSeedNodes(seeds):
                others = tuple(sorted(seeds - {self.cluster.self_address}, key=lambda a: (a.host, a.port)))
                if others:
                    _log.info("Joining [%s] to existing cluster [%s]", self.cluster.self_address.root, ", ".join(a.root for a in others))
                    self.cluster.manager.tell(JoinSeedNodes(others))
                    return Behaviors.stopped()
            case JoinSelf():
                _log.info(
                    "Initiating new cluster, self-joining [%s]. Other nodes are expected to locate this cluster via continued contact-point probing.",
                    self.cluster.self_address.root,
                )
                self.cluster.manager.tell(Join(self.cluster.self_address))
                return Behaviors.stopped()
            case KeepProbing():
                pass
        return Behaviors.same

    def behavior(self) -> Behavior[Any]:
        def on_message(msg: Any) -> Behavior[Any]:
            match msg:
                case _DiscoverTick():
                    self.discover()
                case _Resolved(resolved):
                    self.on_resolved(resolved)
                case _ResolveFailed(error):
                    _log.warning("Resolve attempt failed! Cause: %r", error)
                    self.last_contacts = None  # no join decision until a discovery succeeds
                    self.failures += 1
                    self.schedule_discovery()
                case _Observed(observation):
                    if self.last_contacts is not None and observation.contact_point in self.last_contacts[1]:
                        _log.info(
                            "Contact point [%s] returned [%d] seed-nodes [%s]",
                            observation.self_node.root,
                            len(observation.seed_nodes),
                            ", ".join(a.root for a in observation.seed_nodes),
                        )
                        self.observations[observation.contact_point] = observation
                    if observation.seed_nodes:
                        return self.decide()
                case _ProbingFailed(contact_point):
                    _log.info("Received signal that probing has failed, scheduling contact point probing again")
                    self.observations.pop(contact_point, None)
                    self.probes.pop(contact_point, None)
                    self.schedule_discovery()
                case _DecideTick():
                    return self.decide()
                case _:
                    pass
            return Behaviors.same

        return Behaviors.receive_message(on_message)


def _coordinator_behavior(settings: ClusterBootstrapSettings, discovery: ServiceDiscovery, self_contact_point: tuple[str, int], base_path: str) -> Behavior[Any]:
    behavior: Behavior[Any] = Behaviors.setup(lambda ctx: _Coordinator(ctx, settings, discovery, self_contact_point, base_path).behavior())
    return Behaviors.supervise(behavior).on_failure(SupervisorStrategy.restart())  # as Pekko's system guardian restarts it


# --- the extension (ClusterBootstrap.scala) ----------------------------------------------------------------------------


class ClusterBootstrap:
    """``ClusterBootstrap(system)``: started by ``PekkoManagement.start()`` when the management settings carry ``bootstrap``."""

    system: ActorSystem[Any]
    settings: ClusterBootstrapSettings
    discovery: ServiceDiscovery
    _started: bool

    def __new__(cls, system: ActorSystem[Any]) -> ClusterBootstrap:
        bootstrap = system.management.bootstrap
        if bootstrap is None:
            raise RuntimeError("no bootstrap configured: pass management=PekkoManagementSettings(bootstrap=..., discovery=...) to ActorSystem")
        return bootstrap

    @classmethod
    def _start(cls, system: ActorSystem[Any], settings: ClusterBootstrapSettings, discovery: ServiceDiscovery) -> ClusterBootstrap:
        self = object.__new__(cls)
        self.system = system
        self.settings = settings
        self.discovery = discovery
        self._started = False
        return self

    def start(self, self_contact_point: tuple[str, int], base_path: str = "") -> None:
        cluster = Cluster(self.system)
        if cluster.settings.seed_nodes:
            _log.warning(
                "Application is configured with specific seed nodes: %s, bailing out of the bootstrap process! If you want to use the automatic bootstrap "
                "mechanism, make sure to NOT set explicit seed nodes in the configuration. This node will attempt to join the configured seed nodes.",
                [a.root for a in cluster.settings.seed_nodes],
            )
            return
        if self._started:
            _log.warning("Bootstrap already initiated, yet start() method was called again. Ignoring.")
            return
        self._started = True
        _log.info("Initiating bootstrap procedure using %s...", type(self.discovery).__name__)
        root = self.system._system_root  # pyright: ignore[reportPrivateUsage]
        root.spawn_child(_coordinator_behavior(self.settings, self.discovery, self_contact_point, base_path), "bootstrapCoordinator", None)
