"""The cluster extension and its core daemon at ``/system/cluster``, as Pekko's Cluster, ClusterCoreDaemon,
the seed node processes and ClusterDomainEventPublisher, folded into one actor. Every function below is the
Scala one of the same name, minus data centers, roles, app versions and config compatibility."""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from ..behavior import Behavior, Behaviors
from ..context import ActorContext
from ..cell import ActorCell, is_local_actor
from ..coordinated_shutdown import ClusterDowningReason, ClusterLeavingReason, CoordinatedShutdown
from ..ref import ActorRef, FunctionRef
from ..remote import Address, UniqueAddress
from ..remote.serialization import remotable
from ..signals import ChildFailed, PostStop, Signal, Terminated
from .failure_detector import FailureDetectorRegistry, HeartbeatNodeRing, HeartbeatSenderState, PhiAccrualFailureDetector
from .reachability import Reachability
from .gossip import (
    REMOVE_UNREACHABLE_WITH_MEMBER_STATUS,
    UP_NUMBER_UNSET,
    Gossip,
    GossipTargetSelector,
    Member,
    MembershipState,
    MemberStatus,
    vclock_name,
)
from .vector_clock import VectorClock

if TYPE_CHECKING:
    from ..system import ActorSystem
    from .sbr import SplitBrainResolverSettings


@dataclass(frozen=True)
class ClusterSettings:
    """Pekko's cluster settings with their defaults, in seconds."""

    seed_nodes: tuple[Address, ...] = ()
    seed_node_timeout: float = 5.0
    retry_unsuccessful_join_after: float = 10.0
    periodic_tasks_initial_delay: float = 1.0
    gossip_interval: float = 1.0
    leader_actions_interval: float = 1.0
    unreachable_nodes_reaper_interval: float = 1.0
    heartbeat_interval: float = 1.0
    heartbeat_expected_response_after: float = 1.0
    monitored_by_nr_of_members: int = 9
    failure_detector_threshold: float = 8.0
    failure_detector_max_sample_size: int = 1000
    failure_detector_min_std_deviation: float = 0.1
    acceptable_heartbeat_pause: float = 3.0
    allow_weakly_up_members: float | None = 7.0
    """Move Joining members to WeaklyUp after this long without convergence; None disables it."""
    min_nr_of_members: int = 1
    prune_gossip_tombstones_after: float = 86400.0
    gossip_different_view_probability: float = 0.8
    reduce_gossip_different_view_probability: int = 400
    max_gossips_before_shutting_down_myself: int = 5
    max_ticks_before_shutting_down_myself: int = 4
    receptionist_resend_interval: float = 1.0
    """How often a node re-sends its receptionist row to the others (anti-entropy); not a Pekko setting."""
    downing: SplitBrainResolverSettings | None = None
    """The split-brain resolver; None leaves downing manual, as Pekko without a downing provider."""
    down_removal_margin: float = 0.0
    """Pekko's down-removal-margin: 0 means the resolver's stable_after; see effective_down_removal_margin."""

    def __post_init__(self) -> None:
        positive = (
            "seed_node_timeout", "retry_unsuccessful_join_after", "gossip_interval", "leader_actions_interval",
            "unreachable_nodes_reaper_interval", "heartbeat_interval", "heartbeat_expected_response_after",
            "failure_detector_threshold", "failure_detector_min_std_deviation", "prune_gossip_tombstones_after",
            "monitored_by_nr_of_members", "failure_detector_max_sample_size", "min_nr_of_members",
            "reduce_gossip_different_view_probability", "receptionist_resend_interval",
        )  # fmt: skip
        non_negative = (
            "periodic_tasks_initial_delay", "acceptable_heartbeat_pause", "max_gossips_before_shutting_down_myself",
            "max_ticks_before_shutting_down_myself", "down_removal_margin",
        )  # fmt: skip
        for name in positive:
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        for name in non_negative:
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be >= 0")
        if not 0.0 < self.gossip_different_view_probability <= 1.0:
            raise ValueError("gossip_different_view_probability must be in (0, 1]")
        if self.allow_weakly_up_members is not None and self.allow_weakly_up_members < 0:
            raise ValueError("allow_weakly_up_members must be >= 0 or None")

    @property
    def effective_down_removal_margin(self) -> float:
        """Margin before a downed node's singletons and shards start elsewhere: the explicit value, else the resolver's
        stable_after, else zero (Pekko's SplitBrainResolverProvider.downRemovalMargin and NoDowning)."""
        if self.down_removal_margin > 0:
            return self.down_removal_margin
        return self.downing.stable_after if self.downing is not None else 0.0


# --- commands and subscriptions (Pekko Typed's ClusterCommand and ClusterStateSubscription) ---------------


@dataclass(frozen=True)
class Join:
    address: Address


@dataclass(frozen=True)
class JoinSeedNodes:
    seed_nodes: tuple[Address, ...]


@dataclass(frozen=True)
class Leave:
    address: Address


@dataclass(frozen=True)
class Down:
    address: Address


type ClusterCommand = Join | JoinSeedNodes | Leave | Down


class ClusterDomainEvent:
    """Base of everything a subscriber can receive."""


@dataclass(frozen=True)
class MemberEvent(ClusterDomainEvent):
    member: Member


@dataclass(frozen=True)
class MemberJoined(MemberEvent): ...


@dataclass(frozen=True)
class MemberWeaklyUp(MemberEvent): ...


@dataclass(frozen=True)
class MemberUp(MemberEvent): ...


@dataclass(frozen=True)
class MemberLeft(MemberEvent): ...


@dataclass(frozen=True)
class MemberExited(MemberEvent): ...


@dataclass(frozen=True)
class MemberDowned(MemberEvent): ...


@dataclass(frozen=True)
class MemberRemoved(MemberEvent):
    previous_status: MemberStatus


@dataclass(frozen=True)
class ReachabilityEvent(ClusterDomainEvent):
    member: Member


@dataclass(frozen=True)
class UnreachableMember(ReachabilityEvent): ...


@dataclass(frozen=True)
class ReachableMember(ReachabilityEvent): ...


@dataclass(frozen=True)
class LeaderChanged(ClusterDomainEvent):
    leader: Address | None


@dataclass(frozen=True)
class SeenChanged(ClusterDomainEvent):
    """Internal, as Pekko's: the set of nodes that have seen the current gossip, or convergence, changed."""

    convergence: bool
    seen_by: frozenset[Address]


@dataclass(frozen=True)
class ReachabilityChanged(ClusterDomainEvent):
    """Internal, as Pekko's: the reachability table changed."""

    reachability: Reachability


@dataclass(frozen=True)
class CurrentClusterState:
    members: tuple[Member, ...]
    unreachable: frozenset[Member]
    seen_by: frozenset[Address]
    leader: Address | None


@dataclass(frozen=True)
class SelfUp(ClusterDomainEvent):
    current_cluster_state: CurrentClusterState


@dataclass(frozen=True)
class SelfRemoved(ClusterDomainEvent):
    previous_status: MemberStatus


@dataclass(frozen=True)
class Subscribe:
    """Current state arrives first, as events (Pekko's InitialStateAsEvents), then every change."""

    subscriber: ActorRef[Any]
    event_class: type[ClusterDomainEvent]


@dataclass(frozen=True)
class Unsubscribe:
    subscriber: ActorRef[Any]


@dataclass(frozen=True)
class GetCurrentState:
    reply_to: ActorRef[CurrentClusterState]


type ClusterStateSubscription = Subscribe | Unsubscribe | GetCurrentState


# --- wire messages between cluster daemons (InternalClusterAction) -----------------------------------------


@remotable
@dataclass(frozen=True)
class InitJoinAck:
    address: Address


@remotable
@dataclass(frozen=True)
class InitJoinNack:
    address: Address


@remotable
@dataclass(frozen=True)
class InitJoin:
    reply_to: ActorRef[InitJoinAck | InitJoinNack]


@remotable
@dataclass(frozen=True)
class Welcome:
    from_: UniqueAddress
    gossip: Gossip


@remotable
@dataclass(frozen=True)
class JoinNode:
    """Pekko's internal ``Join(node)``, named apart from the ``Join(address)`` command."""

    node: UniqueAddress
    reply_to: ActorRef[Welcome]


@remotable
@dataclass(frozen=True)
class GossipEnvelope:
    from_: UniqueAddress
    to: UniqueAddress
    gossip: Gossip


@remotable
@dataclass(frozen=True)
class GossipStatus:
    from_: UniqueAddress
    version: VectorClock
    seen_digest: str


@remotable
@dataclass(frozen=True)
class ExitingConfirmed:
    node: UniqueAddress


@remotable
@dataclass(frozen=True)
class HeartbeatRsp:
    from_: UniqueAddress
    seq: int
    created_ns: int


@remotable
@dataclass(frozen=True)
class Heartbeat:
    """Sent to a peer's ``/system/cluster/heartbeat``, which answers ``reply_to`` with a HeartbeatRsp."""

    from_: Address
    seq: int
    created_ns: int
    reply_to: ActorRef[HeartbeatRsp]


# --- ticks ---------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _GossipTick: ...


@dataclass(frozen=True)
class _GossipSpeedupTick: ...


@dataclass(frozen=True)
class _ReapUnreachableTick: ...


@dataclass(frozen=True)
class _LeaderActionsTick: ...


@dataclass(frozen=True)
class _JoinSeedNodeTick: ...


@dataclass(frozen=True)
class _ExitingCompleted:
    """CoordinatedShutdown's cluster-exiting-done task: finish the exit and reply, as Pekko's ExitingCompleted ask."""

    reply_to: ActorRef[None] | None = None


@dataclass(frozen=True)
class _HeartbeatTick: ...


@dataclass(frozen=True)
class _ExpectedFirstHeartbeat:
    from_: UniqueAddress


type _Tick = _GossipTick | _GossipSpeedupTick | _ReapUnreachableTick | _LeaderActionsTick
type _Wire = (
    InitJoin | InitJoinAck | InitJoinNack | JoinNode | Welcome | GossipEnvelope | GossipStatus | ExitingConfirmed | HeartbeatRsp
)
type _Msg = (
    ClusterCommand
    | ClusterStateSubscription
    | _Wire
    | _Tick
    | _JoinSeedNodeTick
    | _ExitingCompleted
    | _HeartbeatTick
    | _ExpectedFirstHeartbeat
)


# --- the extension -------------------------------------------------------------------------------------------


class Cluster:
    """``Cluster(system)``: this node's view and the refs to drive it, as Pekko Typed's Cluster extension.
    Created by ``ActorSystem(..., cluster=ClusterSettings(...))``."""

    system: ActorSystem[Any]
    settings: ClusterSettings
    self_unique_address: UniqueAddress
    failure_detector: FailureDetectorRegistry
    manager: ActorRef[ClusterCommand]
    subscriptions: ActorRef[ClusterStateSubscription]
    self_exiting: asyncio.Future[None]
    _core: ActorCell[Any]
    _membership_state: MembershipState
    _terminated: bool

    def __new__(cls, system: ActorSystem[Any]) -> Cluster:
        if system.cluster is None:
            raise RuntimeError(f"system {system.name!r} has no cluster: pass cluster=ClusterSettings(...) to ActorSystem")
        return system.cluster

    @classmethod
    def _start(cls, system: ActorSystem[Any], settings: ClusterSettings) -> Cluster:
        self = object.__new__(cls)
        assert system.address is not None
        self.system = system
        self.settings = settings
        self.self_unique_address = system.address
        self.failure_detector = FailureDetectorRegistry(
            lambda: PhiAccrualFailureDetector(
                threshold=settings.failure_detector_threshold,
                max_sample_size=settings.failure_detector_max_sample_size,
                min_std_deviation=settings.failure_detector_min_std_deviation,
                acceptable_heartbeat_pause=settings.acceptable_heartbeat_pause,
                first_heartbeat_estimate=settings.heartbeat_interval,
            )
        )
        self._membership_state = MembershipState(Gossip.empty(), system.address)
        self._terminated = False
        self.self_exiting = asyncio.get_running_loop().create_future()  # Exiting seen in gossip, or the daemon stopped
        core = system._system_root.spawn_child(_core(self), "cluster", None)  # pyright: ignore[reportPrivateUsage]
        self._core = core
        core.done.add_done_callback(lambda _: self._core_stopped())
        self.manager = core.ref
        self.subscriptions = core.ref
        shutdown = CoordinatedShutdown(system)  # ClusterDaemon.scala: the cluster's four coordinated-shutdown tasks
        shutdown.add_task("cluster-leave", "leave", self._leave_task)
        shutdown.add_task("cluster-exiting", "wait-exiting", self._wait_exiting_task)
        shutdown.add_task("cluster-exiting-done", "exiting-completed", self._exiting_completed_task)
        shutdown.add_task("cluster-shutdown", "wait-shutdown", self._wait_shutdown_task)
        return self

    def _core_stopped(self) -> None:
        """A stopped daemon ends the node: Pekko's ClusterDaemon.postStop runs the shutdown with ClusterDowningReason
        (a no-op when the run that stopped it, a leave or terminate, is already going)."""
        self._terminated = True
        if not self.self_exiting.done():
            self.self_exiting.set_result(None)
        CoordinatedShutdown(self.system).run(ClusterDowningReason())

    def _skip_cluster_tasks(self) -> bool:
        """Pekko's guard on the leave and exiting-completed tasks: nothing to do once terminated or Down."""
        return self.is_terminated or self.self_member.status == "down"

    async def _leave_task(self) -> None:
        """CoordinatedShutdownLeave: leave, then wait until this member is Leaving, Exiting, Down or Removed."""
        if self._skip_cluster_tasks() or not self.state.members:  # not joined: nothing to leave
            return
        done: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def on_event(event: ClusterDomainEvent) -> None:
            match event:
                case MemberLeft(member=m) | MemberExited(member=m) | MemberDowned(member=m) | MemberRemoved(member=m):
                    if m.unique_address == self.self_unique_address and not done.done():
                        done.set_result(None)
                case _:
                    pass

        listener = FunctionRef(f"{self.self_address.root}/system/cluster/leave", on_event)
        self.subscriptions.tell(Subscribe(listener, MemberEvent))
        self.manager.tell(Leave(self.self_address))
        try:
            await asyncio.shield(done)
        finally:
            self.subscriptions.tell(Unsubscribe(listener))

    async def _wait_exiting_task(self) -> None:
        if not self.state.members:  # not joined yet
            return
        await asyncio.shield(self.self_exiting)

    async def _exiting_completed_task(self) -> None:
        if self._skip_cluster_tasks():
            return
        done: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        reply_to: ActorRef[None] = FunctionRef(f"{self.self_address.root}/system/cluster/exiting-completed", lambda _: done.set_result(None) if not done.done() else None)
        self._core.ref.tell(_ExitingCompleted(reply_to))
        await asyncio.shield(done)

    async def _wait_shutdown_task(self) -> None:
        await asyncio.shield(self._core.done)

    @property
    def self_address(self) -> Address:
        return self.self_unique_address.address

    @property
    def self_member(self) -> Member:
        return self._membership_state.self_member

    @property
    def state(self) -> CurrentClusterState:
        return _current_state(self._membership_state)

    @property
    def is_terminated(self) -> bool:
        return self._terminated or self.system.is_terminated

    def add_exiting_task(self, fn: Callable[[], Awaitable[None]]) -> None:
        """Transitional: a task in the cluster-exiting phase; use CoordinatedShutdown(system).add_task."""
        CoordinatedShutdown(self.system).add_task("cluster-exiting", f"exiting-task-{id(fn)}", fn)

    def shutdown(self) -> None:
        """Leave the cluster for good: stops the cluster daemon, whose end runs the coordinated shutdown and so
        terminates the ActorSystem, as Pekko's Cluster.shutdown through ClusterDaemon.postStop."""
        if not self._terminated:
            self._terminated = True
            self._core.send_sys(_stop_message())


# --- events from state changes (ClusterEvent.diff*) -------------------------------------------------------


def _current_state(state: MembershipState) -> CurrentClusterState:
    gossip = state.latest_gossip
    unreachable = frozenset(
        gossip.member(n) for n in gossip.reachability.all_unreachable_or_terminated if n != state.self_unique_address
    )
    leader = state.leader
    return CurrentClusterState(
        members=gossip.members,
        unreachable=unreachable,
        seen_by=frozenset(n.address for n in gossip.seen_by),
        leader=leader.address if leader is not None else None,
    )


def diff_member_events(old: MembershipState, new: MembershipState) -> list[MemberEvent]:
    if new is old:
        return []
    old_by, new_by = {m.unique_address: m for m in old.members}, {m.unique_address: m for m in new.members}
    events: list[MemberEvent] = [
        MemberRemoved(replace(m, status="removed"), m.status) for m in old.members if m.unique_address not in new_by
    ]
    for m in new.members:
        o = old_by.get(m.unique_address)
        if o is not None and o.status == m.status and o.up_number == m.up_number:
            continue
        match m.status:
            case "joining":
                events.append(MemberJoined(m))
            case "weakly_up":
                events.append(MemberWeaklyUp(m))
            case "up":
                events.append(MemberUp(m))
            case "leaving":
                events.append(MemberLeft(m))
            case "exiting":
                events.append(MemberExited(m))
            case "down":
                events.append(MemberDowned(m))
            case "removed":
                pass
    return events


def diff_unreachable(old: MembershipState, new: MembershipState) -> list[UnreachableMember]:
    if new is old:
        return []
    before = old.latest_gossip.reachability.all_unreachable_or_terminated
    return [
        UnreachableMember(new.latest_gossip.member(n))
        for n in new.latest_gossip.reachability.all_unreachable_or_terminated
        if n not in before and n != new.self_unique_address
    ]


def diff_reachable(old: MembershipState, new: MembershipState) -> list[ReachableMember]:
    if new is old:
        return []
    gossip = new.latest_gossip
    return [
        ReachableMember(gossip.member(n))
        for n in old.latest_gossip.reachability.all_unreachable
        if gossip.has_member(n) and gossip.reachability.is_reachable(n) and n != new.self_unique_address
    ]


def diff_leader(old: MembershipState, new: MembershipState) -> list[LeaderChanged]:
    leader = new.leader
    if leader != old.leader:
        return [LeaderChanged(leader.address if leader is not None else None)]
    return []


def diff_seen(old: MembershipState, new: MembershipState) -> list[SeenChanged]:
    if new is old:
        return []
    new_convergence = new.convergence(frozenset())
    new_seen_by = frozenset(n.address for n in new.latest_gossip.seen_by)
    if new_convergence != old.convergence(frozenset()) or new_seen_by != frozenset(n.address for n in old.latest_gossip.seen_by):
        return [SeenChanged(new_convergence, new_seen_by)]
    return []


def diff_reachability(old: MembershipState, new: MembershipState) -> list[ReachabilityChanged]:
    if new.latest_gossip.reachability == old.latest_gossip.reachability:
        return []
    return [ReachabilityChanged(new.latest_gossip.reachability)]


def publish_diff(old: MembershipState, new: MembershipState, pub: Callable[[ClusterDomainEvent], None]) -> None:
    for e in diff_member_events(old, new):
        pub(e)
        if e.member.unique_address == new.self_unique_address:  # Pekko Typed's SelfUp and SelfRemoved
            if isinstance(e, MemberUp):
                pub(SelfUp(_current_state(new)))
            elif isinstance(e, MemberRemoved):
                pub(SelfRemoved(e.previous_status))
    for e in diff_unreachable(old, new):
        pub(e)
    for e in diff_reachable(old, new):
        pub(e)
    for e in diff_leader(old, new):
        pub(e)
    for e in diff_seen(old, new):
        pub(e)
    for e in diff_reachability(old, new):
        pub(e)


def is_gossip_speedup_needed(gossip: Gossip) -> bool:
    """A Down member, or fewer than half the members having seen this gossip, calls for faster gossip."""
    return any(m.status == "down" for m in gossip.members) or len(gossip.seen_by) < len(gossip.members) // 2


# --- the core daemon -----------------------------------------------------------------------------------------


def _stop_message() -> object:
    from ..cell import _STOP  # pyright: ignore[reportPrivateUsage]

    return _STOP


def _core(cluster: Cluster) -> Behavior[_Msg]:
    async def setup(ctx: ActorContext[_Msg]) -> Behavior[_Msg]:
        remoting = ctx.system.remoting
        assert remoting is not None
        await remoting.listening.wait()  # replies must be able to reach us before we ask anyone anything
        return _CoreDaemon(ctx, cluster).start()

    return Behaviors.setup(setup)


def _heartbeat_receiver(self_unique_address: UniqueAddress) -> Behavior[Heartbeat]:
    """ClusterHeartbeatReceiver: answers every heartbeat."""

    def on_heartbeat(hb: Heartbeat) -> Behavior[Heartbeat]:
        hb.reply_to.tell(HeartbeatRsp(self_unique_address, hb.seq, hb.created_ns))
        return Behaviors.same

    return Behaviors.receive_message(on_heartbeat)


class _CoreDaemon:
    """ClusterCoreDaemon's state and functions. Lives in the actor's closure; the behaviors it returns are the
    three states of Pekko's daemon: uninitialized, tryingToJoin and initialized."""

    def __init__(self, ctx: ActorContext[_Msg], cluster: Cluster) -> None:
        self.ctx = ctx
        self.cluster = cluster
        self.settings = cluster.settings
        self.log = ctx.log
        self.loop = asyncio.get_running_loop()
        self.self_unique_address = cluster.self_unique_address
        self.self_address = cluster.self_address
        self.vclock_node = vclock_name(self.self_unique_address)
        self.failure_detector = cluster.failure_detector
        self.selector = GossipTargetSelector(
            self.settings.gossip_different_view_probability, self.settings.reduce_gossip_different_view_probability
        )
        self.empty_state = MembershipState(Gossip.empty(), self.self_unique_address)
        self.membership_state = self.empty_state
        self.published_state = self.empty_state
        self.is_currently_leader = False
        self.seed_nodes = self.settings.seed_nodes
        self.first_seed: tuple[set[Address], float] | None = None  # remaining seeds, deadline: FirstSeedNodeProcess
        self.join_seed_attempt: int | None = None  # JoinSeedNodeProcess
        self.leader_action_counter = 0
        self.self_down_counter = 0
        self.exiting_tasks_in_progress = False
        self.exiting_confirmed: frozenset[UniqueAddress] = frozenset()
        self.subscribers: dict[ActorRef[Any], set[type[ClusterDomainEvent]]] = {}
        self.heartbeat_state: HeartbeatSenderState | None = None  # ClusterHeartbeatSender, alive while initialized
        self.heartbeat_seq = 0
        self.downing_provider: ActorRef[Any] | None = None
        self.uninitialized: Behavior[_Msg] = Behaviors.receive(self._uninitialized).receive_signal(self._on_signal)
        self.initialized: Behavior[_Msg] = Behaviors.receive(self._initialized).receive_signal(self._on_signal)

    @property
    def latest_gossip(self) -> Gossip:
        return self.membership_state.latest_gossip

    def start(self) -> Behavior[_Msg]:
        s, timers = self.settings, self.ctx.timers
        timers.start_timer_with_fixed_delay(
            "gossip", _GossipTick(), s.gossip_interval, max(s.periodic_tasks_initial_delay, s.gossip_interval)
        )
        timers.start_timer_with_fixed_delay(
            "reap",
            _ReapUnreachableTick(),
            s.unreachable_nodes_reaper_interval,
            max(s.periodic_tasks_initial_delay, s.unreachable_nodes_reaper_interval),
        )
        timers.start_timer_with_fixed_delay(
            "leader",
            _LeaderActionsTick(),
            s.leader_actions_interval,
            max(s.periodic_tasks_initial_delay, s.leader_actions_interval),
        )
        self.ctx.spawn(_heartbeat_receiver(self.self_unique_address), "heartbeat")
        if self.seed_nodes:
            self.ctx.self.tell(JoinSeedNodes(self.seed_nodes))
        else:
            self.log.info("No seed nodes configured, manual cluster join required")
        return self.uninitialized

    # --- the three states --------------------------------------------------------------------------------

    def _uninitialized(self, _ctx: ActorContext[_Msg], msg: _Msg) -> Behavior[_Msg]:
        match msg:
            case InitJoin(reply_to):
                self.log.info("Received InitJoin message from [%s], but this node is not initialized yet", reply_to.path)
                reply_to.tell(InitJoinNack(self.self_address))
            case Join(address):
                return self.join(address)
            case JoinSeedNodes(seeds):
                return self.join_seed_nodes(seeds)
            case Welcome(from_, gossip):
                return self.welcome(from_.address, from_, gossip)
            case Subscribe() | Unsubscribe() | GetCurrentState():
                self.subscription(msg)
            case InitJoinAck() | InitJoinNack() | _JoinSeedNodeTick():
                self.seed_node_process(msg)
            case _ExitingCompleted(reply_to):
                self.exiting_completed(reply_to)
            case _:
                pass
        return Behaviors.same

    def trying_to_join(self, join_with: Address, deadline: float) -> Behavior[_Msg]:
        def receive(_ctx: ActorContext[_Msg], msg: _Msg) -> Behavior[_Msg]:
            match msg:
                case Welcome(from_, gossip):
                    return self.welcome(join_with, from_, gossip)
                case InitJoin(reply_to):
                    self.log.info("Received InitJoin message from [%s], but this node is not a member yet", reply_to.path)
                    reply_to.tell(InitJoinNack(self.self_address))
                case Join(address):
                    self.stop_seed_node_process()
                    return self.join(address)
                case JoinSeedNodes(seeds):
                    self.stop_seed_node_process()
                    return self.join_seed_nodes(seeds)
                case Subscribe() | Unsubscribe() | GetCurrentState():
                    self.subscription(msg)
                case _GossipTick() | _GossipSpeedupTick() | _ReapUnreachableTick() | _LeaderActionsTick():
                    if self.loop.time() >= deadline:
                        self.stop_seed_node_process()
                        if self.seed_nodes:
                            return self.join_seed_nodes(self.seed_nodes)
                        return self.join(join_with)
                case _ExitingCompleted(reply_to):
                    self.exiting_completed(reply_to)
                case _:
                    pass
            return Behaviors.same

        return Behaviors.receive(receive).receive_signal(self._on_signal)

    def _initialized(self, _ctx: ActorContext[_Msg], msg: _Msg) -> Behavior[_Msg]:
        match msg:
            case GossipEnvelope():
                self.receive_gossip(msg)
            case GossipStatus():
                self.receive_gossip_status(msg)
            case _GossipTick():
                self.gossip_tick()
            case _GossipSpeedupTick():
                self.gossip_speedup_tick()
            case _ReapUnreachableTick():
                self.reap_unreachable_members()
            case _LeaderActionsTick():
                self.leader_actions()
            case _HeartbeatTick():
                self.heartbeat()
            case HeartbeatRsp():
                self.heartbeat_rsp(msg)
            case _ExpectedFirstHeartbeat(from_):
                self.trigger_first_heartbeat(from_)
            case InitJoin(reply_to):
                self.log.info("Received InitJoin message from [%s] to [%s]", reply_to.path, self.self_address)
                self.init_join(reply_to)
            case JoinNode(node, reply_to):
                self.joining(node, reply_to)
            case Down(address):
                self.downing(address)
            case Leave(address):
                self.leaving(address)
            case Subscribe() | Unsubscribe() | GetCurrentState():
                self.subscription(msg)
            case Join(address):
                self.log.info("Trying to join [%s] when already part of a cluster, ignoring", address)
            case JoinSeedNodes(seeds):
                self.log.info("Trying to join seed nodes [%s] when already part of a cluster, ignoring", seeds)
            case ExitingConfirmed(node):
                self.receive_exiting_confirmed(node)
            case _ExitingCompleted(reply_to):
                self.exiting_completed(reply_to)
            case InitJoinAck() | InitJoinNack() | _JoinSeedNodeTick() | Welcome():
                pass  # late answers from a join that already succeeded
        return Behaviors.same

    def _on_signal(self, _ctx: ActorContext[_Msg], signal: Signal) -> Behavior[_Msg]:
        if signal is PostStop:
            self.publish_changes(self.empty_state)  # ClusterDomainEventPublisher.postStop: everyone is Removed now
            if not self.cluster.self_exiting.done():
                self.cluster.self_exiting.set_result(None)  # a stopped daemon never blocks the cluster-exiting phase
        elif isinstance(signal, Terminated) and self.is_downing_provider(signal.ref):
            for ref in [r for r in self.subscribers if r.path == signal.ref.path]:  # refs compare by identity
                del self.subscribers[ref]
            # The downing provider is vital to a working cluster: its failure, or its stop before this member is
            # Removed or Down, ends the daemon and with it the node. Its stop on self Removed is the normal exit.
            self_status = self.latest_gossip.member(self.self_unique_address).status if self.latest_gossip.has_member(self.self_unique_address) else "removed"
            if isinstance(signal, ChildFailed) or self_status not in ("down", "removed"):
                self.log.error("Cluster downing provider stopped (%s); shutting down the node", signal)
                return Behaviors.stopped()
        return Behaviors.same

    def become_initialized(self) -> Behavior[_Msg]:
        self.stop_seed_node_process()
        s = self.settings
        gossip = self.latest_gossip
        ring = HeartbeatNodeRing(self.self_unique_address, frozenset({self.self_unique_address}), frozenset(), s.monitored_by_nr_of_members)
        self.heartbeat_state = HeartbeatSenderState(ring, frozenset(), self.failure_detector).init(
            frozenset(m.unique_address for m in gossip.members),
            frozenset(n for n in gossip.reachability.all_unreachable_or_terminated if gossip.has_member(n)),
        )
        self.ctx.timers.start_timer_with_fixed_delay(
            "heartbeat", _HeartbeatTick(), s.heartbeat_interval, max(s.periodic_tasks_initial_delay, s.heartbeat_interval)
        )
        if s.downing is not None and self.downing_provider is None:
            from .sbr import split_brain_resolver  # the resolver imports this module's events

            self.downing_provider = self.ctx.spawn(
                split_brain_resolver(s.downing, self.self_unique_address, self.ctx.self), "downingProvider"
            )
            self.ctx.watch(self.downing_provider)
        return self.initialized

    # --- joining -------------------------------------------------------------------------------------------

    def cluster_core(self, address: Address) -> ActorRef[Any]:
        return self.ctx.system.resolve(f"{address.root}/system/cluster")

    def init_join(self, reply_to: ActorRef[InitJoinAck | InitJoinNack]) -> None:
        if self.latest_gossip.member(self.self_unique_address).status in REMOVE_UNREACHABLE_WITH_MEMBER_STATUS:
            self.log.info("Sending InitJoinNack message from node [%s] to [%s]", self.self_address, reply_to.path)
            reply_to.tell(InitJoinNack(self.self_address))
        else:
            self.log.info("Sending InitJoinAck message from node [%s] to [%s]", self.self_address, reply_to.path)
            reply_to.tell(InitJoinAck(self.self_address))

    def join_seed_nodes(self, new_seed_nodes: tuple[Address, ...]) -> Behavior[_Msg]:
        if new_seed_nodes:
            self.stop_seed_node_process()
            self.seed_nodes = new_seed_nodes  # keep them for retry
            if new_seed_nodes == (self.self_address,):
                self.ctx.self.tell(Join(self.self_address))
            elif new_seed_nodes[0] == self.self_address:  # FirstSeedNodeProcess
                self.first_seed = (set(new_seed_nodes) - {self.self_address}, self.loop.time() + self.settings.seed_node_timeout)
                self.ctx.self.tell(_JoinSeedNodeTick())
                # ponytail: Pekko retries every second; capped by the seed timeout so millisecond settings work
                self.ctx.timers.start_timer_with_fixed_delay(
                    "seed", _JoinSeedNodeTick(), min(1.0, self.settings.seed_node_timeout)
                )
            else:  # JoinSeedNodeProcess
                self.join_seed_attempt = 0
                self.ctx.self.tell(_JoinSeedNodeTick())
                self.ctx.timers.start_timer_with_fixed_delay("seed", _JoinSeedNodeTick(), self.settings.seed_node_timeout)
        return self.uninitialized

    def seed_node_process(self, msg: InitJoinAck | InitJoinNack | _JoinSeedNodeTick) -> None:
        """FirstSeedNodeProcess and JoinSeedNodeProcess, as state in this actor rather than children."""
        if self.first_seed is not None:
            remaining, deadline = self.first_seed
            match msg:
                case _JoinSeedNodeTick():
                    if self.loop.time() < deadline:
                        self.send_init_join(remaining)
                    else:
                        self.log.debug("Couldn't join other seed nodes, will join myself. seed-nodes=[%s]", self.seed_nodes)
                        self.ctx.self.tell(Join(self.self_address))
                        self.stop_seed_node_process()
                case InitJoinAck(address):
                    self.ctx.self.tell(Join(address))
                    self.stop_seed_node_process()
                case InitJoinNack(address):
                    self.log.info("Received InitJoinNack message from [%s] to [%s]", address, self.self_address)
                    remaining.discard(address)
                    if not remaining:
                        self.ctx.self.tell(Join(self.self_address))
                        self.stop_seed_node_process()
        elif self.join_seed_attempt is not None:
            match msg:
                case _JoinSeedNodeTick():
                    self.join_seed_attempt += 1
                    if self.join_seed_attempt > 2:
                        self.log.warning(
                            "Couldn't join seed nodes after [%s] attempts, will try again. seed-nodes=[%s]",
                            self.join_seed_attempt - 1,
                            [a for a in self.seed_nodes if a != self.self_address],
                        )
                    self.send_init_join(set(self.seed_nodes) - {self.self_address})
                case InitJoinAck(address):  # the first answer wins
                    self.ctx.self.tell(Join(address))
                    self.stop_seed_node_process()
                case InitJoinNack():
                    pass  # that seed was uninitialized

    def send_init_join(self, seeds: Iterable[Address]) -> None:
        for a in seeds:
            self.cluster_core(a).tell(InitJoin(self.ctx.self))

    def stop_seed_node_process(self) -> None:
        self.first_seed = None
        self.join_seed_attempt = None
        self.ctx.timers.cancel("seed")

    def join(self, address: Address) -> Behavior[_Msg]:
        if address.name != self.self_address.name:
            self.log.warning(
                "Trying to join member with wrong ActorSystem name, but was ignored, expected [%s] but was [%s]",
                self.self_address.name,
                address.name,
            )
            return self.uninitialized
        assert not self.latest_gossip.members, "Join can only be done from empty state"
        self.stop_seed_node_process()
        if address == self.self_address:
            behavior = self.become_initialized()
            self.joining(self.self_unique_address, None)
            return behavior
        deadline = self.loop.time() + self.settings.retry_unsuccessful_join_after
        self.log.debug("Trying to join [%s]", address)
        self.cluster_core(address).tell(JoinNode(self.self_unique_address, self.ctx.self))
        return self.trying_to_join(address, deadline)

    def joining(self, node: UniqueAddress, reply_to: ActorRef[Welcome] | None) -> None:
        self_status = self.latest_gossip.member(self.self_unique_address).status
        if node.name != self.self_address.name:
            self.log.warning(
                "Member with wrong ActorSystem name tried to join, but was ignored, expected [%s] but was [%s]",
                self.self_address.name,
                node.name,
            )
        elif self_status in REMOVE_UNREACHABLE_WITH_MEMBER_STATUS:
            self.log.info("Trying to join [%s] to [%s] member, ignoring. Use a member that is Up instead.", node, self_status)
        else:
            local_members = self.latest_gossip.members
            existing = next((m for m in local_members if m.address == node.address), None)
            if existing is not None and existing.unique_address == node:
                self.log.info("Existing member [%s] is joining again.", existing)
                if node != self.self_unique_address and reply_to is not None:
                    reply_to.tell(Welcome(self.self_unique_address, self.latest_gossip))
            elif existing is not None:
                self.log.info(
                    "New incarnation of existing member [%s] is trying to join. Existing will be removed from the "
                    "cluster and then new member will be allowed to join.",
                    existing,
                )
                if existing.status != "down":
                    reachability = self.latest_gossip.reachability.terminated(self.self_unique_address, existing.unique_address)
                    self.update_latest_gossip(self.latest_gossip.with_reachability(reachability))
                    self.downing(existing.address)
            else:
                self.failure_detector.remove(node.address)
                new_gossip = self.latest_gossip.with_member(Member(node, UP_NUMBER_UNSET, "joining")).with_member(
                    Member(self.self_unique_address, UP_NUMBER_UNSET, "joining")
                )
                self.update_latest_gossip(new_gossip)
                if node == self.self_unique_address:
                    self.log.info("Node [%s] is JOINING itself and forming new cluster", node.address)
                    if not local_members:
                        self.leader_actions()  # important for deterministic oldest when bootstrapping
                else:
                    self.log.info("Node [%s] is JOINING", node.address)
                    if reply_to is not None:
                        reply_to.tell(Welcome(self.self_unique_address, self.latest_gossip))
                self.publish_membership_state()

    def welcome(self, join_with: Address, from_: UniqueAddress, gossip: Gossip) -> Behavior[_Msg]:
        assert not self.latest_gossip.members, "Join can only be done from empty state"
        if join_with != from_.address:
            self.log.info("Ignoring welcome from [%s] when trying to join with [%s]", from_.address, join_with)
            return Behaviors.same
        self.membership_state = replace(self.membership_state, latest_gossip=gossip).seen()
        self.log.info("Welcome from [%s]", from_.address)
        self.publish_membership_state()
        if from_ != self.self_unique_address:
            self.gossip_to(from_)
        return self.become_initialized()

    # --- leaving, exiting, downing ---------------------------------------------------------------------------

    def leaving(self, address: Address) -> None:
        existing = next((m for m in self.latest_gossip.members if m.address == address), None)
        if existing is not None and existing.status in ("joining", "weakly_up", "up"):
            self.update_latest_gossip(self.latest_gossip.update([existing.with_status("leaving")]))
            self.log.info("Marked address [%s] as [leaving]", address)
            self.publish_membership_state()
            self.gossip()

    def start_exiting(self) -> None:
        """Self is Exiting: the cluster-exiting phase may proceed, and the coordinated shutdown runs if nothing started
        it yet (Pekko: ExitingCompleted arrives through CoordinatedShutdown; the gossip is not marked seen meanwhile)."""
        if not self.cluster.self_exiting.done():
            self.cluster.self_exiting.set_result(None)
        CoordinatedShutdown(self.ctx.system).run(ClusterLeavingReason())

    def exiting_completed(self, reply_to: ActorRef[None] | None = None) -> None:
        self.log.info("Exiting completed")
        self.exiting_tasks_in_progress = False
        if self.membership_state.self_member.status != "removed":
            self.membership_state = self.membership_state.seen()
            self.publish_membership_state()
            self.gossip_random_n(self.settings.max_gossips_before_shutting_down_myself)
            others = [m for m in self.latest_gossip.members if m.unique_address != self.self_unique_address]
            node1 = self.membership_state.leader_of(others)
            if node1 is not None:
                self.cluster_core(node1.address).tell(ExitingConfirmed(self.self_unique_address))
                node2 = self.membership_state.leader_of([m for m in others if m.unique_address != node1])
                if node2 is not None:
                    self.cluster_core(node2.address).tell(ExitingConfirmed(self.self_unique_address))
        if reply_to is not None:
            reply_to.tell(None)
        self.shutdown()

    def receive_exiting_confirmed(self, node: UniqueAddress) -> None:
        self.log.info("Exiting confirmed [%s]", node.address)
        self.exiting_confirmed |= {node}

    def cleanup_exiting_confirmed(self) -> None:
        if self.exiting_confirmed:
            self.exiting_confirmed = frozenset(n for n in self.exiting_confirmed if self.latest_gossip.has_member(n))

    def shutdown(self) -> None:
        self.cluster.shutdown()

    def downing(self, address: Address) -> None:
        local_gossip = self.latest_gossip
        m = next((m for m in local_gossip.members if m.address == address), None)
        if m is None:
            self.log.info("Ignoring down of unknown node [%s]", address)
        elif m.status != "down":
            reachable = local_gossip.reachability.is_reachable(m.unique_address)
            self.log.info("Marking %snode [%s] as [down]", "" if reachable else "unreachable ", m.address)
            self.update_latest_gossip(local_gossip.mark_as_down(m))
            self.publish_membership_state()
            if address == self.self_address:
                self.gossip_random_n(self.settings.max_gossips_before_shutting_down_myself)
            else:
                self.gossip_to(m.unique_address)

    # --- gossip ----------------------------------------------------------------------------------------------

    def receive_gossip_status(self, status: GossipStatus) -> None:
        from_ = status.from_
        if not self.latest_gossip.has_member(from_):
            self.log.info("Ignoring received gossip status from unknown [%s]", from_)
        elif not self.latest_gossip.is_reachable(self.self_unique_address, from_):
            self.log.info("Ignoring received gossip status from unreachable [%s]", from_)
        elif status.seen_digest != self.latest_gossip.seen_digest:
            self.gossip_to(from_)
        else:
            match status.version.compare(self.latest_gossip.version):
                case "same":
                    pass
                case "after":
                    self.gossip_status_to(from_)  # remote is newer
                case _:
                    self.gossip_to(from_)  # conflicting or local is newer

    def receive_gossip(self, envelope: GossipEnvelope) -> str:
        from_, remote_gossip, local_gossip = envelope.from_, envelope.gossip, self.latest_gossip
        if envelope.to != self.self_unique_address:
            self.log.info("Ignoring received gossip intended for someone else, from [%s] to [%s]", from_.address, envelope.to)
            return "ignored"
        if not local_gossip.has_member(from_):
            self.log.info("Ignoring received gossip from unknown [%s]", from_)
            return "ignored"
        if not local_gossip.is_reachable(self.self_unique_address, from_):
            self.log.info("Ignoring received gossip from unreachable [%s]", from_)
            return "ignored"
        if not remote_gossip.has_member(self.self_unique_address):
            self.log.info("Ignoring received gossip that does not contain myself, from [%s]", from_)
            return "ignored"
        comparison = remote_gossip.version.compare(local_gossip.version)
        match comparison:
            case "same":
                talkback = not self.exiting_tasks_in_progress and not remote_gossip.seen_by_node(self.self_unique_address)
                winning, gossip_type = remote_gossip.merge_seen(local_gossip), "same"
            case "before":
                winning, talkback, gossip_type = local_gossip, True, "older"
            case "after":
                talkback = not self.exiting_tasks_in_progress and not remote_gossip.seen_by_node(self.self_unique_address)
                winning, gossip_type = remote_gossip, "newer"
            case "concurrent":
                pruned_local = local_gossip
                for m in local_gossip.members:
                    if m.status in REMOVE_UNREACHABLE_WITH_MEMBER_STATUS and m not in remote_gossip.members:
                        self.log.debug("Pruned conflicting local gossip: %s", m)
                        pruned_local = pruned_local.prune(vclock_name(m.unique_address))
                pruned_remote = remote_gossip
                for m in remote_gossip.members:
                    if m.status in REMOVE_UNREACHABLE_WITH_MEMBER_STATUS and m not in local_gossip.members:
                        self.log.debug("Pruned conflicting remote gossip: %s", m)
                        pruned_remote = pruned_remote.prune(vclock_name(m.unique_address))
                winning, talkback, gossip_type = pruned_remote.merge(pruned_local), True, "merge"
        self.membership_state = replace(
            self.membership_state,
            latest_gossip=winning if self.exiting_tasks_in_progress else winning.seen(self.self_unique_address),
        )
        for m in self.latest_gossip.members:
            if m not in local_gossip.members:
                self.failure_detector.remove(m.address)
        self.log.debug("Receiving gossip from [%s]", from_)
        self.publish_membership_state()
        self_status = self.latest_gossip.member(self.self_unique_address).status
        if self_status == "exiting" and not self.exiting_tasks_in_progress:
            self.exiting_tasks_in_progress = True
            self.log.info("Exiting, starting coordinated shutdown")
            self.start_exiting()
        if self_status == "down" and local_gossip.member(self.self_unique_address).status != "down":
            self.log.warning("Received gossip where this member has been downed, from [%s]", from_.address)
            self.shutdown_self_when_down()
        if talkback:
            self.gossip_to(from_)
        return gossip_type

    def gossip_tick(self) -> None:
        self.gossip()
        if self.is_gossip_speedup_needed:
            interval = self.settings.gossip_interval
            self.ctx.timers.start_single_timer("speedup-1", _GossipSpeedupTick(), interval / 3)
            self.ctx.timers.start_single_timer("speedup-2", _GossipSpeedupTick(), interval * 2 / 3)

    def gossip_speedup_tick(self) -> None:
        if self.is_gossip_speedup_needed:
            self.gossip()

    @property
    def is_gossip_speedup_needed(self) -> bool:
        return is_gossip_speedup_needed(self.latest_gossip)

    def gossip_random_n(self, n: int) -> None:
        if not self.latest_gossip.is_singleton and n > 0:
            for node in self.selector.random_nodes_for_full_gossip(self.membership_state, n):
                self.gossip_to(node)

    def gossip(self) -> None:
        if not self.latest_gossip.is_singleton:
            peer = self.selector.gossip_target(self.membership_state)
            if peer is not None:
                if self.latest_gossip.seen_by_node(peer):
                    self.gossip_status_to(peer)
                else:
                    self.gossip_to(peer)

    def gossip_to(self, node: UniqueAddress) -> None:
        if self.membership_state.valid_node_for_gossip(node):
            self.cluster_core(node.address).tell(GossipEnvelope(self.self_unique_address, node, self.latest_gossip))

    def gossip_status_to(self, node: UniqueAddress) -> None:
        if self.membership_state.valid_node_for_gossip(node):
            gossip = self.latest_gossip
            self.cluster_core(node.address).tell(GossipStatus(self.self_unique_address, gossip.version, gossip.seen_digest))

    # --- leader ----------------------------------------------------------------------------------------------

    def leader_actions(self) -> None:
        if self.membership_state.is_leader(self.self_unique_address):
            if not self.is_currently_leader:
                self.log.info("is the new leader among reachable nodes (more leaders may exist)")
                self.is_currently_leader = True
            first_notice, periodic_notice = 20, 60
            if self.membership_state.convergence(self.exiting_confirmed):
                if self.leader_action_counter >= first_notice:
                    self.log.info("Leader can perform its duties again")
                self.leader_action_counter = 0
                self.leader_actions_on_convergence()
            else:
                self.leader_action_counter += 1
                weakly_up_after = self.settings.allow_weakly_up_members
                if weakly_up_after is not None and self.settings.leader_actions_interval * self.leader_action_counter >= weakly_up_after:
                    self.move_joining_to_weakly_up()
                if self.leader_action_counter == first_notice or self.leader_action_counter % periodic_notice == 0:
                    self.log.info(
                        "Leader can currently not perform its duties, reachability status: [%s], member status: [%s]",
                        self.membership_state.reachability_excluding_downed_observers,
                        ", ".join(
                            f"{m.address} {m.status} seen={self.latest_gossip.seen_by_node(m.unique_address)}"
                            for m in self.latest_gossip.members
                        ),
                    )
        elif self.is_currently_leader:
            self.log.info("is no longer leader")
            self.is_currently_leader = False
        self.cleanup_exiting_confirmed()
        self.shutdown_self_when_down()

    def shutdown_self_when_down(self) -> None:
        if self.latest_gossip.member(self.self_unique_address).status == "down":
            unreachable = self.latest_gossip.reachability.all_unreachable_or_terminated
            downed = [m.unique_address for m in self.latest_gossip.members if m.status == "down"]
            if self.self_down_counter >= self.settings.max_ticks_before_shutting_down_myself or all(
                n in unreachable or self.latest_gossip.seen_by_node(n) for n in downed
            ):
                self.log.info("Node has been marked as DOWN. Shutting down myself")
                self.gossip_random_n(self.settings.max_gossips_before_shutting_down_myself)
                self.shutdown()
            else:
                self.self_down_counter += 1

    @property
    def is_min_nr_of_members_fulfilled(self) -> bool:
        return len(self.latest_gossip.members) >= self.settings.min_nr_of_members

    def leader_actions_on_convergence(self) -> None:
        gossip = self.latest_gossip
        removed_unreachable = {
            gossip.member(n)
            for n in gossip.reachability.all_unreachable_or_terminated
            if gossip.member(n).status in REMOVE_UNREACHABLE_WITH_MEMBER_STATUS
        }
        removed_exiting_confirmed = frozenset(n for n in self.exiting_confirmed if gossip.member(n).status == "exiting")
        enough_members = self.is_min_nr_of_members_fulfilled
        changed: list[Member] = []
        up_number = 0
        for m in gossip.members:
            if m.status in ("joining", "weakly_up") and enough_members:
                if up_number == 0:
                    youngest = self.membership_state.youngest_member
                    up_number = 1 + (0 if youngest.up_number == UP_NUMBER_UNSET else youngest.up_number)
                else:
                    up_number += 1
                changed.append(m.copy_up(up_number))
            elif m.status == "leaving":
                changed.append(m.with_status("exiting"))
        if removed_unreachable or removed_exiting_confirmed or changed:
            removed = {m.unique_address for m in removed_unreachable} | removed_exiting_confirmed
            new_gossip = gossip.update(changed).remove_all(removed, time.time())
            if not self.exiting_tasks_in_progress and new_gossip.member(self.self_unique_address).status == "exiting":
                self.exiting_tasks_in_progress = True
                self.log.info("Exiting (leader), starting coordinated shutdown")
                self.start_exiting()
            self.exiting_confirmed -= removed_exiting_confirmed
            for m in changed:
                self.log.info("Leader is moving node [%s] to [%s]", m.address, m.status)
            for m in removed_unreachable:
                self.log.info("Leader is removing %s node [%s]", "exiting" if m.status == "exiting" else "unreachable", m.address)
            for n in removed_exiting_confirmed:
                self.log.info("Leader is removing confirmed Exiting node [%s]", n.address)
            updated = new_gossip
        else:
            updated = gossip
        pruned = updated.prune_tombstones(time.time() - self.settings.prune_gossip_tombstones_after)
        if pruned is not gossip:
            self.update_latest_gossip(pruned)
            self.publish_membership_state()
            self.gossip_exiting_members_to_oldest([m for m in changed if m.status == "exiting"])

    def gossip_exiting_members_to_oldest(self, exiting: list[Member]) -> None:
        for m in self.membership_state.gossip_targets_for_exiting_members(exiting):
            self.gossip_to(m.unique_address)

    def move_joining_to_weakly_up(self) -> None:
        gossip = self.latest_gossip
        enough_members = self.is_min_nr_of_members_fulfilled
        reachability = self.membership_state.reachability_excluding_downed_observers
        changed = [
            m.with_status("weakly_up")
            for m in gossip.members
            if m.status == "joining" and enough_members and reachability.is_reachable(m.unique_address)
        ]
        if changed:
            self.update_latest_gossip(gossip.update(changed))
            for m in changed:
                self.log.info("Leader is moving node [%s] to [%s]", m.address, m.status)
            self.publish_membership_state()

    # --- heartbeats (ClusterHeartbeatSender) ---------------------------------------------------------------------

    def heartbeat_receiver(self, address: Address) -> ActorRef[Any]:
        return self.ctx.system.resolve(f"{address.root}/system/cluster/heartbeat")

    def heartbeat(self) -> None:
        if self.heartbeat_state is None:
            return
        self.heartbeat_seq += 1
        hb = Heartbeat(self.self_address, self.heartbeat_seq, time.monotonic_ns(), self.ctx.self)
        for to in self.heartbeat_state.active_receivers:
            if not self.failure_detector.is_monitoring(to.address):
                self.ctx.timers.start_single_timer(
                    ("first-heartbeat", to, self.heartbeat_seq),
                    _ExpectedFirstHeartbeat(to),
                    self.settings.heartbeat_expected_response_after,
                )
            self.heartbeat_receiver(to.address).tell(hb)

    def heartbeat_rsp(self, response: HeartbeatRsp) -> None:
        if self.heartbeat_state is not None:
            self.heartbeat_state = self.heartbeat_state.heartbeat_rsp(response.from_)

    def trigger_first_heartbeat(self, from_: UniqueAddress) -> None:
        """No answer yet from a new receiver: start its detector so silence counts from now."""
        if (
            self.heartbeat_state is not None
            and from_ in self.heartbeat_state.active_receivers
            and not self.failure_detector.is_monitoring(from_.address)
        ):
            self.failure_detector.heartbeat(from_.address)

    def heartbeat_event(self, event: ClusterDomainEvent) -> None:
        """What the sender learns from its subscription to MemberEvent and ReachabilityEvent."""
        state = self.heartbeat_state
        if state is None:
            return
        match event:
            case MemberRemoved(member=m) if m.unique_address == self.self_unique_address:
                for a in state.active_receivers:  # the sender's postStop
                    self.failure_detector.remove(a.address)
                self.ctx.timers.cancel("heartbeat")
                self.heartbeat_state = None
            case MemberRemoved(member=m):
                self.heartbeat_state = state.remove_member(m.unique_address)
            case MemberEvent(member=m):
                if m.unique_address != self.self_unique_address and not state.contains(m.unique_address):
                    self.heartbeat_state = state.add_member(m.unique_address)
            case UnreachableMember(member=m):
                self.heartbeat_state = state.unreachable_member(m.unique_address)
            case ReachableMember(member=m):
                self.heartbeat_state = state.reachable_member(m.unique_address)
            case _:
                pass

    # --- failure detection -------------------------------------------------------------------------------------

    def reap_unreachable_members(self) -> None:
        if self.latest_gossip.is_singleton:
            return
        gossip = self.latest_gossip
        reachability = gossip.reachability
        newly_unreachable = [
            m
            for m in gossip.members
            if m.unique_address != self.self_unique_address
            and reachability.status(self.self_unique_address, m.unique_address) == "reachable"
            and not self.failure_detector.is_available(m.address)
        ]
        newly_reachable = [
            gossip.member(n)
            for n in reachability.all_unreachable_from(self.self_unique_address)
            if n != self.self_unique_address and self.failure_detector.is_available(n.address)
        ]
        if not newly_unreachable and not newly_reachable:
            return
        new_reachability = reachability
        for m in newly_unreachable:
            new_reachability = new_reachability.unreachable(self.self_unique_address, m.unique_address)
        for m in newly_reachable:
            new_reachability = new_reachability.reachable(self.self_unique_address, m.unique_address)
        if new_reachability is not reachability:
            self.update_latest_gossip(gossip.with_reachability(new_reachability))
            for m in newly_unreachable:
                if m.status == "exiting":
                    self.log.info("Marking exiting node as UNREACHABLE [%s]. This is expected and it will be removed.", m)
                else:
                    self.log.warning("Marking node as UNREACHABLE [%s].", m)
            for m in newly_reachable:
                self.log.info("Marking node as REACHABLE [%s].", m)
            self.publish_membership_state()

    # --- state and events ---------------------------------------------------------------------------------------

    def update_latest_gossip(self, gossip: Gossip) -> None:
        versioned = gossip.increment(self.vclock_node)
        new_gossip = versioned.clear_seen() if self.exiting_tasks_in_progress else versioned.only_seen(self.self_unique_address)
        self.membership_state = replace(self.membership_state, latest_gossip=new_gossip)

    def publish_membership_state(self) -> None:
        self.publish_changes(self.membership_state)

    def publish_changes(self, new_state: MembershipState) -> None:
        old_state, self.published_state = self.published_state, new_state
        self.cluster._membership_state = new_state  # pyright: ignore[reportPrivateUsage]  # the read view
        publish_diff(old_state, new_state, self.publish)

    def publish(self, event: ClusterDomainEvent) -> None:
        self.heartbeat_event(event)
        for subscriber, classes in self.subscribers.items():
            if isinstance(event, tuple(classes)):
                subscriber.tell(event)

    def is_downing_provider(self, ref: ActorRef[Any]) -> bool:
        return self.downing_provider is not None and ref.path == self.downing_provider.path

    def subscription(self, msg: ClusterStateSubscription) -> None:
        match msg:
            case Subscribe(subscriber, event_class):
                if is_local_actor(subscriber) and not self.is_downing_provider(subscriber):
                    # A stopped subscriber drops out, as from Pekko's event stream. The downing provider keeps its
                    # plain watch from become_initialized, so its death arrives as a signal, not as an Unsubscribe.
                    self.ctx.watch_with(subscriber, Unsubscribe(subscriber))
                self.subscribers.setdefault(subscriber, set()).add(event_class)
                publish_diff(self.empty_state, self.published_state, lambda e: subscriber.tell(e) if isinstance(e, event_class) else None)
            case Unsubscribe(subscriber):
                self.subscribers.pop(subscriber, None)
            case GetCurrentState(reply_to):
                reply_to.tell(_current_state(self.published_state))


