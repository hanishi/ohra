"""Apache Pekko's ClusterSharding across nodes: a ShardCoordinator singleton allocating shards to regions with the
LeastShardAllocationStrategy, a ShardRegion on every node, hand-off for rebalancing and for a leaving node. The
coordinator rebuilds its state from the regions' registrations instead of distributed data."""
from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from ..behavior import Behavior, Behaviors
from ..context import ActorContext
from ..ref import ActorRef
from ..remote import Address, UniqueAddress
from ..serialization import Serialization
from ..cell import is_local_actor
from ..signals import PostStop, Signal
from ..supervision import SupervisorStrategy
from ..sharding import (
    ClusterShardingSettings,
    CurrentShardRegionState,
    Entity,
    EntityRef,
    EntityTypeKey,
    GetShardRegionState,
    HandOff,
    ShardingEnvelope,
    ShardStopped,
    _aggregate_shard_states,  # pyright: ignore[reportPrivateUsage]
    shard_behavior,
    shard_id_of,
)
from .cluster import (
    Cluster,
    ClusterDomainEvent,
    CurrentClusterState,
    MemberDowned,
    MemberEvent,
    MemberExited,
    MemberLeft,
    MemberRemoved,
    MemberUp,
)
from .cluster import Subscribe as ClusterSubscribe
from .gossip import Member
from .singleton import ClusterSingleton, SingletonActor
from .singleton import _manager_name  # pyright: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    from ..cell import ActorCell
    from ..system import ActorSystem

_LEAVING_STATUSES = frozenset({"leaving", "exiting", "down"})
_JOINING_STATUSES = frozenset({"joining", "weakly_up"})


# --- LeastShardAllocationStrategy ----------------------------------------------------------------------------


@dataclass(frozen=True)
class RegionEntry:
    region: str
    member: Member
    shard_ids: tuple[str, ...]

    @property
    def suitability(self) -> tuple[bool, int, int, tuple[str, int]]:
        """Pekko's ShardSuitabilityOrdering: regions not on their way out first, then fewest shards; ties go to the
        oldest member so the choice is deterministic."""
        m = self.member
        return (m.status in _LEAVING_STATUSES, len(self.shard_ids), m.up_number, (m.unique_address.host, m.unique_address.port))


class LeastShardAllocationStrategy:
    """Pekko's LeastShardAllocationStrategy: allocate to the least loaded region; rebalance in two phases, capped
    by the absolute and relative limits. ``absolute_limit`` 0 means no absolute cap."""

    def __init__(self, absolute_limit: int, relative_limit: float) -> None:
        self.absolute_limit = absolute_limit
        self.relative_limit = relative_limit

    def _entries(self, allocations: dict[str, tuple[str, ...]], members_by_region: dict[str, Member]) -> list[RegionEntry]:
        entries = [RegionEntry(r, members_by_region[r], ids) for r, ids in allocations.items() if r in members_by_region]
        return sorted(entries, key=lambda e: e.suitability)

    def allocate_shard(self, shard: str, allocations: dict[str, tuple[str, ...]], members_by_region: dict[str, Member]) -> str | None:
        entries = self._entries(allocations, members_by_region)
        return entries[0].region if entries else None

    def _limit(self, number_of_shards: int) -> int:
        cap = self.absolute_limit if self.absolute_limit > 0 else number_of_shards
        return max(1, min(int(self.relative_limit * number_of_shards), cap))

    @staticmethod
    def is_a_good_time_to_rebalance(entries: Iterable[RegionEntry], cluster_state: CurrentClusterState) -> bool:
        if not any(True for _ in entries):
            return False
        if cluster_state.unreachable:
            return False
        return not any(m.status in _JOINING_STATUSES for m in cluster_state.members)

    def rebalance(
        self,
        allocations: dict[str, tuple[str, ...]],
        members_by_region: dict[str, Member],
        rebalance_in_progress: frozenset[str],
        cluster_state: CurrentClusterState,
    ) -> frozenset[str]:
        if rebalance_in_progress:
            return frozenset()
        entries = self._entries(allocations, members_by_region)
        if not self.is_a_good_time_to_rebalance(entries, cluster_state):
            return frozenset()
        number_of_shards = sum(len(e.shard_ids) for e in entries)
        number_of_regions = len(entries)
        if number_of_regions == 0 or number_of_shards == 0:
            return frozenset()
        optimal_per_region = number_of_shards // number_of_regions + (0 if number_of_shards % number_of_regions == 0 else 1)
        # phase one: shards above the optimal count per region
        selected: list[str] = []
        for e in entries:
            if len(e.shard_ids) > optimal_per_region:
                selected.extend(e.shard_ids[: len(e.shard_ids) - optimal_per_region])
        if selected:
            return frozenset(selected[: self._limit(number_of_shards)])
        # phase two: fill regions below the optimal count
        count_below_optimal = sum(max(0, (optimal_per_region - 1) - len(e.shard_ids)) for e in entries)
        if count_below_optimal == 0:
            return frozenset()
        selected = [e.shard_ids[0] for e in entries if len(e.shard_ids) >= optimal_per_region]
        return frozenset(selected[: min(count_below_optimal, self._limit(number_of_shards))])


# --- the coordinator's state (ShardCoordinator.State and its DomainEvents) ---------------------------------


@dataclass(frozen=True)
class ShardRegionRegistered:
    region: str


@dataclass(frozen=True)
class ShardRegionTerminated:
    region: str


@dataclass(frozen=True)
class ShardHomeAllocated:
    shard: str
    region: str


@dataclass(frozen=True)
class ShardHomeDeallocated:
    shard: str


type DomainEvent = ShardRegionRegistered | ShardRegionTerminated | ShardHomeAllocated | ShardHomeDeallocated


@dataclass(frozen=True)
class CoordinatorState:
    """shard -> region path, and region path -> its shards. Regions are keyed by path so a local and a remote ref
    to the same region compare equal."""

    shards: dict[str, str] = field(default_factory=lambda: {})
    regions: dict[str, tuple[str, ...]] = field(default_factory=lambda: {})

    def updated(self, event: DomainEvent) -> CoordinatorState:
        match event:
            case ShardRegionRegistered(region):
                if region in self.regions:
                    raise ValueError(f"Region {region} already registered: {self}")
                return CoordinatorState(dict(self.shards), {**self.regions, region: ()})
            case ShardRegionTerminated(region):
                if region not in self.regions:
                    raise ValueError(f"Terminated region {region} not registered: {self}")
                gone = set(self.regions[region])
                return CoordinatorState({s: r for s, r in self.shards.items() if s not in gone}, {r: ids for r, ids in self.regions.items() if r != region})
            case ShardHomeAllocated(shard, region):
                if region not in self.regions:
                    raise ValueError(f"Region {region} not registered: {self}")
                if shard in self.shards:
                    raise ValueError(f"Shard [{shard}] already allocated: {self}")
                return CoordinatorState({**self.shards, shard: region}, {**self.regions, region: (*self.regions[region], shard)})
            case ShardHomeDeallocated(shard):
                if shard not in self.shards:
                    raise ValueError(f"Shard [{shard}] not allocated: {self}")
                region = self.shards[shard]
                if region not in self.regions:
                    raise ValueError(f"Region {region} for shard [{shard}] not registered: {self}")
                return CoordinatorState(
                    {s: r for s, r in self.shards.items() if s != shard},
                    {**self.regions, region: tuple(s for s in self.regions[region] if s != shard)},
                )

    @property
    def all_shards(self) -> frozenset[str]:
        return frozenset(self.shards)


# --- wire messages (ShardCoordinator.Internal), with explicit refs where Pekko uses sender() ------------------


@dataclass(frozen=True)
class Register:
    """A region announcing itself to the coordinator with the shards it hosts right now."""

    region: ActorRef[Any]
    hosted_shards: tuple[str, ...]


@dataclass(frozen=True)
class RegisterAck:
    coordinator: ActorRef[Any]


@dataclass(frozen=True)
class GetShardHome:
    shard: str
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class ShardHome:
    shard: str
    region: ActorRef[Any]


@dataclass(frozen=True)
class ShardHomes:
    homes: tuple[tuple[ActorRef[Any], tuple[str, ...]], ...]


@dataclass(frozen=True)
class HostShard:
    shard: str
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class ShardStarted:
    shard: str


@dataclass(frozen=True)
class BeginHandOff:
    shard: str
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class BeginHandOffAck:
    shard: str
    from_: ActorRef[Any]


@dataclass(frozen=True)
class GracefulShutdownReq:
    region: ActorRef[Any]


@dataclass(frozen=True)
class CoordinatorStopped:
    """Sent by a stopping coordinator to its regions, in place of their remote DeathWatch of it."""

    coordinator: ActorRef[Any]


for _cls in (Register, RegisterAck, GetShardHome, ShardHome, ShardHomes, HostShard, ShardStarted, BeginHandOff, BeginHandOffAck, GracefulShutdownReq, CoordinatorStopped):
    Serialization.register(_cls, f"Sharding.{_cls.__name__}")  # the Receptionist already owns the manifest "Register"


# --- internal messages -----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _ResendShardHost:
    shard: str
    region: str


@dataclass(frozen=True)
class _WaitingForStateTimeout: ...


@dataclass(frozen=True)
class _RebalanceTick: ...


@dataclass(frozen=True)
class _RebalanceDone:
    shard: str
    ok: bool


@dataclass(frozen=True)
class _Retry: ...


@dataclass(frozen=True)
class _RegisterRetry: ...


@dataclass(frozen=True)
class _ShardTerminated:
    shard: str


@dataclass(frozen=True)
class _SelfExiting: ...


@dataclass(frozen=True)
class _RegionTerminatedForWorker:
    region: str


@dataclass(frozen=True)
class _Terminate:
    """The coordinator singleton's stop message: stop once the local region has handed off (Pekko's Terminate)."""


@dataclass(frozen=True)
class _RegionStopped:
    region: str


@dataclass(frozen=True)
class _HandOffTimeout: ...


class _ExitingProgress:
    """The region's exiting task: ask the region to shut down gracefully and wait until it holds nothing."""

    def __init__(self) -> None:
        self.future: asyncio.Future[None] | None = None

    def task(self, cluster: Cluster, region: ActorRef[Any]) -> Callable[[], Awaitable[None]]:
        async def run() -> None:
            if cluster.is_terminated or cluster.self_member.status == "down":
                return
            region.tell(_SelfExiting())
            if self.future is not None:
                await asyncio.shield(self.future)

        return run


_OF_INTEREST = frozenset({"up", "leaving", "exiting"})  # Pekko's memberStatusOfInterest for coordinator placement


def _is_at(ref_or_path: ActorRef[Any] | str, address: Address) -> bool:
    path = ref_or_path if isinstance(ref_or_path, str) else ref_or_path.path
    return path.startswith(address.root + "/")


def _by_age(members: Iterable[Member]) -> list[Member]:
    ordered: list[Member] = []
    for m in members:
        i = 0
        while i < len(ordered) and ordered[i].is_older_than(m):
            i += 1
        ordered.insert(i, m)
    return ordered


# --- the hand-off worker (ShardCoordinator.RebalanceWorker) -------------------------------------------------------


def _rebalance_worker(
    type_name: str, shard: str, from_region: ActorRef[Any], regions: dict[str, ActorRef[Any]], handoff_timeout: float, is_rebalance: bool
) -> Behavior[Any]:
    """BeginHandOff to every region, wait for all acks (a terminated region counts), HandOff to the host, wait for
    ShardStopped; report _RebalanceDone to the coordinator either way."""

    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        remaining = set(regions)
        for region in regions.values():
            region.tell(BeginHandOff(shard, ctx.self))
        ctx.log.info("%s: %s shard [%s] from region [%s]: asking [%d] region(s) to hand off", type_name, "Rebalance" if is_rebalance else "Shutting down", shard, from_region.path, len(regions))
        ctx.timers.start_single_timer("hand-off-timeout", _HandOffTimeout(), handoff_timeout)
        parent = ctx.system.resolve(ctx.self.path.rsplit("/", 1)[0])

        def done(ok: bool) -> Behavior[Any]:
            parent.tell(_RebalanceDone(shard, ok))
            return Behaviors.stopped()

        def stopping_shard(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            match msg:
                case ShardStopped(stopped) if stopped == shard:
                    return done(True)
                case _HandOffTimeout():
                    return done(False)
                case _RegionTerminatedForWorker(region) if region == from_region.path:
                    return done(True)
                case _:
                    return Behaviors.same

        def acked(region: str) -> Behavior[Any]:
            remaining.discard(region)
            if remaining:
                return Behaviors.same
            from_region.tell(HandOff(shard, ctx.self))
            return Behaviors.receive(stopping_shard)

        def waiting_for_acks(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            match msg:
                case BeginHandOffAck(acked_shard, from_) if acked_shard == shard:
                    return acked(from_.path)
                case _RegionTerminatedForWorker(region) if region in remaining:
                    return acked(region)
                case _HandOffTimeout():
                    return done(False)
                case _:
                    return Behaviors.same

        return Behaviors.receive(waiting_for_acks)

    return Behaviors.setup(setup)


# --- the coordinator (ShardCoordinator), run as the cluster singleton's instance ------------------------------


class _Coordinator:
    """ShardCoordinator's vars and `active` receive; starts by rebuilding its state from the regions."""

    def __init__(self, ctx: ActorContext[Any], type_name: str, settings: ClusterShardingSettings) -> None:
        self.ctx = ctx
        self.type_name = type_name
        self.settings = settings
        self.log = ctx.log
        self.loop = asyncio.get_running_loop()
        self.cluster = Cluster(ctx.system)
        self.strategy = LeastShardAllocationStrategy(settings.rebalance_absolute_limit, settings.rebalance_relative_limit)
        self.state = CoordinatorState()
        self.refs: dict[str, ActorRef[Any]] = {}
        self.alive_regions: set[str] = set()
        self.graceful_shutdown_in_progress: set[str] = set()
        self.region_termination_in_progress: set[str] = set()
        self.rebalance_in_progress: dict[str, list[ActorRef[Any]]] = {}
        self.rebalance_workers: dict[str, ActorRef[Any]] = {}
        self.registered_addresses: set[UniqueAddress] = set()
        self.waiting_deadline = self.loop.time() + settings.retry_interval  # Pekko's waiting-for-state-timeout
        self.waiting_for_local_region_to_terminate = False
        self.stop_now = False
        self.cluster.subscriptions.tell(ClusterSubscribe(ctx.self, ClusterDomainEvent))
        ctx.timers.start_timer_with_fixed_delay("rebalance", _RebalanceTick(), settings.rebalance_interval)

    # --- membership views ---------------------------------------------------------------------------------------

    def members_of_interest(self) -> list[Member]:
        return [m for m in self.cluster.state.members if m.status in _OF_INTEREST]

    def member_for(self, ref_or_path: ActorRef[Any] | str) -> Member | None:
        return next((m for m in self.cluster.state.members if _is_at(ref_or_path, m.address)), None)

    def is_member(self, region: ActorRef[Any]) -> bool:
        m = self.member_for(region)
        return m is not None and m.status in _OF_INTEREST

    def has_all_regions_registered(self) -> bool:
        """The rebuild gate: every reachable member has registered (or the wait has run out), and no unreachable
        member is unaccounted for, so an unknown shard cannot still be hosted somewhere."""
        unreachable = {m.unique_address for m in self.cluster.state.unreachable}
        for m in self.members_of_interest():
            if m.unique_address in self.registered_addresses:
                continue
            if m.unique_address in unreachable:
                return False  # it may host shards we do not know about: wait for it to answer or be removed
            if self.loop.time() < self.waiting_deadline:
                return False
        return True

    def members_by_region(self, regions: Iterable[str]) -> dict[str, Member]:
        out: dict[str, Member] = {}
        for r in regions:
            m = self.member_for(r)
            if m is not None:
                out[r] = m
        return out

    # --- registration and shard homes -----------------------------------------------------------------------------

    def register(self, region: ActorRef[Any], hosted: tuple[str, ...]) -> None:
        path = region.path
        if not self.is_member(region):
            self.log.info("%s: ShardRegion [%s] was not registered since the coordinator does not know about its node", self.type_name, path)
            return
        self.refs[path] = region
        self.alive_regions.add(path)
        self.graceful_shutdown_in_progress.discard(path)
        member = self.member_for(region)
        if member is not None:
            self.registered_addresses.add(member.unique_address)
        if path not in self.state.regions:
            self.state = self.state.updated(ShardRegionRegistered(path))
            if is_local_actor(region):  # Pekko's RegionStopped: the local region's end is what Terminate waits for
                self.ctx.watch_with(region, _RegionStopped(path))
        for shard in hosted:  # the region is the ground truth of what it hosts (Pekko reads this from ddata)
            owner = self.state.shards.get(shard)
            if owner is None:
                self.state = self.state.updated(ShardHomeAllocated(shard, path))
            elif owner != path:
                # a registration that arrived after the gate opened: keep the allocation, stop the late copy
                self.log.error("%s: shard [%s] reported by [%s] but allocated to [%s]; the late copy is handed off", self.type_name, shard, path, owner)
                region.tell(HandOff(shard, self.ctx.system.ignore_ref))
        region.tell(RegisterAck(self.ctx.self))
        self.inform_about_current_shards(region)

    def inform_about_current_shards(self, ref: ActorRef[Any]) -> None:
        homes = tuple(
            (self.refs[r], tuple(s for s in shards if s not in self.rebalance_in_progress))
            for r, shards in self.state.regions.items()
            if shards and r in self.refs
        )
        if homes:
            ref.tell(ShardHomes(homes))

    def get_shard_home(self, shard: str, reply_to: ActorRef[Any]) -> None:
        if shard in self.rebalance_in_progress:
            self.rebalance_in_progress[shard].append(reply_to)  # deferred until the hand-off completes
            return
        if not self.has_all_regions_registered():
            self.log.debug("%s: GetShardHome [%s] ignored, not all regions have registered yet", self.type_name, shard)
            return
        known = self.state.shards.get(shard)
        if known is not None:
            if known not in self.region_termination_in_progress:
                reply_to.tell(ShardHome(shard, self.refs[known]))
            return
        active = [r for r in self.state.regions if r not in self.graceful_shutdown_in_progress and r not in self.region_termination_in_progress]
        if not active:
            return
        region = self.strategy.allocate_shard(shard, {r: self.state.regions[r] for r in active}, self.members_by_region(active))
        if region is not None:
            self.continue_get_shard_home(shard, region, reply_to)

    def continue_get_shard_home(self, shard: str, region: str, reply_to: ActorRef[Any]) -> None:
        known = self.state.shards.get(shard)
        if known is not None:
            reply_to.tell(ShardHome(shard, self.refs[known]))
            return
        if region in self.state.regions and region not in self.graceful_shutdown_in_progress and region not in self.region_termination_in_progress:
            self.state = self.state.updated(ShardHomeAllocated(shard, region))
            self.log.info("%s: Shard [%s] allocated at [%s]", self.type_name, shard, region)
            self.send_host_shard(shard, region)
            reply_to.tell(ShardHome(shard, self.refs[region]))
        else:
            self.log.debug("%s: allocated region [%s] for shard [%s] is no longer registered", self.type_name, region, shard)

    def send_host_shard(self, shard: str, region: str) -> None:
        self.refs[region].tell(HostShard(shard, self.ctx.self))
        self.ctx.timers.start_single_timer(("host", shard), _ResendShardHost(shard, region), self.settings.shard_start_timeout)

    def region_terminated(self, path: str) -> None:
        for worker in self.rebalance_workers.values():
            worker.tell(_RegionTerminatedForWorker(path))
        if path in self.state.regions:
            self.log.info("%s: ShardRegion terminated: [%s]", self.type_name, path)
            self.state = self.state.updated(ShardRegionTerminated(path))
        self.graceful_shutdown_in_progress.discard(path)
        self.region_termination_in_progress.discard(path)
        self.alive_regions.discard(path)
        self.refs.pop(path, None)
        if self.waiting_for_local_region_to_terminate and _is_at(path, self.cluster.self_address):
            self.stop_now = True

    def terminate(self) -> None:
        """Pekko's Terminate: stop at once, unless the local region still has shards to hand off."""
        local = [r for r in self.alive_regions if _is_at(r, self.cluster.self_address)]
        if local:
            self.waiting_for_local_region_to_terminate = True
        else:
            self.stop_now = True

    def on_member_removed(self, m: Member) -> None:
        self.registered_addresses.discard(m.unique_address)
        for path in [r for r in list(self.state.regions) + list(self.refs) if _is_at(r, m.address)]:
            self.region_terminated(path)

    # --- the behavior ---------------------------------------------------------------------------------------------

    def behavior(self) -> Behavior[Any]:
        def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            match msg:
                case Register(region, hosted):
                    self.register(region, hosted)
                case GetShardHome(shard, reply_to):
                    self.get_shard_home(shard, reply_to)
                case ShardStarted(shard):
                    self.ctx.timers.cancel(("host", shard))
                case _ResendShardHost(shard, region):
                    if self.state.shards.get(shard) == region and region in self.refs:
                        self.send_host_shard(shard, region)
                case MemberRemoved(member=m):
                    self.touch_gate()
                    self.on_member_removed(m)
                case MemberUp() | MemberLeft() | MemberExited():
                    self.touch_gate()
                case ClusterDomainEvent():
                    pass
                case _RebalanceTick() | _RebalanceDone() | GracefulShutdownReq():
                    self.on_rebalance_message(msg)
                case _RegionStopped(path):
                    self.region_terminated(path)
                case _Terminate():
                    self.terminate()
                case _:
                    pass
            return Behaviors.stopped() if self.stop_now else Behaviors.same

        def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
            if sig is PostStop:
                for path in self.alive_regions:  # what their DeathWatch of the coordinator would tell them
                    if path in self.refs:
                        self.refs[path].tell(CoordinatorStopped(self.ctx.self))
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    def touch_gate(self) -> None:
        """The rebuild gate waits retry_interval of membership quiescence, not from the coordinator's start."""
        self.waiting_deadline = self.loop.time() + self.settings.retry_interval

    # --- rebalance and hand-off ---------------------------------------------------------------------------------------

    def on_rebalance_message(self, msg: Any) -> None:
        match msg:
            case _RebalanceTick():
                if self.state.regions:
                    shards = self.strategy.rebalance(
                        dict(self.state.regions), self.members_by_region(self.state.regions), frozenset(self.rebalance_in_progress), self.cluster.state
                    )
                    self.continue_rebalance(shards)
            case _RebalanceDone(shard, ok):
                self.rebalance_done(shard, ok)
            case GracefulShutdownReq(region):
                path = region.path
                if path not in self.graceful_shutdown_in_progress and path in self.state.regions:
                    self.log.info("%s: Graceful shutdown of region [%s] with shards [%s] started", self.type_name, path, self.state.regions[path])
                    self.graceful_shutdown_in_progress.add(path)
                    self.shutdown_shards(path, self.state.regions[path])
            case _:
                pass

    def continue_rebalance(self, shards: frozenset[str]) -> None:
        if shards:
            self.log.info("%s: Starting rebalance for shards [%s]. In progress: [%s]", self.type_name, sorted(shards), sorted(self.rebalance_in_progress))
        for shard in shards:
            if shard not in self.rebalance_in_progress and shard in self.state.shards:
                self.start_shard_rebalance(shard, self.state.shards[shard], is_rebalance=True)

    def shutdown_shards(self, region: str, shards: tuple[str, ...]) -> None:
        for shard in shards:
            if shard not in self.rebalance_in_progress:
                self.start_shard_rebalance(shard, region, is_rebalance=False)

    def start_shard_rebalance(self, shard: str, from_region: str, is_rebalance: bool) -> None:
        if from_region not in self.refs:
            return
        self.rebalance_in_progress[shard] = []
        regions = {r: self.refs[r] for r in self.alive_regions if r in self.refs}
        worker = self.ctx.spawn_anonymous(
            _rebalance_worker(self.type_name, shard, self.refs[from_region], regions, self.settings.handoff_timeout, is_rebalance)
        )
        self.rebalance_workers[shard] = worker

    def rebalance_done(self, shard: str, ok: bool) -> None:
        self.rebalance_workers.pop(shard, None)
        if ok:
            if shard in self.state.shards:
                self.state = self.state.updated(ShardHomeDeallocated(shard))
            self.clear_rebalance_in_progress(shard)
            self.get_shard_home(shard, self.ctx.system.ignore_ref)  # reallocate at once, as Pekko
        else:
            self.log.warning("%s: Shard [%s] deallocation did not complete within [%s]s", self.type_name, shard, self.settings.handoff_timeout)
            region = self.state.shards.get(shard)
            if region is not None:
                self.graceful_shutdown_in_progress.discard(region)
            self.clear_rebalance_in_progress(shard)

    def clear_rebalance_in_progress(self, shard: str) -> None:
        for asker in self.rebalance_in_progress.pop(shard, []):
            self.get_shard_home(shard, asker)


def _coordinator(type_name: str, settings: ClusterShardingSettings) -> Behavior[Any]:
    return Behaviors.setup(lambda ctx: _Coordinator(ctx, type_name, settings).behavior())


# --- the region (ShardRegion) -----------------------------------------------------------------------------------


class _Region:
    def __init__(self, ctx: ActorContext[Any], entity: Entity[Any], settings: ClusterShardingSettings, progress: _ExitingProgress) -> None:
        self.ctx = ctx
        self.entity = entity
        self.settings = settings
        self.progress = progress
        progress.future = asyncio.get_running_loop().create_future()
        self.type_name = entity.type_key.name
        self.log = ctx.log
        self.cluster = Cluster(ctx.system)
        self.members_by_age: list[Member] = []
        self.coordinator: ActorRef[Any] | None = None
        self.shards: dict[str, ActorRef[Any]] = {}
        self.region_by_shard: dict[str, ActorRef[Any]] = {}
        self.shard_buffers: dict[str, deque[ShardingEnvelope[Any]]] = {}
        self.buffered = 0
        self.next_registration_delay = max(0.1, settings.retry_interval / 8)
        self.graceful_shutdown_in_progress = False
        self.cluster.subscriptions.tell(ClusterSubscribe(ctx.self, MemberEvent))
        ctx.timers.start_timer_with_fixed_delay("retry", _Retry(), settings.retry_interval)

    # --- coordinator location -----------------------------------------------------------------------------------------

    def coordinator_path(self, member: Member) -> str:
        return f"{member.address.root}/system/{_manager_name(f'{self.type_name}Coordinator')}/singleton"

    def change_members(self, new_members: list[Member]) -> None:
        before = self.members_by_age[0].unique_address if self.members_by_age else None
        self.members_by_age = new_members
        after = new_members[0].unique_address if new_members else None
        if before != after:
            self.log.info("%s: Coordinator moved from [%s] to [%s]", self.type_name, before, after)
            self.coordinator = None
            self.start_registration()

    def start_registration(self) -> None:
        self.next_registration_delay = max(0.1, self.settings.retry_interval / 8)
        self.register()
        self.schedule_next_registration()

    def schedule_next_registration(self) -> None:
        if self.next_registration_delay < self.settings.retry_interval:
            self.ctx.timers.start_single_timer("register-retry", _RegisterRetry(), self.next_registration_delay)
            self.next_registration_delay *= 2

    def coordinator_selection(self) -> list[Member]:
        """Pekko's coordinatorSelection: the members by age up to and including the first Up one, since the
        coordinator may still be on an older member that is leaving."""
        selected: list[Member] = []
        for m in self.members_by_age:
            selected.append(m)
            if m.status == "up":
                break
        return selected

    def register(self) -> None:
        for m in self.coordinator_selection():
            try:
                coordinator = self.ctx.system.resolve(self.coordinator_path(m))
            except LookupError:  # the local singleton instance is not up yet: the next retry asks again
                continue
            coordinator.tell(Register(self.ctx.self, tuple(self.shards)))

    # --- delivery -----------------------------------------------------------------------------------------------------

    def shard_for(self, shard: str) -> ActorRef[Any]:
        ref = self.shards.get(shard)
        if ref is None:
            ref = self.ctx.spawn(shard_behavior(self.entity, shard, self.ctx.self), shard)
            self.ctx.watch_with(ref, _ShardTerminated(shard))
            self.shards[shard] = ref
        return ref

    def deliver(self, envelope: ShardingEnvelope[Any]) -> None:
        shard = shard_id_of(envelope.entity_id, self.settings.number_of_shards)
        home = self.region_by_shard.get(shard)
        if home is None:
            self.buffer(shard, envelope)
            if self.coordinator is not None and len(self.shard_buffers[shard]) == 1:
                self.coordinator.tell(GetShardHome(shard, self.ctx.self))
        elif home.path == self.ctx.self.path:
            self.shard_for(shard).tell(envelope)
        else:
            home.tell(envelope)

    def buffer(self, shard: str, envelope: ShardingEnvelope[Any]) -> None:
        if self.buffered >= self.settings.buffer_size:
            self.log.warning("%s: buffer is full, dropping message for shard [%s]", self.type_name, shard)
            self.ctx.system.dead_letter(envelope, self.ctx.self)
            return
        self.shard_buffers.setdefault(shard, deque()).append(envelope)
        self.buffered += 1

    def deliver_buffered(self, shard: str, receiver: ActorRef[Any]) -> None:
        for envelope in self.shard_buffers.pop(shard, ()):
            self.buffered -= 1
            receiver.tell(envelope)

    def receive_shard_home(self, shard: str, region: ActorRef[Any]) -> None:
        current = self.region_by_shard.get(shard)
        if current is not None and current.path == self.ctx.self.path and region.path != self.ctx.self.path:
            # Pekko throws here because distributed data makes it impossible; without it a registration that
            # arrives after the coordinator's gate opened can lose a shard it hosts: hand the local copy off
            self.log.error("%s: shard [%s] moved from self to [%s]; stopping the local copy", self.type_name, shard, region.path)
            local = self.shards.get(shard)
            if local is not None:
                local.tell(HandOff(shard, self.ctx.system.ignore_ref))
            for envelope in self.shard_buffers.pop(shard, ()):
                self.buffered -= 1
                self.ctx.system.dead_letter(envelope, self.ctx.self)
        self.region_by_shard[shard] = region
        if region.path == self.ctx.self.path:
            self.deliver_buffered(shard, self.shard_for(shard))
        else:
            self.deliver_buffered(shard, region)

    def request_shard_buffer_homes(self) -> None:
        if self.coordinator is not None:
            for shard in self.shard_buffers:
                self.coordinator.tell(GetShardHome(shard, self.ctx.self))

    # --- the behavior ---------------------------------------------------------------------------------------------

    def behavior(self) -> Behavior[Any]:
        def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            if isinstance(msg, ShardingEnvelope):
                envelope = cast(ShardingEnvelope[Any], msg)
                if not envelope.entity_id:
                    self.ctx.system.dead_letter(envelope.message, self.ctx.self)
                else:
                    self.deliver(envelope)
                return Behaviors.stopped() if self.try_complete_graceful_shutdown() else Behaviors.same
            match msg:
                case GetShardRegionState(reply_to):
                    if not self.shards:
                        reply_to.tell(CurrentShardRegionState(frozenset()))
                    else:
                        self.ctx.spawn_anonymous(_aggregate_shard_states(list(self.shards.values()), reply_to, self.settings.shard_region_query_timeout))
                case MemberUp(member=m) | MemberLeft(member=m) | MemberExited(member=m):
                    self.change_members(_by_age([x for x in self.members_by_age if x.unique_address != m.unique_address] + [m]))
                case MemberRemoved(member=m) if m.unique_address == self.cluster.self_unique_address:
                    return Behaviors.stopped()
                case MemberDowned(member=m) if m.unique_address == self.cluster.self_unique_address:
                    return Behaviors.stopped()
                case MemberRemoved(member=m):
                    for shard in [s for s, r in self.region_by_shard.items() if r.path != self.ctx.self.path and _is_at(r, m.address)]:
                        del self.region_by_shard[shard]
                    self.change_members([x for x in self.members_by_age if x.unique_address != m.unique_address])
                case MemberEvent():
                    pass
                case RegisterAck(coordinator):
                    self.coordinator = coordinator
                    self.ctx.timers.cancel("register-retry")
                    self.request_shard_buffer_homes()
                case CoordinatorStopped(coordinator):
                    if self.coordinator is not None and self.coordinator.path == coordinator.path:
                        self.coordinator = None
                        self.start_registration()
                case HostShard(shard, reply_to):
                    if self.graceful_shutdown_in_progress:
                        self.send_graceful_shutdown_to_coordinator()
                    else:
                        self.region_by_shard[shard] = self.ctx.self
                        self.shard_for(shard)
                        reply_to.tell(ShardStarted(shard))
                        self.deliver_buffered(shard, self.shards[shard])
                case ShardHome(shard, region):
                    self.receive_shard_home(shard, region)
                case ShardHomes(homes):
                    for region, shards in homes:
                        for shard in shards:
                            self.receive_shard_home(shard, region)
                case BeginHandOff(shard, reply_to):
                    self.region_by_shard.pop(shard, None)
                    reply_to.tell(BeginHandOffAck(shard, self.ctx.self))
                case HandOff(shard, reply_to):
                    dropped = self.shard_buffers.pop(shard, None)
                    if dropped:  # Pekko: avoid reordering buffered messages across a hand-off
                        self.buffered -= len(dropped)
                        for envelope in dropped:
                            self.ctx.system.dead_letter(envelope, self.ctx.self)
                    local = self.shards.get(shard)
                    if local is not None:
                        local.tell(msg)
                    else:
                        reply_to.tell(ShardStopped(shard))
                case _ShardTerminated(shard):
                    self.shards.pop(shard, None)
                    if self.region_by_shard.get(shard) is not None and self.region_by_shard[shard].path == self.ctx.self.path:
                        del self.region_by_shard[shard]
                case _Retry():
                    if self.coordinator is None:
                        self.register()
                    else:
                        self.request_shard_buffer_homes()
                    if self.graceful_shutdown_in_progress:
                        self.send_graceful_shutdown_to_coordinator()
                case _RegisterRetry():
                    if self.coordinator is None:
                        self.register()
                        self.schedule_next_registration()
                case _SelfExiting():
                    self.graceful_shutdown_in_progress = True
                    self.send_graceful_shutdown_to_coordinator()
                case _:
                    pass
            return Behaviors.stopped() if self.try_complete_graceful_shutdown() else Behaviors.same

        def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
            if sig is PostStop:
                self.exiting_done()
            return Behaviors.same

        return Behaviors.receive(on_message).receive_signal(on_signal)

    def send_graceful_shutdown_to_coordinator(self) -> None:
        if self.graceful_shutdown_in_progress and self.coordinator is not None:
            self.coordinator.tell(GracefulShutdownReq(self.ctx.self))

    def try_complete_graceful_shutdown(self) -> bool:
        """Pekko's tryCompleteGracefulShutdownIfInProgress: every shard handed off and nothing buffered."""
        if self.graceful_shutdown_in_progress and not self.shards and not self.shard_buffers:
            self.exiting_done()
            return True
        return False

    def exiting_done(self) -> None:
        if self.progress.future is not None and not self.progress.future.done():
            self.progress.future.set_result(None)


def _region(entity: Entity[Any], settings: ClusterShardingSettings, progress: _ExitingProgress) -> Behavior[Any]:
    behavior: Behavior[Any] = Behaviors.setup(lambda ctx: _Region(ctx, entity, settings, progress).behavior())
    return Behaviors.supervise(behavior).on_failure(SupervisorStrategy.restart())  # as under Pekko's system guardian


# --- the extension -------------------------------------------------------------------------------------------------


class ClusterSharding:
    """``ClusterSharding(system).init(Entity(key, create))`` returns the region ref; ``entity_ref_for(key, id)``
    addresses an entity wherever it lives, as Pekko Typed."""

    system: ActorSystem[Any]
    _regions: dict[str, ActorRef[Any]]
    _parent: ActorCell[Any] | None

    def __new__(cls, system: ActorSystem[Any]) -> ClusterSharding:
        if system.cluster is None:
            raise RuntimeError(f"system {system.name!r} has no cluster: pass cluster=ClusterSettings(...) to ActorSystem")
        if system.cluster_sharding is None:
            self = object.__new__(cls)
            self.system = system
            self._regions = {}
            self._parent = None
            system.cluster_sharding = self
        return system.cluster_sharding

    def init[M](self, entity: Entity[M]) -> ActorRef[ShardingEnvelope[M]]:
        name = entity.type_key.name
        existing = self._regions.get(name)
        if existing is not None:
            return existing
        settings = entity.effective_settings
        if self._parent is None:
            self._parent = self.system._system_root.spawn_child(Behaviors.empty, "sharding", None)  # pyright: ignore[reportPrivateUsage]
        progress = _ExitingProgress()
        region = self._parent.spawn_child(_region(entity, settings, progress), name, None)
        Cluster(self.system).add_exiting_task(progress.task(Cluster(self.system), region.ref))
        ClusterSingleton(self.system).init(SingletonActor(_coordinator(name, settings), f"{name}Coordinator").with_stop_message(_Terminate()))
        self._regions[name] = region.ref
        return region.ref

    def entity_ref_for[M](self, key: EntityTypeKey[M], entity_id: str) -> EntityRef[M]:
        region = self._regions.get(key.name)
        if region is None:
            raise LookupError(f"ClusterSharding.init was not called for {key.name!r} on this node")
        if not entity_id:
            raise ValueError("entity id must not be empty")
        return EntityRef(region, entity_id)
