"""Apache Pekko's ClusterSharding across nodes: a ShardCoordinator singleton allocating shards to
regions with the LeastShardAllocationStrategy, a ShardRegion on every node, hand-off for
rebalancing and for a leaving node. The coordinator keeps its state in Distributed Data, as
Pekko's DDataShardCoordinator."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, ClassVar

from ...behavior import Behavior, Behaviors, StashBuffer
from ...context import ActorContext
from ...ref import ActorRef
from ...remote import Address, UniqueAddress
from ...remote.serialization import Serialization
from ...cell import is_local_actor
from ...signals import PostStop, PreRestart, Signal
from .shard import (
    ClusterShardingSettings,
    HandOff,
    ShardStopped,
)
from ..ddata import SelfUniqueAddress
from ..ddata.distributed_data import ReplicatorMessageAdapter
from ..ddata.lww_register import LWWRegister, LWWRegisterKey
from ..ddata.replicator import Replicator
from ..core import (
    Cluster,
    ClusterDomainEvent,
    CurrentClusterState,
    MemberRemoved,
)
from ..core import Subscribe as ClusterSubscribe
from ..gossip import Member

_LEAVING_STATUSES = frozenset({"leaving", "exiting", "down"})
_JOINING_STATUSES = frozenset({"joining", "weakly_up"})


# --- LeastShardAllocationStrategy ----------------------------------------------------------------


@dataclass(frozen=True)
class RegionEntry:
    region: str
    member: Member
    shard_ids: tuple[str, ...]

    @property
    def suitability(self) -> tuple[bool, int, int, tuple[str, int]]:
        """Pekko's ShardSuitabilityOrdering: regions not on their way out first, then fewest
        shards; ties go to the oldest member so the choice is deterministic."""
        m = self.member
        return (
            m.status in _LEAVING_STATUSES,
            len(self.shard_ids),
            m.up_number,
            (m.unique_address.host, m.unique_address.port),
        )


class LeastShardAllocationStrategy:
    """Pekko's LeastShardAllocationStrategy: allocate to the least loaded region; rebalance in two
    phases, capped by the absolute and relative limits. ``absolute_limit`` 0 means no absolute
    cap."""

    def __init__(self, absolute_limit: int, relative_limit: float) -> None:
        self.absolute_limit = absolute_limit
        self.relative_limit = relative_limit

    def _entries(
        self, allocations: dict[str, tuple[str, ...]], members_by_region: dict[str, Member]
    ) -> list[RegionEntry]:
        entries = [
            RegionEntry(r, members_by_region[r], ids)
            for r, ids in allocations.items()
            if r in members_by_region
        ]
        return sorted(entries, key=lambda e: e.suitability)

    def allocate_shard(
        self,
        shard: str,
        allocations: dict[str, tuple[str, ...]],
        members_by_region: dict[str, Member],
    ) -> str | None:
        entries = self._entries(allocations, members_by_region)
        return entries[0].region if entries else None

    def _limit(self, number_of_shards: int) -> int:
        cap = self.absolute_limit if self.absolute_limit > 0 else number_of_shards
        return max(1, min(int(self.relative_limit * number_of_shards), cap))

    @staticmethod
    def is_a_good_time_to_rebalance(
        entries: Iterable[RegionEntry], cluster_state: CurrentClusterState
    ) -> bool:
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
        optimal_per_region = number_of_shards // number_of_regions + (
            0 if number_of_shards % number_of_regions == 0 else 1
        )
        # phase one: shards above the optimal count per region
        selected: list[str] = []
        for e in entries:
            if len(e.shard_ids) > optimal_per_region:
                selected.extend(e.shard_ids[: len(e.shard_ids) - optimal_per_region])
        if selected:
            return frozenset(selected[: self._limit(number_of_shards)])
        # phase two: fill regions below the optimal count
        count_below_optimal = sum(
            max(0, (optimal_per_region - 1) - len(e.shard_ids)) for e in entries
        )
        if count_below_optimal == 0:
            return frozenset()
        selected = [e.shard_ids[0] for e in entries if len(e.shard_ids) >= optimal_per_region]
        return frozenset(selected[: min(count_below_optimal, self._limit(number_of_shards))])


# --- the coordinator's state (ShardCoordinator.State and its DomainEvents) -----------------------


@dataclass(frozen=True)
class ShardRegionRegistered:
    region: str
    node: UniqueAddress


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


type DomainEvent = (
    ShardRegionRegistered | ShardRegionTerminated | ShardHomeAllocated | ShardHomeDeallocated
)


@dataclass(frozen=True)
class CoordinatorState:
    """ShardCoordinator.State (ShardCoordinator.scala ~455-540): shard -> region path, and region
    path -> its shards. Regions are keyed by path so a local and a remote ref to the same region
    compare equal. It is the value of the coordinator's LWWRegister, so it travels on the wire.
    Actor paths carry no uid here, so ``region_nodes`` records each region's node."""

    shards: dict[str, str]
    regions: dict[str, tuple[str, ...]]
    region_nodes: dict[str, UniqueAddress]
    EMPTY: ClassVar[CoordinatorState]

    def updated(self, event: DomainEvent) -> CoordinatorState:
        match event:
            case ShardRegionRegistered(region, node):
                if region in self.regions:
                    raise ValueError(f"Region {region} already registered: {self}")
                return CoordinatorState(
                    dict(self.shards),
                    {**self.regions, region: ()},
                    {**self.region_nodes, region: node},
                )
            case ShardRegionTerminated(region):
                if region not in self.regions:
                    raise ValueError(f"Terminated region {region} not registered: {self}")
                gone = set(self.regions[region])
                return CoordinatorState(
                    {s: r for s, r in self.shards.items() if s not in gone},
                    {r: ids for r, ids in self.regions.items() if r != region},
                    {r: n for r, n in self.region_nodes.items() if r != region},
                )
            case ShardHomeAllocated(shard, region):
                if region not in self.regions:
                    raise ValueError(f"Region {region} not registered: {self}")
                if shard in self.shards:
                    raise ValueError(f"Shard [{shard}] already allocated: {self}")
                return CoordinatorState(
                    {**self.shards, shard: region},
                    {**self.regions, region: (*self.regions[region], shard)},
                    self.region_nodes,
                )
            case ShardHomeDeallocated(shard):
                if shard not in self.shards:
                    raise ValueError(f"Shard [{shard}] not allocated: {self}")
                region = self.shards[shard]
                if region not in self.regions:
                    raise ValueError(f"Region {region} for shard [{shard}] not registered: {self}")
                return CoordinatorState(
                    {s: r for s, r in self.shards.items() if s != shard},
                    {**self.regions, region: tuple(s for s in self.regions[region] if s != shard)},
                    self.region_nodes,
                )

    @property
    def all_shards(self) -> frozenset[str]:
        return frozenset(self.shards)


CoordinatorState.EMPTY = CoordinatorState({}, {}, {})


# --- wire messages (ShardCoordinator.Internal), with explicit refs where Pekko uses sender() -----


@dataclass(frozen=True)
class Register:
    region: ActorRef[Any]


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


@dataclass(frozen=True)
class RegionStopped:
    """A region that stopped tells its coordinator (ShardRegion.postStop), locally through the
    coordinator's watch and remotely by this message: Pekko's RegionStopped, which makes a
    handed-off region gone at once."""

    region: str


for _cls in (
    Register,
    RegisterAck,
    GetShardHome,
    ShardHome,
    ShardHomes,
    HostShard,
    ShardStarted,
    BeginHandOff,
    BeginHandOffAck,
    GracefulShutdownReq,
    CoordinatorStopped,
    RegionStopped,
):
    # the Receptionist already owns the manifest "Register"
    Serialization.register(_cls, f"Sharding.{_cls.__name__}")
# the Receptionist owns plain names; the state is remotable under its Sharding name
Serialization.register(CoordinatorState, "Sharding.CoordinatorState")


# --- internal messages ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ResendShardHost:
    shard: str
    region: str


@dataclass(frozen=True)
class _StateInitialized: ...


@dataclass(frozen=True)
class _GetResult:
    response: Replicator.GetResponse[LWWRegister[CoordinatorState]]


@dataclass(frozen=True)
class _UpdateResult:
    """An Update's response with the event it wrote: Pekko's ``Some(evt)`` request."""

    evt: DomainEvent
    response: Replicator.UpdateResponse[LWWRegister[CoordinatorState]]


@dataclass(frozen=True)
class _RebalanceTick: ...


@dataclass(frozen=True)
class _RebalanceDone:
    shard: str
    ok: bool


@dataclass(frozen=True)
class _RegionTerminatedForWorker:
    region: str


@dataclass(frozen=True)
class _Terminate:
    """The coordinator singleton's stop message: stop once the local region has handed off (Pekko's
    Terminate)."""


@dataclass(frozen=True)
class _HandOffTimeout: ...


@dataclass(frozen=True)
class _DelayedShardRegionTerminated:
    """A removed member's region is marked terminated only after the down-removal margin
    (ShardCoordinator.scala 1096), so its shards are not started elsewhere while the downed side
    may still run them."""

    region: str


# Pekko's memberStatusOfInterest for coordinator placement
_OF_INTEREST = frozenset({"up", "leaving", "exiting"})


def _is_at(ref_or_path: ActorRef[Any] | str, address: Address) -> bool:
    path = ref_or_path if isinstance(ref_or_path, str) else ref_or_path.path
    return path.startswith(address.root + "/")


def by_age(members: Iterable[Member]) -> list[Member]:
    ordered: list[Member] = []
    for m in members:
        i = 0
        while i < len(ordered) and ordered[i].is_older_than(m):
            i += 1
        ordered.insert(i, m)
    return ordered


# --- the hand-off worker (ShardCoordinator.RebalanceWorker) --------------------------------------


def _rebalance_worker(
    type_name: str,
    shard: str,
    from_region: ActorRef[Any],
    regions: dict[str, ActorRef[Any]],
    handoff_timeout: float,
    is_rebalance: bool,
) -> Behavior[Any]:
    """BeginHandOff to every region, wait for all acks (a terminated region counts), HandOff to the
    host, wait for ShardStopped; report _RebalanceDone to the coordinator either way."""

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        remaining = set(regions)
        for region in regions.values():
            region.tell(BeginHandOff(shard, ctx.self))
        ctx.log.info(
            "%s: %s shard [%s] from region [%s]: asking [%d] region(s) to hand off",
            type_name,
            "Rebalance" if is_rebalance else "Shutting down",
            shard,
            from_region.path,
            len(regions),
        )
        ctx.timers.start_single_timer("hand-off-timeout", _HandOffTimeout(), handoff_timeout)
        parent = ctx.system.resolve(ctx.self.path.rsplit("/", 1)[0])

        def done(ok: bool) -> Behavior[Any]:
            parent.tell(_RebalanceDone(shard, ok))
            return Behaviors.stopped()

        @Behaviors.receive
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
            return stopping_shard

        @Behaviors.receive
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

        return waiting_for_acks

    return setup


# --- the coordinator (ShardCoordinator and DDataShardCoordinator), as the cluster singleton ------

type _StateRegister = LWWRegister[CoordinatorState]

_STASH_CAPACITY = 100_000
"""Pekko's classic Stash has no bound; a typed StashBuffer needs one."""


class _Coordinator:
    """ShardCoordinator's vars and receives (ShardCoordinator.scala 600-1385), with
    DDataShardCoordinator's (1535-1976): the state is the value of an LWWRegister in sharding's
    Replicator, read by majority when the coordinator starts and written by majority before each
    change takes effect. Pekko's ``context.become`` is ``self.receive``; ``waitingForUpdate`` is
    on top of it while ``self.updating`` holds an update."""

    def __init__(
        self,
        ctx: ActorContext[Any],
        type_name: str,
        settings: ClusterShardingSettings,
        replicator: ActorRef[Replicator.Command],
        stash: StashBuffer[Any],
    ) -> None:
        self.ctx = ctx
        self.type_name = type_name
        self.settings = settings
        self.log = ctx.log
        self.cluster = Cluster(ctx.system)
        self.strategy = LeastShardAllocationStrategy(
            settings.rebalance_absolute_limit, settings.rebalance_relative_limit
        )
        self.state = CoordinatorState.EMPTY
        self.refs: dict[str, ActorRef[Any]] = {}
        self.alive_regions: set[str] = set()
        self.graceful_shutdown_in_progress: set[str] = set()
        self.region_termination_in_progress: set[str] = set()
        self.rebalance_in_progress: dict[str, list[ActorRef[Any]]] = {}
        self.rebalance_workers: dict[str, ActorRef[Any]] = {}
        self.waiting_for_local_region_to_terminate = False
        self.stop_now = False
        # DDataShardCoordinator's
        self.replicator = ReplicatorMessageAdapter[Any, _StateRegister](
            ctx, replicator, settings.updating_state_timeout + 1
        )
        self.key = LWWRegisterKey[CoordinatorState](f"{type_name}CoordinatorState")
        self.self_unique_address = SelfUniqueAddress(self.cluster.self_unique_address)
        self.read_consistency = settings.state_read_consistency()
        self.write_consistency = settings.state_write_consistency()
        self.stash = stash
        self.terminating = False
        self.get_shard_home_requests: deque[GetShardHome] = deque()
        self.initial_state_retries = 0
        self.update_state_retries = 0
        self.receive: Callable[[Any], Behavior[Any]] = self.waiting_for_initial_state
        self.updating: tuple[DomainEvent, Callable[[], None]] | None = None
        self.queued_updates: deque[tuple[DomainEvent, Callable[[], None]]] = deque()
        self.cluster.subscriptions.tell(ClusterSubscribe(ctx.self, ClusterDomainEvent))
        ctx.timers.start_timer_with_fixed_delay(
            "rebalance", _RebalanceTick(), settings.rebalance_interval
        )
        # get state from ddata replicator, repeat until GetSuccess
        self.get_coordinator_state()

    # --- membership views ------------------------------------------------------------------------

    def member_for(self, ref_or_path: ActorRef[Any] | str) -> Member | None:
        return next(
            (m for m in self.cluster.state.members if _is_at(ref_or_path, m.address)), None
        )

    def members_by_region(self, regions: Iterable[str]) -> dict[str, Member]:
        out: dict[str, Member] = {}
        for r in regions:
            m = self.member_for(r)
            if m is not None:
                out[r] = m
        return out

    # --- the behavior ----------------------------------------------------------------------------

    def behavior(self) -> Behavior[Any]:
        @Behaviors.receive
        def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            updating = self.updating
            if updating is not None:
                next_behavior = self.waiting_for_update(msg, *updating)
            else:
                next_behavior = self.receive(msg)
            return Behaviors.stopped() if self.stop_now else next_behavior

        def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
            if sig is PostStop or sig is PreRestart:
                # what their DeathWatch of the coordinator would tell them; a restarted one has
                # lost its state, so they register again
                for path in self.alive_regions:
                    if path in self.refs:
                        self.refs[path].tell(CoordinatorStopped(self.ctx.self))
            return Behaviors.same

        return on_message.receive_signal(on_signal)

    # --- DDataShardCoordinator: the initial state (ShardCoordinator.scala 1592-1700) -------------

    def get_coordinator_state(self) -> None:
        """getCoordinatorState (1916-1918)."""
        self.replicator.ask_get(
            lambda reply_to: Replicator.Get(self.key, self.read_consistency, reply_to), _GetResult
        )

    def waiting_for_initial_state(self, msg: Any) -> Behavior[Any]:
        """waitingForInitialState (1592-1647): every other message is dropped, since the regions
        retry."""
        match msg:
            case _GetResult(Replicator.GetSuccess() as success):
                loaded = success.get(self.key).value
                self.log.debug(
                    "%s: Received initial coordinator state with [%d] shards",
                    self.type_name,
                    len(loaded.shards),
                )
                return self.on_initial_state(loaded)
            case _GetResult(Replicator.GetFailure()):
                self.initial_state_retries += 1
                if self.initial_state_retries == 1:
                    log = self.log.info
                elif self.initial_state_retries < 5:
                    log = self.log.warning
                else:
                    log = self.log.error
                log(
                    "%s: The ShardCoordinator was unable to get an initial state within "
                    "'waiting-for-state-timeout': %d millis (retrying). Has ClusterSharding been "
                    "started on all nodes?",
                    self.type_name,
                    int(self.settings.waiting_for_state_timeout * 1000),
                )
                # repeat until GetSuccess
                self.get_coordinator_state()
            case _GetResult(Replicator.NotFound()):
                self.log.debug("%s: Initial coordinator is empty.", self.type_name)
                # this.state is empty initially
                return self.on_initial_state(self.state)
            case _Terminate():
                self.log.debug(
                    "%s: Received termination message while waiting for state", self.type_name
                )
                return Behaviors.stopped()
            case Register(region):
                self.log.debug(
                    "%s: ShardRegion tried to register but ShardCoordinator not initialized yet: "
                    "[%s]",
                    self.type_name,
                    region.path,
                )
            case _:
                pass
        return Behaviors.same

    def on_initial_state(self, loaded: CoordinatorState) -> Behavior[Any]:
        """onInitialState (1649-1665), without remembered entities."""
        self.state = loaded
        if not loaded.shards and not loaded.regions:
            # empty state, activate immediately
            return self.activate()
        self.receive = self.waiting_for_state_initialized
        # note that watchStateActors may call update
        self.watch_state_actors()
        return Behaviors.same

    def waiting_for_state_initialized(self, msg: Any) -> Behavior[Any]:
        """waitingForStateInitialized (1667-1700): stash everything until StateInitialized, which
        watchStateActors scheduled."""
        match msg:
            case _StateInitialized():
                self.unstash_one_get_shard_home_request()
                self.state_initialized()
                self.activate()
                return self.stash.unstash_all(Behaviors.same)
            case GetShardHome():
                self.stash_get_shard_home_request(msg)
            case _Terminate():
                self.log.debug(
                    "%s: Received termination message while waiting for state initialized",
                    self.type_name,
                )
                return Behaviors.stopped()
            case _:
                self.stash.stash(msg)
        return Behaviors.same

    def activate(self) -> Behavior[Any]:
        """activate (1868-1873)."""
        self.receive = self.active
        self.log.info(
            "%s: ShardCoordinator was moved to the active state with [%d] shards",
            self.type_name,
            len(self.state.shards),
        )
        return Behaviors.same

    def watch_state_actors(self) -> None:
        """watchStateActors (1174-1202). A region whose node incarnation is not a member is
        terminated: paths carry no uid here, so its node is recorded in the state (D5). The rest
        are alive; a local one is watched, a remote one through membership (MemberRemoved)."""
        members = {m.unique_address for m in self.cluster.state.members}
        for path in list(self.state.regions):
            if self.state.region_nodes.get(path) not in members:
                self.region_terminated(path)  # not part of cluster
                continue
            try:
                region = self.ctx.system.resolve(path)
            except LookupError:  # a local region that is gone: Pekko's watch tells it at once
                self.region_terminated(path)
                continue
            self.refs[path] = region
            self.alive_regions.add(path)
            if is_local_actor(region):
                self.ctx.watch_with(region, RegionStopped(path))
        # Let the quick Terminated messages be processed before starting to reply to GetShardHome
        self.ctx.timers.start_single_timer("state-initialized", _StateInitialized(), 0.5)

    def state_initialized(self) -> None:
        """stateInitialized (1204-1207)."""
        for shard, region in self.state.shards.items():
            if region in self.refs:
                self.send_host_shard(shard, region)

    # --- DDataShardCoordinator: update (1702-1866, 1891-1940) ------------------------------------

    def update[E: DomainEvent](self, evt: E, f: Callable[[E], None]) -> None:
        """update (1891-1914), without remembered entities: write the state with ``evt``, and run
        ``f`` once the write succeeds, in waitingForUpdate meanwhile."""
        pending = (evt, lambda: f(evt))
        if self.updating is not None:
            # a second update from the same message waits for the first; Pekko stacks both, and
            # the second write can then miss the first's change
            self.queued_updates.append(pending)
            return
        self.updating = pending
        self.send_coordinator_state_update(evt)

    def send_coordinator_state_update(self, evt: DomainEvent) -> None:
        """sendCoordinatorStateUpdate (1928-1940): Pekko's ``Some(evt)`` request is the response
        adapter's ``evt``."""
        s = self.state.updated(evt)
        self.replicator.ask_update(
            lambda reply_to: Replicator.Update(
                self.key,
                LWWRegister.create(self.self_unique_address, CoordinatorState.EMPTY),
                self.write_consistency,
                reply_to,
                lambda reg: reg.with_value(self.self_unique_address, s),
            ),
            lambda rsp: _UpdateResult(evt, rsp),
        )

    def waiting_for_update(
        self, msg: Any, evt: DomainEvent, f: Callable[[], None]
    ) -> Behavior[Any]:
        """waitingForUpdate (1702-1837), without remembered entities."""
        match msg:
            case _UpdateResult(e, Replicator.UpdateSuccess()) if e == evt:
                self.update_state_retries = 0
                self.log.debug(
                    "%s: The coordinator state was successfully updated with %s",
                    self.type_name,
                    evt,
                )
                return self.unbecome_after_update(f)
            case _UpdateResult(e, Replicator.UpdateTimeout()) if e == evt:
                self.update_state_retries += 1
                log = self.log.warning if self.update_state_retries < 5 else self.log.error
                log(
                    "%s: The ShardCoordinator was unable to update a distributed state within "
                    "'updating-state-timeout': %d millis (%s). Attempt %d. Perhaps the "
                    "ShardRegion has not started on all active nodes yet? event=%s",
                    self.type_name,
                    int(self.settings.updating_state_timeout * 1000),
                    "terminating" if self.terminating else "retrying",
                    self.update_state_retries,
                    evt,
                )
                if self.terminating:
                    return Behaviors.stopped()
                # repeat until UpdateSuccess
                self.send_coordinator_state_update(evt)
            case _UpdateResult(_, Replicator.ModifyFailure(key, error, cause)):
                self.log.error(
                    "%s: The ShardCoordinator was unable to update a distributed state %s with "
                    "error %s and event %s. %s",
                    self.type_name,
                    key,
                    error,
                    evt,
                    "Coordinator will be terminated due to Terminate message received"
                    if self.terminating
                    else "Coordinator will be restarted",
                    exc_info=cause,
                )
                if self.terminating:
                    return Behaviors.stopped()
                raise cause
            case GetShardHome(shard, reply_to):
                if not self.handle_get_shard_home(shard, reply_to):
                    # must wait for update that is in progress
                    self.stash_get_shard_home_request(msg)
            case _Terminate():
                self.log.debug(
                    "%s: The ShardCoordinator received termination message while waiting for "
                    "update",
                    self.type_name,
                )
                self.terminating = True
                self.stash.stash(msg)
            case _:
                self.stash.stash(msg)
        return Behaviors.same

    def unbecome_after_update(self, f: Callable[[], None]) -> Behavior[Any]:
        """unbecomeAfterUpdate (1839-1846)."""
        self.updating = None
        f()
        if self.updating is None and self.queued_updates:
            self.updating = self.queued_updates.popleft()
            self.send_coordinator_state_update(self.updating[0])
        self.unstash_one_get_shard_home_request()
        return self.stash.unstash_all(Behaviors.same)

    def stash_get_shard_home_request(self, request: GetShardHome) -> None:
        """stashGetShardHomeRequest (1848-1856): Pekko's set, kept in arrival order."""
        self.log.debug(
            "%s: GetShardHome [%s] request from [%s] stashed, because waiting for initial state "
            "or update of state. It will be handled afterwards.",
            self.type_name,
            request.shard,
            request.reply_to.path,
        )
        if request not in self.get_shard_home_requests:
            self.get_shard_home_requests.append(request)

    def unstash_one_get_shard_home_request(self) -> None:
        """unstashOneGetShardHomeRequest (1858-1866): unstash one, the next follows when it is
        handled or the update completes."""
        if self.get_shard_home_requests:
            self.ctx.self.tell(self.get_shard_home_requests.popleft())

    # --- ShardCoordinator: active (740-967) ------------------------------------------------------

    def active(self, msg: Any) -> Behavior[Any]:
        match msg:
            case Register(region):
                self.register(region)
            case GetShardHome(shard, reply_to):
                self.get_shard_home(shard, reply_to)
            case ShardStarted(shard):
                self.ctx.timers.cancel(("host", shard))
            case _ResendShardHost(shard, region):
                if self.state.shards.get(shard) == region and region in self.refs:
                    self.send_host_shard(shard, region)
            case MemberRemoved(member=m):
                self.on_member_removed(m)
            case ClusterDomainEvent():
                pass
            case _RebalanceTick() | _RebalanceDone() | GracefulShutdownReq():
                self.on_rebalance_message(msg)
            case RegionStopped(path):
                self.region_terminated(path)
            case _DelayedShardRegionTerminated(path):
                self.region_terminated(path)
            case _Terminate():
                self.terminate()
            case _:
                pass
        return Behaviors.same

    # --- registration and shard homes ------------------------------------------------------------

    def register(self, region: ActorRef[Any]) -> None:
        """active's Register (742-766)."""
        path = region.path
        member = self.member_for(region)
        if member is None or member.status not in _OF_INTEREST:
            self.log.debug(
                "%s: ShardRegion [%s] was not registered since the coordinator currently does not "
                "know about a node of that region",
                self.type_name,
                path,
            )
            return
        node = member.unique_address
        known = self.state.region_nodes.get(path)
        if known is not None and known != node:
            # A new incarnation at the address of one in the state (D5): the old process is gone
            # for certain, so its shards are released now and the registration follows. Pekko's
            # refs carry the uid, so there the new incarnation is simply another region.
            self.ctx.timers.cancel(("region-removed", path))
            self.region_terminated(path)
            self.ctx.self.tell(Register(region))
            return
        self.log.debug("%s: ShardRegion registered: [%s]", self.type_name, path)
        self.refs[path] = region
        self.alive_regions.add(path)
        if path in self.state.regions:
            region.tell(RegisterAck(self.ctx.self))
            self.inform_about_current_shards(region)
        else:
            self.graceful_shutdown_in_progress.discard(path)
            # we don't have to wait for updated state for this
            self.inform_about_current_shards(region)

            def registered(evt: ShardRegionRegistered) -> None:
                self.state = self.state.updated(evt)
                # Pekko's RegionStopped: the local region's end is what Terminate waits for
                if is_local_actor(region):
                    self.ctx.watch_with(region, RegionStopped(path))
                region.tell(RegisterAck(self.ctx.self))

            self.update(ShardRegionRegistered(path, node), registered)

    def inform_about_current_shards(self, ref: ActorRef[Any]) -> None:
        homes = tuple(
            (self.refs[r], tuple(s for s in shards if s not in self.rebalance_in_progress))
            for r, shards in self.state.regions.items()
            if shards and r in self.refs
        )
        if homes:
            ref.tell(ShardHomes(homes))

    def handle_get_shard_home(self, shard: str, reply_to: ActorRef[Any]) -> bool:
        """handleGetShardHome (1061-1091): True when answered without a state update, that is
        known, deferred or ignored."""
        if shard in self.rebalance_in_progress:
            # deferred until the hand-off completes
            self.rebalance_in_progress[shard].append(reply_to)
            self.unstash_one_get_shard_home_request()  # continue unstashing
            return True
        known = self.state.shards.get(shard)
        if known is None:
            return False  # location not known, yet, caller will handle allocation
        if known in self.region_termination_in_progress:
            self.log.debug(
                "%s: GetShardHome [%s] request ignored, due to region [%s] termination in "
                "progress.",
                self.type_name,
                shard,
                known,
            )
        else:
            reply_to.tell(ShardHome(shard, self.refs[known]))
        self.unstash_one_get_shard_home_request()  # continue unstashing
        return True

    def get_shard_home(self, shard: str, reply_to: ActorRef[Any]) -> None:
        """active's GetShardHome (788-811)."""
        if self.handle_get_shard_home(shard, reply_to):
            return
        active = [
            r
            for r in self.state.regions
            if r not in self.graceful_shutdown_in_progress
            and r not in self.region_termination_in_progress
        ]
        if not active:
            return
        region = self.strategy.allocate_shard(
            shard, {r: self.state.regions[r] for r in active}, self.members_by_region(active)
        )
        if region is not None:
            self.continue_get_shard_home(shard, region, reply_to)

    def continue_get_shard_home(self, shard: str, region: str, reply_to: ActorRef[Any]) -> None:
        """continueGetShardHome (1276-1314)."""
        if shard in self.rebalance_in_progress:
            self.rebalance_in_progress[shard].append(reply_to)
            return
        known = self.state.shards.get(shard)
        if known is not None:
            reply_to.tell(ShardHome(shard, self.refs[known]))
            return
        if (
            region in self.state.regions
            and region not in self.graceful_shutdown_in_progress
            and region not in self.region_termination_in_progress
        ):

            def allocated(evt: ShardHomeAllocated) -> None:
                self.state = self.state.updated(evt)
                self.log.info(
                    "%s: Shard [%s] allocated at [%s]", self.type_name, evt.shard, evt.region
                )
                self.send_host_shard(evt.shard, evt.region)
                reply_to.tell(ShardHome(evt.shard, self.refs[evt.region]))

            self.update(ShardHomeAllocated(shard, region), allocated)
        else:
            self.log.debug(
                "%s: Allocated region [%s] for shard [%s] is not (any longer) one of the "
                "registered regions.",
                self.type_name,
                region,
                shard,
            )

    def send_host_shard(self, shard: str, region: str) -> None:
        self.refs[region].tell(HostShard(shard, self.ctx.self))
        self.ctx.timers.start_single_timer(
            ("host", shard), _ResendShardHost(shard, region), self.settings.shard_start_timeout
        )

    def region_terminated(self, path: str) -> None:
        """regionTerminated (1218-1245)."""
        for worker in self.rebalance_workers.values():
            worker.tell(_RegionTerminatedForWorker(path))
        if path not in self.state.regions:
            return
        self.log.info("%s: ShardRegion terminated: [%s]", self.type_name, path)
        self.region_termination_in_progress.add(path)
        for shard in self.state.regions[path]:
            self.ctx.self.tell(GetShardHome(shard, self.ctx.system.ignore_ref))

        def terminated(evt: ShardRegionTerminated) -> None:
            self.state = self.state.updated(evt)
            self.graceful_shutdown_in_progress.discard(path)
            self.region_termination_in_progress.discard(path)
            self.alive_regions.discard(path)
            self.refs.pop(path, None)
            if self.waiting_for_local_region_to_terminate and _is_at(
                path, self.cluster.self_address
            ):
                # handoff optimization: the local region completed its hand-off
                self.terminate()

        self.update(ShardRegionTerminated(path), terminated)

    def terminate(self) -> None:
        """Pekko's Terminate: stop at once, unless the local region still has shards to hand
        off."""
        local = [r for r in self.alive_regions if _is_at(r, self.cluster.self_address)]
        if local:
            self.waiting_for_local_region_to_terminate = True
        else:
            self.stop_now = True

    def on_member_removed(self, m: Member) -> None:
        """A removed member's regions are terminated, in place of receiveTerminated (1093-1105)
        for its address. One still alive, that never said it stopped, may still run its shards on
        a crashed or partitioned node, so it waits the down-removal margin (Pekko's
        addressTerminated on a live region); one that stopped after handing off told us so and is
        already gone. Only a region of that incarnation is terminated (D5)."""
        margin = self.cluster.settings.effective_down_removal_margin
        for path in [r for r, n in self.state.region_nodes.items() if n == m.unique_address]:
            if margin > 0 and path in self.alive_regions:
                # GetShardHome for its shards waits, as Pekko's
                self.region_termination_in_progress.add(path)
                self.ctx.timers.start_single_timer(
                    ("region-removed", path), _DelayedShardRegionTerminated(path), margin
                )
            else:
                self.region_terminated(path)

    # --- rebalance and hand-off ------------------------------------------------------------------

    def on_rebalance_message(self, msg: Any) -> None:
        match msg:
            case _RebalanceTick():
                if self.state.regions:
                    shards = self.strategy.rebalance(
                        dict(self.state.regions),
                        self.members_by_region(self.state.regions),
                        frozenset(self.rebalance_in_progress),
                        self.cluster.state,
                    )
                    self.continue_rebalance(shards)
            case _RebalanceDone(shard, ok):
                self.rebalance_done(shard, ok)
            case GracefulShutdownReq(region):
                path = region.path
                if path not in self.graceful_shutdown_in_progress and path in self.state.regions:
                    self.log.info(
                        "%s: Graceful shutdown of region [%s] with shards [%s] started",
                        self.type_name,
                        path,
                        self.state.regions[path],
                    )
                    self.graceful_shutdown_in_progress.add(path)
                    self.shutdown_shards(path, self.state.regions[path])
            case _:
                pass

    def continue_rebalance(self, shards: frozenset[str]) -> None:
        if shards:
            self.log.info(
                "%s: Starting rebalance for shards [%s]. In progress: [%s]",
                self.type_name,
                sorted(shards),
                sorted(self.rebalance_in_progress),
            )
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
            _rebalance_worker(
                self.type_name,
                shard,
                self.refs[from_region],
                regions,
                self.settings.handoff_timeout,
                is_rebalance,
            )
        )
        self.rebalance_workers[shard] = worker

    def rebalance_done(self, shard: str, ok: bool) -> None:
        """active's RebalanceDone (849-890)."""
        self.rebalance_workers.pop(shard, None)
        if ok:
            # The shard could have been removed by ShardRegionTerminated
            if shard in self.state.shards:

                def deallocated(evt: ShardHomeDeallocated) -> None:
                    self.state = self.state.updated(evt)
                    self.clear_rebalance_in_progress(shard)
                    # reallocate at once
                    self.ctx.self.tell(GetShardHome(shard, self.ctx.system.ignore_ref))

                self.update(ShardHomeDeallocated(shard), deallocated)
            else:
                self.clear_rebalance_in_progress(shard)
        else:
            self.log.warning(
                "%s: Shard [%s] deallocation did not complete within [%s]s",
                self.type_name,
                shard,
                self.settings.handoff_timeout,
            )
            region = self.state.shards.get(shard)
            if region is not None:
                self.graceful_shutdown_in_progress.discard(region)
            self.clear_rebalance_in_progress(shard)

    def clear_rebalance_in_progress(self, shard: str) -> None:
        """clearRebalanceInProgress (1000-1010)."""
        for asker in self.rebalance_in_progress.pop(shard, []):
            self.ctx.self.tell(GetShardHome(shard, asker))


def coordinator_behavior(
    type_name: str,
    settings: ClusterShardingSettings,
    replicator: ActorRef[Replicator.Command],
) -> Behavior[Any]:
    return Behaviors.with_stash(
        _STASH_CAPACITY,
        lambda stash: Behaviors.setup(
            lambda ctx: _Coordinator(ctx, type_name, settings, replicator, stash).behavior()
        ),
    )


# --- the region (ShardRegion) --------------------------------------------------------------------
