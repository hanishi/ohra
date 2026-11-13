"""Apache Pekko's ClusterSharding across nodes: a ShardCoordinator singleton allocating shards to
regions with the LeastShardAllocationStrategy, a ShardRegion on every node, hand-off for
rebalancing and for a leaving node. The coordinator keeps its state in Distributed Data, as
Pekko's DDataShardCoordinator."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from typing import Any, ClassVar

from ...behavior import Behavior, Behaviors, StashBuffer
from ...context import ActorContext
from ...ref import ActorRef
from ...remote import Address, UniqueAddress
from ...remote.serialization import Serialization
from ...cell import is_local_actor
from ...signals import PostStop, PreRestart, Signal
from ...supervision import SupervisorStrategy
from .shard import (
    ClusterShardingSettings,
    ClusterShardingStats,
    GetClusterShardingStats,
    GetShardRegionStats,
    HandOff,
    ShardRegionStats,
    ShardStopped,
    _ask_all,  # pyright: ignore[reportPrivateUsage]
)
from .remember_entities import DDataRememberEntitiesProvider, RememberEntitiesCoordinatorStore
from ..ddata import SelfUniqueAddress
from ..ddata.distributed_data import ReplicatorMessageAdapter
from ..ddata.lww_register import LWWRegister, LWWRegisterKey
from ..ddata.replicator import Replicator
from ..core import (
    Cluster,
    ClusterDomainEvent,
    CurrentClusterState,
    MemberPreparingForShutdown,
    MemberReadyForShutdown,
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
class ShardRegionProxyRegistered:
    region_proxy: str
    node: UniqueAddress


@dataclass(frozen=True)
class ShardRegionProxyTerminated:
    region_proxy: str


@dataclass(frozen=True)
class ShardHomeAllocated:
    shard: str
    region: str


@dataclass(frozen=True)
class ShardHomeDeallocated:
    shard: str


type DomainEvent = (
    ShardRegionRegistered
    | ShardRegionProxyRegistered
    | ShardRegionTerminated
    | ShardRegionProxyTerminated
    | ShardHomeAllocated
    | ShardHomeDeallocated
)


@dataclass(frozen=True)
class CoordinatorState:
    """ShardCoordinator.State (ShardCoordinator.scala 455-521): shard -> region path, and region
    path -> its shards. Regions are keyed by path so a local and a remote ref to the same region
    compare equal. It is the value of the coordinator's LWWRegister, so it travels on the wire.
    Actor paths carry no uid here, so ``region_nodes`` records each region's node, and
    ``region_proxies`` each proxy's. ``unallocated_shards`` is only used if remembered entities is
    enabled."""

    shards: dict[str, str]
    regions: dict[str, tuple[str, ...]]
    region_nodes: dict[str, UniqueAddress]
    region_proxies: dict[str, UniqueAddress] = field(default_factory=dict[str, UniqueAddress])
    unallocated_shards: frozenset[str] = frozenset()
    remember_entities: bool = False
    EMPTY: ClassVar[CoordinatorState]

    def with_remember_entities(self, enabled: bool) -> CoordinatorState:
        """withRememberEntities (474-479)."""
        if enabled:
            return replace(self, remember_entities=True)
        return replace(self, unallocated_shards=frozenset(), remember_entities=False)

    @property
    def is_empty(self) -> bool:
        """isEmpty (481-482): unallocated shards do not count."""
        return not self.shards and not self.regions and not self.region_proxies

    def updated(self, event: DomainEvent) -> CoordinatorState:
        match event:
            case ShardRegionRegistered(region, node):
                if region in self.regions:
                    raise ValueError(f"Region {region} already registered: {self}")
                return replace(
                    self,
                    regions={**self.regions, region: ()},
                    region_nodes={**self.region_nodes, region: node},
                )
            case ShardRegionProxyRegistered(proxy, node):
                if proxy in self.region_proxies:
                    raise ValueError(f"Region proxy {proxy} already registered: {self}")
                return replace(self, region_proxies={**self.region_proxies, proxy: node})
            case ShardRegionProxyTerminated(proxy):
                if proxy not in self.region_proxies:
                    raise ValueError(f"Terminated region proxy {proxy} not registered: {self}")
                return replace(
                    self,
                    region_proxies={p: n for p, n in self.region_proxies.items() if p != proxy},
                )
            case ShardRegionTerminated(region):
                if region not in self.regions:
                    raise ValueError(f"Terminated region {region} not registered: {self}")
                gone = set(self.regions[region])
                return replace(
                    self,
                    shards={s: r for s, r in self.shards.items() if s not in gone},
                    regions={r: ids for r, ids in self.regions.items() if r != region},
                    region_nodes={r: n for r, n in self.region_nodes.items() if r != region},
                    unallocated_shards=self.unallocated_shards | gone
                    if self.remember_entities
                    else self.unallocated_shards,
                )
            case ShardHomeAllocated(shard, region):
                if region not in self.regions:
                    raise ValueError(f"Region {region} not registered: {self}")
                if shard in self.shards:
                    raise ValueError(f"Shard [{shard}] already allocated: {self}")
                return replace(
                    self,
                    shards={**self.shards, shard: region},
                    regions={**self.regions, region: (*self.regions[region], shard)},
                    unallocated_shards=self.unallocated_shards - {shard}
                    if self.remember_entities
                    else self.unallocated_shards,
                )
            case ShardHomeDeallocated(shard):
                if shard not in self.shards:
                    raise ValueError(f"Shard [{shard}] not allocated: {self}")
                region = self.shards[shard]
                if region not in self.regions:
                    raise ValueError(f"Region {region} for shard [{shard}] not registered: {self}")
                return replace(
                    self,
                    shards={s: r for s, r in self.shards.items() if s != shard},
                    regions={
                        **self.regions,
                        region: tuple(s for s in self.regions[region] if s != shard),
                    },
                    unallocated_shards=self.unallocated_shards | {shard}
                    if self.remember_entities
                    else self.unallocated_shards,
                )

    @property
    def all_shards(self) -> frozenset[str]:
        """allShards (484)."""
        return frozenset(self.shards) | self.unallocated_shards


CoordinatorState.EMPTY = CoordinatorState({}, {}, {})


# --- wire messages (ShardCoordinator.Internal), with explicit refs where Pekko uses sender() -----


@dataclass(frozen=True)
class Register:
    region: ActorRef[Any]


@dataclass(frozen=True)
class RegisterProxy:
    """A proxy-only region registers so, until it gets ``RegisterAck`` (ShardCoordinator.scala
    357-360)."""

    region_proxy: ActorRef[Any]


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
    RegisterProxy,
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


# DDataShardCoordinator's (ShardCoordinator.scala 1514-1523)


@dataclass(frozen=True)
class _RememberEntitiesStoreStopped: ...


@dataclass(frozen=True)
class _RememberEntitiesTimeout:
    shard_id: str


@dataclass(frozen=True)
class _RememberEntitiesLoadTimeout: ...


_REMEMBER_ENTITIES_TIMEOUT_KEY = "RememberEntityTimeout"
"""Both timers share it (1522), so a new AddShard's timer replaces a pending load timer."""


# The member statuses whose region may register. Pekko's isMember (734-738) takes self or Up only;
# here a region may also register while its member is Leaving or Exiting, or prepares for a full
# cluster shutdown, so the regions register again after a coordinator restart meanwhile.
_OF_INTEREST = frozenset(
    {"up", "leaving", "exiting", "preparing_for_shutdown", "ready_for_shutdown"}
)


def _is_at(ref_or_path: ActorRef[Any] | str, address: Address) -> bool:
    path = ref_or_path if isinstance(ref_or_path, str) else ref_or_path.path
    return path.startswith(address.root + "/")


def _address_of(path: str) -> Address:
    """The address of the node an actor path is on."""
    return Address.parse(path[: path.index("/", len("ohra://"))])


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


def coordinator_behavior(
    type_name: str,
    settings: ClusterShardingSettings,
    replicator: ActorRef[Replicator.Command],
    remember_entities_provider: DDataRememberEntitiesProvider | None = None,
) -> Behavior[Any]:
    """ShardCoordinator (ShardCoordinator.scala 600-1385) with DDataShardCoordinator's
    (1535-1976): the state is the value of an LWWRegister in sharding's Replicator, read by
    majority when the coordinator starts and written by majority before each change takes effect.
    With a ``remember_entities_provider``, its coordinator store, a child, remembers every
    allocated shard, and the remembered shards are allocated again when the coordinator starts.
    Pekko's receive states are the nested functions returning behaviors; a helper that becomes
    ``waitingForUpdate`` sets ``become``, which the state's handler returns."""

    def coordinator(stash: StashBuffer[Any]) -> Behavior[Any]:
        @Behaviors.setup
        def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
            log = ctx.log
            cluster = Cluster(ctx.system)
            strategy = LeastShardAllocationStrategy(
                settings.rebalance_absolute_limit, settings.rebalance_relative_limit
            )
            # initEmptyState (1565)
            init_empty_state = CoordinatorState.EMPTY.with_remember_entities(
                settings.remember_entities
            )
            state = init_empty_state
            refs: dict[str, ActorRef[Any]] = {}
            alive_regions: set[str] = set()
            # minMembers (686-690): the shard role's minimum, else min-nr-of-members
            min_members = (
                cluster.settings.min_nr_of_members
                if settings.role is None
                else cluster.settings.min_nr_of_members_of_role.get(settings.role, 1)
            )
            all_regions_registered = False
            graceful_shutdown_in_progress: set[str] = set()
            region_termination_in_progress: set[str] = set()
            rebalance_in_progress: dict[str, list[ActorRef[Any]]] = {}
            rebalance_workers: dict[str, ActorRef[Any]] = {}
            waiting_for_local_region_to_terminate = False
            stop_now = False
            preparing_for_shutdown = False
            # DDataShardCoordinator's
            adapter = ReplicatorMessageAdapter[Any, _StateRegister](
                ctx,
                replicator,
                # past both the Get's and the Update's own timeout, by which the Replicator
                # answers
                max(settings.waiting_for_state_timeout, settings.updating_state_timeout) + 1,
            )
            key = LWWRegisterKey[CoordinatorState](f"{type_name}CoordinatorState")
            self_unique_address = SelfUniqueAddress(cluster.self_unique_address)
            read_consistency = settings.state_read_consistency()
            write_consistency = settings.state_write_consistency()
            terminating = False
            get_shard_home_requests: deque[GetShardHome] = deque()
            initial_state_retries = 0
            update_state_retries = 0
            # The waitingForUpdate a helper's update started, for the state's handler to return.
            become: Behavior[Any] | None = None
            # ``become`` and ``unbecome_to`` together are Pekko's two-deep become stack: update
            # does context.become(waitingForUpdate, discardOld = false) (1913) over the base
            # state, and unbecomeAfterUpdate does context.unbecome() (1840) back to it.
            # ``unbecome_to`` is that base state, set where Pekko does a plain context.become of
            # one, and assigned below once the states are defined. A handler that can reach
            # update() must end in take_become.
            unbecome_to: Callable[[], Behavior[Any]]
            queued_updates: deque[tuple[DomainEvent, Callable[[], None]]] = deque()
            # rememberEntitiesStore (1572-1579)
            remember_entities_store: ActorRef[RememberEntitiesCoordinatorStore.Command] | None = (
                None
            )
            if remember_entities_provider is not None:
                log.debug(
                    "%s: Starting remember entities store from provider %s",
                    type_name,
                    remember_entities_provider,
                )
                # Pekko's classic default supervisor restarts a store that throws
                remember_entities_store = ctx.spawn(
                    Behaviors.supervise(
                        remember_entities_provider.coordinator_store_behavior()
                    ).on_failure(SupervisorStrategy.restart()),
                    "RememberEntitiesStore",
                )
                ctx.watch_with(remember_entities_store, _RememberEntitiesStoreStopped())
            remember_entities = remember_entities_store is not None
            cluster.subscriptions.tell(ClusterSubscribe(ctx.self, ClusterDomainEvent))
            ctx.timers.start_timer_with_fixed_delay(
                "rebalance", _RebalanceTick(), settings.rebalance_interval
            )

            def take_become(otherwise: Behavior[Any]) -> Behavior[Any]:
                """The waitingForUpdate a helper became during this message, else
                ``otherwise``."""
                nonlocal become
                pending, become = become, None
                return otherwise if pending is None else pending

            # --- membership views ----------------------------------------------------------------

            def member_for(ref_or_path: ActorRef[Any] | str) -> Member | None:
                return next(
                    (m for m in cluster.state.members if _is_at(ref_or_path, m.address)), None
                )

            def members_by_region(regions: Iterable[str]) -> dict[str, Member]:
                out: dict[str, Member] = {}
                for r in regions:
                    m = member_for(r)
                    if m is not None:
                        out[r] = m
                return out

            def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
                if sig is PostStop or sig is PreRestart:
                    # what their DeathWatch of the coordinator would tell them: they register again
                    for path in (*alive_regions, *state.region_proxies):
                        if path in refs:
                            refs[path].tell(CoordinatorStopped(ctx.self))
                return Behaviors.same

            # --- DDataShardCoordinator: the initial state (ShardCoordinator.scala 1592-1700) -----

            def get_coordinator_state() -> None:
                """getCoordinatorState (1916-1918)."""
                adapter.ask_get(
                    lambda reply_to: Replicator.Get(key, read_consistency, reply_to), _GetResult
                )

            def get_all_remembered_shards() -> None:
                """getAllRememberedShards (1920-1926)."""
                ctx.timers.start_single_timer(
                    _REMEMBER_ENTITIES_TIMEOUT_KEY,
                    _RememberEntitiesLoadTimeout(),
                    settings.waiting_for_state_timeout,
                )
                if remember_entities_store is not None:
                    remember_entities_store.tell(
                        RememberEntitiesCoordinatorStore.GetShards(ctx.self)
                    )

            def remember_shard_allocated(shard: str) -> None:
                """rememberShardAllocated (1941-1948)."""
                log.debug("%s: Remembering shard allocation [%s]", type_name, shard)
                if remember_entities_store is not None:
                    remember_entities_store.tell(
                        RememberEntitiesCoordinatorStore.AddShard(shard, ctx.self)
                    )
                ctx.timers.start_single_timer(
                    _REMEMBER_ENTITIES_TIMEOUT_KEY,
                    _RememberEntitiesTimeout(shard),
                    settings.updating_state_timeout,
                )

            def on_remember_entities_update_failed(shard: str) -> Behavior[Any]:
                """onRememberEntitiesUpdateFailed (1956-1968)."""
                log.error(
                    "%s: The ShardCoordinator was unable to update remembered shard [%s] within "
                    "'updating-state-timeout': %d millis, %s",
                    type_name,
                    shard,
                    int(settings.updating_state_timeout * 1000),
                    "terminating" if terminating else "retrying",
                )
                if terminating:
                    return Behaviors.stopped()
                # retry until successful
                remember_shard_allocated(shard)
                return Behaviors.same

            def on_remember_entities_store_stopped() -> Behavior[Any]:
                """onRememberEntitiesStoreStopped (1970-1974)."""
                # rely on backoff supervision of coordinator
                log.error(
                    "%s: The ShardCoordinator stopping because the remember entities store "
                    "stopped",
                    type_name,
                )
                return Behaviors.stopped()

            def waiting_for_initial_state(
                remembered_shards: frozenset[str] = frozenset(),
            ) -> Behavior[Any]:
                """waitingForInitialState (1592-1647): every other message is dropped, since the
                regions retry. The remembered shards can arrive here or later, and do not keep
                the coordinator in this state."""

                def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                    nonlocal initial_state_retries
                    match msg:
                        case _GetResult(Replicator.GetSuccess() as success):
                            existing_state = success.get(key).value.with_remember_entities(
                                settings.remember_entities
                            )
                            log.debug(
                                "%s: Received initial coordinator state with [%d] shards",
                                type_name,
                                len(existing_state.shards)
                                + len(existing_state.unallocated_shards),
                            )
                            return take_become(on_initial_state(existing_state, remembered_shards))
                        case _GetResult(Replicator.GetFailure()):
                            initial_state_retries += 1
                            if initial_state_retries == 1:
                                log_at = log.info
                            elif initial_state_retries < 5:
                                log_at = log.warning
                            else:
                                log_at = log.error
                            log_at(
                                "%s: The ShardCoordinator was unable to get an initial state "
                                "within 'waiting-for-state-timeout': %d millis (retrying). Has "
                                "ClusterSharding been started on all nodes?",
                                type_name,
                                int(settings.waiting_for_state_timeout * 1000),
                            )
                            # repeat until GetSuccess
                            get_coordinator_state()
                        case _GetResult(Replicator.NotFound()):
                            log.debug("%s: Initial coordinator is empty.", type_name)
                            # this.state is empty initially
                            return take_become(on_initial_state(state, remembered_shards))
                        case RememberEntitiesCoordinatorStore.RememberedShards(shard_ids):
                            log.debug(
                                "%s: Received [%d] remembered shard ids (when "
                                "waitingForInitialState)",
                                type_name,
                                len(shard_ids),
                            )
                            ctx.timers.cancel(_REMEMBER_ENTITIES_TIMEOUT_KEY)
                            return waiting_for_initial_state(shard_ids)
                        case _RememberEntitiesLoadTimeout():
                            # repeat until successful
                            get_all_remembered_shards()
                        case _Terminate():
                            log.debug(
                                "%s: Received termination message while waiting for state",
                                type_name,
                            )
                            return Behaviors.stopped()
                        case Register(region):
                            log.debug(
                                "%s: ShardRegion tried to register but ShardCoordinator not "
                                "initialized yet: [%s]",
                                type_name,
                                region.path,
                            )
                        case RegisterProxy(proxy):
                            log.debug(
                                "%s: ShardRegion proxy tried to register but ShardCoordinator not "
                                "initialized yet: [%s]",
                                type_name,
                                proxy.path,
                            )
                        case _RememberEntitiesStoreStopped():
                            # receiveTerminated (1950-1954)
                            return on_remember_entities_store_stopped()
                        case _:
                            pass
                    return Behaviors.same

                return Behaviors.receive(on_message).receive_signal(on_signal)

            def on_initial_state(
                loaded_state: CoordinatorState, remembered_shards: frozenset[str]
            ) -> Behavior[Any]:
                """onInitialState (1649-1665): the next state. An update watchStateActors starts
                is in ``become``, over this state."""
                nonlocal state, unbecome_to
                if settings.remember_entities and remembered_shards:
                    # Note that we don't wait for shards from store so they could also arrive
                    # later
                    new_unallocated_shards = state.unallocated_shards | (
                        remembered_shards.difference(state.shards)
                    )
                    state = replace(loaded_state, unallocated_shards=new_unallocated_shards)
                else:
                    state = loaded_state
                if state.is_empty:
                    # empty state, activate immediately
                    return activate()
                unbecome_to = waiting_for_state_initialized
                # note that watchStateActors may call update
                watch_state_actors()
                return waiting_for_state_initialized()

            def waiting_for_state_initialized() -> Behavior[Any]:
                """waitingForStateInitialized (1667-1700): stash everything until
                StateInitialized, which watchStateActors scheduled. Remembered shards that arrive
                here are kept: Pekko's 1686-1687 builds the new state and discards it."""

                def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                    nonlocal state
                    match msg:
                        case _StateInitialized():
                            unstash_one_get_shard_home_request()
                            state_initialized()
                            return stash.unstash_all(activate())
                        case GetShardHome():
                            stash_get_shard_home_request(msg)
                        case _Terminate():
                            log.debug(
                                "%s: Received termination message while waiting for state "
                                "initialized",
                                type_name,
                            )
                            return Behaviors.stopped()
                        case RememberEntitiesCoordinatorStore.RememberedShards(shard_ids):
                            log.debug(
                                "%s: Received [%d] remembered shard ids (when "
                                "waitingForStateInitialized)",
                                type_name,
                                len(shard_ids),
                            )
                            new_unallocated_shards = state.unallocated_shards | (
                                shard_ids.difference(state.shards)
                            )
                            state = replace(state, unallocated_shards=new_unallocated_shards)
                            ctx.timers.cancel(_REMEMBER_ENTITIES_TIMEOUT_KEY)
                        case _RememberEntitiesLoadTimeout():
                            # repeat until successful
                            get_all_remembered_shards()
                        case _RememberEntitiesStoreStopped():
                            return on_remember_entities_store_stopped()
                        case _:
                            stash.stash(msg)
                    return Behaviors.same

                return Behaviors.receive(on_message).receive_signal(on_signal)

            def activate() -> Behavior[Any]:
                """activate (1868-1873): the active state, with receiveLateRememberedEntities."""
                nonlocal unbecome_to
                unbecome_to = active
                log.info(
                    "%s: ShardCoordinator was moved to the active state with [%d] shards",
                    type_name,
                    len(state.shards),
                )
                return active()

            def receive_late_remembered_entities(msg: Any) -> None:
                """receiveLateRememberedEntities (1875-1889): only used once the coordinator is
                initialized."""
                nonlocal state
                match msg:
                    case RememberEntitiesCoordinatorStore.RememberedShards(shard_ids):
                        log.debug(
                            "%s: Received [%d] remembered shard ids (after state initialized)",
                            type_name,
                            len(shard_ids),
                        )
                        if shard_ids:
                            new_unallocated_shards = state.unallocated_shards | (
                                shard_ids.difference(state.shards)
                            )
                            state = replace(state, unallocated_shards=new_unallocated_shards)
                            allocate_shard_homes_for_remember_entities()
                        ctx.timers.cancel(_REMEMBER_ENTITIES_TIMEOUT_KEY)
                    case _RememberEntitiesLoadTimeout():
                        # repeat until successful
                        get_all_remembered_shards()
                    case _:
                        pass

            def watch_state_actors() -> None:
                """watchStateActors (1174-1202). A region whose node incarnation is not a member
                is terminated: paths carry no uid here, so its node is recorded in the state (D5).
                The rest are alive; a local one is watched, a remote one through membership
                (MemberRemoved)."""
                members = {m.unique_address for m in cluster.state.members}
                for path in list(state.regions):
                    if state.region_nodes.get(path) not in members:
                        region_terminated(path)  # not part of cluster
                        continue
                    try:
                        region = ctx.system.resolve(path)
                    # a local region that is gone: Pekko's watch tells it at once
                    except LookupError:
                        region_terminated(path)
                        continue
                    refs[path] = region
                    alive_regions.add(path)
                    if is_local_actor(region):
                        ctx.watch_with(region, RegionStopped(path))
                # 1189-1195: the proxies alike
                for path in list(state.region_proxies):
                    if state.region_proxies[path] not in members:
                        region_proxy_terminated(path)  # not part of cluster
                        continue
                    try:
                        proxy = ctx.system.resolve(path)
                    except LookupError:
                        region_proxy_terminated(path)
                        continue
                    refs[path] = proxy
                    if is_local_actor(proxy):
                        ctx.watch_with(proxy, RegionStopped(path))
                # Let the quick Terminated messages be processed before starting to reply to
                # GetShardHome
                ctx.timers.start_single_timer("state-initialized", _StateInitialized(), 0.5)

            def state_initialized() -> None:
                """stateInitialized (1204-1207)."""
                for shard, region in state.shards.items():
                    if region in refs:
                        send_host_shard(shard, region)
                allocate_shard_homes_for_remember_entities()

            def allocate_shard_homes_for_remember_entities() -> None:
                """allocateShardHomesForRememberEntities (1269-1274), in shard id order."""
                if settings.remember_entities and state.unallocated_shards:
                    for shard in sorted(state.unallocated_shards):
                        ctx.self.tell(GetShardHome(shard, ctx.system.ignore_ref))

            # --- DDataShardCoordinator: update (1702-1866, 1891-1940) ----------------------------

            def update[E: DomainEvent](evt: E, f: Callable[[E], None]) -> None:
                """update (1891-1914): write the state with ``evt``, and run ``f`` once the write
                succeeds, and the remember write of a new shard too, in waitingForUpdate
                meanwhile, which ``become`` holds for the state's handler."""
                nonlocal become

                def after() -> None:
                    f(evt)

                if become is not None:
                    # a second update from the same message waits for the first; Pekko stacks
                    # both, and the second write can then miss the first's change
                    queued_updates.append((evt, after))
                    return
                become = start_update(evt, after)

            def start_update(evt: DomainEvent, after: Callable[[], None]) -> Behavior[Any]:
                """update's body (1892-1912): send the state write, and the remember write of a
                shard allocated for the first time, and return the waitingForUpdate for both."""
                send_coordinator_state_update(evt)
                match evt:
                    case ShardHomeAllocated(shard) if (
                        remember_entities and shard not in state.shards
                    ):
                        remember_shard_allocated(shard)
                        return waiting_for_update(
                            evt,
                            shard,
                            waiting_for_state_write=True,
                            waiting_for_remember_shard=True,
                            after=after,
                        )
                    case _:
                        # no update of shards, already known
                        return waiting_for_update(
                            evt,
                            None,
                            waiting_for_state_write=True,
                            waiting_for_remember_shard=False,
                            after=after,
                        )

            def send_coordinator_state_update(evt: DomainEvent) -> None:
                """sendCoordinatorStateUpdate (1928-1940): Pekko's ``Some(evt)`` request is the
                response adapter's ``evt``."""
                s = state.updated(evt)
                adapter.ask_update(
                    lambda reply_to: Replicator.Update(
                        key,
                        LWWRegister.create(self_unique_address, init_empty_state),
                        write_consistency,
                        reply_to,
                        lambda reg: reg.with_value(self_unique_address, s),
                    ),
                    lambda rsp: _UpdateResult(evt, rsp),
                )

            def waiting_for_update(
                evt: DomainEvent,
                shard_id: str | None,
                waiting_for_state_write: bool,
                waiting_for_remember_shard: bool,
                after: Callable[[], None],
            ) -> Behavior[Any]:
                """waitingForUpdate (1702-1837): stash everything until the state write succeeds
                and, for a shard allocated for the first time, its remember write too. Each
                half that completes first becomes a waitingForUpdate for the other."""

                def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                    nonlocal update_state_retries, terminating
                    match msg:
                        case _UpdateResult(e, Replicator.UpdateSuccess()) if e == evt:
                            update_state_retries = 0
                            if not waiting_for_remember_shard:
                                log.debug(
                                    "%s: The coordinator state was successfully updated with %s",
                                    type_name,
                                    evt,
                                )
                                if shard_id is not None:
                                    ctx.timers.cancel(_REMEMBER_ENTITIES_TIMEOUT_KEY)
                                return unbecome_after_update(after)
                            log.debug(
                                "%s: The coordinator state was successfully updated with %s, "
                                "waiting for remember shard update",
                                type_name,
                                evt,
                            )
                            return waiting_for_update(
                                evt,
                                shard_id,
                                waiting_for_state_write=False,
                                waiting_for_remember_shard=True,
                                after=after,
                            )
                        case _UpdateResult(e, Replicator.UpdateTimeout()) if e == evt:
                            update_state_retries += 1
                            log_at = log.warning if update_state_retries < 5 else log.error
                            log_at(
                                "%s: The ShardCoordinator was unable to update a distributed "
                                "state within 'updating-state-timeout': %d millis (%s). Attempt "
                                "%d. Perhaps the ShardRegion has not started on all active nodes "
                                "yet? event=%s",
                                type_name,
                                int(settings.updating_state_timeout * 1000),
                                "terminating" if terminating else "retrying",
                                update_state_retries,
                                evt,
                            )
                            if terminating:
                                return Behaviors.stopped()
                            # repeat until UpdateSuccess
                            send_coordinator_state_update(evt)
                        case _UpdateResult(_, Replicator.ModifyFailure(failed_key, error, cause)):
                            log.error(
                                "%s: The ShardCoordinator was unable to update a distributed "
                                "state %s with error %s and event %s. %s",
                                type_name,
                                failed_key,
                                error,
                                evt,
                                "Coordinator will be terminated due to Terminate message received"
                                if terminating
                                else "Coordinator will be restarted",
                                exc_info=cause,
                            )
                            if terminating:
                                return Behaviors.stopped()
                            raise cause
                        case GetShardHome(shard, reply_to):
                            if not handle_get_shard_home(shard, reply_to):
                                # must wait for update that is in progress
                                stash_get_shard_home_request(msg)
                        case _Terminate():
                            log.debug(
                                "%s: The ShardCoordinator received termination message while "
                                "waiting for update",
                                type_name,
                            )
                            terminating = True
                            stash.stash(msg)
                        case RememberEntitiesCoordinatorStore.UpdateDone(shard):
                            if shard != shard_id:
                                log.warning(
                                    "%s: Saw remember entities update complete for shard id "
                                    "[%s], while waiting for [%s]",
                                    type_name,
                                    shard,
                                    shard_id or "",
                                )
                            elif not waiting_for_state_write:
                                log.debug(
                                    "%s: The ShardCoordinator saw remember shard start "
                                    "successfully written %s",
                                    type_name,
                                    evt,
                                )
                                ctx.timers.cancel(_REMEMBER_ENTITIES_TIMEOUT_KEY)
                                return unbecome_after_update(after)
                            else:
                                log.debug(
                                    "%s: The ShardCoordinator saw remember shard start "
                                    "successfully written %s, waiting for state update",
                                    type_name,
                                    evt,
                                )
                                return waiting_for_update(
                                    evt,
                                    shard_id,
                                    waiting_for_state_write=True,
                                    waiting_for_remember_shard=False,
                                    after=after,
                                )
                        case RememberEntitiesCoordinatorStore.UpdateFailed(shard):
                            if shard == shard_id:
                                return on_remember_entities_update_failed(shard)
                            log.warning(
                                "%s: Got an remember entities update failed for [%s] while "
                                "waiting for [%s], ignoring",
                                type_name,
                                shard,
                                shard_id or "",
                            )
                        case _RememberEntitiesTimeout(shard):
                            if shard == shard_id:
                                return on_remember_entities_update_failed(shard)
                            log.warning(
                                "%s: Got an remember entities update timeout for [%s] while "
                                "waiting for [%s], ignoring",
                                type_name,
                                shard,
                                shard_id or "",
                            )
                        case _RememberEntitiesStoreStopped():
                            return on_remember_entities_store_stopped()
                        case RememberEntitiesCoordinatorStore.RememberedShards():
                            log.debug(
                                "%s: Late arrival of remembered shards while waiting for update, "
                                "stashing",
                                type_name,
                            )
                            stash.stash(msg)
                        case _:
                            stash.stash(msg)
                    return Behaviors.same

                return Behaviors.receive(on_message).receive_signal(on_signal)

            def unbecome_after_update(after: Callable[[], None]) -> Behavior[Any]:
                """unbecomeAfterUpdate (1839-1846): back to ``unbecome_to``, or on to the queued
                update, with the stash replayed into it."""
                nonlocal become
                after()
                if stop_now:  # after completed a deferred Terminate: nothing is processed after it
                    return Behaviors.stopped()
                if become is None and queued_updates:
                    evt, queued_after = queued_updates.popleft()
                    become = start_update(evt, queued_after)
                unstash_one_get_shard_home_request()
                return stash.unstash_all(take_become(unbecome_to()))

            def stash_get_shard_home_request(request: GetShardHome) -> None:
                """stashGetShardHomeRequest (1848-1856): Pekko's set, kept in arrival order."""
                log.debug(
                    "%s: GetShardHome [%s] request from [%s] stashed, because waiting for initial "
                    "state or update of state. It will be handled afterwards.",
                    type_name,
                    request.shard,
                    request.reply_to.path,
                )
                if request not in get_shard_home_requests:
                    get_shard_home_requests.append(request)

            def unstash_one_get_shard_home_request() -> None:
                """unstashOneGetShardHomeRequest (1858-1866): unstash one, the next follows when
                it is handled or the update completes."""
                if get_shard_home_requests:
                    ctx.self.tell(get_shard_home_requests.popleft())

            # --- ShardCoordinator: active (740-967) ----------------------------------------------

            def active() -> Behavior[Any]:
                """active (740-967), with receiveLateRememberedEntities (1875-1889) and the
                store's end (1950-1954). ``StopShards`` (1118) has no ohra counterpart."""

                def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                    nonlocal preparing_for_shutdown
                    match msg:
                        case Register(region):
                            register(region)
                        case RegisterProxy(proxy):
                            register_proxy(proxy)
                        case GetShardHome(shard, reply_to):
                            get_shard_home(shard, reply_to)
                        case ShardStarted(shard):
                            ctx.timers.cancel(("host", shard))
                        case _ResendShardHost(shard, region):
                            if state.shards.get(shard) == region and region in refs:
                                send_host_shard(shard, region)
                        case MemberRemoved(member=m):
                            on_member_removed(m)
                        case MemberPreparingForShutdown() | MemberReadyForShutdown():
                            # 949-956
                            if not preparing_for_shutdown:
                                log.info(
                                    "%s: Shard coordinator detected prepare for full cluster "
                                    "shutdown. No new rebalances will take place.",
                                    type_name,
                                )
                                ctx.timers.cancel("rebalance")
                                preparing_for_shutdown = True
                        case ClusterDomainEvent():
                            pass
                        case _RebalanceTick() | _RebalanceDone() | GracefulShutdownReq():
                            on_rebalance_message(msg)
                        case RegionStopped(path):
                            # receiveTerminated (1093-1103): a region, else a proxy
                            if path in state.region_proxies:
                                region_proxy_terminated(path)
                            else:
                                region_terminated(path)
                        case _DelayedShardRegionTerminated(path):
                            region_terminated(path)
                        case _Terminate():
                            terminate()
                        case GetClusterShardingStats(timeout, reply_to):
                            cluster_sharding_stats(timeout, reply_to)
                        case _RememberEntitiesStoreStopped():
                            # receiveTerminated (1950-1954)
                            return on_remember_entities_store_stopped()
                        case _:
                            receive_late_remembered_entities(msg)
                    if stop_now:
                        return Behaviors.stopped()
                    return take_become(Behaviors.same)

                return Behaviors.receive(on_message).receive_signal(on_signal)

            def cluster_sharding_stats(
                timeout: float, reply_to: ActorRef[ClusterShardingStats]
            ) -> None:
                """active's GetClusterShardingStats (917-940): a child asks every alive region for
                its stats within ``timeout``, and replies them by the address of each region's
                node, or nothing when a region did not answer, as Pekko's ``recover`` of the
                failed ``Future.sequence``. Pekko maps a local region's address to the cluster's
                self address; an ohra path always carries its node's address."""

                def done(responses: dict[str, ShardRegionStats], failed: frozenset[str]) -> None:
                    if failed:
                        reply_to.tell(ClusterShardingStats({}))
                    else:
                        regions = {_address_of(path): stats for path, stats in responses.items()}
                        reply_to.tell(ClusterShardingStats(regions))

                regions = {p: refs[p] for p in alive_regions if p in refs}
                ctx.spawn_anonymous(_ask_all(regions, GetShardRegionStats, timeout, done))

            # --- registration and shard homes ----------------------------------------------------

            def register(region: ActorRef[Any]) -> None:
                """active's Register (742-766)."""
                path = region.path
                member = member_for(region)
                if member is None or member.status not in _OF_INTEREST:
                    log.debug(
                        "%s: ShardRegion [%s] was not registered since the coordinator currently "
                        "does not know about a node of that region",
                        type_name,
                        path,
                    )
                    return
                node = member.unique_address
                known = state.region_nodes.get(path)
                if known is not None and known != node:
                    # A new incarnation at the address of one in the state (D5): the old process
                    # is gone for certain, so its shards are released now and the registration
                    # follows. Pekko's refs carry the uid, so there the new incarnation is simply
                    # another region.
                    ctx.timers.cancel(("region-removed", path))
                    region_terminated(path)
                    ctx.self.tell(Register(region))
                    return
                log.debug("%s: ShardRegion registered: [%s]", type_name, path)
                refs[path] = region
                alive_regions.add(path)
                if path in state.regions:
                    region.tell(RegisterAck(ctx.self))
                    inform_about_current_shards(region)
                    allocate_shard_homes_for_remember_entities()
                else:
                    graceful_shutdown_in_progress.discard(path)
                    # we don't have to wait for updated state for this
                    inform_about_current_shards(region)

                    def registered(evt: ShardRegionRegistered) -> None:
                        nonlocal state
                        state = state.updated(evt)
                        # Pekko's RegionStopped: the local region's end is what Terminate waits
                        # for
                        if is_local_actor(region):
                            ctx.watch_with(region, RegionStopped(path))
                        region.tell(RegisterAck(ctx.self))
                        allocate_shard_homes_for_remember_entities()

                    update(ShardRegionRegistered(path, node), registered)

            def register_proxy(proxy: ActorRef[Any]) -> None:
                """active's RegisterProxy (767-779). Its node is recorded as a region's is (D5),
                and a local proxy is watched; a remote one is seen through its RegionStopped and
                its member's removal."""
                path = proxy.path
                member = member_for(proxy)
                if member is None or member.status not in _OF_INTEREST:
                    return
                log.debug("%s: ShardRegion proxy registered: [%s]", type_name, path)
                # we don't have to wait for updated state for this
                inform_about_current_shards(proxy)
                if path in state.region_proxies:
                    refs[path] = proxy
                    proxy.tell(RegisterAck(ctx.self))
                    return

                def registered(evt: ShardRegionProxyRegistered) -> None:
                    nonlocal state
                    state = state.updated(evt)
                    refs[path] = proxy
                    if is_local_actor(proxy):
                        ctx.watch_with(proxy, RegionStopped(path))
                    proxy.tell(RegisterAck(ctx.self))

                update(ShardRegionProxyRegistered(path, member.unique_address), registered)

            def inform_about_current_shards(ref: ActorRef[Any]) -> None:
                homes = tuple(
                    (refs[r], tuple(s for s in shards if s not in rebalance_in_progress))
                    for r, shards in state.regions.items()
                    if shards and r in refs
                )
                if homes:
                    ref.tell(ShardHomes(homes))

            def handle_get_shard_home(shard: str, reply_to: ActorRef[Any]) -> bool:
                """handleGetShardHome (1061-1091): True when answered without a state update,
                that is known, deferred or ignored."""
                if shard in rebalance_in_progress:
                    # deferred until the hand-off completes
                    rebalance_in_progress[shard].append(reply_to)
                    unstash_one_get_shard_home_request()  # continue unstashing
                    return True
                if not has_all_regions_registered():
                    log.debug(
                        "%s: GetShardHome [%s] request from [%s] ignored, because not all "
                        "regions have registered yet.",
                        type_name,
                        shard,
                        reply_to.path,
                    )
                    return True
                known = state.shards.get(shard)
                if known is None:
                    return False  # location not known, yet, caller will handle allocation
                if known in region_termination_in_progress:
                    log.debug(
                        "%s: GetShardHome [%s] request ignored, due to region [%s] termination "
                        "in progress.",
                        type_name,
                        shard,
                        known,
                    )
                else:
                    reply_to.tell(ShardHome(shard, refs[known]))
                unstash_one_get_shard_home_request()  # continue unstashing
                return True

            def has_all_regions_registered() -> bool:
                """hasAllRegionsRegistered (1209-1216): the check is only for startup, i.e. once
                all have registered we don't check more."""
                nonlocal all_regions_registered
                if not all_regions_registered:
                    all_regions_registered = len(alive_regions) >= min_members
                return all_regions_registered

            def get_shard_home(shard: str, reply_to: ActorRef[Any]) -> None:
                """active's GetShardHome (788-811)."""
                if handle_get_shard_home(shard, reply_to):
                    return
                candidates = [
                    r
                    for r in state.regions
                    if r not in graceful_shutdown_in_progress
                    and r not in region_termination_in_progress
                ]
                if not candidates:
                    return
                region = strategy.allocate_shard(
                    shard,
                    {r: state.regions[r] for r in candidates},
                    members_by_region(candidates),
                )
                if region is not None:
                    continue_get_shard_home(shard, region, reply_to)

            def continue_get_shard_home(shard: str, region: str, reply_to: ActorRef[Any]) -> None:
                """continueGetShardHome (1276-1314)."""
                if shard in rebalance_in_progress:
                    rebalance_in_progress[shard].append(reply_to)
                    return
                known = state.shards.get(shard)
                if known is not None:
                    reply_to.tell(ShardHome(shard, refs[known]))
                    return
                if (
                    region in state.regions
                    and region not in graceful_shutdown_in_progress
                    and region not in region_termination_in_progress
                ):

                    def allocated(evt: ShardHomeAllocated) -> None:
                        nonlocal state
                        state = state.updated(evt)
                        log.info(
                            "%s: Shard [%s] allocated at [%s]", type_name, evt.shard, evt.region
                        )
                        send_host_shard(evt.shard, evt.region)
                        reply_to.tell(ShardHome(evt.shard, refs[evt.region]))

                    update(ShardHomeAllocated(shard, region), allocated)
                else:
                    log.debug(
                        "%s: Allocated region [%s] for shard [%s] is not (any longer) one of the "
                        "registered regions.",
                        type_name,
                        region,
                        shard,
                    )

            def send_host_shard(shard: str, region: str) -> None:
                refs[region].tell(HostShard(shard, ctx.self))
                ctx.timers.start_single_timer(
                    ("host", shard), _ResendShardHost(shard, region), settings.shard_start_timeout
                )

            def region_terminated(path: str) -> None:
                """regionTerminated (1218-1245)."""
                for worker in rebalance_workers.values():
                    worker.tell(_RegionTerminatedForWorker(path))
                if path not in state.regions:
                    return
                log.info("%s: ShardRegion terminated: [%s]", type_name, path)
                region_termination_in_progress.add(path)
                for shard in state.regions[path]:
                    ctx.self.tell(GetShardHome(shard, ctx.system.ignore_ref))

                def terminated(evt: ShardRegionTerminated) -> None:
                    nonlocal state
                    state = state.updated(evt)
                    graceful_shutdown_in_progress.discard(path)
                    region_termination_in_progress.discard(path)
                    alive_regions.discard(path)
                    refs.pop(path, None)
                    allocate_shard_homes_for_remember_entities()
                    if waiting_for_local_region_to_terminate and _is_at(
                        path, cluster.self_address
                    ):
                        # handoff optimization: the local region completed its hand-off
                        terminate()

                update(ShardRegionTerminated(path), terminated)

            def region_proxy_terminated(path: str) -> None:
                """regionProxyTerminated (1249-1256)."""
                for worker in rebalance_workers.values():
                    worker.tell(_RegionTerminatedForWorker(path))
                if path not in state.region_proxies:
                    return
                log.debug("%s: ShardRegion proxy terminated: [%s]", type_name, path)

                def terminated(evt: ShardRegionProxyTerminated) -> None:
                    nonlocal state
                    state = state.updated(evt)
                    refs.pop(path, None)

                update(ShardRegionProxyTerminated(path), terminated)

            def terminate() -> None:
                """Pekko's Terminate: stop at once, unless the local region still has shards to
                hand off."""
                nonlocal waiting_for_local_region_to_terminate, stop_now
                local = [r for r in alive_regions if _is_at(r, cluster.self_address)]
                if local:
                    waiting_for_local_region_to_terminate = True
                else:
                    stop_now = True

            def on_member_removed(m: Member) -> None:
                """A removed member's regions are terminated, in place of receiveTerminated
                (1093-1105) for its address. One still alive, that never said it stopped, may
                still run its shards on a crashed or partitioned node, so it waits the
                down-removal margin (Pekko's addressTerminated on a live region); one that stopped
                after handing off told us so and is already gone. Only a region of that
                incarnation is terminated (D5)."""
                margin = cluster.settings.effective_down_removal_margin
                for path in [r for r, n in state.region_nodes.items() if n == m.unique_address]:
                    if margin > 0 and path in alive_regions:
                        # GetShardHome for its shards waits, as Pekko's
                        region_termination_in_progress.add(path)
                        ctx.timers.start_single_timer(
                            ("region-removed", path), _DelayedShardRegionTerminated(path), margin
                        )
                    else:
                        region_terminated(path)
                # a proxy has no shards to keep: no margin, as Pekko's regionProxyTerminated
                for path in [p for p, n in state.region_proxies.items() if n == m.unique_address]:
                    region_proxy_terminated(path)

            # --- rebalance and hand-off ----------------------------------------------------------

            def on_rebalance_message(msg: Any) -> None:
                match msg:
                    case _RebalanceTick():
                        if state.regions and not preparing_for_shutdown:
                            shards = strategy.rebalance(
                                dict(state.regions),
                                members_by_region(state.regions),
                                frozenset(rebalance_in_progress),
                                cluster.state,
                            )
                            continue_rebalance(shards)
                    case _RebalanceDone(shard, ok):
                        rebalance_done(shard, ok)
                    case GracefulShutdownReq(region):
                        path = region.path
                        if path not in graceful_shutdown_in_progress and path in state.regions:
                            log.info(
                                "%s: Graceful shutdown of region [%s] with shards [%s] started",
                                type_name,
                                path,
                                state.regions[path],
                            )
                            graceful_shutdown_in_progress.add(path)
                            shutdown_shards(path, state.regions[path])
                    case _:
                        pass

            def continue_rebalance(shards: frozenset[str]) -> None:
                if shards:
                    log.info(
                        "%s: Starting rebalance for shards [%s]. In progress: [%s]",
                        type_name,
                        sorted(shards),
                        sorted(rebalance_in_progress),
                    )
                for shard in shards:
                    if shard not in rebalance_in_progress and shard in state.shards:
                        start_shard_rebalance(shard, state.shards[shard], is_rebalance=True)

            def shutdown_shards(region: str, shards: tuple[str, ...]) -> None:
                for shard in shards:
                    if shard not in rebalance_in_progress:
                        start_shard_rebalance(shard, region, is_rebalance=False)

            def start_shard_rebalance(shard: str, from_region: str, is_rebalance: bool) -> None:
                if from_region not in refs:
                    return
                rebalance_in_progress[shard] = []
                # 1340: the regions and the proxies
                regions = {
                    r: refs[r] for r in (*alive_regions, *state.region_proxies) if r in refs
                }
                worker = ctx.spawn_anonymous(
                    _rebalance_worker(
                        type_name,
                        shard,
                        refs[from_region],
                        regions,
                        settings.handoff_timeout,
                        is_rebalance,
                    )
                )
                rebalance_workers[shard] = worker

            def rebalance_done(shard: str, ok: bool) -> None:
                """active's RebalanceDone (849-890)."""
                rebalance_workers.pop(shard, None)
                if ok:
                    # The shard could have been removed by ShardRegionTerminated
                    if shard in state.shards:

                        def deallocated(evt: ShardHomeDeallocated) -> None:
                            nonlocal state
                            state = state.updated(evt)
                            clear_rebalance_in_progress(shard)
                            allocate_shard_homes_for_remember_entities()
                            # reallocate at once
                            ctx.self.tell(GetShardHome(shard, ctx.system.ignore_ref))

                        update(ShardHomeDeallocated(shard), deallocated)
                    else:
                        clear_rebalance_in_progress(shard)
                else:
                    log.warning(
                        "%s: Shard [%s] deallocation did not complete within [%s]s",
                        type_name,
                        shard,
                        settings.handoff_timeout,
                    )
                    region = state.shards.get(shard)
                    if region is not None:
                        graceful_shutdown_in_progress.discard(region)
                    clear_rebalance_in_progress(shard)

            def clear_rebalance_in_progress(shard: str) -> None:
                """clearRebalanceInProgress (1000-1010)."""
                for asker in rebalance_in_progress.pop(shard, []):
                    ctx.self.tell(GetShardHome(shard, asker))

            unbecome_to = waiting_for_initial_state
            # get state from ddata replicator, repeat until GetSuccess
            get_coordinator_state()
            if settings.remember_entities:
                get_all_remembered_shards()
            return waiting_for_initial_state()

        return setup

    return Behaviors.with_stash(_STASH_CAPACITY, coordinator)


# --- the region (ShardRegion) --------------------------------------------------------------------
