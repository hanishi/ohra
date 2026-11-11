"""Sharding: one entity actor per id, created on its first message, passivated by the passivation
strategy or on request.

Apache Pekko's layering: a region routes each message by shard id to a Shard actor (Pekko's
``Shard``, Shard.scala 424-1200), which hosts the entities of that shard and passivates them by
``ClusterShardingSettings.passivation_strategy_settings``, idle after 120 s by default.
``Sharding.init`` is the region inside one process; ``ClusterSharding`` in
``ohra.cluster.sharding`` runs the same Shards under regions that span the cluster. Messages for a
passivating entity are buffered, up to ``buffer_size`` for the whole shard, and delivered to its
next incarnation; what is still buffered when the shard stops is dropped, and a shard handing off
leaves every message unhandled. An entity that stops on its own, by returning
``Behaviors.stopped()``, loses what is queued behind it, as in Pekko, so entities should passivate
rather than stop themselves.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import zlib
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import quote

from ...behavior import Behavior, Behaviors, StashBuffer
from ...context import ActorContext
from ...coordinated_shutdown import CoordinatedShutdown
from ...coordination.lease import Lease, LeaseProvider, LeaseUsageSettings
from ...eventstream import DeadLetterSuppression
from ...ref import ActorRef
from ...remote.serialization import remotable
from ...result import Failure, Success, Try
from ...signals import PostStop, Signal, Terminated
from ...util.message_buffer import MessageBufferMap
from ..ddata.replicator import Replicator
from ..ddata.settings import ReplicatorSettings
from .entities import (
    Active,
    Entities,
    Passivating,
    RememberedButNotCreated,
    RememberingStart,
    RememberingStop,
    WaitingForRestart,
)
from .passivation import EntityPassivationStrategy, PassivateEntities
from .passivation_settings import (
    NoPassivationStrategy,
    PassivationStrategy,
    PassivationStrategySettings,
    describe_passivation_strategy,
    passivation_strategy,
)

if TYPE_CHECKING:
    from ..core import ClusterSettings
    from ..singleton import ClusterSingletonSettings


def _child_name(entity_id: str) -> str:
    """Any id becomes a valid, unique child name. Pure-dot ids are escaped so paths never read as
    '.' or '..'."""
    name = quote(entity_id, safe="")
    return name.replace(".", "%2E") if set(name) == {"."} else name


def shard_id_of(entity_id: str, number_of_shards: int) -> str:
    """Pekko's HashCodeMessageExtractor with a hash that is the same on every node."""
    return str(zlib.crc32(entity_id.encode()) % number_of_shards)


@dataclass(frozen=True)
class ClusterShardingSettings:
    """Pekko's cluster sharding settings with their defaults, in seconds. The single-node region
    uses ``number_of_shards``, ``buffer_size``, ``handoff_timeout`` and
    ``shard_region_query_timeout`` too, and the shard passivates its entities by
    ``passivation_strategy_settings``."""

    number_of_shards: int = 1000
    retry_interval: float = 2.0
    rebalance_interval: float = 10.0
    handoff_timeout: float = 60.0
    shard_start_timeout: float = 10.0
    coordinator_failure_backoff: float = 5.0
    buffer_size: int = 100_000
    """The messages a region buffers, and a shard buffers across its entities (buffer-size)."""
    rebalance_absolute_limit: int = 0
    """0 means no absolute cap, as Pekko."""
    rebalance_relative_limit: float = 0.1
    shard_region_query_timeout: float = 3.0
    coordinator_singleton_settings: ClusterSingletonSettings | None = None
    """Settings of the coordinator's singleton manager (Pekko's coordinator-singleton); None means
    the defaults."""
    waiting_for_state_timeout: float = 2.0
    """How long the coordinator waits for its state to be read from the Replicator (Pekko's
    waiting-for-state-timeout, reference.conf)."""
    updating_state_timeout: float = 5.0
    """How long a state update to the Replicator may take (updating-state-timeout)."""
    coordinator_state_read_majority_plus: int | Literal["all"] = 5
    """Additional nodes beyond a majority the state is read from; "all" reads from all nodes
    (coordinator-state.read-majority-plus)."""
    coordinator_state_write_majority_plus: int | Literal["all"] = 3
    """As above for writes (coordinator-state.write-majority-plus)."""
    majority_min_cap: int = 5
    """Minimum number of nodes a majority-plus read or write reaches
    (distributed-data.majority-min-cap)."""
    distributed_data: ReplicatorSettings | None = None
    """Sharding's own Replicator (ClusterSharding.scala 730-815); None takes the cluster's
    distributed_data with prefer_oldest=True and max_delta_elements=5. Never durable."""

    passivation_strategy_settings: PassivationStrategySettings = (
        PassivationStrategySettings.default_idle_strategy
    )
    """Pekko's passivationStrategySettings; the default passivates idle entities after 120 s."""

    verbose_debug_logging: bool = False
    """Debug logs per routed message (verbose-debug-logging)."""
    fail_on_invalid_entity_state_transition: bool = False
    """An invalid entity state transition crashes the shard instead of being logged
    (fail-on-invalid-entity-state-transition)."""
    entity_restart_backoff: float = 10.0
    """How long a remembered entity that stopped without passivating waits to restart
    (entity-restart-backoff). Unused until remembered entities exist."""
    shard_failure_backoff: float = 10.0
    """How long a remembering shard that failed to store waits to restart
    (shard-failure-backoff). Unused until remembered entities exist."""
    lease_settings: LeaseUsageSettings | None = None
    """The lease each shard acquires before it starts its entities, and how long it waits to
    retry a failed acquire (use-lease, lease-retry-interval); None means no lease."""

    def __post_init__(self) -> None:
        for name in (
            "retry_interval", "rebalance_interval", "handoff_timeout", "shard_start_timeout",
            "coordinator_failure_backoff", "shard_region_query_timeout",
            "entity_restart_backoff", "shard_failure_backoff",
        ):  # fmt: skip
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        if self.number_of_shards < 1 or self.buffer_size < 1:
            raise ValueError("number_of_shards and buffer_size must be >= 1")
        if self.rebalance_absolute_limit < 0:
            raise ValueError("rebalance_absolute_limit must be >= 0")
        if not 0.0 < self.rebalance_relative_limit <= 1.0:
            raise ValueError("rebalance_relative_limit must be in (0, 1]")
        for name in ("waiting_for_state_timeout", "updating_state_timeout"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        for name in (
            "coordinator_state_read_majority_plus",
            "coordinator_state_write_majority_plus",
        ):
            v = getattr(self, name)
            if v != "all" and (not isinstance(v, int) or v < 0):
                raise ValueError(f"{name} must be an int >= 0 or 'all'")
        if self.majority_min_cap < 0:
            raise ValueError("majority_min_cap must be >= 0")

    def state_read_consistency(self) -> Replicator.ReadConsistency:
        """ClusterSharding.scala 782-788 (``stateReadConsistency``)."""
        n = self.coordinator_state_read_majority_plus
        if n == "all":
            return Replicator.ReadAll(self.waiting_for_state_timeout)
        return Replicator.ReadMajorityPlus(
            self.waiting_for_state_timeout, n, self.majority_min_cap
        )

    def state_write_consistency(self) -> Replicator.WriteConsistency:
        """ClusterSharding.scala 790-796 (``stateWriteConsistency``)."""
        n = self.coordinator_state_write_majority_plus
        if n == "all":
            return Replicator.WriteAll(self.updating_state_timeout)
        return Replicator.WriteMajorityPlus(self.updating_state_timeout, n, self.majority_min_cap)

    def effective_replicator_settings(
        self, cluster_settings: ClusterSettings
    ) -> ReplicatorSettings:
        """ClusterSharding.scala 730-745: sharding's data is never durable."""
        if self.distributed_data is None:
            base = replace(
                cluster_settings.distributed_data, prefer_oldest=True, max_delta_elements=5
            )
        else:
            base = self.distributed_data
        return replace(base, durable_keys=frozenset())

    @property
    def passivation_strategy(self) -> PassivationStrategy:
        """Classic 941-942: what the settings resolve to."""
        return passivation_strategy(self.passivation_strategy_settings)

    def with_passivation_strategy(
        self, settings: PassivationStrategySettings
    ) -> ClusterShardingSettings:
        """Typed 833-834."""
        return replace(self, passivation_strategy_settings=settings)

    def with_no_passivation_strategy(self) -> ClusterShardingSettings:
        """Typed 836-837."""
        return replace(self, passivation_strategy_settings=PassivationStrategySettings.disabled)

    def with_lease_settings(self, lease_settings: LeaseUsageSettings) -> ClusterShardingSettings:
        """Classic 975."""
        return replace(self, lease_settings=lease_settings)

    @property
    def entity_hand_off_timeout(self) -> float:
        """What a Shard gives its entities to stop on hand-off, as Pekko's entityHandOffTimeout."""
        return max(self.handoff_timeout - 5.0, 1.0)


@dataclass(frozen=True)
class EntityTypeKey[T]:
    """Names an entity type; the type parameter is the entity's message type."""

    name: str


class ShardCommand:
    """Messages an entity may send to its shard (``EntityContext.shard``)."""

    __slots__ = ()


@dataclass(frozen=True)
class Passivate(ShardCommand):
    """Sent by an entity to ask the shard to stop it. Messages meanwhile are buffered for the next
    incarnation."""

    entity: ActorRef[Any]


@dataclass(frozen=True)
class EntityContext[T]:
    entity_type_key: EntityTypeKey[T]
    entity_id: str
    shard: ActorRef[ShardCommand]


@remotable
@dataclass(frozen=True)
class ShardingEnvelope[T]:
    """What the region accepts: a message addressed to an entity id."""

    entity_id: str
    message: T


@dataclass(frozen=True)
class CurrentShardRegionState:
    """The live entity ids, and the shard ids this region hosts (a moved shard may hold no entity
    yet)."""

    entity_ids: frozenset[str]
    shard_ids: frozenset[str] = frozenset()


@dataclass(frozen=True)
class GetShardRegionState:
    reply_to: ActorRef[CurrentShardRegionState]


@dataclass(frozen=True)
class CurrentShardState:
    shard: str
    entity_ids: frozenset[str]


@dataclass(frozen=True)
class GetShardState:
    reply_to: ActorRef[CurrentShardState]


@remotable
@dataclass(frozen=True)
class ShardStopped:
    shard: str


@remotable
@dataclass(frozen=True)
class HandOff:
    """Stop this shard and every entity in it, then answer ``ShardStopped``; a rebalance or a
    leaving node."""

    shard: str
    reply_to: ActorRef[ShardStopped]


@dataclass(frozen=True)
class Entity[T]:
    """How to create an entity of a type, plus its sharding settings."""

    type_key: EntityTypeKey[T]
    create_behavior: Callable[[EntityContext[T]], Behavior[T]]
    stop_message: T | None = None
    # None takes the value from settings; a value set here wins over settings
    buffer_size: int | None = None
    settings: ClusterShardingSettings | None = None

    def with_buffer_size(self, size: int) -> Entity[T]:
        """Pekko's buffer-size: caps the messages a shard buffers for its passivating entities,
        in total across them, and the messages the region buffers. The shard drops what is
        beyond it."""
        return replace(self, buffer_size=size)

    def with_stop_message(self, msg: T) -> Entity[T]:
        """Passivation delivers msg and lets the entity stop itself, instead of stopping it
        outright."""
        return replace(self, stop_message=msg)

    def with_settings(self, settings: ClusterShardingSettings) -> Entity[T]:
        return replace(self, settings=settings)

    @property
    def effective_settings(self) -> ClusterShardingSettings:
        """``settings``, with ``buffer_size`` set on the entity on top, whichever order they
        were set in."""
        settings = self.settings or ClusterShardingSettings()
        if self.buffer_size is not None:
            settings = replace(settings, buffer_size=self.buffer_size)
        return settings


class EntityRef[T](ActorRef[T]):
    """A ref to an entity by id. The entity need not exist yet: the region creates it on the first
    message."""

    __slots__ = ("_region", "entity_id")

    def __init__(self, region: ActorRef[ShardingEnvelope[T]], entity_id: str) -> None:
        self._region = region
        self.entity_id = entity_id

    @property
    def path(self) -> str:
        return f"{self._region.path}/{_child_name(self.entity_id)}"

    @property
    def region(self) -> ActorRef[ShardingEnvelope[T]]:
        return self._region

    def tell(self, msg: T) -> None:
        self._region.tell(ShardingEnvelope(self.entity_id, msg))

    async def offer(self, msg: T) -> None:
        await self._region.offer(ShardingEnvelope(self.entity_id, msg))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, EntityRef) and (self.path, self.entity_id) == (
            other.path,
            other.entity_id,
        )

    def __hash__(self) -> int:
        return hash((self._region.path, self.entity_id))


@dataclass(frozen=True)
class _QueryTimeout:
    pass


@dataclass(frozen=True)
class ShardInitialized:
    """ShardRegion.scala 235: a shard tells its region it is ready for messages."""

    shard_id: str


@dataclass(frozen=True)
class SetActiveEntityLimit:
    """ShardRegion.scala 505: the region's active entity limit, for the shard's passivation
    strategy."""

    per_region_limit: int


@dataclass(frozen=True)
class ShardsUpdated(DeadLetterSuppression):
    """ShardRegion.scala 514: the number of shards active in the region changed."""

    active_shards: int


@remotable
@dataclass(frozen=True)
class ShardStats:
    """Shard.scala 92: the number of entities of a shard, passivating ones included."""

    shard_id: str
    entity_count: int


@remotable
@dataclass(frozen=True)
class GetShardStats:
    """Shard.scala 89."""

    reply_to: ActorRef[ShardStats]


@dataclass(frozen=True)
class _EntityTerminated:
    """Shard.scala 124: an entity's termination, watched with this message."""

    ref: ActorRef[Any]


@dataclass(frozen=True)
class _PassivateIntervalTick:
    """Shard.scala 122: the passivation strategy's interval has passed."""


@dataclass(frozen=True)
class _LeaseAcquireResult(DeadLetterSuppression):
    """Shard.scala 94."""

    acquired: bool
    reason: BaseException | None


@dataclass(frozen=True)
class _LeaseLost(DeadLetterSuppression):
    """Shard.scala 95."""

    reason: BaseException | None


@dataclass(frozen=True)
class _LeaseRetry(DeadLetterSuppression):
    """Shard.scala 97."""


_LEASE_RETRY_TIMER = "lease-retry"
"""Shard.scala 98."""


type _ShardCmd[T] = (
    ShardingEnvelope[T]
    | ShardCommand
    | HandOff
    | GetShardState
    | GetShardStats
    | SetActiveEntityLimit
    | ShardsUpdated
    | _EntityTerminated
    | _PassivateIntervalTick
)


class _Shard[T]:
    """Pekko's ``Shard`` actor (Shard.scala 424-1200), for one shard id of one entity type.
    Remembered entities wait for sub-project 10, so the shard is never remembering: it starts
    in ``awaiting_lease`` or ``idle``, and each method below leaves out its remember-store
    branches. The member events of a full cluster shutdown (Task 6) are not ported yet.

    ``self.receive`` is Pekko's ``context.become``. Pekko's ``context.parent`` is ``region``,
    ``context.stop(self)`` is returning ``Behaviors.stopped()``, and a typed message has no
    sender, so the envelope is what is buffered and redelivered."""

    def __init__(
        self,
        ctx: ActorContext[Any],
        entity: Entity[T],
        shard_id: str,
        region: ActorRef[Any],
        stash: StashBuffer[Any],
    ) -> None:
        """Shard.scala 449-506. Pekko's ``Stash`` is ``stash``, and ``preStart`` is the end of
        this constructor."""
        self.ctx = ctx
        self.stash = stash
        self.entity = entity
        self.shard_id = shard_id
        self.region = region
        self.settings = entity.effective_settings
        self.type_name = entity.type_key.name
        self.log = ctx.log
        self.verbose_debug = self.settings.verbose_debug_logging
        self.entities = Entities(
            ctx.log,
            False,
            self.verbose_debug,
            self.settings.fail_on_invalid_entity_state_transition,
        )
        # Messages are buffered while an entity is passivating
        self.message_buffers = MessageBufferMap[str]()
        self.hand_off_stopper: ActorRef[Any] | None = None
        self.passivation_strategy = EntityPassivationStrategy.create(
            self.settings, lambda: ctx.system.scheduler.current_time
        )
        interval = self.passivation_strategy.scheduled_interval
        if interval is not None:
            ctx.timers.start_timer_with_fixed_delay(
                _PassivateIntervalTick, _PassivateIntervalTick(), interval
            )
        lease_settings = self.settings.lease_settings
        self.lease: Lease | None = None
        if lease_settings is not None:
            system = ctx.system
            cluster = system.cluster
            if cluster is None:
                owner = system.name
            else:  # Pekko's selfAddress.hostPort
                a = cluster.self_address
                owner = f"{a.name}@{a.host}:{a.port}"
            self.lease = LeaseProvider(system).get_lease(
                f"{system.name}-shard-{self.type_name}-{shard_id}",
                lease_settings.lease_implementation,
                owner,
            )
        self.lease_retry_interval = (
            lease_settings.lease_retry_interval if lease_settings is not None else 5.0  # not used
        )
        self.receive: Callable[[Any], Behavior[Any]] = self.idle
        self.acquire_lease_if_needed()

    def behavior(self) -> Behavior[Any]:
        def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            return self.receive(msg)

        async def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
            if isinstance(sig, Terminated):
                return self.receive_terminated(sig.ref)
            if sig is PostStop:
                await self.post_stop()
                return Behaviors.same
            return Behaviors.unhandled

        return Behaviors.receive(on_message).receive_signal(on_signal)

    def acquire_lease_if_needed(self) -> None:
        """Shard.scala 508-516."""
        if self.lease is not None:
            self.try_get_lease(self.lease)
            self.receive = self.awaiting_lease
        else:
            # Pekko's tryLoadRememberedEntities without a store
            self.shard_initialized()

    def awaiting_lease(self, msg: Any) -> Behavior[Any]:
        """Shard.scala 518-544. ShardInitialized is not sent back, so the region buffers the
        messages for this shard while it waits for the lease. Once acquired it goes straight
        to ``shard_initialized``: Pekko's ``tryLoadRememberedEntities`` (563-576) without a
        remember store. The member events come with Task 6."""
        match msg:
            case _LeaseAcquireResult(True, _):
                self.log.debug("%s: Lease acquired", self.type_name)
                return self.shard_initialized()
            case _LeaseAcquireResult(False, None):
                self.log.error(
                    "%s: Failed to get lease for shard id [%s]. Retry in %s",
                    self.type_name,
                    self.shard_id,
                    f"{self.lease_retry_interval:g} s",
                )
                self.ctx.timers.start_single_timer(
                    _LEASE_RETRY_TIMER, _LeaseRetry(), self.lease_retry_interval
                )
            case _LeaseAcquireResult(False, t):
                self.log.error(
                    "%s: Failed to get lease for shard id [%s]. Retry in %s",
                    self.type_name,
                    self.shard_id,
                    f"{self.lease_retry_interval:g} s",
                    exc_info=t,
                )
                self.ctx.timers.start_single_timer(
                    _LEASE_RETRY_TIMER, _LeaseRetry(), self.lease_retry_interval
                )
            case _LeaseRetry():
                assert self.lease is not None
                self.try_get_lease(self.lease)
            case _LeaseLost():
                return self.receive_lease_lost(msg)
            case _:
                if self.verbose_debug:
                    self.log.debug(
                        "%s: Got msg of type [%s] while waiting for lease, stashing",
                        self.type_name,
                        type(msg).__name__,
                    )
                self.stash.stash(msg)
        return Behaviors.same

    def try_get_lease(self, lease: Lease) -> None:
        """Shard.scala 555-560."""
        self.log.info("%s: Acquiring lease %s", self.type_name, lease.settings)
        me = self.ctx.self

        def adapt(result: Try[bool]) -> Any:
            match result:
                case Success(acquired):
                    return _LeaseAcquireResult(acquired, None)
                case Failure(exc):
                    return _LeaseAcquireResult(False, exc)

        self.ctx.pipe_to_self(lease.acquire(lambda reason: me.tell(_LeaseLost(reason))), adapt)

    def shard_initialized(self) -> Behavior[Any]:
        """Shard.scala 614-619."""
        self.log.debug("%s: Shard initialized", self.type_name)
        self.region.tell(ShardInitialized(self.shard_id))
        self.receive = self.idle
        return self.stash.unstash_all(Behaviors.same)

    def idle(self, msg: Any) -> Behavior[Any]:
        """Shard.scala 624-639, without the remember-entity commands, ``StartEntity`` and the
        store's crash; the member events come with Task 6. ``Terminated`` is a signal, handled
        in ``behavior``."""
        match msg:
            case _EntityTerminated(ref):
                self.entity_terminated(ref)
            case HandOff():
                return self.receive_coordinator_message(msg)
            case SetActiveEntityLimit():
                self.active_entity_limit_updated(msg)
            case ShardsUpdated():
                self.shards_updated(msg)
            case Passivate(ref):
                self.passivate(ref, self.entity.stop_message)
            case _PassivateIntervalTick():
                self.passivate_entities_after_interval()
            case GetShardState() | GetShardStats():
                self.receive_shard_query(msg)
            case _LeaseLost():
                return self.receive_lease_lost(msg)
            case ShardingEnvelope():
                self.deliver_message(cast(ShardingEnvelope[T], msg))
            case _:
                return Behaviors.unhandled
        return Behaviors.same

    def receive_coordinator_message(self, msg: HandOff) -> Behavior[Any]:
        """Shard.scala 852-858. ``HandOff`` is the only coordinator message a shard gets."""
        if msg.shard == self.shard_id:
            return self.hand_off(msg.reply_to)
        self.log.warning(
            "%s: Shard [%s] can not hand off for another Shard [%s]",
            self.type_name,
            self.shard_id,
            msg.shard,
        )
        return Behaviors.same

    def receive_shard_query(self, msg: GetShardState | GetShardStats) -> None:
        """Shard.scala 860-870. ``GetShardState`` is Pekko's ``GetCurrentShardState``."""
        match msg:
            case GetShardState(reply_to):
                if self.verbose_debug:
                    self.log.debug(
                        "%s: GetCurrentShardState, full state: [%s], active: [%s]",
                        self.type_name,
                        self.entities,
                        self.entities.active_entity_ids(),
                    )
                reply_to.tell(CurrentShardState(self.shard_id, self.entities.active_entity_ids()))
            case GetShardStats(reply_to):
                reply_to.tell(ShardStats(self.shard_id, self.entities.size))

    def hand_off(self, reply_to: ActorRef[ShardStopped]) -> Behavior[Any]:
        """Shard.scala 872-905, without the ``preparingForShutdown`` branch (Task 6)."""
        if self.hand_off_stopper is not None:
            self.log.warning(
                "%s: HandOff shard [%s] received during existing handOff",
                self.type_name,
                self.shard_id,
            )
            return Behaviors.same
        self.log.debug("%s: HandOff shard [%s]", self.type_name, self.shard_id)
        # does conversion so only do once
        active_entities = self.entities.active_entities()
        if active_entities:
            entity_hand_off_timeout = self.settings.entity_hand_off_timeout
            self.log.debug(
                "%s: Starting HandOffStopper for shard [%s] to terminate [%d] entities.",
                self.type_name,
                self.shard_id,
                len(active_entities),
            )
            for ref in active_entities:
                self.ctx.unwatch(ref)
            stopper = self.ctx.spawn(
                hand_off_stopper(
                    self.type_name,
                    self.shard_id,
                    reply_to,
                    active_entities,
                    self.entity.stop_message,
                    entity_hand_off_timeout,
                    lambda ref, after_pending: self.ctx.stop(ref, after_pending=after_pending),
                ),
                "HandOffStopper",
            )
            self.ctx.watch(stopper)
            self.hand_off_stopper = stopper
            # During hand off we only care about watching for termination of the hand off
            # stopper
            self.receive = self.handing_off
            return Behaviors.same
        reply_to.tell(ShardStopped(self.shard_id))
        return Behaviors.stopped()

    def handing_off(self, msg: Any) -> Behavior[Any]:
        """Shard.scala 898-900, Pekko's anonymous ``become``: every message is unhandled, and
        the stopper's end arrives as the ``Terminated`` signal."""
        return Behaviors.unhandled

    def receive_terminated(self, ref: ActorRef[Any]) -> Behavior[Any]:
        """Shard.scala 907-910."""
        if self.hand_off_stopper is not None and self.hand_off_stopper == ref:
            return Behaviors.stopped()
        return Behaviors.same

    def receive_lease_lost(self, msg: _LeaseLost) -> Behavior[Any]:
        """Shard.scala 772-785."""
        # The shard region will re-create this when it receives a message for this shard
        self.log.error(
            "%s: Shard id [%s] lease lost, stopping shard and killing [%d] entities.%s",
            self.type_name,
            self.shard_id,
            self.entities.size,
            "" if msg.reason is None else f" Reason for losing lease: {msg.reason!r}",
        )
        # Stop entities ASAP rather than send termination message
        return Behaviors.stopped()

    def entity_terminated(self, ref: ActorRef[Any]) -> None:
        """Shard.scala 913-975, without the remember-store branches of ``Active`` (restart after
        backoff) and ``Passivating`` (store the stop)."""
        entity_id = self.entities.entity_id(ref)
        if entity_id is None:
            self.log.warning("%s: Unexpected entity terminated: %s", self.type_name, ref)
            return
        self.passivation_strategy.entity_terminated(entity_id)
        match self.entities.entity_state(entity_id):
            case RememberingStop():
                if self.verbose_debug:
                    self.log.debug(
                        "%s: Stop of [%s] arrived, already is among the pending stops",
                        self.type_name,
                        entity_id,
                    )
            case Active():
                self.log.debug("%s: Entity [%s] terminated", self.type_name, entity_id)
                self.entities.remove_entity(entity_id)
            case Passivating():
                if self.message_buffers.get_or_empty(entity_id):
                    if self.verbose_debug:
                        self.log.debug(
                            "%s: [%s] terminated after passivating, buffered messages found, "
                            "restarting",
                            self.type_name,
                            entity_id,
                        )
                    self.entities.remove_entity(entity_id)
                    self.get_or_create_entity(entity_id)
                    self.send_msg_buffer(entity_id)
                else:
                    if self.verbose_debug:
                        self.log.debug(
                            "%s: [%s] terminated after passivating", self.type_name, entity_id
                        )
                    self.entities.remove_entity(entity_id)
            case unexpected:
                self.log.warning(
                    "%s: Got a terminated for [%s], entityId [%s] which is in unexpected state "
                    "[%s]",
                    self.type_name,
                    self.entities.entity(entity_id),
                    entity_id,
                    unexpected,
                )

    def passivate(self, entity: ActorRef[Any], stop_message: object | None) -> None:
        """Shard.scala 977-997. With no stop message the entity is stopped after its pending
        messages, as Pekko's ``PoisonPill``."""
        entity_id = self.entities.entity_id(entity)
        if entity_id is None:
            self.log.debug(
                "%s: Unknown entity passivating [%s]. Not sending stopMessage back to entity",
                self.type_name,
                entity,
            )
        elif self.entities.is_passivating(entity_id):
            self.log.debug(
                "%s: Passivation already in progress for [%s]. Not sending stopMessage back to "
                "entity",
                self.type_name,
                entity_id,
            )
        elif self.message_buffers.get_or_empty(entity_id):
            self.log.debug(
                "%s: Passivation when there are buffered messages for [%s], ignoring passivation",
                self.type_name,
                entity_id,
            )
        else:
            if self.verbose_debug:
                self.log.debug("%s: Passivation started for [%s]", self.type_name, entity_id)
            self.entities.entity_passivating(entity_id)
            if stop_message is None:
                self.ctx.stop(entity, after_pending=True)
            else:
                entity.tell(stop_message)

    def active_entity_limit_updated(self, updated: SetActiveEntityLimit) -> None:
        """Shard.scala 999-1002."""
        self.passivate_entities(self.passivation_strategy.limit_updated(updated.per_region_limit))

    def shards_updated(self, updated: ShardsUpdated) -> None:
        """Shard.scala 1004-1007."""
        self.passivate_entities(self.passivation_strategy.shards_updated(updated.active_shards))

    def passivate_entities_after_interval(self) -> None:
        """Shard.scala 1009-1012."""
        self.passivate_entities(self.passivation_strategy.interval_passed())

    def passivate_entities(self, entities_to_passivate: PassivateEntities) -> None:
        """Shard.scala 1014-1024."""
        if entities_to_passivate:
            refs_to_passivate = [
                ref
                for entity_id in entities_to_passivate
                if (ref := self.entities.entity(entity_id)) is not None
            ]
            if refs_to_passivate:
                self.log.debug(
                    "%s: Passivating [%d] entities", self.type_name, len(refs_to_passivate)
                )
                for ref in refs_to_passivate:
                    self.passivate(ref, self.entity.stop_message)

    def deliver_message(self, envelope: ShardingEnvelope[T]) -> None:
        """Shard.scala 1049-1124, without ``StartEntity`` and the remembering branches: an
        entity in no state is created and the message delivered at once."""
        entity_id, payload = envelope.entity_id, envelope.message
        if not entity_id:
            self.log.warning(
                "%s: Id must not be empty, dropping message [%s]",
                self.type_name,
                type(envelope).__name__,
            )
            self.ctx.system.dropped(envelope, "No recipient entity id", self.ctx.self)
            return
        match self.entities.entity_state(entity_id):
            case Active(ref):
                if self.verbose_debug:
                    self.log.debug(
                        "%s: Delivering message of type [%s] to [%s]",
                        self.type_name,
                        type(payload).__name__,
                        entity_id,
                    )
                entities_to_passivate = self.passivation_strategy.entity_touched(entity_id)
                ref.tell(payload)
                self.passivate_entities(entities_to_passivate)
            case RememberingStart() | RememberingStop() | Passivating():
                self.append_to_message_buffer(entity_id, envelope)
            case WaitingForRestart() | RememberedButNotCreated() as state:
                if self.verbose_debug:
                    self.log.debug(
                        "%s: Delivering message of type [%s] to [%s] (starting because [%s])",
                        self.type_name,
                        type(payload).__name__,
                        entity_id,
                        state,
                    )
                self.get_or_create_entity(entity_id).tell(payload)
            case _:  # NoState: don't buffer if remember entities not enabled
                ref = self.get_or_create_entity(entity_id)
                entities_to_passivate = self.passivation_strategy.entity_touched(entity_id)
                ref.tell(payload)
                self.passivate_entities(entities_to_passivate)

    def get_or_create_entity(self, entity_id: str) -> ActorRef[T]:
        """Shard.scala 1127-1139. The behavior is created inside the child's own setup, so a
        failing factory fails that entity, not the shard."""
        child = self.entities.entity(entity_id)
        if child is not None:
            return child
        entity_ctx = EntityContext(self.entity.type_key, entity_id, self.ctx.self)
        create = self.entity.create_behavior
        behavior: Behavior[T] = Behaviors.setup(lambda _ctx: create(entity_ctx))
        a = self.ctx.spawn(behavior, _child_name(entity_id))
        self.ctx.watch_with(a, _EntityTerminated(a))
        self.log.debug(
            "%s: Started entity [%s] with entity id [%s] in shard [%s]",
            self.type_name,
            a,
            entity_id,
            self.shard_id,
        )
        self.entities.add_entity(entity_id, a)
        self.entity_created(entity_id)
        return a

    def entity_created(self, entity_id: str) -> int:
        """Shard.scala 1146: called when an entity has been created; the number of active
        entities."""
        return self.entities.nr_active_entities()

    def append_to_message_buffer(self, entity_id: str, msg: ShardingEnvelope[T]) -> None:
        """Shard.scala 1149-1163: ``buffer_size`` caps the messages of all the shard's buffers
        together."""
        if self.message_buffers.total_size >= self.settings.buffer_size:
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.debug(
                    "%s: Buffer is full, dropping message of type [%s] for entity [%s]",
                    self.type_name,
                    type(msg).__name__,
                    entity_id,
                )
            self.ctx.system.dropped(msg, f"Buffer for [{entity_id}] is full", self.ctx.self)
        else:
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.debug(
                    "%s: Message of type [%s] for entity [%s] buffered",
                    self.type_name,
                    type(msg).__name__,
                    entity_id,
                )
            self.message_buffers.append(entity_id, msg)

    def send_msg_buffer(self, entity_id: str) -> None:
        """Shard.scala 1166-1181, without ``StartEntity``."""
        # Get the buffered messages and remove the buffer
        messages = self.message_buffers.get_or_empty(entity_id)
        self.message_buffers.remove(entity_id)
        if messages:
            self.get_or_create_entity(entity_id)
            self.log.debug(
                "%s: Sending message buffer for entity [%s] ([%d] messages)",
                self.type_name,
                entity_id,
                len(messages),
            )
            # Now there is no deliveryBuffer we can try to redeliver
            # and as the child exists, the message will be directly forwarded
            for msg in messages:
                self.deliver_message(cast(ShardingEnvelope[T], msg))

    async def post_stop(self) -> None:
        """Shard.scala 1194-1198. The interval timer stops with the shard; what is still
        buffered is dropped silently, as Pekko's buffers are. Pekko releases the lease without
        waiting; this awaits the release, bounded by the lease's operation timeout, so nothing
        outlives the shard."""
        if self.lease is not None:
            try:
                await asyncio.wait_for(
                    self.lease.release(), self.lease.settings.timeout_settings.operation_timeout
                )
            except Exception:
                self.log.warning(
                    "%s: Failed to release the lease of shard id [%s]",
                    self.type_name,
                    self.shard_id,
                    exc_info=True,
                )
        self.log.debug("%s: Shard [%s] shutting down", self.type_name, self.shard_id)


def shard_behavior[T](
    entity: Entity[T], shard_id: str, region: ActorRef[ShardingEnvelope[T]]
) -> Behavior[_ShardCmd[T]]:
    """Pekko's Shard: hosts the entities of one shard id, and tells ``region`` it is
    initialized, once it holds its lease if it has lease settings. While the lease is pending
    it stashes without a limit, as Pekko's classic ``Stash`` does (``stash-capacity = -1``)."""
    return Behaviors.with_stash(
        sys.maxsize,
        lambda stash: Behaviors.setup(
            lambda ctx: _Shard(ctx, entity, shard_id, region, stash).behavior()
        ),
    )


def _aggregate_shard_states(
    shards: list[ActorRef[Any]], reply_to: ActorRef[CurrentShardRegionState], timeout: float
) -> Behavior[CurrentShardState | _QueryTimeout]:
    """Pekko's region query: ask every Shard, reply the union when all have answered or at the
    timeout."""

    @Behaviors.setup
    def setup(
        ctx: ActorContext[CurrentShardState | _QueryTimeout],
    ) -> Behavior[CurrentShardState | _QueryTimeout]:
        ids: set[str] = set()
        shard_ids: set[str] = set()
        remaining = len(shards)
        for shard in shards:
            shard.tell(GetShardState(ctx.self))
        ctx.timers.start_single_timer("timeout", _QueryTimeout(), timeout)

        @Behaviors.receive
        def on_message(
            _ctx: ActorContext[Any], msg: CurrentShardState | _QueryTimeout
        ) -> Behavior[Any]:
            nonlocal remaining
            if isinstance(msg, CurrentShardState):
                ids.update(msg.entity_ids)
                shard_ids.add(msg.shard)
                remaining -= 1
                if remaining > 0:
                    return Behaviors.same
            reply_to.tell(CurrentShardRegionState(frozenset(ids), frozenset(shard_ids)))
            return Behaviors.stopped()

        return on_message

    return setup


type _RegionCmd[T] = (
    ShardingEnvelope[T] | GetShardRegionState | ShardInitialized | SetActiveEntityLimit
)


def _log_passivation_strategy(
    log: logging.Logger, type_name: str, strategy: PassivationStrategy
) -> None:
    """ShardRegion.scala 698-705, without the remember-entities branch (sub-project 10)."""
    log.info(
        "%s: Automatic entity passivation: %s", type_name, describe_passivation_strategy(strategy)
    )


class Sharding:
    @staticmethod
    def init[T](
        entity: Entity[T],
    ) -> Behavior[ShardingEnvelope[T] | GetShardRegionState | SetActiveEntityLimit]:
        """The shard region behavior for one entity type, inside one process. Spawn it; message it
        through entity refs. Each shard id gets a child Shard actor on first use. As Pekko's
        region, it passes ``SetActiveEntityLimit`` to every shard, and tells every shard the
        shard count when it changes; it has no graceful shutdown."""
        settings = entity.effective_settings
        strategy = settings.passivation_strategy

        @Behaviors.setup
        def setup(ctx: ActorContext[_RegionCmd[T]]) -> Behavior[_RegionCmd[T]]:
            shards: dict[str, ActorRef[_ShardCmd[T]]] = {}
            ids_by_ref: dict[ActorRef[Any], str] = {}
            _log_passivation_strategy(ctx.log, entity.type_key.name, strategy)

            def shards_updated() -> None:
                """ShardRegion.scala 1013-1016 and 1358-1361."""
                if strategy != NoPassivationStrategy():
                    update = ShardsUpdated(len(shards))
                    for shard in shards.values():
                        shard.tell(update)

            def shard_for(entity_id: str) -> ActorRef[_ShardCmd[T]]:
                shard_id = shard_id_of(entity_id, settings.number_of_shards)
                shard = shards.get(shard_id)
                if shard is None:
                    shard = ctx.spawn(shard_behavior(entity, shard_id, ctx.self), shard_id)
                    ctx.watch(shard)
                    shards[shard_id] = shard
                    ids_by_ref[shard] = shard_id
                    shards_updated()
                return shard

            @Behaviors.receive
            def on_message(
                _ctx: ActorContext[_RegionCmd[T]], msg: _RegionCmd[T]
            ) -> Behavior[_RegionCmd[T]]:
                match msg:
                    case ShardingEnvelope("", message):
                        ctx.log.warning("dropping message for empty entity id: %r", message)
                        ctx.system.dead_letter(message, ctx.self)
                    case ShardingEnvelope(entity_id, _):
                        shard_for(entity_id).tell(msg)
                    case ShardInitialized():
                        pass  # the shard stashes while it waits for its lease
                    case SetActiveEntityLimit():
                        # ShardRegion.scala 767 and 1278 (deliverToAllShards)
                        for shard in shards.values():
                            shard.tell(msg)
                    case GetShardRegionState(reply_to):
                        if not shards:
                            reply_to.tell(CurrentShardRegionState(frozenset()))
                        else:
                            ctx.spawn_anonymous(
                                _aggregate_shard_states(
                                    list(shards.values()),
                                    reply_to,
                                    settings.shard_region_query_timeout,
                                )
                            )
                return Behaviors.same

            def on_signal(
                _ctx: ActorContext[_RegionCmd[T]], sig: Signal
            ) -> Behavior[_RegionCmd[T]]:
                if isinstance(sig, Terminated):
                    shard_id = ids_by_ref.pop(sig.ref, None)
                    if shard_id is not None:
                        shards.pop(shard_id, None)
                        shards_updated()
                    return Behaviors.same
                return Behaviors.unhandled

            return on_message.receive_signal(on_signal)

        return setup

    @staticmethod
    def entity_ref_for[T](region: ActorRef[ShardingEnvelope[T]], entity_id: str) -> EntityRef[T]:
        if not entity_id:
            raise ValueError("entity id must not be empty")
        return EntityRef(region, entity_id)


STOP_TIMEOUT_WARNING_AFTER = 5.0
"""Pekko's ``HandOffStopper.StopTimeoutWarningAfter`` (ShardRegion.scala 593)."""


@dataclass(frozen=True)
class _StopTimeoutWarning:
    pass


@dataclass(frozen=True)
class _StopTimeout:
    pass


def hand_off_stopper(
    type_name: str,
    shard: str,
    reply_to: ActorRef[ShardStopped],
    entities: frozenset[ActorRef[Any]],
    stop_message: object | None,
    handoff_timeout: float,
    stop: Callable[[ActorRef[Any], bool], None],
) -> Behavior[Any]:
    """Pekko's ``HandOffStopper`` (ShardRegion.scala 521-597): sends the stop message to the
    entities and, when all of them have terminated, replies ``ShardStopped``. After
    ``STOP_TIMEOUT_WARNING_AFTER`` it warns; at ``handoff_timeout`` it stops what is left.

    ohra can stop only a child, and the entities are the shard's children, so ``stop(ref,
    after_pending)`` is the shard's ``ctx.stop``. Pekko's ``a ! PoisonPill`` (no stop message) is
    ``stop(ref, True)``, and ``context.stop(ref)`` at the timeout is ``stop(ref, False)``."""
    name = "PoisonPill" if stop_message is None else type(stop_message).__name__

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        ctx.timers.start_single_timer(
            _StopTimeoutWarning, _StopTimeoutWarning(), STOP_TIMEOUT_WARNING_AFTER
        )
        ctx.timers.start_single_timer(_StopTimeout, _StopTimeout(), handoff_timeout)
        for a in entities:
            ctx.watch(a)
            if stop_message is None:
                stop(a, True)
            else:
                a.tell(stop_message)
        return running(entities)

    def running(remaining: frozenset[ActorRef[Any]]) -> Behavior[Any]:
        def on_message(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            match msg:
                case _StopTimeoutWarning():
                    ctx.log.warning(
                        "%s: [%d] of the entities in shard [%s] not stopped after [%s]. "
                        "Maybe the handOffStopMessage [%s] is not handled? %s",
                        type_name,
                        len(remaining),
                        shard,
                        f"{STOP_TIMEOUT_WARNING_AFTER:g} s",
                        name,
                        ""
                        if CoordinatedShutdown(ctx.system).shutdown_reason is not None
                        else f"Waiting additional [{handoff_timeout:g} s] before stopping the "
                        "remaining entities.",
                    )
                case _StopTimeout():
                    ctx.log.warning(
                        "%s: HandOffStopMessage [%s] is not handled by some of the entities in "
                        "shard [%s] after [%s], stopping the remaining [%d] entities.",
                        type_name,
                        name,
                        shard,
                        f"{handoff_timeout:g} s",
                        len(remaining),
                    )
                    for ref in remaining:
                        stop(ref, False)
                case _:
                    return Behaviors.unhandled
            return Behaviors.same

        def on_signal(_ctx: ActorContext[Any], signal: Signal) -> Behavior[Any]:
            if isinstance(signal, Terminated):
                left = remaining - {signal.ref}
                if not left:
                    reply_to.tell(ShardStopped(shard))
                    return Behaviors.stopped()
                return running(left)
            return Behaviors.unhandled

        return Behaviors.receive(on_message).receive_signal(on_signal)

    return setup
