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
rather than stop themselves. With ``remember_entities``, a shard of ``ClusterSharding`` loads the
ids its store remembers before it is initialized, and has them started again.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal, Self, cast, overload
from urllib.parse import quote

from ...behavior import Behavior, Behaviors, StashBuffer
from ...context import ActorContext
from ...coordinated_shutdown import CoordinatedShutdown
from ...coordination.lease import Lease, LeaseProvider, LeaseUsageSettings
from ...eventstream import DeadLetterSuppression
from ...patterns import Replyable
from ...ref import ActorRef
from ...remote import Address
from ...remote.serialization import remotable
from ...result import Failure, Success, Try
from ...signals import PostStop, PreRestart, Signal, Terminated
from ...supervision import SupervisorStrategy
from ...util.message_buffer import MessageBufferMap
from ..core import (
    SELF_DATA_CENTER,
    MemberEvent,
    MemberPreparingForShutdown,
    MemberReadyForShutdown,
    Subscribe,
)
from ..ddata.replicator import Replicator
from ..gossip import DC_ROLE_PREFIX
from ..ddata.settings import ReplicatorSettings
from .entities import (
    Active,
    Entities,
    NoState,
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
from .remember_entities import DDataRememberEntitiesProvider, RememberEntitiesShardStore

if TYPE_CHECKING:
    from ..core import Cluster, ClusterSettings
    from .message_extractor import ShardingMessageExtractor
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
    distributed_data with prefer_oldest=True and max_delta_elements=5. Durable only under
    remember_entities (see ``effective_replicator_settings``). Settings given here keep their own
    ``durable_keys``: pass ``durable_keys=frozenset({"shard-*"})`` with them, or remembered
    entities do not survive a full cluster restart (Pekko's reference.conf has that default)."""

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
    (entity-restart-backoff)."""
    shard_failure_backoff: float = 10.0
    """How long a remembering shard that failed to store waits to restart
    (shard-failure-backoff)."""
    remember_entities: bool = False
    """Whether the shard remembers its entities and restarts them after a rebalance or crash
    (remember-entities). It turns passivation off and makes the ``shard-*`` keys durable."""
    remember_entities_store: Literal["ddata"] = "ddata"
    """Where the entities are remembered (remember-entities-store). Only "ddata" is ported:
    "eventsourced" needs event sourcing, which ohra does not have."""
    entity_recovery_strategy: Literal["all", "constant"] = "all"
    """"all" starts every remembered entity at once, "constant" starts
    ``entity_recovery_constant_rate_number_of_entities`` every
    ``entity_recovery_constant_rate_frequency`` seconds (entity-recovery-strategy)."""
    entity_recovery_constant_rate_frequency: float = 0.1
    """The pause between batches of the "constant" strategy (entity-recovery-constant-rate-strategy
    .frequency)."""
    entity_recovery_constant_rate_number_of_entities: int = 5
    """The entities per batch of the "constant" strategy (.number-of-entities)."""
    lease_settings: LeaseUsageSettings | None = None
    """The lease each shard acquires before it starts its entities, and how long it waits to
    retry a failed acquire (use-lease, lease-retry-interval); None means no lease."""
    role: str | None = None
    """Shards live only on members with this role; ``init`` on any other member starts a
    proxy-only region (Pekko's role). ``""`` means none, as Pekko's ``roleOption``."""
    coordinator_singleton_override_role: bool = True
    """The coordinator runs on the members with ``role``, whatever role
    ``coordinator_singleton_settings`` names (coordinator-singleton-role-override)."""

    def __post_init__(self) -> None:
        if self.role == "":
            object.__setattr__(self, "role", None)
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
        if self.remember_entities_store != "ddata":
            raise ValueError(
                f"remember_entities_store must be 'ddata', got {self.remember_entities_store!r}: "
                "the event-sourced store is not ported because ohra has no event sourcing"
            )
        # ClusterShardingSettings.scala 880-882
        if self.entity_recovery_strategy not in ("all", "constant"):
            raise ValueError(
                f"Unknown 'entity-recovery-strategy' [{self.entity_recovery_strategy}], "
                "valid values are 'all' or 'constant'"
            )
        if self.entity_recovery_constant_rate_frequency <= 0:
            raise ValueError("entity_recovery_constant_rate_frequency must be > 0")
        if self.entity_recovery_constant_rate_number_of_entities < 1:
            raise ValueError("entity_recovery_constant_rate_number_of_entities must be >= 1")

    def with_role(self, role: str | None) -> ClusterShardingSettings:
        """Pekko's withRole: None or ``""`` means no role."""
        return replace(self, role=role or None)

    @property
    def coordinator_singleton_role(self) -> str | None:
        """ClusterShardingSettings.scala 937-938: the shard role under the override, else the
        coordinator singleton settings' role."""
        if self.coordinator_singleton_override_role:
            return self.role
        settings = self.coordinator_singleton_settings
        return settings.role if settings is not None else None

    def should_host_shard(self, cluster: Cluster) -> bool:
        """927-928: this member runs the region, else only a proxy."""
        return self.role is None or self.role in cluster.self_member.roles

    def should_host_coordinator(self, cluster: Cluster) -> bool:
        """932-933: this member runs the coordinator's singleton manager."""
        role = self.coordinator_singleton_role
        return role is None or role in cluster.self_member.roles

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
        self, cluster_settings: ClusterSettings, role: str | None
    ) -> ReplicatorSettings:
        """ClusterSharding.scala 731-741: the Replicator of ``role`` counts the members of the
        data center with that role, and sharding's data is durable only under remember entities,
        by the configured keys, else ``shard-*`` (reference.conf 398)."""
        if self.distributed_data is None:
            base = replace(
                cluster_settings.distributed_data, prefer_oldest=True, max_delta_elements=5
            )
        else:
            base = self.distributed_data
        base = replace(
            base,
            roles=frozenset({f"{DC_ROLE_PREFIX}{SELF_DATA_CENTER}"} | ({role} if role else set())),
        )
        if not self.remember_entities:
            return replace(base, durable_keys=frozenset())
        if self.distributed_data is None:
            return replace(base, durable_keys=frozenset({"shard-*"}))
        return base

    @property
    def passivation_strategy(self) -> PassivationStrategy:
        """Classic 747-750: no passivation under remember entities, else what the settings
        resolve to."""
        if self.remember_entities:
            return NoPassivationStrategy()
        return passivation_strategy(self.passivation_strategy_settings)

    def with_passivation_strategy(
        self, settings: PassivationStrategySettings
    ) -> ClusterShardingSettings:
        """Typed 833-834."""
        return replace(self, passivation_strategy_settings=settings)

    def with_no_passivation_strategy(self) -> ClusterShardingSettings:
        """Typed 836-837."""
        return replace(self, passivation_strategy_settings=PassivationStrategySettings.disabled)

    def with_remember_entities(self, remember_entities: bool) -> ClusterShardingSettings:
        """Classic 948-949."""
        return replace(self, remember_entities=remember_entities)

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
    yet). ``failed`` holds the shards that did not answer in time, as Pekko's does."""

    entity_ids: frozenset[str]
    shard_ids: frozenset[str] = frozenset()
    failed: frozenset[str] = frozenset()


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
    role: str | None = None
    """Wins over ``settings.role`` (Pekko Typed's ``settingsWithRole``)."""
    message_extractor: ShardingMessageExtractor[Any, T] | None = None
    """None is ``HashCodeMessageExtractor(number_of_shards)``: see ``with_message_extractor``."""

    def with_buffer_size(self, size: int) -> Self:
        """Pekko's buffer-size: caps the messages a shard buffers for its passivating entities,
        in total across them, and the messages the region buffers. The shard drops what is
        beyond it."""
        return replace(self, buffer_size=size)

    def with_stop_message(self, msg: T) -> Self:
        """Passivation delivers msg and lets the entity stop itself, instead of stopping it
        outright."""
        return replace(self, stop_message=msg)

    def with_settings(self, settings: ClusterShardingSettings) -> Self:
        return replace(self, settings=settings)

    def with_role(self, role: str) -> Self:
        """The entities run only on members with ``role``; others get a proxy."""
        return replace(self, role=role)

    @property
    def effective_settings(self) -> ClusterShardingSettings:
        """``settings``, with ``buffer_size`` and ``role`` set on the entity on top, whichever
        order they were set in."""
        settings = self.settings or ClusterShardingSettings()
        if self.buffer_size is not None:
            settings = replace(settings, buffer_size=self.buffer_size)
        if self.role is not None:
            settings = settings.with_role(self.role)
        return settings

    def with_message_extractor[E](
        self, extractor: ShardingMessageExtractor[E, T]
    ) -> ExtractedEntity[T, E]:
        """Pekko Typed's ``withMessageExtractor``: the region takes the messages ``E`` and the
        extractor gives their entity ids, the shard of each entity id and the entity's message.
        ``number_of_shards`` of the settings is then only what the default extractor uses."""
        return ExtractedEntity(
            self.type_key,
            self.create_behavior,
            self.stop_message,
            self.buffer_size,
            self.settings,
            self.role,
            extractor,
        )

    @property
    def extractor(self) -> ShardingMessageExtractor[Any, T]:
        """The message extractor, or the default for ``number_of_shards`` (Pekko Typed's
        ``messageExtractor.getOrElse(HashCodeMessageExtractor(numberOfShards))``)."""
        if self.message_extractor is not None:
            return self.message_extractor
        from .message_extractor import HashCodeMessageExtractor

        return HashCodeMessageExtractor[T](self.effective_settings.number_of_shards)


@dataclass(frozen=True)
class ExtractedEntity[T, E](Entity[T]):
    """An ``Entity`` with a message extractor of the messages ``E``, from
    ``Entity.with_message_extractor``. It is a class of its own so that ``ClusterSharding.init``
    can type the region as taking ``E``, as Pekko's ``Entity[M, E]`` does."""

    message_extractor: ShardingMessageExtractor[E, T] | None = None


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


@remotable
@dataclass(frozen=True)
class ShardRegionStats:
    """ShardRegion.scala 321: the number of entities of each shard of a region, and the shards
    that did not answer within ``shard_region_query_timeout``."""

    stats: dict[str, int]
    failed: frozenset[str] = frozenset()


@remotable
@dataclass(frozen=True)
class GetShardRegionStats:
    """ShardRegion.scala 311: ask a region for its ``ShardRegionStats``."""

    reply_to: ActorRef[ShardRegionStats]


@remotable
@dataclass(frozen=True)
class ClusterShardingStats:
    """ShardRegion.scala 289: every region's stats, by the address of its node; empty when a
    region did not answer in time."""

    regions: dict[Address, ShardRegionStats]


@remotable
@dataclass(frozen=True)
class GetClusterShardingStats:
    """ShardRegion.scala 281: ask a region for the stats of every region of its entity type,
    gathered by the coordinator within ``timeout`` seconds."""

    timeout: float
    reply_to: ActorRef[ClusterShardingStats]


@remotable
@dataclass(frozen=True)
class StartEntity:
    """ShardRegion.scala 488-492 and typed scaladsl ClusterSharding.scala 380-390: start the
    entity with this id in its shard, if it is not running; the region answers with
    ``StartEntityAck``."""

    entity_id: str


@remotable
@dataclass(frozen=True)
class StartEntityAck(DeadLetterSuppression):
    """ShardRegion.scala 494-500: the entity ``entity_id`` was started in shard ``shard_id``,
    which can differ from the shard that remembered it."""

    entity_id: str
    shard_id: str


@remotable
@dataclass(frozen=True)
class _StartEntity:
    """Pekko's ``StartEntity`` with its sender: ``ack_to`` gets the ``StartEntityAck``."""

    entity_id: str
    ack_to: ActorRef[StartEntityAck] | None


@dataclass(frozen=True)
class StartEntities:
    """RememberEntityStarter.scala 41-42: start these remembered entities of a shard. It lives
    here so that this module never imports the starter."""

    shard: ActorRef[Any]
    shard_id: str
    ids: frozenset[str]


@dataclass(frozen=True)
class _RestartTerminatedEntity:
    """Shard.scala 72."""

    entity_id: str


@dataclass(frozen=True)
class EntitiesMovedToOtherShard:
    """Shard.scala 78: internal, named without the underscore as the starter's tests import it."""

    ids: frozenset[str]


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


@dataclass(frozen=True)
class _RememberEntityStoreCrashed:
    """Shard.scala 128: the store stopped, watched with this message."""

    store: ActorRef[Any]


_REMEMBER_ENTITY_TIMEOUT_KEY = "RememberEntityTimeout"
"""Shard.scala 130."""


@dataclass(frozen=True)
class _RememberEntityTimeout:
    """Shard.scala 131: the store did not answer ``operation`` in time."""

    operation: object


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
    | _LeaseAcquireResult
    | _LeaseLost
    | _LeaseRetry
    | _RememberEntityStoreCrashed
    | _RememberEntityTimeout
    | RememberEntitiesShardStore.RememberedEntities
    | RememberEntitiesShardStore.UpdateDone
    | _StartEntity
    | _RestartTerminatedEntity
    | EntitiesMovedToOtherShard
    | MemberEvent
)


def shard_behavior[T](
    entity: Entity[T],
    shard_id: str,
    region: ActorRef[Any],
    remember_entities_provider: DDataRememberEntitiesProvider | None = None,
    remember_entity_starter_manager: ActorRef[StartEntities] | None = None,
) -> Behavior[_ShardCmd[T]]:
    """Pekko's ``Shard`` actor (Shard.scala 424-1200), for one shard id of one entity type: hosts
    the entities of that shard id, and tells ``region`` it is initialized, once it holds its lease
    if it has lease settings and, with a ``remember_entities_provider``, once its store has
    loaded the remembered entities, which it hands to ``remember_entity_starter_manager``. Until
    then it stashes without a limit, as Pekko's classic ``Stash`` does (``stash-capacity = -1``).

    The shard is supervised inside, with restart, as Pekko's classic default: the store is its
    child, and a store that stops makes the shard throw and restart, releasing its lease
    (Pekko's default ``preRestart`` runs ``postStop``). The stash outlives the restart, as
    Pekko's ``preRestart`` puts the stashed messages back in the mailbox. Pekko's receive states
    are the nested functions returning behaviors, and its ``context.become`` is returning one,
    from the state's handler or from the helper the handler returns. Pekko's ``context.parent``
    is ``region``, ``context.stop(self)`` is returning ``Behaviors.stopped()``, and a typed
    message has no sender, so the envelope is what is buffered and redelivered. Pekko's
    ``StartEntity`` with its sender is ``_StartEntity``, whose ``ack_to`` gets the
    ``StartEntityAck``; a ``StartEntity`` in an envelope has no one to ack."""

    def shard(stash: StashBuffer[Any]) -> Behavior[Any]:
        @Behaviors.setup
        def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
            """Shard.scala 449-506. Pekko's ``Stash`` is ``stash``, and ``preStart`` is the end
            of this function. A system with no cluster has no member events, so a single-node
            ``Sharding`` never prepares for shutdown."""
            parent: ActorRef[Any] = region
            settings = entity.effective_settings
            type_name = entity.type_key.name
            log = ctx.log
            verbose_debug = settings.verbose_debug_logging
            remember_entities = remember_entities_provider is not None
            remember_entities_store: ActorRef[Any] | None = None
            if remember_entities_provider is not None:
                remember_entities_store = ctx.spawn(
                    Behaviors.supervise(
                        remember_entities_provider.shard_store_behavior(shard_id)
                    ).on_failure(SupervisorStrategy.restart()),
                    "RememberEntitiesStore",
                )
                ctx.watch_with(
                    remember_entities_store, _RememberEntityStoreCrashed(remember_entities_store)
                )
            entities = Entities(
                ctx.log,
                settings.remember_entities,
                verbose_debug,
                settings.fail_on_invalid_entity_state_transition,
            )
            # Messages are buffered while an entity is passivating
            message_buffers = MessageBufferMap[str]()
            # Pekko's handOffStopper; ``hand_off_stopper`` is the stopper's behavior
            hand_off_stopper_ref: ActorRef[Any] | None = None
            preparing_for_shutdown = False
            passivation_strategy = EntityPassivationStrategy.create(
                settings, lambda: ctx.system.scheduler.current_time
            )
            interval = passivation_strategy.scheduled_interval
            if interval is not None:
                ctx.timers.start_timer_with_fixed_delay(
                    _PassivateIntervalTick, _PassivateIntervalTick(), interval
                )
            lease_settings = settings.lease_settings
            lease: Lease | None = None
            if lease_settings is not None:
                system = ctx.system
                cluster = system.cluster
                if cluster is None:
                    owner = system.name
                else:  # Pekko's selfAddress.hostPort
                    address = cluster.self_address
                    owner = f"{address.name}@{address.host}:{address.port}"
                lease = LeaseProvider(system).get_lease(
                    f"{system.name}-shard-{type_name}-{shard_id}",
                    lease_settings.lease_implementation,
                    owner,
                )
            lease_retry_interval = (
                lease_settings.lease_retry_interval
                if lease_settings is not None
                else 5.0  # not used
            )
            # The state a helper became during this message, for the state's handler to return:
            # Pekko's context.become from inside a helper, where the last become wins.
            become: Behavior[Any] | None = None

            def context_become(behavior: Behavior[Any]) -> None:
                nonlocal become
                become = behavior

            def take_become(otherwise: Behavior[Any]) -> Behavior[Any]:
                """The state a helper became during this message, else ``otherwise``."""
                nonlocal become
                pending, become = become, None
                return otherwise if pending is None else pending

            async def on_signal(_ctx: ActorContext[Any], sig: Signal) -> Behavior[Any]:
                if isinstance(sig, Terminated):
                    return receive_terminated(sig.ref)
                if sig is PostStop or sig is PreRestart:
                    await post_stop()
                    return Behaviors.same
                return Behaviors.unhandled

            def acquire_lease_if_needed() -> Behavior[Any]:
                """Shard.scala 508-516."""
                if lease is not None:
                    try_get_lease(lease)
                    return awaiting_lease()
                return try_load_remembered_entities()

            def awaiting_lease() -> Behavior[Any]:
                """Shard.scala 518-544. ShardInitialized is not sent back, so the region buffers
                the messages for this shard while it waits for the lease."""

                def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                    match msg:
                        case _LeaseAcquireResult(True, _):
                            log.debug("%s: Lease acquired", type_name)
                            return try_load_remembered_entities()
                        case _LeaseAcquireResult(False, None):
                            log.error(
                                "%s: Failed to get lease for shard id [%s]. Retry in %s",
                                type_name,
                                shard_id,
                                f"{lease_retry_interval:g} s",
                            )
                            ctx.timers.start_single_timer(
                                _LEASE_RETRY_TIMER, _LeaseRetry(), lease_retry_interval
                            )
                        case _LeaseAcquireResult(False, t):
                            log.error(
                                "%s: Failed to get lease for shard id [%s]. Retry in %s",
                                type_name,
                                shard_id,
                                f"{lease_retry_interval:g} s",
                                exc_info=t,
                            )
                            ctx.timers.start_single_timer(
                                _LEASE_RETRY_TIMER, _LeaseRetry(), lease_retry_interval
                            )
                        case _LeaseRetry():
                            assert lease is not None
                            try_get_lease(lease)
                        case _LeaseLost():
                            return receive_lease_lost(msg)
                        case MemberEvent():
                            receive_member_event(msg)
                        case _:
                            if verbose_debug:
                                log.debug(
                                    "%s: Got msg of type [%s] while waiting for lease, stashing",
                                    type_name,
                                    type(msg).__name__,
                                )
                            stash.stash(msg)
                    return Behaviors.same

                return Behaviors.receive(on_message).receive_signal(on_signal)

            def receive_member_event(event: MemberEvent) -> None:
                """Shard.scala 546-553."""
                nonlocal preparing_for_shutdown
                match event:
                    case MemberReadyForShutdown() | MemberPreparingForShutdown():
                        if not preparing_for_shutdown:
                            log.info("%s: Preparing for shutdown", type_name)
                            preparing_for_shutdown = True
                    case _:
                        pass

            def try_get_lease(lease: Lease) -> None:
                """Shard.scala 555-560."""
                log.info("%s: Acquiring lease %s", type_name, lease.settings)
                me = ctx.self

                def adapt(result: Try[bool]) -> Any:
                    match result:
                        case Success(acquired):
                            return _LeaseAcquireResult(acquired, None)
                        case Failure(exc):
                            return _LeaseAcquireResult(False, exc)

                ctx.pipe_to_self(lease.acquire(lambda reason: me.tell(_LeaseLost(reason))), adapt)

            def try_load_remembered_entities() -> Behavior[Any]:
                """Shard.scala 563-576."""
                store = remember_entities_store
                if store is None:
                    return shard_initialized()
                log.debug(
                    "%s: Waiting for load of entity ids using [%s] to complete", type_name, store
                )
                get_entities = RememberEntitiesShardStore.GetEntities(ctx.self)
                store.tell(get_entities)
                ctx.timers.start_single_timer(
                    _REMEMBER_ENTITY_TIMEOUT_KEY,
                    _RememberEntityTimeout(get_entities),
                    settings.waiting_for_state_timeout,
                )
                return awaiting_remembered_entities()

            def awaiting_remembered_entities() -> Behavior[Any]:
                """Shard.scala 578-594."""

                def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                    match msg:
                        case RememberEntitiesShardStore.RememberedEntities(entity_ids):
                            ctx.timers.cancel(_REMEMBER_ENTITY_TIMEOUT_KEY)
                            return on_entities_remembered(entity_ids)
                        case _RememberEntityTimeout(RememberEntitiesShardStore.GetEntities()):
                            return loading_entity_ids_failed()
                        case MemberEvent():
                            receive_member_event(msg)
                        case _:
                            if verbose_debug:
                                log.debug(
                                    "%s: Got msg of type [%s] while waiting for remember "
                                    "entities, stashing",
                                    type_name,
                                    type(msg).__name__,
                                )
                            stash.stash(msg)
                    return Behaviors.same

                return Behaviors.receive(on_message).receive_signal(on_signal)

            def loading_entity_ids_failed() -> Behavior[Any]:
                """Shard.scala 596-603."""
                log.error(
                    "%s: Failed to load initial entity ids from remember entities store within "
                    "[%s], stopping shard for backoff and restart",
                    type_name,
                    f"{settings.waiting_for_state_timeout:g} s",
                )
                # parent ShardRegion supervisor will notice that it terminated and will start it
                # again, after backoff
                return Behaviors.stopped()

            def on_entities_remembered(ids: frozenset[str]) -> Behavior[Any]:
                """Shard.scala 605-612."""
                if ids:
                    entities.already_remembered(ids)
                    log.debug("%s: Restarting set of [%d] entities", type_name, len(ids))
                    if remember_entity_starter_manager is not None:
                        remember_entity_starter_manager.tell(
                            StartEntities(ctx.self, shard_id, ids)
                        )
                return shard_initialized()

            def shard_initialized() -> Behavior[Any]:
                """Shard.scala 614-619: ``idle``, after the messages stashed while awaiting the
                lease and the remembered entities."""
                log.debug("%s: Shard initialized", type_name)
                parent.tell(ShardInitialized(shard_id))
                return stash.unstash_all(idle())

            def idle() -> Behavior[Any]:
                """Shard.scala 624-639. ``Terminated`` is a signal, handled in ``on_signal``."""

                def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                    return take_become(receive(msg))

                def receive(msg: Any) -> Behavior[Any]:
                    match msg:
                        case MemberEvent():
                            receive_member_event(msg)
                        case _EntityTerminated(ref):
                            entity_terminated(ref)
                        case HandOff():
                            return receive_coordinator_message(msg)
                        case _RestartTerminatedEntity() | EntitiesMovedToOtherShard():
                            receive_remember_entity_command(msg)
                        case _StartEntity(entity_id, ack_to):
                            start_entity(entity_id, ack_to)
                        case SetActiveEntityLimit():
                            active_entity_limit_updated(msg)
                        case ShardsUpdated():
                            shards_updated(msg)
                        case Passivate(ref):
                            passivate(ref, entity.stop_message)
                        case _PassivateIntervalTick():
                            passivate_entities_after_interval()
                        case GetShardState() | GetShardStats():
                            receive_shard_query(msg)
                        case _LeaseLost():
                            return receive_lease_lost(msg)
                        case _RememberEntityStoreCrashed():
                            remember_entity_store_crashed(msg)
                        case ShardingEnvelope():
                            deliver_message(cast(ShardingEnvelope[T], msg))
                        case _:
                            return Behaviors.unhandled
                    return Behaviors.same

                return Behaviors.receive(on_message).receive_signal(on_signal)

            def remember_update(
                add: frozenset[str] = frozenset(), remove: frozenset[str] = frozenset()
            ) -> None:
                """Shard.scala 641-648."""
                store = remember_entities_store
                if store is None:
                    on_update_done(add, remove)
                else:
                    send_to_remember_store(store, add, remove)

            def send_to_remember_store(
                store: ActorRef[Any], storing_starts: frozenset[str], storing_stops: frozenset[str]
            ) -> None:
                """Shard.scala 650-675."""
                if verbose_debug:
                    log.debug(
                        "%s: Remember update [%s] and stops [%s] triggered",
                        type_name,
                        ", ".join(storing_starts),
                        ", ".join(storing_stops),
                    )
                start_time = ctx.system.scheduler.current_time
                update = RememberEntitiesShardStore.Update(storing_starts, storing_stops, ctx.self)
                store.tell(update)
                ctx.timers.start_single_timer(
                    _REMEMBER_ENTITY_TIMEOUT_KEY,
                    _RememberEntityTimeout(update),
                    settings.updating_state_timeout,
                )
                context_become(waiting_for_remember_entities_store(update, start_time))

            def waiting_for_remember_entities_store(
                update: RememberEntitiesShardStore.Update, start_time: float
            ) -> Behavior[Any]:
                """Shard.scala 677-728."""

                def on_message(_ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                    return take_become(receive(msg))

                def receive(msg: Any) -> Behavior[Any]:
                    match msg:
                        # none of the current impls will send back a partial update, yet!
                        case RememberEntitiesShardStore.UpdateDone(stored_starts, stored_stops):
                            duration = ctx.system.scheduler.current_time - start_time
                            if verbose_debug:
                                log.debug(
                                    "%s: Update done for ids, started [%s], stopped [%s]. "
                                    "Duration %d ms",
                                    type_name,
                                    ", ".join(stored_starts),
                                    ", ".join(stored_stops),
                                    int(duration * 1000),
                                )
                            ctx.timers.cancel(_REMEMBER_ENTITY_TIMEOUT_KEY)
                            on_update_done(stored_starts, stored_stops)
                        case _RememberEntityTimeout(operation) if operation == update:
                            log.error(
                                "%s: Remember entity store did not respond, restarting shard",
                                type_name,
                            )
                            raise RuntimeError(
                                "Async write timed out after "
                                f"{settings.updating_state_timeout:g} s"
                            )
                        case _StartEntity(entity_id, ack_to):
                            start_entity(entity_id, ack_to)
                        case MemberEvent():
                            receive_member_event(msg)
                        case _EntityTerminated(ref):
                            entity_terminated(ref)
                        case HandOff():
                            stash.stash(msg)
                        case _RestartTerminatedEntity() | EntitiesMovedToOtherShard():
                            receive_remember_entity_command(msg)
                        case _LeaseLost():
                            return receive_lease_lost(msg)
                        case Passivate(ref):
                            if verbose_debug:
                                passivating_id = entities.entity_id(ref)
                                log.debug(
                                    "%s: Passivation of [%s] arrived while updating",
                                    type_name,
                                    f"Unknown actor {ref}"
                                    if passivating_id is None
                                    else passivating_id,
                                )
                            passivate(ref, entity.stop_message)
                        case GetShardState() | GetShardStats():
                            receive_shard_query(msg)
                        case _PassivateIntervalTick():
                            stash.stash(msg)
                        case _RememberEntityStoreCrashed():
                            remember_entity_store_crashed(msg)
                        case ShardsUpdated():
                            shards_updated(msg)
                        case ShardingEnvelope():
                            deliver_message(cast(ShardingEnvelope[T], msg))
                        case _:
                            # shouldn't be any other message types, but just in case
                            log.warning(
                                "%s: Stashing unexpected message [%s] while waiting for remember "
                                "entities update of starts [%s], stops [%s]",
                                type_name,
                                type(msg).__name__,
                                ", ".join(update.started),
                                ", ".join(update.stopped),
                            )
                            stash.stash(msg)
                    return Behaviors.same

                return Behaviors.receive(on_message).receive_signal(on_signal)

            def on_update_done(starts: frozenset[str], stops: frozenset[str]) -> None:
                """Shard.scala 730-770."""
                # entities can contain both ids from start/stops and pending ones, so we need
                # to mark the completed ones as complete to get the set of pending ones
                for entity_id in starts:
                    state_before_start = entities.entity_state(entity_id)
                    # this will start the entity and transition the entity state in sessions to
                    # active
                    get_or_create_entity(entity_id)
                    send_msg_buffer(entity_id)
                    if isinstance(state_before_start, RememberingStart):
                        for ack_to in state_before_start.ack_to:
                            ack_to.tell(StartEntityAck(entity_id, shard_id))
                for entity_id in stops:
                    match entities.entity_state(entity_id):
                        case RememberingStop():
                            # this updates entity state
                            passivate_completed(entity_id)
                        case state:
                            raise RuntimeError(
                                f"Unexpected state [{state}] when storing stop completed for "
                                f"entity id [{entity_id}]"
                            )
                pending_starts, pending_stops = entities.pending_remember_entities()
                if not pending_starts and not pending_stops:
                    if verbose_debug:
                        log.debug(
                            "%s: Update complete, no pending updates, going to idle", type_name
                        )
                    context_become(stash.unstash_all(idle()))
                else:
                    # Note: no unstashing as long as we are batching, is that a problem?
                    pending_start_ids = frozenset(pending_starts)
                    if verbose_debug:
                        log.debug(
                            "%s: Update complete, pending updates, doing another write. "
                            "Starts [%s], stops [%s]",
                            type_name,
                            ", ".join(pending_start_ids),
                            ", ".join(pending_stops),
                        )
                    remember_update(pending_start_ids, pending_stops)

            def receive_remember_entity_command(
                msg: _RestartTerminatedEntity | EntitiesMovedToOtherShard,
            ) -> None:
                """Shard.scala 787-816."""
                match msg:
                    case _RestartTerminatedEntity(entity_id):
                        match entities.entity_state(entity_id):
                            case WaitingForRestart():
                                if verbose_debug:
                                    log.debug(
                                        "%s: Restarting entity unexpectedly terminated entity "
                                        "[%s]",
                                        type_name,
                                        entity_id,
                                    )
                                get_or_create_entity(entity_id)
                            case Active():
                                # it up could already have been started, that's fine
                                if verbose_debug:
                                    log.debug(
                                        "%s: Got RestartTerminatedEntity for [%s] but it is "
                                        "already running",
                                        type_name,
                                        entity_id,
                                    )
                            case other:
                                raise RuntimeError(
                                    f"Unexpected state for [{entity_id}] when getting "
                                    f"RestartTerminatedEntity: [{other}]"
                                )
                    case EntitiesMovedToOtherShard(moved_entity_ids):
                        log.info(
                            "%s: Clearing [%d] remembered entities started elsewhere because of "
                            "changed shard id extractor",
                            type_name,
                            len(moved_entity_ids),
                        )
                        for entity_id in moved_entity_ids:
                            match entities.entity_state(entity_id):
                                case RememberedButNotCreated():
                                    entities.remembering_stop(entity_id)
                                case other:
                                    raise RuntimeError(
                                        f"Unexpected state for [{entity_id}] when getting "
                                        f"ShardIdsMoved: [{other}]"
                                    )
                        remember_update(remove=moved_entity_ids)

            def start_entity(entity_id: str, ack_to: ActorRef[StartEntityAck] | None) -> None:
                """Shard.scala 820-850: this could be because of a start message or due to a new
                message for the entity; if it is a start entity then start entity ack is sent
                after it is created."""
                match entities.entity_state(entity_id):
                    case Active():
                        if verbose_debug:
                            log.debug(
                                "%s: Request to start entity [%s] (Already started)",
                                type_name,
                                entity_id,
                            )
                        if ack_to is not None:
                            ack_to.tell(StartEntityAck(entity_id, shard_id))
                    case RememberingStart():
                        entities.remembering_start(entity_id, ack_to)
                    case RememberedButNotCreated() | WaitingForRestart() as state:
                        # already remembered or waiting for backoff to restart, just start it -
                        # this is the normal path for initially remembered entities getting
                        # started
                        log.debug(
                            "%s: Request to start entity [%s] (in state [%s])",
                            type_name,
                            entity_id,
                            state,
                        )
                        get_or_create_entity(entity_id)
                        if ack_to is not None:
                            ack_to.tell(StartEntityAck(entity_id, shard_id))
                    case Passivating():
                        # since StartEntity is handled in deliverMsg we can buffer a StartEntity
                        # to handle when passivation completes (triggering an immediate restart)
                        message_buffers.append(entity_id, _StartEntity(entity_id, ack_to))
                    case RememberingStop():
                        # Optimally: if stop is already write in progress, we want to stash, if
                        # it is batched for later write we'd want to cancel but for now
                        stash.stash(_StartEntity(entity_id, ack_to))
                    case _:  # NoState
                        # started manually from the outside, or the shard id extractor was
                        # changed since the entity was remembered we need to store that it was
                        # started
                        log.debug(
                            "%s: Request to start entity [%s] and ack to [%s]",
                            type_name,
                            entity_id,
                            ack_to,
                        )
                        entities.remembering_start(entity_id, ack_to)
                        remember_update(add=frozenset({entity_id}))

            def receive_coordinator_message(msg: HandOff) -> Behavior[Any]:
                """Shard.scala 852-858. ``HandOff`` is the only coordinator message a shard
                gets."""
                if msg.shard == shard_id:
                    return hand_off(msg.reply_to)
                log.warning(
                    "%s: Shard [%s] can not hand off for another Shard [%s]",
                    type_name,
                    shard_id,
                    msg.shard,
                )
                return Behaviors.same

            def receive_shard_query(msg: GetShardState | GetShardStats) -> None:
                """Shard.scala 860-870. ``GetShardState`` is Pekko's ``GetCurrentShardState``."""
                match msg:
                    case GetShardState(reply_to):
                        if verbose_debug:
                            log.debug(
                                "%s: GetCurrentShardState, full state: [%s], active: [%s]",
                                type_name,
                                entities,
                                entities.active_entity_ids(),
                            )
                        reply_to.tell(CurrentShardState(shard_id, entities.active_entity_ids()))
                    case GetShardStats(reply_to):
                        reply_to.tell(ShardStats(shard_id, entities.size))

            def hand_off(reply_to: ActorRef[ShardStopped]) -> Behavior[Any]:
                """Shard.scala 872-905. While preparing for shutdown, an entity with no stop
                message is stopped after its pending messages, as Pekko's ``PoisonPill``. With a
                stopper started, it returns Pekko's anonymous ``become`` (898-900): every message
                is unhandled, and the stopper's end arrives as the ``Terminated`` signal."""
                nonlocal hand_off_stopper_ref
                if hand_off_stopper_ref is not None:
                    log.warning(
                        "%s: HandOff shard [%s] received during existing handOff",
                        type_name,
                        shard_id,
                    )
                    return Behaviors.same
                log.debug("%s: HandOff shard [%s]", type_name, shard_id)
                # does conversion so only do once
                active_entities = entities.active_entities()
                if preparing_for_shutdown:
                    log.info(
                        "%s: HandOff shard [%s] while preparing for shutdown. "
                        "Stopping right away.",
                        type_name,
                        shard_id,
                    )
                    stop_message = entity.stop_message
                    for ref in active_entities:
                        if stop_message is None:
                            ctx.stop(ref, after_pending=True)
                        else:
                            ref.tell(stop_message)
                    reply_to.tell(ShardStopped(shard_id))
                    return Behaviors.stopped()
                if active_entities:
                    entity_hand_off_timeout = settings.entity_hand_off_timeout
                    log.debug(
                        "%s: Starting HandOffStopper for shard [%s] to terminate [%d] entities.",
                        type_name,
                        shard_id,
                        len(active_entities),
                    )
                    for ref in active_entities:
                        ctx.unwatch(ref)
                    stopper = ctx.spawn(
                        hand_off_stopper(
                            type_name,
                            shard_id,
                            reply_to,
                            active_entities,
                            entity.stop_message,
                            entity_hand_off_timeout,
                            lambda ref, after_pending: ctx.stop(ref, after_pending=after_pending),
                        ),
                        "HandOffStopper",
                    )
                    ctx.watch(stopper)
                    hand_off_stopper_ref = stopper
                    # During hand off we only care about watching for termination of the hand off
                    # stopper
                    return Behaviors.receive_signal(on_signal)
                reply_to.tell(ShardStopped(shard_id))
                return Behaviors.stopped()

            def receive_terminated(ref: ActorRef[Any]) -> Behavior[Any]:
                """Shard.scala 907-910."""
                if hand_off_stopper_ref is not None and hand_off_stopper_ref == ref:
                    return Behaviors.stopped()
                return Behaviors.same

            def receive_lease_lost(msg: _LeaseLost) -> Behavior[Any]:
                """Shard.scala 772-785."""
                # The shard region will re-create this when it receives a message for this shard
                log.error(
                    "%s: Shard id [%s] lease lost, stopping shard and killing [%d] entities.%s",
                    type_name,
                    shard_id,
                    entities.size,
                    "" if msg.reason is None else f" Reason for losing lease: {msg.reason!r}",
                )
                # Stop entities ASAP rather than send termination message
                return Behaviors.stopped()

            def entity_terminated(ref: ActorRef[Any]) -> None:
                """Shard.scala 913-975."""
                entity_id = entities.entity_id(ref)
                if entity_id is None:
                    log.warning("%s: Unexpected entity terminated: %s", type_name, ref)
                    return
                passivation_strategy.entity_terminated(entity_id)
                match entities.entity_state(entity_id):
                    case RememberingStop():
                        if verbose_debug:
                            log.debug(
                                "%s: Stop of [%s] arrived, already is among the pending stops",
                                type_name,
                                entity_id,
                            )
                    case Active():
                        if remember_entities_store is not None:
                            log.debug(
                                "%s: Entity [%s] stopped without passivating, will restart after "
                                "backoff",
                                type_name,
                                entity_id,
                            )
                            entities.waiting_for_restart(entity_id)
                            msg = _RestartTerminatedEntity(entity_id)
                            ctx.timers.start_single_timer(
                                msg, msg, settings.entity_restart_backoff
                            )
                        else:
                            log.debug("%s: Entity [%s] terminated", type_name, entity_id)
                            entities.remove_entity(entity_id)
                    case Passivating():
                        if remember_entities_store is not None:
                            if entities.pending_remembered_entities_exist():
                                # will go in next batch update
                                if verbose_debug:
                                    log.debug(
                                        "%s: [%s] terminated after passivating, arrived while "
                                        "updating, adding it to batch of pending stops",
                                        type_name,
                                        entity_id,
                                    )
                                entities.remembering_stop(entity_id)
                            else:
                                entities.remembering_stop(entity_id)
                                remember_update(remove=frozenset({entity_id}))
                        elif message_buffers.get_or_empty(entity_id):
                            if verbose_debug:
                                log.debug(
                                    "%s: [%s] terminated after passivating, buffered messages "
                                    "found, restarting",
                                    type_name,
                                    entity_id,
                                )
                            entities.remove_entity(entity_id)
                            get_or_create_entity(entity_id)
                            send_msg_buffer(entity_id)
                        else:
                            if verbose_debug:
                                log.debug(
                                    "%s: [%s] terminated after passivating", type_name, entity_id
                                )
                            entities.remove_entity(entity_id)
                    case unexpected:
                        log.warning(
                            "%s: Got a terminated for [%s], entityId [%s] which is in unexpected "
                            "state [%s]",
                            type_name,
                            entities.entity(entity_id),
                            entity_id,
                            unexpected,
                        )

            def passivate(entity_ref: ActorRef[Any], stop_message: object | None) -> None:
                """Shard.scala 977-997. With no stop message the entity is stopped after its
                pending messages, as Pekko's ``PoisonPill``."""
                entity_id = entities.entity_id(entity_ref)
                if entity_id is None:
                    log.debug(
                        "%s: Unknown entity passivating [%s]. Not sending stopMessage back to "
                        "entity",
                        type_name,
                        entity_ref,
                    )
                elif entities.is_passivating(entity_id):
                    log.debug(
                        "%s: Passivation already in progress for [%s]. Not sending stopMessage "
                        "back to entity",
                        type_name,
                        entity_id,
                    )
                elif message_buffers.get_or_empty(entity_id):
                    log.debug(
                        "%s: Passivation when there are buffered messages for [%s], ignoring "
                        "passivation",
                        type_name,
                        entity_id,
                    )
                else:
                    if verbose_debug:
                        log.debug("%s: Passivation started for [%s]", type_name, entity_id)
                    entities.entity_passivating(entity_id)
                    if stop_message is None:
                        ctx.stop(entity_ref, after_pending=True)
                    else:
                        entity_ref.tell(stop_message)

            def active_entity_limit_updated(updated: SetActiveEntityLimit) -> None:
                """Shard.scala 999-1002."""
                passivate_entities(passivation_strategy.limit_updated(updated.per_region_limit))

            def shards_updated(updated: ShardsUpdated) -> None:
                """Shard.scala 1004-1007."""
                passivate_entities(passivation_strategy.shards_updated(updated.active_shards))

            def passivate_entities_after_interval() -> None:
                """Shard.scala 1009-1012."""
                passivate_entities(passivation_strategy.interval_passed())

            def passivate_entities(entities_to_passivate: PassivateEntities) -> None:
                """Shard.scala 1014-1024."""
                if entities_to_passivate:
                    refs_to_passivate = [
                        ref
                        for entity_id in entities_to_passivate
                        if (ref := entities.entity(entity_id)) is not None
                    ]
                    if refs_to_passivate:
                        log.debug(
                            "%s: Passivating [%d] entities", type_name, len(refs_to_passivate)
                        )
                        for ref in refs_to_passivate:
                            passivate(ref, entity.stop_message)

            def passivate_completed(entity_id: str) -> None:
                """Shard.scala 1027-1047: after entity stopped."""
                has_buffered_messages = bool(message_buffers.get_or_empty(entity_id))
                entities.remove_entity(entity_id)
                if has_buffered_messages:
                    log.debug(
                        "%s: Entity stopped after passivation [%s], but will be started again due "
                        "to buffered messages",
                        type_name,
                        entity_id,
                    )
                    if remember_entities:
                        # trigger start or batch in case we're already writing to the remember
                        # store
                        entities.remembering_start(entity_id, None)
                        if not entities.pending_remembered_entities_exist():
                            remember_update(frozenset({entity_id}))
                    else:
                        get_or_create_entity(entity_id)
                        send_msg_buffer(entity_id)
                else:
                    log.debug("%s: Entity stopped after passivation [%s]", type_name, entity_id)

            def deliver_message(envelope: ShardingEnvelope[T]) -> None:
                """Shard.scala 1049-1124. A ``StartEntity`` in an envelope has no sender, so no
                one is acked."""
                entity_id, payload = envelope.entity_id, envelope.message
                if not entity_id:
                    log.warning(
                        "%s: Id must not be empty, dropping message [%s]",
                        type_name,
                        type(envelope).__name__,
                    )
                    ctx.system.dropped(envelope, "No recipient entity id", ctx.self)
                    return
                if isinstance(payload, StartEntity):
                    # Handling StartEntity both here and in the receives allows for sending it
                    # both as is and in an envelope to be extracted by the entity id extractor.

                    # we can only start a new entity if we are not currently waiting for another
                    # write
                    # Deviation: Shard.scala 1061-1066 batches whatever the state; ohra batches
                    # only an entity with no state, as any other (Active, Passivating) cannot
                    # become RememberingStart, and start_entity handles them (appendix B).
                    if entities.pending_remembered_entities_exist() and isinstance(
                        entities.entity_state(entity_id), NoState
                    ):
                        if verbose_debug:
                            log.debug(
                                "%s: StartEntity(%s) from [%s], adding to batch",
                                type_name,
                                payload.entity_id,
                                None,
                            )
                        entities.remembering_start(entity_id, None)
                    else:
                        if verbose_debug:
                            log.debug(
                                "%s: StartEntity(%s) from [%s], starting",
                                type_name,
                                payload.entity_id,
                                None,
                            )
                        start_entity(payload.entity_id, None)
                    return
                match entities.entity_state(entity_id):
                    case Active(ref):
                        if verbose_debug:
                            log.debug(
                                "%s: Delivering message of type [%s] to [%s]",
                                type_name,
                                type(payload).__name__,
                                entity_id,
                            )
                        entities_to_passivate = passivation_strategy.entity_touched(entity_id)
                        ref.tell(payload)
                        passivate_entities(entities_to_passivate)
                    case RememberingStart() | RememberingStop() | Passivating():
                        append_to_message_buffer(entity_id, envelope)
                    case WaitingForRestart() | RememberedButNotCreated() as state:
                        if verbose_debug:
                            log.debug(
                                "%s: Delivering message of type [%s] to [%s] (starting because "
                                "[%s])",
                                type_name,
                                type(payload).__name__,
                                entity_id,
                                state,
                            )
                        get_or_create_entity(entity_id).tell(payload)
                    case _ if not remember_entities:  # NoState
                        # don't buffer if remember entities not enabled
                        ref = get_or_create_entity(entity_id)
                        entities_to_passivate = passivation_strategy.entity_touched(entity_id)
                        ref.tell(payload)
                        passivate_entities(entities_to_passivate)
                    case _ if entities.pending_remembered_entities_exist():
                        # No actor running and write in progress for some other entity id (can
                        # only happen with remember entities enabled)
                        if verbose_debug:
                            log.debug(
                                "%s: Buffer message [%s] to [%s] (which is not started) because "
                                "of write in progress for [%s]",
                                type_name,
                                type(payload).__name__,
                                entity_id,
                                entities.pending_remember_entities(),
                            )
                        append_to_message_buffer(entity_id, envelope)
                        entities.remembering_start(entity_id, None)
                    case _:
                        # No actor running and no write in progress, start actor and deliver
                        # message when started
                        if verbose_debug:
                            log.debug(
                                "%s: Buffering message [%s] to [%s] and starting actor",
                                type_name,
                                type(payload).__name__,
                                entity_id,
                            )
                        append_to_message_buffer(entity_id, envelope)
                        entities.remembering_start(entity_id, None)
                        remember_update(add=frozenset({entity_id}))

            def get_or_create_entity(entity_id: str) -> ActorRef[T]:
                """Shard.scala 1127-1139. The behavior is created inside the child's own setup, so
                a failing factory fails that entity, not the shard."""
                child = entities.entity(entity_id)
                if child is not None:
                    return child
                entity_ctx = EntityContext(entity.type_key, entity_id, ctx.self)
                create = entity.create_behavior
                behavior: Behavior[T] = Behaviors.setup(lambda _ctx: create(entity_ctx))
                a = ctx.spawn(behavior, _child_name(entity_id))
                ctx.watch_with(a, _EntityTerminated(a))
                log.debug(
                    "%s: Started entity [%s] with entity id [%s] in shard [%s]",
                    type_name,
                    a,
                    entity_id,
                    shard_id,
                )
                entities.add_entity(entity_id, a)
                entity_created(entity_id)
                return a

            def entity_created(entity_id: str) -> int:
                """Shard.scala 1146: called when an entity has been created; the number of active
                entities."""
                return entities.nr_active_entities()

            def append_to_message_buffer(entity_id: str, msg: ShardingEnvelope[T]) -> None:
                """Shard.scala 1149-1163: ``buffer_size`` caps the messages of all the shard's
                buffers together."""
                if message_buffers.total_size >= settings.buffer_size:
                    if log.isEnabledFor(logging.DEBUG):
                        log.debug(
                            "%s: Buffer is full, dropping message of type [%s] for entity [%s]",
                            type_name,
                            type(msg).__name__,
                            entity_id,
                        )
                    ctx.system.dropped(msg, f"Buffer for [{entity_id}] is full", ctx.self)
                else:
                    if log.isEnabledFor(logging.DEBUG):
                        log.debug(
                            "%s: Message of type [%s] for entity [%s] buffered",
                            type_name,
                            type(msg).__name__,
                            entity_id,
                        )
                    message_buffers.append(entity_id, msg)

            def send_msg_buffer(entity_id: str) -> None:
                """Shard.scala 1166-1181: after entity started."""
                # Get the buffered messages and remove the buffer
                messages = message_buffers.get_or_empty(entity_id)
                message_buffers.remove(entity_id)
                if messages:
                    get_or_create_entity(entity_id)
                    log.debug(
                        "%s: Sending message buffer for entity [%s] ([%d] messages)",
                        type_name,
                        entity_id,
                        len(messages),
                    )
                    # Now there is no deliveryBuffer we can try to redeliver
                    # and as the child exists, the message will be directly forwarded
                    for msg in messages:
                        match msg:
                            case _StartEntity(start_id, ack_to):
                                start_entity(start_id, ack_to)
                            case _:
                                deliver_message(cast(ShardingEnvelope[T], msg))

            def remember_entity_store_crashed(msg: _RememberEntityStoreCrashed) -> None:
                """Shard.scala 1190-1192."""
                raise RuntimeError(f"Remember entities store [{msg.store}] crashed")

            async def post_stop() -> None:
                """Shard.scala 1194-1198, and Pekko's default ``preRestart``, which runs
                ``postStop``. The interval timer stops with the shard; what is still
                buffered is dropped silently, as Pekko's buffers are. Pekko releases the lease
                without waiting; this awaits the release, bounded by the lease's operation
                timeout, so nothing outlives the shard."""
                if lease is not None:
                    try:
                        await asyncio.wait_for(
                            lease.release(), lease.settings.timeout_settings.operation_timeout
                        )
                    except Exception:
                        log.warning(
                            "%s: Failed to release the lease of shard id [%s]",
                            type_name,
                            shard_id,
                            exc_info=True,
                        )
                log.debug("%s: Shard [%s] shutting down", type_name, shard_id)

            # Pekko's preStart
            if ctx.system.cluster is not None:
                for cls in (MemberPreparingForShutdown, MemberReadyForShutdown):
                    ctx.system.cluster.subscriptions.tell(Subscribe(ctx.self, cls))
            return acquire_lease_if_needed()

        return setup

    def outer(_ctx: ActorContext[Any]) -> Behavior[Any]:
        # One stash per actor: a restart re-runs ``shard``'s setup with the same buffer
        stash = StashBuffer[Any](sys.maxsize)
        return Behaviors.supervise(shard(stash)).on_failure(SupervisorStrategy.restart())

    return Behaviors.setup(outer)


def _ask_all[R](
    targets: Mapping[str, ActorRef[Any]],
    query: Callable[[ActorRef[R]], Replyable[R]],
    timeout: float,
    done: Callable[[dict[str, R], frozenset[str]], None],
) -> Behavior[Any]:
    """Pekko's ``Future.traverse`` of ``askOne`` (ShardRegion.scala 1087-1098): a short-lived
    child that asks every target, each ask with its own ``timeout``, and once each has answered
    or failed calls ``done`` with the answers and the keys of the targets that failed. With no
    targets it calls ``done`` at once."""
    targets = dict(targets)  # as Pekko's immutable map, whatever the caller does next

    @Behaviors.setup
    def setup(ctx: ActorContext[tuple[str, Try[R]]]) -> Behavior[tuple[str, Try[R]]]:
        responses: dict[str, R] = {}
        failed: set[str] = set()

        def complete() -> Behavior[tuple[str, Try[R]]]:
            done(responses, frozenset(failed))
            return Behaviors.stopped()

        if not targets:
            return complete()

        def ask_one(key: str) -> Callable[[Try[R]], tuple[str, Try[R]]]:
            return lambda result: (key, result)

        for key, target in targets.items():
            ctx.ask(target, query, ask_one(key), timeout)

        @Behaviors.receive
        def on_message(
            _ctx: ActorContext[tuple[str, Try[R]]], msg: tuple[str, Try[R]]
        ) -> Behavior[tuple[str, Try[R]]]:
            match msg:
                case (key, Success(value)):
                    responses[key] = value
                case (key, _):
                    failed.add(key)
            return complete() if len(responses) + len(failed) == len(targets) else Behaviors.same

        return on_message

    return setup


def query_shards[R](
    type_name: str,
    shards: Mapping[str, ActorRef[Any]],
    query: Callable[[ActorRef[R]], Replyable[R]],
    timeout: float,
    done: Callable[[dict[str, R], frozenset[str]], None],
) -> Behavior[Any]:
    """Pekko's ``queryShards`` (ShardRegion.scala 1084-1101) as a short-lived child: ask every
    shard, and pass ``done`` the answers by shard id and the shards that did not answer within
    ``timeout``. When some failed, it logs Pekko's ``ShardsQueryResult.toString``
    (ShardingQueries.scala 37-57) at WARNING; every shard is queried, so ``queried`` is the
    total."""

    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        def logged(responses: dict[str, R], failed: frozenset[str]) -> None:
            if failed:
                ctx.log.warning(
                    "%s: Queried [%d] shards: [%d] responsive, [%d] failed after %s.",
                    type_name,
                    len(failed) + len(responses),
                    len(responses),
                    len(failed),
                    f"{timeout:g} s",
                )
            done(responses, failed)

        return _ask_all(shards, query, timeout, logged)

    return Behaviors.setup(setup)


def region_state_query(
    type_name: str,
    shards: Mapping[str, ActorRef[Any]],
    reply_to: ActorRef[CurrentShardRegionState],
    timeout: float,
) -> Behavior[Any]:
    """``replyToRegionStateQuery`` (ShardRegion.scala 1056-1064): the union of what the shards
    that answered hold."""

    def done(responses: dict[str, CurrentShardState], failed: frozenset[str]) -> None:
        ids = frozenset(i for r in responses.values() for i in r.entity_ids)
        shard_ids = frozenset(r.shard for r in responses.values())
        reply_to.tell(CurrentShardRegionState(ids, shard_ids, failed))

    return query_shards(type_name, shards, GetShardState, timeout, done)


def region_stats_query(
    type_name: str,
    shards: Mapping[str, ActorRef[Any]],
    reply_to: ActorRef[ShardRegionStats],
    timeout: float,
) -> Behavior[Any]:
    """``replyToRegionStatsQuery`` (ShardRegion.scala 1066-1072)."""

    def done(responses: dict[str, ShardStats], failed: frozenset[str]) -> None:
        stats = {s.shard_id: s.entity_count for s in responses.values()}
        reply_to.tell(ShardRegionStats(stats, failed))

    return query_shards(type_name, shards, GetShardStats, timeout, done)


type _RegionCmd[T] = (
    ShardingEnvelope[T]
    | GetShardRegionState
    | GetShardRegionStats
    | ShardInitialized
    | SetActiveEntityLimit
)


def _log_passivation_strategy(
    log: logging.Logger,
    type_name: str,
    strategy: PassivationStrategy,
    remember_entities: bool = False,
) -> None:
    """ShardRegion.scala 698-705."""
    if remember_entities:
        log.debug(
            "%s: Entities will not be passivated automatically because 'rememberEntities' is "
            "enabled.",
            type_name,
        )
    else:
        log.info(
            "%s: Automatic entity passivation: %s",
            type_name,
            describe_passivation_strategy(strategy),
        )


class Sharding:
    @overload
    @staticmethod
    def init[T, E](
        entity: ExtractedEntity[T, E],
    ) -> Behavior[
        E | ShardingEnvelope[T] | GetShardRegionState | GetShardRegionStats | SetActiveEntityLimit
    ]: ...
    @overload
    @staticmethod
    def init[T](
        entity: Entity[T],
    ) -> Behavior[
        ShardingEnvelope[T] | GetShardRegionState | GetShardRegionStats | SetActiveEntityLimit
    ]: ...
    @staticmethod
    def init[T](entity: Entity[T]) -> Behavior[Any]:
        """The shard region behavior for one entity type, inside one process. Spawn it; message it
        through entity refs. Each shard id gets a child Shard actor on first use. As Pekko's
        region, it passes ``SetActiveEntityLimit`` to every shard, and tells every shard the
        shard count when it changes; it has no graceful shutdown. It answers
        ``GetShardRegionState`` and ``GetShardRegionStats`` through a child that asks every
        shard. It refuses ``remember_entities``, which needs ``ClusterSharding``'s Replicator."""
        settings = entity.effective_settings
        if settings.remember_entities:
            raise ValueError(
                "remember_entities needs ClusterSharding: the single-node Sharding has no "
                "Replicator to remember entities in"
            )
        strategy = settings.passivation_strategy
        extractor = entity.extractor

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
                shard_id = extractor.shard_id(entity_id)
                shard = shards.get(shard_id)
                if shard is None:
                    shard = ctx.spawn(shard_behavior(entity, shard_id, ctx.self), shard_id)
                    ctx.watch(shard)
                    shards[shard_id] = shard
                    ids_by_ref[shard] = shard_id
                    shards_updated()
                return shard

            def deliver(envelope: ShardingEnvelope[T]) -> None:
                if not envelope.entity_id:
                    ctx.log.warning("dropping message for empty entity id: %r", envelope.message)
                    ctx.system.dead_letter(envelope.message, ctx.self)
                else:
                    shard_for(envelope.entity_id).tell(envelope)

            @Behaviors.receive
            def on_message(
                _ctx: ActorContext[_RegionCmd[T]], msg: _RegionCmd[T]
            ) -> Behavior[_RegionCmd[T]]:
                match msg:
                    case ShardingEnvelope():
                        deliver(msg)
                    case ShardInitialized():
                        pass  # the shard stashes while it waits for its lease
                    case SetActiveEntityLimit():
                        # ShardRegion.scala 767 and 1278 (deliverToAllShards)
                        for shard in shards.values():
                            shard.tell(msg)
                    case GetShardRegionState(reply_to):
                        ctx.spawn_anonymous(
                            region_state_query(
                                entity.type_key.name,
                                shards,
                                reply_to,
                                settings.shard_region_query_timeout,
                            )
                        )
                    case GetShardRegionStats(reply_to):
                        ctx.spawn_anonymous(
                            region_stats_query(
                                entity.type_key.name,
                                shards,
                                reply_to,
                                settings.shard_region_query_timeout,
                            )
                        )
                    case _ if entity.message_extractor is not None:
                        # Pekko Typed's ExtractorAdapter: a message of the extractor's own
                        message: Any = msg
                        deliver(
                            ShardingEnvelope(
                                extractor.entity_id(message), extractor.unwrap_message(message)
                            )
                        )
                    case _:
                        pass
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
