"""Apache Pekko's ClusterSharding across nodes: a ShardCoordinator singleton allocating shards to
regions with the LeastShardAllocationStrategy, a ShardRegion on every node, hand-off for
rebalancing and for a leaving node. The coordinator keeps its state in Distributed Data, on
sharding's own Replicator, as Pekko's DDataShardCoordinator."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload
from urllib.parse import quote_plus

from ...behavior import Behaviors
from ...ref import ActorRef
from ...supervision import SupervisorStrategy
from ...coordinated_shutdown import CoordinatedShutdown
from ..core import Cluster
from ..ddata.replicator import Replicator
from ..singleton import ClusterSingleton, ClusterSingletonSettings, SingletonActor
from .backoff_supervisor import backoff_on_stop_supervisor
from .coordinator import (
    CoordinatorState,
    LeastShardAllocationStrategy,
    ShardHomeAllocated,
    ShardHomeDeallocated,
    ShardRegionRegistered,
    ShardRegionTerminated,
    coordinator_behavior,
    _Terminate,  # pyright: ignore[reportPrivateUsage]
)
from .region import _ExitingProgress, region_behavior  # pyright: ignore[reportPrivateUsage]
from .remember_entities import DDataRememberEntitiesProvider
from .message_extractor import (
    HashCodeMessageExtractor,
    HashCodeNoEnvelopeMessageExtractor,
    ShardingMessageExtractor,
)
from .passivation_settings import PassivationStrategySettings
from .shard import (
    ClusterShardingSettings,
    ClusterShardingStats,
    CurrentShardRegionState,
    Entity,
    EntityContext,
    EntityRef,
    EntityTypeKey,
    ExtractedEntity,
    GetClusterShardingStats,
    GetShardRegionState,
    GetShardRegionStats,
    HandOff,
    Passivate,
    SetActiveEntityLimit,
    ShardCommand,
    Sharding,
    ShardingEnvelope,
    ShardRegionStats,
    ShardStopped,
    StartEntity,
    shard_id_of,
)

if TYPE_CHECKING:
    from ..ddata.settings import ReplicatorSettings
    from ...cell import ActorCell
    from ...system import ActorSystem

__all__ = [
    "ClusterSharding",
    "ClusterShardingSettings",
    "ClusterShardingStats",
    "CoordinatorState",
    "CurrentShardRegionState",
    "Entity",
    "EntityContext",
    "EntityRef",
    "EntityTypeKey",
    "ExtractedEntity",
    "GetClusterShardingStats",
    "GetShardRegionState",
    "GetShardRegionStats",
    "HandOff",
    "HashCodeMessageExtractor",
    "HashCodeNoEnvelopeMessageExtractor",
    "LeastShardAllocationStrategy",
    "Passivate",
    "PassivationStrategySettings",
    "SetActiveEntityLimit",
    "ShardCommand",
    "ShardHomeAllocated",
    "ShardHomeDeallocated",
    "ShardRegionRegistered",
    "ShardRegionStats",
    "ShardRegionTerminated",
    "ShardStopped",
    "Sharding",
    "ShardingEnvelope",
    "ShardingMessageExtractor",
    "StartEntity",
    "shard_id_of",
]


def _replicator_name(role: str | None) -> str:
    """ClusterSharding.scala 755-758: ``replicator``, or the role URL-encoded as Java's
    ``URLEncoder`` does (``*`` kept, ``~`` encoded) and ``Replicator``."""
    if role is None:
        return "replicator"
    return quote_plus(role, safe="*").replace("~", "%7E") + "Replicator"


def _remember_entities_provider(
    name: str, settings: ClusterShardingSettings, replicator: ActorRef[Replicator.Command]
) -> DDataRememberEntitiesProvider:
    """ClusterSharding.scala 762-790: the provider of the remember-entities stores, the
    coordinator's and every shard's, on the given Replicator of sharding's."""
    return DDataRememberEntitiesProvider(name, settings, settings.majority_min_cap, replicator)


# --- the extension -------------------------------------------------------------------------------


class ClusterSharding:
    """``ClusterSharding(system).init(Entity(key, create))`` returns the region ref;
    ``entity_ref_for(key, id)`` addresses an entity wherever it lives, as Pekko Typed."""

    system: ActorSystem[Any]
    _regions: dict[str, ActorRef[Any]]
    _entities: dict[str, Entity[Any]]
    _parent: ActorCell[Any] | None
    _replicators: dict[str | None, ActorRef[Replicator.Command]]
    _replicator_settings: dict[str | None, ReplicatorSettings]

    def __new__(cls, system: ActorSystem[Any]) -> ClusterSharding:
        if system.cluster is None:
            raise RuntimeError(
                f"system {system.name!r} has no cluster: pass cluster=ClusterSettings(...) to "
                "ActorSystem"
            )
        if system.cluster_sharding is None:
            self = object.__new__(cls)
            self.system = system
            self._regions = {}
            self._entities = {}
            self._parent = None
            self._replicators = {}
            self._replicator_settings = {}
            system.cluster_sharding = self
        return system.cluster_sharding

    @overload
    def init[M, E](
        self, entity: ExtractedEntity[M, E]
    ) -> ActorRef[
        E
        | ShardingEnvelope[M]
        | StartEntity
        | GetShardRegionState
        | GetShardRegionStats
        | GetClusterShardingStats
        | SetActiveEntityLimit
    ]: ...
    @overload
    def init[M](
        self, entity: Entity[M]
    ) -> ActorRef[
        ShardingEnvelope[M]
        | StartEntity
        | GetShardRegionState
        | GetShardRegionStats
        | GetClusterShardingStats
        | SetActiveEntityLimit
    ]: ...
    def init[M](self, entity: Entity[M]) -> ActorRef[Any]:
        """The region of the entity type on this node, as Pekko Typed's ``init``. It takes the
        envelopes, ``StartEntity``, and the region queries and ``SetActiveEntityLimit`` that
        Pekko's region takes as its own messages; with ``Entity.with_message_extractor``, the
        extractor's messages too."""
        name = entity.type_key.name
        if name == "replicator":
            raise ValueError(
                "the entity type name 'replicator' is reserved for sharding's Replicator, "
                "/system/sharding/replicator"
            )
        existing = self._regions.get(name)
        if existing is not None:
            return existing
        settings = entity.effective_settings
        if self._parent is None:
            self._parent = self.system._system_root.spawn_child(Behaviors.empty, "sharding", None)  # pyright: ignore[reportPrivateUsage]
        parent = self._parent
        cluster = Cluster(self.system)
        host_shard = settings.should_host_shard(cluster)
        host_coordinator = settings.should_host_coordinator(cluster)
        # a proxy that runs no coordinator needs no store (internalStart 321-333)
        remember = settings.remember_entities and (host_shard or host_coordinator)
        # ClusterSharding.scala 774-777: must run Replicator for DDataRememberEntities on both
        # shard and coordinator roles (if different)
        store_role = (
            settings.role if settings.role == settings.coordinator_singleton_role else None
        )
        region_name = name if host_shard else f"{name}Proxy"
        # Pekko's actorOf fails on a taken name midway; here nothing starts
        roles = {store_role} if remember else set[str | None]()
        if host_coordinator:
            roles.add(settings.coordinator_singleton_role)
        new = [_replicator_name(r) for r in roles if r not in self._replicators]
        for child in [region_name, *new]:
            if child in parent.children or [region_name, *new].count(child) > 1:
                raise ValueError(
                    f"Entity type [{name}] cannot start: /system/sharding/{child} is taken"
                )
        provider: DDataRememberEntitiesProvider | None = None
        if remember:
            provider = _remember_entities_provider(
                name, settings, self._replicator(name, store_role, settings)
            )
        if host_coordinator:
            self._start_coordinator(name, settings, provider)
        progress = _ExitingProgress()
        if host_shard:
            region = parent.spawn_child(
                region_behavior(entity, settings, progress, provider), region_name, None
            )
        else:
            # internalStart (321-333), startProxy and StartProxy (880-905)
            parent.log.debug(
                "Starting Shard Region Proxy [%s] (no actors will be hosted on this node)...",
                name,
            )
            region = parent.spawn_child(
                region_behavior(entity, settings, progress, None, proxy=True),
                region_name,
                None,
            )
        CoordinatedShutdown(self.system).add_task(
            "cluster-sharding-shutdown-region",
            f"region-shutdown-{name}",
            progress.task(cluster, region.ref),
        )
        self._regions[name] = region.ref
        self._entities[name] = entity
        return region.ref

    def _replicator(
        self, name: str, role: str | None, settings: ClusterShardingSettings
    ) -> ActorRef[Replicator.Command]:
        """ClusterSharding.scala 731-760: one Replicator per role, ``replicator`` for none and
        ``<url-encoded role>Replicator`` for a role, with the settings of the first ``init``
        that needs it."""
        assert self._parent is not None
        wanted = settings.effective_replicator_settings(Cluster(self.system).settings, role)
        existing = self._replicators.get(role)
        if existing is not None:
            if wanted != self._replicator_settings[role]:
                self._parent.log.warning(
                    "Entity type [%s] has other distributed_data settings than the first init; "
                    "sharding's Replicator [%s] keeps the first's",
                    name,
                    existing.path,
                )
            return existing
        ref = self._parent.spawn_child(
            Replicator.behavior(wanted), _replicator_name(role), None
        ).ref
        self._replicators[role] = ref
        self._replicator_settings[role] = wanted
        return ref

    def _start_coordinator(
        self,
        name: str,
        settings: ClusterShardingSettings,
        provider: DDataRememberEntitiesProvider | None,
    ) -> None:
        """startCoordinatorIfNeeded (796-838): the coordinator under the on-stop backoff
        supervisor, as the singleton of the coordinator's role, on that role's Replicator."""
        role = settings.coordinator_singleton_role
        replicator = self._replicator(name, role, settings)
        supervised = Behaviors.supervise(
            coordinator_behavior(name, settings, replicator, provider)
        ).on_failure(SupervisorStrategy.restart())
        # ClusterSharding.scala 816-834: Pekko's BackoffOpts.onStop(...).withFinalStopMessage(_ ==
        # Terminate), with the coordinator as its child "coordinator". Its default decider
        # restarts a coordinator that throws at once; the backoff is for one that stops, as when
        # its remember-entities store stops
        backoff = settings.coordinator_failure_backoff
        singleton = backoff_on_stop_supervisor(
            supervised,
            "coordinator",
            backoff,
            backoff * 5,
            0.2,
            lambda m: isinstance(m, _Terminate),
        )
        base = settings.coordinator_singleton_settings or ClusterSingletonSettings()
        # Pekko's withRole(coordinatorSingletonRole) on the singleton settings (831)
        singleton_settings = base.with_role(role) if role is not None else base.with_no_role()
        coordinator = (
            SingletonActor(singleton, f"{name}Coordinator")
            .with_stop_message(_Terminate())
            .with_settings(singleton_settings)
        )
        ClusterSingleton(self.system)._init_manager(coordinator)  # pyright: ignore[reportPrivateUsage]

    def entity_ref_for[M](self, key: EntityTypeKey[M], entity_id: str) -> EntityRef[M]:
        region = self._regions.get(key.name)
        if region is None:
            raise LookupError(f"ClusterSharding.init was not called for {key.name!r} on this node")
        if not entity_id:
            raise ValueError("entity id must not be empty")
        return EntityRef(region, entity_id)
