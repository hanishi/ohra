"""Apache Pekko's ClusterSharding across nodes: a ShardCoordinator singleton allocating shards to
regions with the LeastShardAllocationStrategy, a ShardRegion on every node, hand-off for
rebalancing and for a leaving node. The coordinator keeps its state in Distributed Data, on
sharding's own Replicator, as Pekko's DDataShardCoordinator."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...behavior import Behaviors
from ...ref import ActorRef
from ...supervision import SupervisorStrategy
from ...coordinated_shutdown import CoordinatedShutdown
from ..core import Cluster
from ..ddata.replicator import Replicator
from ..singleton import ClusterSingleton, SingletonActor
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
from .passivation_settings import PassivationStrategySettings
from .shard import (
    ClusterShardingSettings,
    ClusterShardingStats,
    CurrentShardRegionState,
    Entity,
    EntityContext,
    EntityRef,
    EntityTypeKey,
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
    "GetClusterShardingStats",
    "GetShardRegionState",
    "GetShardRegionStats",
    "HandOff",
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
    "StartEntity",
    "shard_id_of",
]


def _remember_entities_provider(
    name: str, settings: ClusterShardingSettings, replicator: ActorRef[Replicator.Command]
) -> DDataRememberEntitiesProvider:
    """ClusterSharding.scala 766-794, without roles: the provider of the remember-entities
    stores, the coordinator's and every shard's, on sharding's Replicator."""
    return DDataRememberEntitiesProvider(name, settings, settings.majority_min_cap, replicator)


# --- the extension -------------------------------------------------------------------------------


class ClusterSharding:
    """``ClusterSharding(system).init(Entity(key, create))`` returns the region ref;
    ``entity_ref_for(key, id)`` addresses an entity wherever it lives, as Pekko Typed."""

    system: ActorSystem[Any]
    _regions: dict[str, ActorRef[Any]]
    _entities: dict[str, Entity[Any]]
    _parent: ActorCell[Any] | None
    _replicator: ActorRef[Replicator.Command] | None
    _replicator_settings: ReplicatorSettings | None

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
            self._replicator = None
            self._replicator_settings = None
            system.cluster_sharding = self
        return system.cluster_sharding

    def init[M](
        self, entity: Entity[M]
    ) -> ActorRef[
        ShardingEnvelope[M]
        | StartEntity
        | GetShardRegionState
        | GetShardRegionStats
        | GetClusterShardingStats
        | SetActiveEntityLimit
    ]:
        """The region of the entity type on this node, as Pekko Typed's ``init``. It takes the
        envelopes, ``StartEntity``, and the region queries and ``SetActiveEntityLimit`` that
        Pekko's region takes as its own messages."""
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
        # ClusterSharding.scala 730-815: the guardian spawns one Replicator, for all entity types
        wanted = settings.effective_replicator_settings(Cluster(self.system).settings)
        if self._replicator is None:
            self._replicator_settings = wanted
            self._replicator = self._parent.spawn_child(
                Replicator.behavior(wanted), "replicator", None
            ).ref
        elif wanted != self._replicator_settings:
            self._parent.log.warning(
                "Entity type [%s] has other distributed_data settings than the first init; "
                "sharding's Replicator keeps the first's",
                name,
            )
        # ClusterSharding.scala 766-794: the stores of the coordinator and of the region's
        # shards, when remember entities is on
        provider = (
            _remember_entities_provider(name, settings, self._replicator)
            if settings.remember_entities
            else None
        )
        progress = _ExitingProgress()
        region = self._parent.spawn_child(
            region_behavior(entity, settings, progress, provider), name, None
        )
        CoordinatedShutdown(self.system).add_task(
            "cluster-sharding-shutdown-region",
            f"region-shutdown-{name}",
            progress.task(Cluster(self.system), region.ref),
        )
        supervised = Behaviors.supervise(
            coordinator_behavior(name, settings, self._replicator, provider)
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
        coordinator = SingletonActor(singleton, f"{name}Coordinator").with_stop_message(
            _Terminate()
        )
        if settings.coordinator_singleton_settings is not None:
            # Pekko's ClusterSharding overrides the role with sharding's own
            # (coordinatorSingletonRole); PR 3 sets it here, until then none
            coordinator = coordinator.with_settings(
                settings.coordinator_singleton_settings.with_no_role()
            )
        ClusterSingleton(self.system)._init_manager(coordinator)  # pyright: ignore[reportPrivateUsage]
        self._regions[name] = region.ref
        self._entities[name] = entity
        return region.ref

    def entity_ref_for[M](self, key: EntityTypeKey[M], entity_id: str) -> EntityRef[M]:
        region = self._regions.get(key.name)
        if region is None:
            raise LookupError(f"ClusterSharding.init was not called for {key.name!r} on this node")
        if not entity_id:
            raise ValueError("entity id must not be empty")
        return EntityRef(region, entity_id)
