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
from .passivation_settings import PassivationStrategySettings
from .shard import (
    ClusterShardingSettings,
    CurrentShardRegionState,
    Entity,
    EntityContext,
    EntityRef,
    EntityTypeKey,
    GetShardRegionState,
    HandOff,
    Passivate,
    ShardCommand,
    Sharding,
    ShardingEnvelope,
    ShardStopped,
    shard_id_of,
)

if TYPE_CHECKING:
    from ..ddata.settings import ReplicatorSettings
    from ...cell import ActorCell
    from ...system import ActorSystem

__all__ = [
    "ClusterSharding",
    "ClusterShardingSettings",
    "CoordinatorState",
    "CurrentShardRegionState",
    "Entity",
    "EntityContext",
    "EntityRef",
    "EntityTypeKey",
    "GetShardRegionState",
    "HandOff",
    "LeastShardAllocationStrategy",
    "Passivate",
    "PassivationStrategySettings",
    "ShardCommand",
    "ShardHomeAllocated",
    "ShardHomeDeallocated",
    "ShardRegionRegistered",
    "ShardRegionTerminated",
    "ShardStopped",
    "Sharding",
    "ShardingEnvelope",
    "shard_id_of",
]


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

    def init[M](self, entity: Entity[M]) -> ActorRef[ShardingEnvelope[M]]:
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
        progress = _ExitingProgress()
        region = self._parent.spawn_child(region_behavior(entity, settings, progress), name, None)
        CoordinatedShutdown(self.system).add_task(
            "cluster-sharding-shutdown-region",
            f"region-shutdown-{name}",
            progress.task(Cluster(self.system), region.ref),
        )
        # Pekko's BackoffOpts.onStop restarts a coordinator that throws at once, with its default
        # decider; the backoff is for one that stops, which this one does only after its stop
        # message
        supervised = Behaviors.supervise(
            coordinator_behavior(name, settings, self._replicator)
        ).on_failure(SupervisorStrategy.restart())
        coordinator = SingletonActor(supervised, f"{name}Coordinator").with_stop_message(
            _Terminate()
        )
        if settings.coordinator_singleton_settings is not None:
            coordinator = coordinator.with_settings(settings.coordinator_singleton_settings)
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
