"""Apache Pekko's ClusterSharding across nodes: a ShardCoordinator singleton allocating shards to
regions with the LeastShardAllocationStrategy, a ShardRegion on every node, hand-off for
rebalancing and for a leaving node. The coordinator rebuilds its state from the regions'
registrations instead of distributed data."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...behavior import Behaviors
from ...ref import ActorRef
from ...supervision import SupervisorStrategy
from ...coordinated_shutdown import CoordinatedShutdown
from ..core import Cluster
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
        region = self._parent.spawn_child(region_behavior(entity, settings, progress), name, None)
        CoordinatedShutdown(self.system).add_task(
            "cluster-sharding-shutdown-region",
            f"region-shutdown-{name}",
            progress.task(Cluster(self.system), region.ref),
        )
        # Pekko's BackoffOpts.onStop restarts a coordinator that throws at once, with its default
        # decider; the backoff is for one that stops, which this in-memory one never does but on
        # its stop message
        supervised = Behaviors.supervise(coordinator_behavior(name, settings)).on_failure(
            SupervisorStrategy.restart()
        )
        coordinator = SingletonActor(supervised, f"{name}Coordinator").with_stop_message(
            _Terminate()
        )
        if settings.coordinator_singleton_settings is not None:
            coordinator = coordinator.with_settings(settings.coordinator_singleton_settings)
        ClusterSingleton(self.system).init(coordinator)
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
