"""Pekko's ShardedDaemonProcessId.scala, ShardedDaemonProcessState.scala (the state, its key and
``initialState``), ShardedDaemonProcessSettings.scala and the ``sharded-daemon-process`` section
of reference.conf: the entity ids, the replicated scaling state and the settings of a sharded
daemon process. Then ShardedDaemonProcessImpl.scala: the ``ShardedDaemonProcess`` extension."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, ClassVar, Final, override

from ...behavior import Behavior
from ...remote.serialization import Serialization
from ..ddata.distributed_data import DistributedData
from ..ddata.key import Key
from ..ddata.replicated_data import ReplicatedData
from ..singleton import ClusterSingleton, ClusterSingletonSettings, SingletonActor
from .message_extractor import ShardingMessageExtractor
from .shard import ClusterShardingSettings, Entity, EntityContext, EntityTypeKey, ShardingEnvelope

if TYPE_CHECKING:
    from ...system import ActorSystem

SEPARATOR: Final = "|"


@dataclass(frozen=True)
class DecodedId:
    revision: int
    total_count: int
    process_number: int

    def encode_entity_id(self) -> str:
        if self.revision == 0:
            return str(self.process_number)
        return f"{self.revision}{SEPARATOR}{self.total_count}{SEPARATOR}{self.process_number}"


def _unexpected(entity_id: str) -> ValueError:
    return ValueError(f"Unexpected id for sharded daemon process: '{entity_id}'")


def decode_entity_id(entity_id: str, initial_number_of_processes: int) -> DecodedId:
    parts = entity_id.split(SEPARATOR)
    if len(parts) == 3:
        return DecodedId(int(parts[0]), int(parts[1]), int(parts[2]))
    if len(parts) == 1:
        return DecodedId(0, initial_number_of_processes, int(parts[0]))
    raise _unexpected(entity_id)


class ShardedDaemonProcessIdExtractor[T](ShardingMessageExtractor[ShardingEnvelope[T], T]):
    """The shard of an entity id is its process number, so each process gets a shard of its own."""

    @override
    def entity_id(self, message: ShardingEnvelope[T]) -> str:
        return message.entity_id

    @override
    def shard_id(self, entity_id: str) -> str:
        parts = entity_id.split(SEPARATOR)
        if len(parts) == 3:
            return parts[2]
        if len(parts) == 1:
            return entity_id
        raise _unexpected(entity_id)

    @override
    def unwrap_message(self, message: ShardingEnvelope[T]) -> T:
        return message.message


def sorted_identities_for(revision: int, number_of_processes: int) -> list[str]:
    return sorted(
        DecodedId(revision, number_of_processes, n).encode_entity_id()
        for n in range(number_of_processes)
    )


def all_shards_for(revision: int, number_of_processes: int) -> frozenset[str]:
    extractor = ShardedDaemonProcessIdExtractor[object]()
    return frozenset(
        extractor.shard_id(i) for i in sorted_identities_for(revision, number_of_processes)
    )


@dataclass(frozen=True)
class ShardedDaemonProcessState(ReplicatedData):
    """Which revision of the process set is current, how many processes it has, and whether the
    scaling to it is complete. ``started`` is epoch seconds."""

    revision: int
    number_of_processes: int
    completed: bool
    started: float

    START_REVISION: ClassVar[int] = 0

    @override
    def merge(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, that: ShardedDaemonProcessState
    ) -> ShardedDaemonProcessState:
        if that.revision == self.revision:
            return self if self.completed else that
        return that if that.revision > self.revision else self

    def start_scaling_to(self, new_number_of_processes: int) -> ShardedDaemonProcessState:
        return ShardedDaemonProcessState(
            self.revision + 1, new_number_of_processes, False, time.time()
        )

    def complete_scaling(self) -> ShardedDaemonProcessState:
        return replace(self, completed=True)

    @staticmethod
    def initial_state(initial_number_of_processes: int) -> ShardedDaemonProcessState:
        return ShardedDaemonProcessState(
            ShardedDaemonProcessState.START_REVISION,
            initial_number_of_processes,
            True,
            time.time(),
        )


@dataclass(frozen=True, eq=False)
class ShardedDaemonProcessStateKey(Key[ShardedDaemonProcessState]):
    @override
    def with_id(self, new_id: str) -> ShardedDaemonProcessStateKey:
        return ShardedDaemonProcessStateKey(new_id)


Serialization.register(ShardedDaemonProcessState, "ShardedDaemonProcess.State")
Serialization.register(ShardedDaemonProcessStateKey, "ShardedDaemonProcess.StateKey")


@dataclass(frozen=True)
class ShardedDaemonProcessSettings:
    """Intervals are in seconds. ``role`` limits the nodes the processes run on."""

    keep_alive_interval: float = 10.0
    sharding_settings: ClusterShardingSettings | None = None
    role: str | None = None
    keep_alive_from_number_of_nodes: int = 3
    keep_alive_throttle_interval: float = 0.1

    def __post_init__(self) -> None:
        if self.role == "":
            object.__setattr__(self, "role", None)

    def with_keep_alive_interval(self, interval: float) -> ShardedDaemonProcessSettings:
        return replace(self, keep_alive_interval=interval)

    def with_sharding_settings(
        self, sharding_settings: ClusterShardingSettings
    ) -> ShardedDaemonProcessSettings:
        return replace(self, sharding_settings=sharding_settings)

    def with_role(self, role: str) -> ShardedDaemonProcessSettings:
        return replace(self, role=role)

    def with_keep_alive_from_number_of_nodes(self, nodes: int) -> ShardedDaemonProcessSettings:
        return replace(self, keep_alive_from_number_of_nodes=nodes)

    def with_keep_alive_throttle_interval(self, interval: float) -> ShardedDaemonProcessSettings:
        return replace(self, keep_alive_throttle_interval=interval)


@dataclass(frozen=True)
class ShardedDaemonProcessContext:
    process_number: int
    total_processes: int
    name: str
    revision: int


def _sharding_settings(
    settings: ShardedDaemonProcessSettings, number_of_instances: int
) -> ClusterShardingSettings:
    """``internalInitWithContext`` (150-175): one shard per process, the daemon's role, and neither
    remembered entities nor passivation."""
    base = settings.sharding_settings or ClusterShardingSettings()
    return replace(
        base,
        number_of_shards=number_of_instances,
        role=settings.role if settings.role is not None else base.role,
        remember_entities=False,
    ).with_no_passivation_strategy()


class ShardedDaemonProcess:
    """``ShardedDaemonProcess(system).init(name, n, factory)`` keeps the processes ``factory(0)``
    to ``factory(n - 1)`` running, each on one node of the cluster, as Pekko Typed's. It keeps no
    state of its own: sharding and the singleton make a second ``init`` of a name a no-op."""

    def __init__(self, system: ActorSystem[Any]) -> None:
        self.system = system

    def init[T](
        self,
        name: str,
        number_of_instances: int,
        behavior_factory: Callable[[int], Behavior[T]],
        settings: ShardedDaemonProcessSettings | None = None,
        stop_message: T | None = None,
    ) -> None:
        """``internalInitWithContext`` (135-210), for a fixed set of processes. ``stop_message``
        is what a process gets when its shard is handed off, instead of being stopped outright."""
        # imported here: the package and the coordinator's module import this one
        from . import ClusterSharding
        from .sharded_daemon_process_coordinator import sharded_daemon_process_coordinator

        settings = settings or ShardedDaemonProcessSettings()
        entity_type_key = EntityTypeKey[T](f"sharded-daemon-process-{name}")
        sharding_settings = _sharding_settings(settings, number_of_instances)

        def create(ctx: EntityContext[T]) -> Behavior[T]:
            decoded_id = decode_entity_id(ctx.entity_id, number_of_instances)
            return behavior_factory(decoded_id.process_number)

        entity = (
            Entity(entity_type_key, create)
            .with_settings(sharding_settings)
            .with_message_extractor(ShardedDaemonProcessIdExtractor[T]())
        )
        if stop_message is not None:
            entity = entity.with_stop_message(stop_message)

        sharding_ref = ClusterSharding(self.system).init(entity)

        # started on all nodes even if using roles to be able to share the default replicator
        DistributedData(self.system)

        singleton_settings = ClusterSingletonSettings()
        if settings.role is not None:
            singleton_settings = singleton_settings.with_role(settings.role)
        singleton = SingletonActor(
            sharded_daemon_process_coordinator(
                settings, sharding_settings, number_of_instances, name, sharding_ref
            ),
            f"ShardedDaemonProcessCoordinator-{name}",
        ).with_settings(singleton_settings)

        ClusterSingleton(self.system).init(singleton)
