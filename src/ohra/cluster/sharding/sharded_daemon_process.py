"""Pekko's ShardedDaemonProcessId.scala, ShardedDaemonProcessState.scala (the state, its key and
``initialState``), ShardedDaemonProcessSettings.scala and the ``sharded-daemon-process`` section
of reference.conf: the entity ids, the replicated scaling state and the settings of a sharded
daemon process."""

from __future__ import annotations

import time
from dataclasses import dataclass, replace
from typing import ClassVar, Final, override

from ...remote.serialization import Serialization
from ..ddata.key import Key
from ..ddata.replicated_data import ReplicatedData
from .message_extractor import ShardingMessageExtractor
from .shard import ClusterShardingSettings, ShardingEnvelope

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
