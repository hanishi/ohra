"""Pekko's ``DataEnvelope`` and ``DeletedData``, from ``Replicator.Internal``, and the messages
Replicators send each other.

The envelope carries Pekko's pruning markers and the delta versions. ``merge`` takes ``now``,
the Replicator's clock, where Pekko reads ``System.currentTimeMillis()`` to drop obsolete
``PruningPerformed`` markers.
A request carries the ``reply_to`` that Pekko answers through ``sender()``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from typing import Any

from ...ref import ActorRef
from ...remote import Address, UniqueAddress
from ...remote.serialization import Serialization, SerializationError, remotable
from .pruning_state import PruningInitialized, PruningPerformed, PruningState
from .replicated_data import (
    DeltaReplicatedData,
    RemovedNodePruning,
    ReplicatedData,
    ReplicatedDelta,
    RequiresCausalDeliveryOfDeltas,
)
from .version_vector import VersionVector


@remotable
@dataclass(frozen=True)
class DeletedData(ReplicatedData):
    """The tombstone of a deleted key. It absorbs every merge."""

    def merge(self, that: DeletedData) -> DeletedData:  # pyright: ignore[reportIncompatibleMethodOverride]
        return self


DELETED_DATA = DeletedData()


@remotable
@dataclass(frozen=True)
class DataEnvelope:
    """Wraps a data entry and carries the state of the pruning process for it."""

    data: ReplicatedData
    pruning: dict[UniqueAddress, PruningState] = field(default_factory=lambda: {})
    delta_versions: VersionVector = field(default_factory=lambda: VersionVector.empty)

    @property
    def is_deleted(self) -> bool:
        return isinstance(self.data, DeletedData)

    def without_delta_versions(self) -> DataEnvelope:
        return (
            self
            if self.delta_versions.is_empty
            else replace(self, delta_versions=VersionVector.empty)
        )

    def _cleaned_delta_versions(self, removed: UniqueAddress) -> VersionVector:
        # We only use the delta versions to track versions per node, not for ordering
        # comparisons, so we can just remove the entry for the removed node.
        return self.delta_versions.pruning_cleanup(removed)

    def need_pruning_from(self, removed: UniqueAddress) -> bool:
        return isinstance(self.data, RemovedNodePruning) and self.data.need_pruning_from(removed)

    def init_removed_node_pruning(
        self, removed: UniqueAddress, owner: UniqueAddress
    ) -> DataEnvelope:
        return replace(
            self,
            pruning={**self.pruning, removed: PruningInitialized(owner, frozenset())},
            delta_versions=self._cleaned_delta_versions(removed),
        )

    def prune(self, removed: UniqueAddress, performed: PruningPerformed) -> DataEnvelope:
        if not isinstance(self.data, RemovedNodePruning):
            return self
        if removed not in self.pruning:  # Pekko's require
            raise ValueError(f"no pruning marker for {removed}")
        match self.pruning[removed]:
            case PruningInitialized(owner):
                return replace(
                    self,
                    data=self.data.prune(removed, owner),
                    pruning={**self.pruning, removed: performed},
                    delta_versions=self._cleaned_delta_versions(removed),
                )
            case _:
                return self

    def merge(self, other: DataEnvelope, now: float) -> DataEnvelope:
        if isinstance(other.data, DeletedData):
            return DELETED_ENVELOPE
        if isinstance(self.data, DeletedData):
            return DELETED_ENVELOPE
        merged_pruning = dict(other.pruning)
        for key, this_value in self.pruning.items():
            that_value = merged_pruning.get(key)
            merged_pruning[key] = (
                this_value if that_value is None else this_value.merge(that_value)
            )
        filtered_pruning = {
            key: p
            for key, p in merged_pruning.items()
            if not (isinstance(p, PruningPerformed) and p.is_obsolete(now))
        }
        # cleanup and merge delta versions
        cleaned_dv = self.delta_versions
        cleaned_other_dv = other.delta_versions
        for removed in filtered_pruning:
            cleaned_dv = cleaned_dv.pruning_cleanup(removed)
            cleaned_other_dv = cleaned_other_dv.pruning_cleanup(removed)
        # cleanup both sides before merging, `merge_data` cleans the other data
        return DataEnvelope(
            _cleaned(self.data, filtered_pruning),
            filtered_pruning,
            cleaned_dv.merge(cleaned_other_dv),
        ).merge_data(other.data)

    def merge_data(self, other_data: ReplicatedData) -> DataEnvelope:
        """Pekko's ``merge(otherData)``: a ``ReplicatedDelta`` is applied with ``merge_delta``."""
        if isinstance(other_data, DeletedData):
            return DELETED_ENVELOPE
        data: Any = self.data
        if isinstance(other_data, ReplicatedDelta) and not isinstance(
            self.data, DeltaReplicatedData
        ):
            raise TypeError("Expected DeltaReplicatedData")
        cleaned = _cleaned(other_data, self.pruning)
        if isinstance(cleaned, ReplicatedDelta):
            if type(cleaned.zero()) is not type(data):
                raise TypeError(
                    f"cannot merge {type(data).__name__} with {type(cleaned).__name__}"
                )
            merged = data.merge_delta(cleaned)
        else:
            if type(data) is not type(cleaned):
                raise TypeError(
                    f"cannot merge {type(data).__name__} with {type(cleaned).__name__}"
                )
            merged = data.merge(cleaned)
        if type(data) is not type(merged):
            raise TypeError(
                f"Wrong type, existing type [{type(data).__name__}], got [{type(merged).__name__}]"
            )
        return replace(self, data=merged)

    def add_seen(self, node: Address) -> DataEnvelope:
        changed = False
        new_pruning: dict[UniqueAddress, PruningState] = {}
        for removed, state in self.pruning.items():
            new_state = state.add_seen(node)
            changed = changed or new_state is not state
            new_pruning[removed] = new_state
        return replace(self, pruning=new_pruning) if changed else self


def _cleaned(data: ReplicatedData, pruning: dict[UniqueAddress, PruningState]) -> ReplicatedData:
    for removed, state in pruning.items():
        if (
            isinstance(data, RemovedNodePruning)
            and isinstance(state, PruningPerformed)
            and data.need_pruning_from(removed)
        ):
            data = data.pruning_cleanup(removed)
    return data


DELETED_ENVELOPE = DataEnvelope(DELETED_DATA)


class _NoDeltaPlaceholder(
    DeltaReplicatedData["_NoDeltaPlaceholder"], RequiresCausalDeliveryOfDeltas
):
    """When a DeltaReplicatedData returns ``None`` from ``delta`` it must still be treated as a
    delta that increases the version counter in ``DeltaPropagationSelector``. Otherwise a later
    delta might be applied before the full state gossip is received and thereby violate
    ``RequiresCausalDeliveryOfDeltas``.

    This is used as a placeholder for such a ``None`` delta. It is filtered out in
    ``create_delta_propagation``, i.e. never sent to the other replicas."""

    delta = None

    def merge(self, that: Any) -> _NoDeltaPlaceholder:
        return self

    def merge_delta(self, that_delta: _NoDeltaPlaceholder) -> _NoDeltaPlaceholder:
        return self

    def reset_delta(self) -> _NoDeltaPlaceholder:
        return self

    def zero(self) -> _NoDeltaPlaceholder:
        return self

    def __repr__(self) -> str:
        return "NoDeltaPlaceholder"


NO_DELTA_PLACEHOLDER = _NoDeltaPlaceholder()


@remotable
@dataclass(frozen=True)
class Write:
    key_id: str
    envelope: DataEnvelope
    from_node: UniqueAddress | None
    reply_to: ActorRef[WriteAck | WriteNack]


@remotable
@dataclass(frozen=True)
class WriteAck:
    pass


@remotable
@dataclass(frozen=True)
class WriteNack:
    pass


@remotable
@dataclass(frozen=True)
class Read:
    key_id: str
    from_node: UniqueAddress | None
    reply_to: ActorRef[ReadResult]


@remotable
@dataclass(frozen=True)
class ReadResult:
    envelope: DataEnvelope | None


@remotable
@dataclass(frozen=True)
class ReadRepair:
    key_id: str
    envelope: DataEnvelope
    reply_to: ActorRef[ReadRepairAck]


@remotable
@dataclass(frozen=True)
class ReadRepairAck:
    pass


@dataclass(frozen=True)
class Status:
    """Pekko's gossip Status: key id to its digest and used timestamp. A timestamp of 0 is not
    set, and a digest of the not-found sentinel asks for the key."""

    digests: dict[str, tuple[str, float]]
    chunk: int
    tot_chunks: int
    to_system_uid: int
    from_system_uid: int
    reply_to: ActorRef[Status | Gossip]


@dataclass(frozen=True)
class Gossip:
    """Pekko's Gossip: key id to its envelope and used timestamp."""

    updated_data: dict[str, tuple[DataEnvelope, float]]
    send_back: bool
    to_system_uid: int
    from_system_uid: int
    reply_to: ActorRef[Status | Gossip]


@remotable
@dataclass(frozen=True)
class Delta:
    envelope: DataEnvelope
    from_seq_nr: int
    to_seq_nr: int

    @property
    def requires_causal_delivery_of_deltas(self) -> bool:
        return isinstance(self.envelope.data, RequiresCausalDeliveryOfDeltas)


@remotable
@dataclass(frozen=True)
class DeltaPropagation:
    from_node: UniqueAddress
    reply: bool
    deltas: dict[str, Delta]
    reply_to: ActorRef[WriteAck | WriteNack | DeltaNack] | None


@remotable
@dataclass(frozen=True)
class DeltaNack:
    pass


# the cluster's own Gossip takes the class name
Serialization.register(Status, "Replicator.Status")
Serialization.register(Gossip, "Replicator.Gossip")
Serialization.register(Delta, "Replicator.Delta")
Serialization.register(DeltaPropagation, "Replicator.DeltaPropagation")
Serialization.register(DeltaNack, "Replicator.DeltaNack")


def send_to_replica[T](
    log: logging.Logger, to: ActorRef[T], msg: T, key_ids: Iterable[str]
) -> None:
    """Send a message to another node's Replicator, aggregator or subscriber. Pekko's remoting
    drops a payload over its frame size with an error log, and the sender carries on; ohra's
    remoting raises SerializationError, which would restart a Replicator or stop an
    aggregator."""
    try:
        to.tell(msg)
    except SerializationError as e:
        log.error(
            "Dropped %s for keys [%s] to [%s]: %s",
            type(msg).__name__,
            ", ".join(key_ids),
            to.path,
            e,
        )
