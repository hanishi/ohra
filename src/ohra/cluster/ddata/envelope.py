"""Pekko's ``DataEnvelope`` and ``DeletedData``, from ``Replicator.Internal``, and the messages
Replicators send each other.

Pruning and delta versions are not here: they come with the sub-projects that need them.
A request carries the ``reply_to`` that Pekko answers through ``sender()``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

from ...ref import ActorRef
from ...remote import UniqueAddress
from ...remote.serialization import Serialization, SerializationError, remotable
from .replicated_data import ReplicatedData


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
    data: ReplicatedData

    @property
    def is_deleted(self) -> bool:
        return isinstance(self.data, DeletedData)

    def merge(self, other: DataEnvelope) -> DataEnvelope:
        if isinstance(other.data, DeletedData):
            return DELETED_ENVELOPE
        if isinstance(self.data, DeletedData):
            return DELETED_ENVELOPE
        if type(self.data) is not type(other.data):
            raise TypeError(
                f"cannot merge {type(self.data).__name__} with {type(other.data).__name__}"
            )
        return DataEnvelope(cast(Any, self.data).merge(other.data))


DELETED_ENVELOPE = DataEnvelope(DELETED_DATA)


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


# the cluster's own Gossip takes the class name
Serialization.register(Status, "Replicator.Status")
Serialization.register(Gossip, "Replicator.Gossip")


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
