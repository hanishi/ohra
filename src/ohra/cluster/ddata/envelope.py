"""Pekko's ``DataEnvelope`` and ``DeletedData``, from ``Replicator.Internal``.

Pruning and delta versions are not here: they come with the sub-projects that need them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from ...remote.serialization import remotable
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
