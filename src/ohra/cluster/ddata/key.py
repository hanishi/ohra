"""Pekko's Key.scala. Each per-type key lives in its type's module."""

from __future__ import annotations

from dataclasses import dataclass

from ...remote.serialization import remotable
from .replicated_data import ReplicatedData


@dataclass(frozen=True, eq=False)
class Key[T: ReplicatedData]:
    """The identifier of an entry in the Replicator. Keys are equal when their ids are, whatever
    their type, as in Pekko."""

    id: str

    def with_id(self, new_id: str) -> Key[ReplicatedData]:
        return UnspecificKey(new_id)

    def __eq__(self, o: object) -> bool:
        return isinstance(o, Key) and self.id == o.id

    def __hash__(self) -> int:
        return hash(self.id)

    def __str__(self) -> str:
        return self.id


@remotable
@dataclass(frozen=True, eq=False)
class UnspecificKey(Key[ReplicatedData]):
    pass
