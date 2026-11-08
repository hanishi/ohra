# pyright: reportPrivateUsage=false
"""Pekko's PNCounterMap.scala: a map of ``PNCounter`` values.

It is an ``ORMap`` of ``PNCounter``s underneath. ``increment`` and ``decrement`` do what
``incrementBy`` and ``decrementBy`` do in Pekko.

Immutable: "modifying" methods return a new instance."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, override

from ...remote import UniqueAddress
from ...remote.serialization import remotable
from .key import Key
from .ormap import DeltaOp, ORMap, ZeroTag
from .pncounter import PNCounter
from .replicated_data import DeltaReplicatedData, RemovedNodePruning

if TYPE_CHECKING:
    from . import SelfUniqueAddress


@remotable
@dataclass(frozen=True)
class PNCounterMapTag(ZeroTag):
    """INTERNAL API"""

    value: ClassVar[int] = 1

    @override
    def zero(self) -> PNCounterMap[Any]:
        return PNCounterMap.empty


@remotable
@dataclass(frozen=True)
class PNCounterMap[K](DeltaReplicatedData[DeltaOp], RemovedNodePruning):
    empty: ClassVar[PNCounterMap[Any]]
    __match_args__ = ("entries",)  # Pekko's unapply: case PNCounterMap(entries)

    underlying: ORMap[K, PNCounter] = field(
        default_factory=lambda: ORMap(zero_tag=PNCounterMapTag())
    )

    @property
    def entries(self) -> dict[K, int]:
        return {k: c.value for k, c in self.underlying.entries.items()}

    def get(self, key: K) -> int | None:
        counter = self.underlying.get(key)
        return None if counter is None else counter.value

    def contains(self, key: K) -> bool:
        return self.underlying.contains(key)

    @property
    def is_empty(self) -> bool:
        return self.underlying.is_empty

    @property
    def size(self) -> int:
        return self.underlying.size

    def increment(self, node: SelfUniqueAddress, key: K, delta: int = 1) -> PNCounterMap[K]:
        """Increment the counter of ``key`` by ``delta``."""
        return self._increment(node.unique_address, key, delta)

    def _increment(self, node: UniqueAddress, key: K, delta: int = 1) -> PNCounterMap[K]:
        """INTERNAL API"""
        return PNCounterMap(
            self.underlying._update(
                node, key, PNCounter.empty, lambda c: c._increment(node, delta)
            )
        )

    def decrement(self, node: SelfUniqueAddress, key: K, delta: int = 1) -> PNCounterMap[K]:
        """Decrement the counter of ``key`` by ``delta``."""
        return self._decrement(node.unique_address, key, delta)

    def _decrement(self, node: UniqueAddress, key: K, delta: int = 1) -> PNCounterMap[K]:
        """INTERNAL API"""
        return PNCounterMap(
            self.underlying._update(
                node, key, PNCounter.empty, lambda c: c._decrement(node, delta)
            )
        )

    def remove(self, node: SelfUniqueAddress, key: K) -> PNCounterMap[K]:
        """Removes an entry from the map. Note that if there is a conflicting update on another
        node the entry will not be removed after merge."""
        return self._remove(node.unique_address, key)

    def _remove(self, node: UniqueAddress, key: K) -> PNCounterMap[K]:
        """INTERNAL API"""
        return PNCounterMap(self.underlying._remove(node, key))

    @override
    def merge(self, that: PNCounterMap[K]) -> PNCounterMap[K]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return PNCounterMap(self.underlying.merge(that.underlying))

    @override
    def reset_delta(self) -> PNCounterMap[K]:
        return PNCounterMap(self.underlying.reset_delta())

    @property
    @override
    def delta(self) -> DeltaOp | None:  # pyright: ignore[reportIncompatibleVariableOverride]
        # Not a field, as in Pekko: it is derived from the underlying map
        return self.underlying.delta

    @override
    def merge_delta(self, that_delta: DeltaOp) -> PNCounterMap[K]:
        return PNCounterMap(self.underlying.merge_delta(that_delta))

    @property
    @override
    def modified_by_nodes(self) -> frozenset[UniqueAddress]:
        return self.underlying.modified_by_nodes

    @override
    def need_pruning_from(self, removed_node: UniqueAddress) -> bool:
        return self.underlying.need_pruning_from(removed_node)

    @override
    def prune(self, removed_node: UniqueAddress, collapse_into: UniqueAddress) -> PNCounterMap[K]:
        return PNCounterMap(self.underlying.prune(removed_node, collapse_into))

    @override
    def pruning_cleanup(self, removed_node: UniqueAddress) -> PNCounterMap[K]:
        return PNCounterMap(self.underlying.pruning_cleanup(removed_node))

    def __repr__(self) -> str:
        return f"PNCounter{self.entries}"


PNCounterMap.empty = PNCounterMap()


@remotable
@dataclass(frozen=True, eq=False)
class PNCounterMapKey[K](Key[PNCounterMap[K]]):
    @override
    def with_id(self, new_id: str) -> PNCounterMapKey[K]:
        return PNCounterMapKey(new_id)
