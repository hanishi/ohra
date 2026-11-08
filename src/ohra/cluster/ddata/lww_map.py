# pyright: reportPrivateUsage=false
"""Pekko's LWWMap.scala: a map of ``LWWRegister`` values, with last-writer-wins on each key.

It is an ``ORMap`` of ``LWWRegister``s underneath. Note that this relies on synchronized clocks:
see ``LWWRegister``.

Immutable: "modifying" methods return a new instance."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, override

from ...remote import UniqueAddress
from ...remote.serialization import remotable
from .key import Key
from .lww_register import Clock, LWWRegister, default_clock
from .ormap import DeltaOp, ORMap, ZeroTag
from .replicated_data import DeltaReplicatedData, RemovedNodePruning

if TYPE_CHECKING:
    from . import SelfUniqueAddress


@remotable
@dataclass(frozen=True)
class LWWMapTag(ZeroTag):
    """INTERNAL API"""

    value: ClassVar[int] = 4

    @override
    def zero(self) -> LWWMap[Any, Any]:
        return LWWMap.empty


@remotable
@dataclass(frozen=True)
class LWWMap[K, V](DeltaReplicatedData[DeltaOp], RemovedNodePruning):
    empty: ClassVar[LWWMap[Any, Any]]
    __match_args__ = ("entries",)  # Pekko's unapply: case LWWMap(entries)

    underlying: ORMap[K, LWWRegister[V]] = field(
        default_factory=lambda: ORMap(zero_tag=LWWMapTag())
    )

    @property
    def entries(self) -> dict[K, V]:
        return {k: r.value for k, r in self.underlying.entries.items()}

    def get(self, key: K) -> V | None:
        register = self.underlying.get(key)
        return None if register is None else register.value

    def contains(self, key: K) -> bool:
        return self.underlying.contains(key)

    @property
    def is_empty(self) -> bool:
        return self.underlying.is_empty

    @property
    def size(self) -> int:
        return self.underlying.size

    def put(
        self, node: SelfUniqueAddress, key: K, value: V, clock: Clock[V] = default_clock
    ) -> LWWMap[K, V]:
        """Adds an entry to the map."""
        return self._put(node.unique_address, key, value, clock)

    def _put(
        self, node: UniqueAddress, key: K, value: V, clock: Clock[V] = default_clock
    ) -> LWWMap[K, V]:
        """INTERNAL API"""
        old = self.underlying.get(key)
        new_register = (
            LWWRegister._create(node, value, clock)
            if old is None
            else old._with_value(node, value, clock)
        )
        return LWWMap(self.underlying._put(node, key, new_register))

    def remove(self, node: SelfUniqueAddress, key: K) -> LWWMap[K, V]:
        """Removes an entry from the map. Note that if there is a conflicting update on another
        node the entry will not be removed after merge."""
        return self._remove(node.unique_address, key)

    def _remove(self, node: UniqueAddress, key: K) -> LWWMap[K, V]:
        """INTERNAL API"""
        return LWWMap(self.underlying._remove(node, key))

    @override
    def reset_delta(self) -> LWWMap[K, V]:
        return LWWMap(self.underlying.reset_delta())

    @property
    @override
    def delta(self) -> DeltaOp | None:  # pyright: ignore[reportIncompatibleVariableOverride]
        # Not a field, as in Pekko: it is derived from the underlying map
        return self.underlying.delta

    @override
    def merge_delta(self, that_delta: DeltaOp) -> LWWMap[K, V]:
        return LWWMap(self.underlying.merge_delta(that_delta))

    @override
    def merge(self, that: LWWMap[K, V]) -> LWWMap[K, V]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return LWWMap(self.underlying.merge(that.underlying))

    @property
    @override
    def modified_by_nodes(self) -> frozenset[UniqueAddress]:
        return self.underlying.modified_by_nodes

    @override
    def need_pruning_from(self, removed_node: UniqueAddress) -> bool:
        return self.underlying.need_pruning_from(removed_node)

    @override
    def prune(self, removed_node: UniqueAddress, collapse_into: UniqueAddress) -> LWWMap[K, V]:
        return LWWMap(self.underlying.prune(removed_node, collapse_into))

    @override
    def pruning_cleanup(self, removed_node: UniqueAddress) -> LWWMap[K, V]:
        return LWWMap(self.underlying.pruning_cleanup(removed_node))

    def __repr__(self) -> str:
        return f"LWW{self.entries}"


LWWMap.empty = LWWMap()


@remotable
@dataclass(frozen=True, eq=False)
class LWWMapKey[K, V](Key[LWWMap[K, V]]):
    @override
    def with_id(self, new_id: str) -> LWWMapKey[K, V]:
        return LWWMapKey(new_id)
