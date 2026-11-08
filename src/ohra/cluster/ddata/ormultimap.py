# pyright: reportPrivateUsage=false
"""Pekko's ORMultiMap.scala: an immutable multi-map, an ``ORMap`` with an ``ORSet`` for the
map's value.

Note that on concurrent adds and removals for the same key (on the same set), removals can be
lost. ``ORMultiMap.empty_with_value_deltas`` ships the deltas of the value sets and clears a set
before removing its key, keeping the empty set as a tombstone that a concurrent update merges
with. A full-state merge still drops an empty tombstone whose key is gone, as Pekko's does, so
there too a removal can be lost, depending on the order replicas merge in.

Immutable: "modifying" methods return a new instance."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, cast, override

from ...remote import UniqueAddress
from ...remote.serialization import remotable
from .key import Key
from .ormap import DeltaOp, ORMap, ZeroTag
from .orset import ORSet
from .replicated_data import DeltaReplicatedData, RemovedNodePruning

if TYPE_CHECKING:
    from . import SelfUniqueAddress


@remotable
@dataclass(frozen=True)
class ORMultiMapTag(ZeroTag):
    """INTERNAL API"""

    value: ClassVar[int] = 2

    @override
    def zero(self) -> ORMultiMap[Any, Any]:
        return ORMultiMap.empty


@remotable
@dataclass(frozen=True)
class ORMultiMapWithValueDeltasTag(ZeroTag):
    """INTERNAL API"""

    value: ClassVar[int] = 3

    @override
    def zero(self) -> ORMultiMap[Any, Any]:
        return ORMultiMap.empty_with_value_deltas


@remotable
@dataclass(frozen=True, eq=False)
class ORMultiMap[K, V](DeltaReplicatedData[DeltaOp], RemovedNodePruning):
    empty: ClassVar[ORMultiMap[Any, Any]]
    empty_with_value_deltas: ClassVar[ORMultiMap[Any, Any]]
    __match_args__ = ("entries",)  # Pekko's unapply: case ORMultiMap(entries)

    underlying: ORMap[K, ORSet[V]] = field(default_factory=lambda: ORMap(zero_tag=ORMultiMapTag()))
    with_value_deltas: bool = False

    @override
    def merge(self, that: ORMultiMap[K, V]) -> ORMultiMap[K, V]:  # pyright: ignore[reportIncompatibleMethodOverride]
        if self.with_value_deltas != that.with_value_deltas:
            raise ValueError("Trying to merge two ORMultiMaps of different map sub-type")
        if self.with_value_deltas:
            return self._without_empty_tombstones(
                self.underlying._merge_retaining_deleted_values(that.underlying)
            )
        return ORMultiMap(self.underlying.merge(that.underlying), self.with_value_deltas)

    def _without_empty_tombstones(self, new_underlying: ORMap[K, ORSet[V]]) -> ORMultiMap[K, V]:
        # Garbage collect the tombstones we no longer need, i.e. those that have Set() as a value.
        new_values = {
            key: value
            for key, value in new_underlying.values.items()
            if new_underlying.keys.contains(key) or not value.is_empty
        }
        return ORMultiMap(
            ORMap(new_underlying.keys, new_values, new_underlying.zero_tag, new_underlying.delta),
            self.with_value_deltas,
        )

    @property
    def entries(self) -> dict[K, frozenset[V]]:
        """All entries of a multimap where keys are strings and values are sets."""
        if self.with_value_deltas:
            return {
                k: v.elements
                for k, v in self.underlying.entries.items()
                if self.underlying.keys.contains(k)
            }
        return {k: v.elements for k, v in self.underlying.entries.items()}

    def get(self, key: K) -> frozenset[V] | None:
        """Get the set associated with the key if there is one."""
        if self.with_value_deltas and not self.underlying.keys.contains(key):
            return None
        value = self.underlying.get(key)
        return None if value is None else value.elements

    def get_or_else(self, key: K, default: frozenset[V]) -> frozenset[V]:
        """Get the set associated with the key if there is one, else return the given
        default."""
        value = self.get(key)
        return default if value is None else value

    def contains(self, key: K) -> bool:
        return self.underlying.keys.contains(key)

    @property
    def is_empty(self) -> bool:
        return self.underlying.keys.is_empty

    @property
    def size(self) -> int:
        return self.underlying.keys.size

    def put(self, node: SelfUniqueAddress, key: K, value: frozenset[V]) -> ORMultiMap[K, V]:
        """Associate an entire set with the key while retaining the history of the previous
        replicated data set."""
        return self._put(node.unique_address, key, value)

    def _put(self, node: UniqueAddress, key: K, value: frozenset[V]) -> ORMultiMap[K, V]:
        """INTERNAL API"""

        def replace(existing: ORSet[V]) -> ORSet[V]:
            s = existing._clear()
            for element in value:
                s = s._add(node, element)
            return s

        new_underlying = self.underlying._update(
            node, key, ORSet[V].empty, replace, value_deltas=self.with_value_deltas
        )
        return ORMultiMap(new_underlying, self.with_value_deltas)

    def remove(self, node: SelfUniqueAddress, key: K) -> ORMultiMap[K, V]:
        """Remove an entire set associated with the key."""
        return self._remove(node.unique_address, key)

    def _remove(self, node: UniqueAddress, key: K) -> ORMultiMap[K, V]:
        """INTERNAL API"""
        if self.with_value_deltas:
            u = self.underlying._update(
                node, key, ORSet[V].empty, lambda existing: existing._clear(), value_deltas=True
            )
            return ORMultiMap(u._remove_key(node, key), self.with_value_deltas)
        return ORMultiMap(self.underlying._remove(node, key), self.with_value_deltas)

    def add_binding(self, node: SelfUniqueAddress, key: K, element: V) -> ORMultiMap[K, V]:
        """Add an element to a set associated with a key. If there is no existing set then one
        will be initialised."""
        return self._add_binding(node.unique_address, key, element)

    def _add_binding(self, node: UniqueAddress, key: K, element: V) -> ORMultiMap[K, V]:
        """INTERNAL API"""
        new_underlying = self.underlying._update(
            node,
            key,
            ORSet[V].empty,
            lambda existing: existing._add(node, element),
            value_deltas=self.with_value_deltas,
        )
        return ORMultiMap(new_underlying, self.with_value_deltas)

    def remove_binding(self, node: SelfUniqueAddress, key: K, element: V) -> ORMultiMap[K, V]:
        """Remove an element of a set associated with a key. If there are no more elements in
        the set then the entire set will be removed."""
        return self._remove_binding(node.unique_address, key, element)

    def _remove_binding(self, node: UniqueAddress, key: K, element: V) -> ORMultiMap[K, V]:
        """INTERNAL API"""
        u = self.underlying._update(
            node,
            key,
            ORSet[V].empty,
            lambda existing: existing._remove(node, element),
            value_deltas=self.with_value_deltas,
        )
        s = u.get(key)
        if s is not None and s.is_empty:
            u = u._remove_key(node, key) if self.with_value_deltas else u._remove(node, key)
        return ORMultiMap(u, self.with_value_deltas)

    def replace_binding(
        self, node: SelfUniqueAddress, key: K, old_element: V, new_element: V
    ) -> ORMultiMap[K, V]:
        """Replace an element of a set associated with a key with a new one if it is different.
        This is useful when an element is removed and another one is added within the same
        Update. The order of addition and removal is important in order to retain history for
        replicated data."""
        return self._replace_binding(node.unique_address, key, old_element, new_element)

    def _replace_binding(
        self, node: UniqueAddress, key: K, old_element: V, new_element: V
    ) -> ORMultiMap[K, V]:
        """INTERNAL API"""
        if new_element != old_element:
            return self._add_binding(node, key, new_element)._remove_binding(
                node, key, old_element
            )
        return self

    @override
    def reset_delta(self) -> ORMultiMap[K, V]:
        return ORMultiMap(self.underlying.reset_delta(), self.with_value_deltas)

    @property
    @override
    def delta(self) -> DeltaOp | None:  # pyright: ignore[reportIncompatibleVariableOverride]
        # Not a field, as in Pekko: it is derived from the underlying map
        return self.underlying.delta

    @override
    def merge_delta(self, that_delta: DeltaOp) -> ORMultiMap[K, V]:
        if self.with_value_deltas:
            return self._without_empty_tombstones(
                self.underlying._merge_delta_retaining_deleted_values(that_delta)
            )
        return ORMultiMap(self.underlying.merge_delta(that_delta), self.with_value_deltas)

    @property
    @override
    def modified_by_nodes(self) -> frozenset[UniqueAddress]:
        return self.underlying.modified_by_nodes

    @override
    def need_pruning_from(self, removed_node: UniqueAddress) -> bool:
        return self.underlying.need_pruning_from(removed_node)

    @override
    def pruning_cleanup(self, removed_node: UniqueAddress) -> ORMultiMap[K, V]:
        return ORMultiMap(self.underlying.pruning_cleanup(removed_node), self.with_value_deltas)

    @override
    def prune(self, removed_node: UniqueAddress, collapse_into: UniqueAddress) -> ORMultiMap[K, V]:
        return ORMultiMap(
            self.underlying.prune(removed_node, collapse_into), self.with_value_deltas
        )

    def __repr__(self) -> str:
        return f"ORMulti{self.entries}"

    @override
    def __eq__(self, o: object) -> bool:
        # Pekko compares the underlying maps only
        if not isinstance(o, ORMultiMap):
            return False
        return self.underlying == cast(ORMultiMap[Any, Any], o).underlying

    @override
    def __hash__(self) -> int:
        return hash(self.underlying)


ORMultiMap.empty = ORMultiMap()
ORMultiMap.empty_with_value_deltas = ORMultiMap(
    ORMap(zero_tag=ORMultiMapWithValueDeltasTag()), True
)


@remotable
@dataclass(frozen=True, eq=False)
class ORMultiMapKey[K, V](Key[ORMultiMap[K, V]]):
    @override
    def with_id(self, new_id: str) -> ORMultiMapKey[K, V]:
        return ORMultiMapKey(new_id)
