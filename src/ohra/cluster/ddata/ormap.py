# pyright: reportPrivateUsage=false
"""Pekko's ORMap.scala: an 'Observed Remove Map' CRDT, also called an 'OR-Map'.

It has similar semantics as an ``ORSet``, but in case of concurrent updates the values are
merged, and must therefore be ``ReplicatedData`` types themselves.

Immutable: "modifying" methods return a new instance.

The zero tags (``ZeroTag``, ``VanillaORMapTag``) and the delta ops (``DeltaOp``,
``AtomicDeltaOp``, ``PutDeltaOp``, ``UpdateDeltaOp``, ``RemoveDeltaOp``, ``RemoveKeyDeltaOp``,
``DeltaGroup``) are Pekko's INTERNAL API, from the companion object."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, cast, override

from ...remote import UniqueAddress
from ...remote.serialization import Serialization, remotable, transient
from . import orset
from .key import Key
from .orset import ORSet
from .replicated_data import (
    DeltaReplicatedData,
    RemovedNodePruning,
    ReplicatedData,
    ReplicatedDelta,
    ReplicatedDeltaSize,
    RequiresCausalDeliveryOfDeltas,
)

if TYPE_CHECKING:
    from . import SelfUniqueAddress


class ZeroTag(ABC):
    """INTERNAL API: Tags for ORMap.DeltaOp's, so that when the Replicator needs to re-create
    full value from delta, the right map type will be used.

    Each tag is a field-less ``@remotable`` frozen dataclass in its map type's module."""

    value: ClassVar[int]

    @abstractmethod
    def zero(self) -> DeltaReplicatedData[Any]: ...


@remotable
@dataclass(frozen=True)
class VanillaORMapTag(ZeroTag):
    """INTERNAL API"""

    value: ClassVar[int] = 0

    @override
    def zero(self) -> ORMap[Any, Any]:
        return ORMap.empty


class DeltaOp(RequiresCausalDeliveryOfDeltas):
    """INTERNAL API"""

    @abstractmethod
    @override
    def merge(self, that: DeltaOp) -> DeltaOp:  # pyright: ignore[reportIncompatibleMethodOverride]
        ...


class AtomicDeltaOp(DeltaOp, ReplicatedDeltaSize):
    """INTERNAL API. Not a dataclass: each subclass lists the fields in Pekko's order."""

    underlying: orset.DeltaOp
    zero_tag: ZeroTag

    @override
    def zero(self) -> DeltaReplicatedData[Any]:
        return self.zero_tag.zero()

    @override
    def merge(self, that: DeltaOp) -> DeltaOp:
        match that:
            case DeltaGroup():
                return DeltaGroup((self, *that.ops))
            case _:  # an AtomicDeltaOp
                return DeltaGroup((self, that))

    @property
    @override
    def delta_size(self) -> int:
        return 1


@dataclass(frozen=True)
class PutDeltaOp[K, V: ReplicatedData](AtomicDeltaOp):
    """INTERNAL API: contains ORSet delta and full value"""

    underlying: orset.DeltaOp
    value: tuple[K, V]
    zero_tag: ZeroTag

    @override
    def merge(self, that: DeltaOp) -> DeltaOp:
        if isinstance(that, PutDeltaOp):
            put = cast(PutDeltaOp[K, V], that)
            if self.value[0] == put.value[0]:
                return PutDeltaOp(self.underlying.merge(put.underlying), put.value, self.zero_tag)
        elif isinstance(that, UpdateDeltaOp):
            update = cast(UpdateDeltaOp[K], that)
            if len(update.values) == 1 and self.value[0] in update.values:
                key, elem1 = self.value
                elem2 = next(iter(update.values.values()))
                if isinstance(elem1, DeltaReplicatedData):
                    new_elem = cast(DeltaReplicatedData[Any], elem1).merge_delta(elem2)
                else:
                    new_elem = elem1.merge(elem2)
                new_value = (key, cast(V, new_elem))
                return PutDeltaOp(
                    self.underlying.merge(update.underlying), new_value, self.zero_tag
                )
        return super().merge(cast(DeltaOp, that))


@dataclass(frozen=True)
class UpdateDeltaOp[K](AtomicDeltaOp):
    """INTERNAL API: contains ORSet delta and either delta of value (in case where underlying
    type supports deltas) or full value"""

    underlying: orset.DeltaOp
    values: dict[K, ReplicatedData]
    zero_tag: ZeroTag

    @override
    def merge(self, that: DeltaOp) -> DeltaOp:
        if isinstance(that, UpdateDeltaOp):
            update = cast(UpdateDeltaOp[K], that)
            merged = dict(self.values)
            for key, value in update.values.items():
                if key in self.values:
                    merged[key] = self.values[key].merge(value)
                else:
                    merged[key] = value
            return UpdateDeltaOp(self.underlying.merge(update.underlying), merged, self.zero_tag)
        if isinstance(that, PutDeltaOp):
            put = cast(PutDeltaOp[K, Any], that)
            if len(self.values) == 1 and put.value[0] in self.values:
                return PutDeltaOp(self.underlying.merge(put.underlying), put.value, self.zero_tag)
        return super().merge(cast(DeltaOp, that))


@dataclass(frozen=True)
class RemoveDeltaOp(AtomicDeltaOp):
    """INTERNAL API: does not contain any value at all - the propagated 'value' map would be
    empty"""

    underlying: orset.DeltaOp
    zero_tag: ZeroTag


@dataclass(frozen=True)
class RemoveKeyDeltaOp[K](AtomicDeltaOp):
    """INTERNAL API: contains a single value - to provide the recipient with the removed key for
    value map"""

    underlying: orset.DeltaOp
    removed_key: K
    zero_tag: ZeroTag


@dataclass(frozen=True)
class DeltaGroup(DeltaOp, ReplicatedDeltaSize):
    """INTERNAL API: effectively a causally ordered list of individual deltas"""

    ops: tuple[DeltaOp, ...]

    @override
    def merge(self, that: DeltaOp) -> DeltaOp:
        match that:
            case DeltaGroup():
                return DeltaGroup((*self.ops, *that.ops))
            case _:  # an AtomicDeltaOp
                last = self.ops[-1]
                if isinstance(last, PutDeltaOp | UpdateDeltaOp):
                    match last.merge(that):
                        case DeltaGroup(ops=that_ops):
                            return DeltaGroup((*self.ops[:-1], *that_ops))
                        case op:
                            return DeltaGroup((*self.ops[:-1], op))
                return DeltaGroup((*self.ops, that))

    @override
    def zero(self) -> DeltaReplicatedData[Any]:
        return self.ops[0].zero() if self.ops else ORMap.empty

    @property
    @override
    def delta_size(self) -> int:
        return len(self.ops)


@remotable
@dataclass(frozen=True, eq=False)
class ORMap[K, V: ReplicatedData](DeltaReplicatedData[DeltaOp], RemovedNodePruning):
    empty: ClassVar[ORMap[Any, Any]]
    __match_args__ = ("entries",)  # Pekko's unapply: case ORMap(entries)

    keys: ORSet[K] = field(default_factory=lambda: ORSet.empty)
    values: dict[K, V] = field(default_factory=lambda: {})
    zero_tag: ZeroTag = VanillaORMapTag()
    delta: DeltaOp | None = transient()

    @property
    def entries(self) -> dict[K, V]:
        """All entries of the map."""
        return self.values

    def get(self, key: K) -> V | None:
        return self.values.get(key)

    def get_or_else(self, key: K, default: V) -> V:
        """Get the value associated with the key if there is one, else return the given
        default."""
        return self.values.get(key, default)

    def contains(self, key: K) -> bool:
        return key in self.values

    @property
    def is_empty(self) -> bool:
        return not self.values

    @property
    def size(self) -> int:
        return len(self.values)

    def put(self, node: SelfUniqueAddress, key: K, value: V) -> ORMap[K, V]:
        """Adds an entry to the map. Note that the new ``value`` will be merged with existing
        values on other nodes and the outcome depends on what ``ReplicatedData`` type that is
        used.

        Consider using ``update`` instead of ``put`` if you want modify existing entry.

        ``ValueError`` is raised if you try to replace an existing ``ORSet`` value, because
        important history can be lost when replacing the ``ORSet`` and undesired effects of
        merging will occur. Use ``ORMultiMap`` or ``update`` instead."""
        return self._put(node.unique_address, key, value)

    def _put(self, node: UniqueAddress, key: K, value: V) -> ORMap[K, V]:
        """INTERNAL API"""
        if issubclass(type(value), ORSet) and key in self.values:
            raise ValueError(
                "`ORMap.put` must not be used to replace an existing `ORSet` "
                "value, because important history can be lost when replacing the `ORSet` and "
                "undesired effects of merging will occur. Use `ORMultiMap` or `ORMap.updated` "
                "instead."
            )
        new_keys = self.keys.reset_delta()._add(node, key)
        assert new_keys.delta is not None
        put_delta_op = PutDeltaOp[K, V](new_keys.delta, (key, value), self.zero_tag)
        # put forcibly damages history, so we consciously propagate full value that will
        # overwrite previous value
        return ORMap(
            new_keys, {**self.values, key: value}, self.zero_tag, self._new_delta(put_delta_op)
        )

    def update(
        self, node: SelfUniqueAddress, key: K, initial: V, modify: Callable[[V], V]
    ) -> ORMap[K, V]:
        """Replace a value by applying the ``modify`` function on the existing value.

        If there is no current value for the ``key`` the ``initial`` value will be passed to the
        ``modify`` function."""
        return self._update(node.unique_address, key, initial, modify)

    def _update(
        self,
        node: UniqueAddress,
        key: K,
        initial: V,
        modify: Callable[[V], V],
        value_deltas: bool = False,
    ) -> ORMap[K, V]:
        """INTERNAL API"""
        has_old_value = key in self.values
        old_value = self.values[key] if has_old_value else initial
        # Optimization: for some types - like GSet, GCounter, PNCounter and ORSet - that are
        # delta based we can emit (and later merge) their deltas instead of full updates.
        # However to avoid necessity of tombstones, the derived map type needs to support this
        # with clearing the value (e.g. removing all elements if value is a set) before removing
        # the key - like e.g. ORMultiMap.emptyWithValueDeltas does
        new_keys = self.keys.reset_delta()._add(node, key)
        assert new_keys.delta is not None
        delta_op: DeltaOp
        if issubclass(type(old_value), DeltaReplicatedData) and value_deltas:
            reset = cast(DeltaReplicatedData[Any], old_value).reset_delta()
            new_value = modify(cast(V, reset))
            new_value_delta = cast(DeltaReplicatedData[Any], new_value).delta
            if new_value_delta is not None and has_old_value:
                delta_op = UpdateDeltaOp(new_keys.delta, {key: new_value_delta}, self.zero_tag)
            else:
                delta_op = PutDeltaOp(new_keys.delta, (key, new_value), self.zero_tag)
        else:
            new_value = modify(old_value)
            delta_op = PutDeltaOp(new_keys.delta, (key, new_value), self.zero_tag)
        return ORMap(
            new_keys, {**self.values, key: new_value}, self.zero_tag, self._new_delta(delta_op)
        )

    def remove(self, node: SelfUniqueAddress, key: K) -> ORMap[K, V]:
        """Removes an entry from the map. Note that if there is a conflicting update on another
        node the entry will not be removed after merge."""
        return self._remove(node.unique_address, key)

    def _remove(self, node: UniqueAddress, key: K) -> ORMap[K, V]:
        """INTERNAL API"""
        # for removals the delta values map emitted will be empty
        new_keys = self.keys.reset_delta()._remove(node, key)
        assert new_keys.delta is not None
        remove_delta_op = RemoveDeltaOp(new_keys.delta, self.zero_tag)
        new_values = {k: v for k, v in self.values.items() if k != key}
        return ORMap(new_keys, new_values, self.zero_tag, self._new_delta(remove_delta_op))

    def _remove_key(self, node: UniqueAddress, key: K) -> ORMap[K, V]:
        """INTERNAL API: This function is only to be used by derived maps that avoid remove
        anomalies by keeping the vvector (in form of key -> value pair) for deleted keys"""
        new_keys = self.keys.reset_delta()._remove(node, key)
        assert new_keys.delta is not None
        remove_key_delta_op = RemoveKeyDeltaOp(new_keys.delta, key, self.zero_tag)
        return ORMap(new_keys, self.values, self.zero_tag, self._new_delta(remove_key_delta_op))

    def _dry_merge(
        self, that: ORMap[K, V], merged_keys: ORSet[K], value_keys: Iterable[K]
    ) -> ORMap[K, V]:
        merged_values: dict[K, V] = {}
        for key in value_keys:
            this_value, that_value = self.values.get(key), that.values.get(key)
            if this_value is not None and that_value is not None:
                this_class, that_class = type(this_value), type(that_value)
                if this_class is not that_class:
                    raise ValueError(
                        f"Wrong type for merging [{key}] in [{_class_name(self)}], existing "
                        f"type [{_class_name(this_value)}], got [{_class_name(that_value)}]"
                    )
                merged_values[key] = this_value.merge(that_value)
            elif this_value is not None:
                if merged_keys.contains(key):
                    merged_values[key] = this_value
                # else this_value is a tombstone, but we don't want to carry it forward, as the
                # other side does not have the element at all
            elif that_value is not None:
                if merged_keys.contains(key):
                    merged_values[key] = that_value
                # else that_value is a tombstone, but we don't want to carry it forward, as the
                # other side does not have the element at all
            else:
                raise RuntimeError(f"missing value for {key}")
        return ORMap(merged_keys, merged_values, self.zero_tag)

    @override
    def merge(self, that: ORMap[K, V]) -> ORMap[K, V]:  # pyright: ignore[reportIncompatibleMethodOverride]
        merged_keys = self.keys.merge(that.keys)
        return self._dry_merge(that, merged_keys, merged_keys.elements_map)

    def _merge_retaining_deleted_values(self, that: ORMap[K, V]) -> ORMap[K, V]:
        """INTERNAL API: This function is only to be used by derived maps that avoid remove
        anomalies by keeping the vvector (in form of key -> value pair) for deleted keys"""
        merged_keys = self.keys.merge(that.keys)
        return self._dry_merge(that, merged_keys, {**self.values, **that.values})

    @override
    def reset_delta(self) -> ORMap[K, V]:
        if self.delta is None:
            return self
        return ORMap(self.keys.reset_delta(), self.values, self.zero_tag)

    def _dry_merge_delta(
        self, that_delta: DeltaOp, with_value_deltas: bool = False
    ) -> ORMap[K, V]:
        def merge_value(lvalue: ReplicatedData, rvalue: ReplicatedData) -> V:
            if issubclass(type(lvalue), DeltaReplicatedData) and isinstance(
                rvalue, ReplicatedDelta
            ):
                return cast(V, cast(DeltaReplicatedData[Any], lvalue).merge_delta(rvalue))
            return cast(V, lvalue.merge(rvalue))

        merged_keys = self.keys
        merged_values = {k: v for k, v in self.values.items() if self.keys.contains(k)}
        tombstoned_vals = {k: v for k, v in self.values.items() if not self.keys.contains(k)}

        def process_delta(op: DeltaOp) -> None:
            nonlocal merged_keys
            match op:
                case PutDeltaOp():
                    put_op = cast(PutDeltaOp[K, V], op)
                    merged_keys = merged_keys.merge_delta(put_op.underlying)
                    # put is destructive and propagates only full values of V!
                    key, value = put_op.value
                    merged_values[key] = value
                case RemoveDeltaOp(underlying=key_delta):
                    # if op is RemoveDeltaOp then it must have exactly one element in the elements
                    if not isinstance(key_delta, orset.RemoveDeltaOp):
                        raise ValueError(
                            "ORMap.RemoveDeltaOp must contain ORSet.RemoveDeltaOp inside"
                        )
                    removed_set = cast(orset.RemoveDeltaOp[K], key_delta).underlying
                    merged_values.pop(next(iter(removed_set.elements_map)), None)
                    merged_keys = merged_keys.merge_delta(op.underlying)
                # please note that if RemoveDeltaOp is not preceded by update clearing the value
                # anomalies may result
                case RemoveKeyDeltaOp():
                    remove_key_op = cast(RemoveKeyDeltaOp[K], op)
                    removed_key = remove_key_op.removed_key
                    # removeKeyOp tombstones values for later use
                    if removed_key in merged_values:
                        tombstoned_vals[removed_key] = merged_values.pop(removed_key)
                    merged_keys = merged_keys.merge_delta(remove_key_op.underlying)
                case UpdateDeltaOp():
                    update_op = cast(UpdateDeltaOp[K], op)
                    merged_keys = merged_keys.merge_delta(update_op.underlying)
                    for key, value in update_op.values.items():
                        if merged_keys.contains(key):
                            if key in merged_values:
                                merged_values[key] = merge_value(merged_values[key], value)
                            elif key in tombstoned_vals:
                                merged_values[key] = merge_value(tombstoned_vals[key], value)
                            elif isinstance(value, ReplicatedDelta):
                                merged_values[key] = merge_value(value.zero(), value)
                            else:
                                merged_values[key] = cast(V, value)
                case _:
                    raise RuntimeError("Cannot nest DeltaGroups")

        match that_delta:
            case DeltaGroup(ops=ops):
                for op in ops:
                    process_delta(op)
            case _:
                process_delta(that_delta)

        if with_value_deltas:
            return ORMap(merged_keys, {**tombstoned_vals, **merged_values}, self.zero_tag)
        return ORMap(merged_keys, merged_values, self.zero_tag)

    @override
    def merge_delta(self, that_delta: DeltaOp) -> ORMap[K, V]:
        this_with_deltas = self._dry_merge_delta(that_delta)
        return self.merge(this_with_deltas)

    def _merge_delta_retaining_deleted_values(self, that_delta: DeltaOp) -> ORMap[K, V]:
        """INTERNAL API: This function is only to be used by derived maps that avoid remove
        anomalies by keeping the vvector (in form of key -> value pair) for deleted keys"""
        this_with_deltas = self._dry_merge_delta(that_delta, with_value_deltas=True)
        return self._merge_retaining_deleted_values(this_with_deltas)

    def _new_delta(self, delta_op: DeltaOp) -> DeltaOp:
        return delta_op if self.delta is None else self.delta.merge(delta_op)

    @property
    @override
    def modified_by_nodes(self) -> frozenset[UniqueAddress]:
        nodes = self.keys.modified_by_nodes
        for data in self.values.values():
            if isinstance(data, RemovedNodePruning):
                nodes |= data.modified_by_nodes
        return nodes

    @override
    def need_pruning_from(self, removed_node: UniqueAddress) -> bool:
        return self.keys.need_pruning_from(removed_node) or any(
            isinstance(data, RemovedNodePruning) and data.need_pruning_from(removed_node)
            for data in self.values.values()
        )

    @override
    def prune(self, removed_node: UniqueAddress, collapse_into: UniqueAddress) -> ORMap[K, V]:
        pruned_keys = self.keys.prune(removed_node, collapse_into)
        pruned_values = {
            key: (
                cast(V, data.prune(removed_node, collapse_into))
                if isinstance(data, RemovedNodePruning) and data.need_pruning_from(removed_node)
                else data
            )
            for key, data in self.values.items()
        }
        return ORMap(pruned_keys, pruned_values, self.zero_tag)

    @override
    def pruning_cleanup(self, removed_node: UniqueAddress) -> ORMap[K, V]:
        pruning_cleanuped_keys = self.keys.pruning_cleanup(removed_node)
        pruning_cleanuped_values = {
            key: (
                cast(V, data.pruning_cleanup(removed_node))
                if isinstance(data, RemovedNodePruning) and data.need_pruning_from(removed_node)
                else data
            )
            for key, data in self.values.items()
        }
        return ORMap(pruning_cleanuped_keys, pruning_cleanuped_values, self.zero_tag)

    def __repr__(self) -> str:
        return f"OR{self.entries}"

    @override
    def __eq__(self, o: object) -> bool:
        if not isinstance(o, ORMap):
            return False
        other = cast(ORMap[Any, Any], o)
        return self.keys == other.keys and self.values == other.values

    @override
    def __hash__(self) -> int:
        # equal maps have equal keys; the values may not hash
        return hash(self.keys)


ORMap.empty = ORMap()


def _class_name(o: object) -> str:
    return f"{type(o).__module__}.{type(o).__qualname__}"


@remotable
@dataclass(frozen=True, eq=False)
class ORMapKey[K, V: ReplicatedData](Key[ORMap[K, V]]):
    @override
    def with_id(self, new_id: str) -> ORMapKey[K, V]:
        return ORMapKey(new_id)


Serialization.register(PutDeltaOp, "ORMap.PutDeltaOp")
Serialization.register(UpdateDeltaOp, "ORMap.UpdateDeltaOp")
Serialization.register(RemoveDeltaOp, "ORMap.RemoveDeltaOp")
Serialization.register(RemoveKeyDeltaOp, "ORMap.RemoveKeyDeltaOp")
Serialization.register(DeltaGroup, "ORMap.DeltaGroup")
