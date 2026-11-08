# pyright: reportPrivateUsage=false
"""Pekko's ORSet.scala: an 'Observed Remove Set' CRDT, also called an 'OR-Set'. Elements can be
added and removed any number of times. Concurrent add wins over remove.

It is not implemented as in the paper "A comprehensive study of Convergent and Commutative
Replicated Data Types". This is more space efficient and doesn't accumulate garbage for removed
elements. It is described in the paper "An optimized conflict-free replicated set". The
implementation is inspired by the Riak DT riak_dt_orswot.

The ORSet has a version vector that is incremented when an element is added to the set. The
``node -> count`` pair for that increment is stored against the element as its "birth dot".
Every time the element is re-added to the set, its "birth dot" is updated to that of the
``node -> count`` version vector entry resulting from the add. When an element is removed, we
simply drop it, no tombstones.

When an element exists in replica A and not replica B, is it because A added it and B has not
yet seen that, or that B removed it and A has not yet seen that? In this implementation we
compare the ``dot`` of the present element to the version vector in the Set it is absent from.
If the element dot is not "seen" by the Set version vector, that means the other set has yet to
see this add, and the item is in the merged Set. If the Set version vector dominates the dot,
that means the other Set has removed this element already, and the item is not in the merged
Set.

Immutable: "modifying" methods return a new instance.

The delta ops (``DeltaOp``, ``AddDeltaOp``, ``RemoveDeltaOp``, ``FullStateDeltaOp``,
``DeltaGroup``) and the module functions are Pekko's INTERNAL API, from the companion
object."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, ClassVar, cast, override

from ...remote import UniqueAddress
from ...remote.serialization import Serialization, remotable, transient
from .fast_merge import FastMerge
from .key import Key
from .replicated_data import (
    DeltaReplicatedData,
    RemovedNodePruning,
    ReplicatedDeltaSize,
    RequiresCausalDeliveryOfDeltas,
)
from .version_vector import ManyVersionVector, OneVersionVector, VersionVector

if TYPE_CHECKING:
    from . import SelfUniqueAddress


class DeltaOp(RequiresCausalDeliveryOfDeltas):
    """INTERNAL API"""

    @abstractmethod
    @override
    def merge(self, that: DeltaOp) -> DeltaOp:  # pyright: ignore[reportIncompatibleMethodOverride]
        ...


@dataclass(frozen=True)
class AtomicDeltaOp[A](DeltaOp, ReplicatedDeltaSize):
    """INTERNAL API"""

    underlying: ORSet[A]

    @override
    def zero(self) -> ORSet[A]:
        return ORSet.empty

    @property
    @override
    def delta_size(self) -> int:
        return 1


@dataclass(frozen=True)
class AddDeltaOp[A](AtomicDeltaOp[A]):
    """INTERNAL API"""

    @override
    def merge(self, that: DeltaOp) -> DeltaOp:
        match that:
            case AddDeltaOp():
                # Note that we only merge deltas originating from the same node
                u = cast(AddDeltaOp[A], that).underlying
                return AddDeltaOp(
                    ORSet(
                        self._concat_elements_map(u.elements_map),
                        self.underlying.vvector.merge(u.vvector),
                    )
                )
            case DeltaGroup():
                return DeltaGroup((self, *that.ops))
            case _:  # an AtomicDeltaOp
                return DeltaGroup((self, that))

    def _concat_elements_map(self, that_map: Mapping[A, VersionVector]) -> dict[A, VersionVector]:
        return {**self.underlying.elements_map, **that_map}


@dataclass(frozen=True)
class RemoveDeltaOp[A](AtomicDeltaOp[A]):
    """INTERNAL API"""

    def __post_init__(self) -> None:
        if self.underlying.size != 1:
            raise ValueError(
                f"RemoveDeltaOp should contain one removed element, but was {self.underlying}"
            )

    @override
    def merge(self, that: DeltaOp) -> DeltaOp:
        match that:
            case DeltaGroup():
                return DeltaGroup((self, *that.ops))
            case _:  # an AtomicDeltaOp: keep it simple for removals
                return DeltaGroup((self, that))


@dataclass(frozen=True)
class FullStateDeltaOp[A](AtomicDeltaOp[A]):
    """INTERNAL API: Used for ``clear`` but could be used for other cases also"""

    @override
    def merge(self, that: DeltaOp) -> DeltaOp:
        match that:
            case DeltaGroup():
                return DeltaGroup((self, *that.ops))
            case _:  # an AtomicDeltaOp
                return DeltaGroup((self, that))


@dataclass(frozen=True)
class DeltaGroup(DeltaOp, ReplicatedDeltaSize):
    """INTERNAL API"""

    ops: tuple[DeltaOp, ...]

    @override
    def merge(self, that: DeltaOp) -> DeltaOp:
        match that:
            case AddDeltaOp():
                that_add = cast(AddDeltaOp[Any], that)
                # merge AddDeltaOp into last AddDeltaOp in the group, if possible
                last = self.ops[-1]
                if isinstance(last, AddDeltaOp):
                    return DeltaGroup((*self.ops[:-1], last.merge(that_add)))
                return DeltaGroup((*self.ops, that_add))
            case DeltaGroup():
                return DeltaGroup((*self.ops, *that.ops))
            case _:
                return DeltaGroup((*self.ops, that))

    @override
    def zero(self) -> ORSet[Any]:
        return ORSet.empty

    @property
    @override
    def delta_size(self) -> int:
        return len(self.ops)


def _subtract_dots(dot: VersionVector, vvector: VersionVector) -> VersionVector:
    """INTERNAL API

    Subtract the ``vvector`` from the ``dot``. What this means is that any (node, version) pair
    in ``dot`` that is <= an entry in ``vvector`` is removed from ``dot``. Example
    [{a, 3}, {b, 2}, {d, 14}, {g, 22}] - [{a, 4}, {b, 1}, {c, 1}, {d, 14}, {e, 5}, {f, 2}] =
    [{b, 2}, {g, 22}]"""
    if dot.is_empty:
        return VersionVector.empty
    if isinstance(dot, OneVersionVector):
        # if dot is dominated by version vector, drop it
        return VersionVector.empty if vvector.version_at(dot.node) >= dot.version else dot
    assert isinstance(dot, ManyVersionVector)
    # a dot dominated by the version vector is dropped
    return VersionVector.create(
        {node: v1 for node, v1 in dot.versions.items() if vvector.version_at(node) < v1}
    )


def _merge_common_keys[A](
    common_keys: Iterable[A], lhs: ORSet[A], rhs: ORSet[A]
) -> dict[A, VersionVector]:
    """INTERNAL API. See ``ORSet.merge``."""
    acc: dict[A, VersionVector] = {}
    for k in common_keys:
        lhs_dots = lhs.elements_map[k]
        rhs_dots = rhs.elements_map[k]
        match (lhs_dots, rhs_dots):
            case (OneVersionVector(node=n1, version=v1), OneVersionVector(node=n2, version=v2)):
                if n1 == n2 and v1 == v2:
                    # one single common dot
                    merged = lhs_dots
                else:
                    # no common, lhs_unique_dots == lhs_dots, rhs_unique_dots == rhs_dots
                    lhs_keep = _subtract_dots(lhs_dots, rhs.vvector)
                    rhs_keep = _subtract_dots(rhs_dots, lhs.vvector)
                    merged = lhs_keep.merge(rhs_keep)
            case (ManyVersionVector(versions=lhs_vs), ManyVersionVector(versions=rhs_vs)):
                common_dots = {n: v for n, v in lhs_vs.items() if rhs_vs.get(n) == v}
                lhs_unique_dots = {n: v for n, v in lhs_vs.items() if n not in common_dots}
                rhs_unique_dots = {n: v for n, v in rhs_vs.items() if n not in common_dots}
                lhs_keep = _subtract_dots(VersionVector.create(lhs_unique_dots), rhs.vvector)
                rhs_keep = _subtract_dots(VersionVector.create(rhs_unique_dots), lhs.vvector)
                merged = lhs_keep.merge(rhs_keep).merge(VersionVector.create(common_dots))
            case (ManyVersionVector(versions=lhs_vs), OneVersionVector(node=n2, version=v2)):
                common_dots = {n1: v1 for n1, v1 in lhs_vs.items() if v1 == v2 and n1 == n2}
                lhs_unique_dots = {n: v for n, v in lhs_vs.items() if n not in common_dots}
                rhs_unique = rhs_dots if not common_dots else VersionVector.empty
                lhs_keep = _subtract_dots(VersionVector.create(lhs_unique_dots), rhs.vvector)
                rhs_keep = _subtract_dots(rhs_unique, lhs.vvector)
                merged = lhs_keep.merge(rhs_keep).merge(VersionVector.create(common_dots))
            case (OneVersionVector(node=n1, version=v1), ManyVersionVector(versions=rhs_vs)):
                common_dots = {n2: v2 for n2, v2 in rhs_vs.items() if v1 == v2 and n1 == n2}
                lhs_unique = lhs_dots if not common_dots else VersionVector.empty
                rhs_unique_dots = {n: v for n, v in rhs_vs.items() if n not in common_dots}
                lhs_keep = _subtract_dots(lhs_unique, rhs.vvector)
                rhs_keep = _subtract_dots(VersionVector.create(rhs_unique_dots), lhs.vvector)
                merged = lhs_keep.merge(rhs_keep).merge(VersionVector.create(common_dots))
            case _:
                raise AssertionError("a VersionVector is a One or a ManyVersionVector")
        # Perfectly possible that an item in both sets should be dropped
        if not merged.is_empty:
            acc[k] = merged
    return acc


def _merge_disjoint_keys[A](
    keys: Iterable[A],
    elements_map: Mapping[A, VersionVector],
    vvector: VersionVector,
    accumulator: Mapping[A, VersionVector],
) -> dict[A, VersionVector]:
    """INTERNAL API. See ``ORSet.merge``."""
    acc = dict(accumulator)
    for k in keys:
        dots = elements_map[k]
        if not (vvector > dots or vvector == dots):
            # Optimise the set of stored dots to include only those unseen
            acc[k] = _subtract_dots(dots, vvector)
    return acc


@remotable
@dataclass(frozen=True, eq=False)
class ORSet[A](DeltaReplicatedData[DeltaOp], RemovedNodePruning, FastMerge):
    empty: ClassVar[ORSet[Any]]
    __match_args__ = ("elements",)  # Pekko's unapply: case ORSet(elements)

    elements_map: dict[A, VersionVector] = field(default_factory=lambda: {})
    vvector: VersionVector = field(default_factory=lambda: VersionVector.empty)
    delta: DeltaOp | None = transient()

    @property
    def elements(self) -> frozenset[A]:
        return frozenset(self.elements_map)

    def contains(self, a: A) -> bool:
        return a in self.elements_map

    @property
    def is_empty(self) -> bool:
        return not self.elements_map

    @property
    def size(self) -> int:
        return len(self.elements_map)

    def add(self, node: SelfUniqueAddress, element: A) -> ORSet[A]:
        """Adds an element to the set."""
        return self._add(node.unique_address, element)

    def _add(self, node: UniqueAddress, element: A) -> ORSet[A]:
        """INTERNAL API"""
        new_vvector = self.vvector._increment(node)
        new_dot = OneVersionVector(node, new_vvector.version_at(node))
        op = AddDeltaOp(ORSet({element: new_dot}, new_dot))
        new_delta = op if self.delta is None else self.delta.merge(op)
        return self._assign_ancestor(
            ORSet({**self.elements_map, element: new_dot}, new_vvector, new_delta)
        )

    def remove(self, node: SelfUniqueAddress, element: A) -> ORSet[A]:
        """Removes an element from the set."""
        return self._remove(node.unique_address, element)

    def _remove(self, node: UniqueAddress, element: A) -> ORSet[A]:
        """INTERNAL API"""
        delta_dot = OneVersionVector(node, self.vvector.version_at(node))
        rm_op = RemoveDeltaOp(ORSet({element: delta_dot}, self.vvector))
        new_delta = rm_op if self.delta is None else self.delta.merge(rm_op)
        new_elements_map = {e: d for e, d in self.elements_map.items() if e != element}
        return self._assign_ancestor(replace(self, elements_map=new_elements_map, delta=new_delta))

    def clear(self, node: SelfUniqueAddress) -> ORSet[A]:
        """Removes all elements from the set, but keeps the history. This has the same result as
        using ``remove`` for each element, but it is more efficient."""
        return self._clear()

    def _clear(self) -> ORSet[A]:
        """INTERNAL API"""
        new_full_state = ORSet[A]({}, self.vvector)
        clear_op = FullStateDeltaOp(new_full_state)
        new_delta = clear_op if self.delta is None else self.delta.merge(clear_op)
        return self._assign_ancestor(replace(new_full_state, delta=new_delta))

    @override
    def merge(self, that: ORSet[A]) -> ORSet[A]:  # pyright: ignore[reportIncompatibleMethodOverride]
        """When element is in this Set but not in that Set: compare the "birth dot" of the
        present element to the version vector in the Set it is absent from. If the element dot
        is not "seen" by other Set version vector, that means the other set has yet to see this
        add, and the element is to be in the merged Set. If the other Set version vector
        dominates the dot, that means the other Set has removed the element already, and the
        element is not to be in the merged Set.

        When element in both this Set and in that Set: some dots may still need to be shed. If
        this Set has dots that the other Set does not have, and the other Set version vector
        dominates those dots, then we need to drop those dots. Keep only common dots, and dots
        that are not dominated by the other sides version vector."""
        if self is that or that._is_ancestor_of(self):
            return self._clear_ancestor()
        if self._is_ancestor_of(that):
            return that._clear_ancestor()
        return self._dry_merge(that, add_delta_op=False)

    def _dry_merge(self, that: ORSet[A], add_delta_op: bool) -> ORSet[A]:
        # share merge impl between full state merge and AddDeltaOp merge
        if len(self.elements_map) < len(that.elements_map):
            common_keys = (k for k in self.elements_map if k in that.elements_map)
        else:
            common_keys = (k for k in that.elements_map if k in self.elements_map)
        entries00 = _merge_common_keys(common_keys, self, that)
        if add_delta_op:
            entries0 = entries00 | {
                e: d for e, d in self.elements_map.items() if e not in that.elements_map
            }
        else:
            this_unique_keys = (k for k in self.elements_map if k not in that.elements_map)
            entries0 = _merge_disjoint_keys(
                this_unique_keys, self.elements_map, that.vvector, entries00
            )
        that_unique_keys = (k for k in that.elements_map if k not in self.elements_map)
        entries = _merge_disjoint_keys(that_unique_keys, that.elements_map, self.vvector, entries0)
        merged_vvector = self.vvector.merge(that.vvector)

        self._clear_ancestor()
        return ORSet(entries, merged_vvector)

    @override
    def merge_delta(self, that_delta: DeltaOp) -> ORSet[A]:
        match that_delta:
            case AddDeltaOp():
                return self._dry_merge(
                    cast(AddDeltaOp[A], that_delta).underlying, add_delta_op=True
                )
            case RemoveDeltaOp():
                return self._merge_remove_delta(cast(RemoveDeltaOp[A], that_delta))
            case FullStateDeltaOp():
                return self._dry_merge(
                    cast(FullStateDeltaOp[A], that_delta).underlying, add_delta_op=False
                )
            case _:
                assert isinstance(that_delta, DeltaGroup)
                acc = self
                for op in that_delta.ops:
                    match op:
                        case AddDeltaOp():
                            acc = acc._dry_merge(
                                cast(AddDeltaOp[A], op).underlying, add_delta_op=True
                            )
                        case RemoveDeltaOp():
                            acc = acc._merge_remove_delta(cast(RemoveDeltaOp[A], op))
                        case FullStateDeltaOp():
                            acc = acc._dry_merge(
                                cast(FullStateDeltaOp[A], op).underlying, add_delta_op=False
                            )
                        case _:
                            raise ValueError("ORSet.DeltaGroup should not be nested")
                return acc

    def _merge_remove_delta(self, that_delta: RemoveDeltaOp[A]) -> ORSet[A]:
        that = that_delta.underlying
        elem, that_dot = next(iter(that.elements_map.items()))
        delete_dots = list(that.vvector._versions_iterator())
        delete_dots_nodes = {dot_node for dot_node, _ in delete_dots}
        this_dot_option = self.elements_map.get(elem)
        delete_dots_are_greater = all(
            this_dot_option is not None and this_dot_option.version_at(dot_node) <= dot_v
            for dot_node, dot_v in delete_dots
        )
        new_elements_map = self.elements_map
        if delete_dots_are_greater and this_dot_option is not None:
            if all(
                this_dot_node in delete_dots_nodes
                for this_dot_node, _ in this_dot_option._versions_iterator()
            ):
                new_elements_map = {e: d for e, d in self.elements_map.items() if e != elem}
        self._clear_ancestor()

        new_vvector = self.vvector.merge(that_dot)
        return ORSet(new_elements_map, new_vvector)

    @override
    def reset_delta(self) -> ORSet[A]:
        if self.delta is None:
            return self
        return self._assign_ancestor(ORSet(self.elements_map, self.vvector))

    @property
    @override
    def modified_by_nodes(self) -> frozenset[UniqueAddress]:
        return self.vvector.modified_by_nodes

    @override
    def need_pruning_from(self, removed_node: UniqueAddress) -> bool:
        return self.vvector.need_pruning_from(removed_node)

    @override
    def prune(self, removed_node: UniqueAddress, collapse_into: UniqueAddress) -> ORSet[A]:
        pruned = {
            elem: dot.prune(removed_node, collapse_into)
            for elem, dot in self.elements_map.items()
            if dot.need_pruning_from(removed_node)
        }
        if not pruned:
            return replace(self, vvector=self.vvector.prune(removed_node, collapse_into))
        # re-add elements that were pruned, to bump dots to right vvector
        new_set = ORSet(
            {**self.elements_map, **pruned}, self.vvector.prune(removed_node, collapse_into)
        )
        for elem in pruned:
            new_set = new_set._add(collapse_into, elem)
        return new_set

    @override
    def pruning_cleanup(self, removed_node: UniqueAddress) -> ORSet[A]:
        updated = {
            elem: dot.pruning_cleanup(removed_node) if dot.need_pruning_from(removed_node) else dot
            for elem, dot in self.elements_map.items()
        }
        return ORSet(updated, self.vvector.pruning_cleanup(removed_node))

    def __repr__(self) -> str:
        return f"OR{set(self.elements)}"

    @override
    def __eq__(self, o: object) -> bool:
        return (
            isinstance(o, ORSet)
            and self.vvector == o.vvector
            and self.elements_map == o.elements_map  # pyright: ignore[reportUnknownMemberType]
        )

    @override
    def __hash__(self) -> int:
        # the dots are VersionVectors, which don't hash: equal sets have equal elements
        return hash(self.elements)


ORSet.empty = ORSet()


@remotable
@dataclass(frozen=True, eq=False)
class ORSetKey[A](Key[ORSet[A]]):
    @override
    def with_id(self, new_id: str) -> ORSetKey[A]:
        return ORSetKey(new_id)


Serialization.register(AddDeltaOp, "ORSet.AddDeltaOp")
Serialization.register(RemoveDeltaOp, "ORSet.RemoveDeltaOp")
Serialization.register(FullStateDeltaOp, "ORSet.FullStateDeltaOp")
Serialization.register(DeltaGroup, "ORSet.DeltaGroup")
