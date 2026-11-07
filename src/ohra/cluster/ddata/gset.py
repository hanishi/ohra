"""Pekko's GSet.scala: a 'Add Set' CRDT, also called a 'G-Set'. You can't remove elements of a
G-Set.

It is described in the paper "A comprehensive study of Convergent and Commutative Replicated
Data Types". A G-Set doesn't accumulate any garbage apart from the elements themselves.

Immutable: "modifying" methods return a new instance."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar, override

from ...remote.serialization import remotable
from .fast_merge import FastMerge
from .key import Key
from .replicated_data import DeltaReplicatedData, ReplicatedDelta


@remotable
@dataclass(frozen=True, eq=False)
class GSet[A](DeltaReplicatedData["GSet[A]"], ReplicatedDelta, FastMerge):
    empty: ClassVar[GSet[object]]

    elements: frozenset[A] = frozenset()
    delta: GSet[A] | None = field(default=None, compare=False, repr=False)

    def contains(self, a: A) -> bool:
        return a in self.elements

    @property
    def is_empty(self) -> bool:
        return not self.elements

    @property
    def size(self) -> int:
        return len(self.elements)

    def add(self, element: A) -> GSet[A]:
        """Adds an element to the set."""
        new_delta = GSet(
            frozenset({element}) if self.delta is None else self.delta.elements | {element}
        )
        return self._assign_ancestor(GSet(self.elements | {element}, new_delta))

    @override
    def merge(self, that: GSet[A]) -> GSet[A]:  # pyright: ignore[reportIncompatibleMethodOverride]
        if self is that or that._is_ancestor_of(self):
            return self._clear_ancestor()
        if self._is_ancestor_of(that):
            return that._clear_ancestor()
        self._clear_ancestor()
        return GSet(self.elements | that.elements)

    @override
    def merge_delta(self, that_delta: GSet[A]) -> GSet[A]:
        return self.merge(that_delta)

    @override
    def zero(self) -> GSet[A]:
        return GSet()

    @override
    def reset_delta(self) -> GSet[A]:
        if self.delta is None:
            return self
        return self._assign_ancestor(GSet(self.elements))

    def __repr__(self) -> str:
        return f"G{set(self.elements)}"

    @override
    def __eq__(self, o: object) -> bool:
        return isinstance(o, GSet) and self.elements == o.elements  # pyright: ignore[reportUnknownMemberType]

    @override
    def __hash__(self) -> int:
        return hash(self.elements)


GSet.empty = GSet()


@remotable
@dataclass(frozen=True, eq=False)
class GSetKey[A](Key[GSet[A]]):
    @override
    def with_id(self, new_id: str) -> GSetKey[A]:
        return GSetKey(new_id)
