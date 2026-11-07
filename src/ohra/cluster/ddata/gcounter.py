"""Pekko's GCounter.scala: a 'Growing Counter' CRDT, also called a 'G-Counter'.

It is described in the paper "A comprehensive study of Convergent and Commutative Replicated
Data Types". A G-Counter is an increment-only counter (inspired by vector clocks) in which only
increment and merge are possible. Divergent histories are resolved by taking the maximum count
for each node. The value of the counter is the sum of all node counts.

Immutable: "modifying" methods return a new instance."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, override

from ...remote import UniqueAddress
from ...remote.serialization import remotable
from .fast_merge import FastMerge
from .key import Key
from .replicated_data import (
    DeltaReplicatedData,
    RemovedNodePruning,
    ReplicatedDelta,
    ReplicatedDeltaSize,
)

if TYPE_CHECKING:
    from . import SelfUniqueAddress


@remotable
@dataclass(frozen=True, eq=False)
class GCounter(
    DeltaReplicatedData["GCounter"],
    ReplicatedDelta,
    ReplicatedDeltaSize,
    RemovedNodePruning,
    FastMerge,
):
    empty: ClassVar[GCounter]

    state: dict[UniqueAddress, int] = field(default_factory=lambda: {})
    delta: GCounter | None = field(default=None, compare=False, repr=False)

    @property
    def value(self) -> int:
        """Current total value of the counter."""
        return sum(self.state.values())

    def increment(self, node: SelfUniqueAddress, n: int = 1) -> GCounter:
        """Increment the counter with the delta ``n`` specified. ``n`` must be zero or positive."""
        return self._increment(node.unique_address, n)

    def _increment(self, key: UniqueAddress, n: int = 1) -> GCounter:
        """INTERNAL API"""
        if n < 0:
            raise ValueError("Can't decrement a GCounter")
        if n == 0:
            return self
        next_value = self.state.get(key, 0) + n
        new_delta = GCounter(
            {key: next_value} if self.delta is None else {**self.delta.state, key: next_value}
        )
        return self._assign_ancestor(GCounter({**self.state, key: next_value}, new_delta))

    @override
    def merge(self, that: GCounter) -> GCounter:  # pyright: ignore[reportIncompatibleMethodOverride]
        if self is that or that._is_ancestor_of(self):
            return self._clear_ancestor()
        if self._is_ancestor_of(that):
            return that._clear_ancestor()
        merged = dict(that.state)
        for key, this_value in self.state.items():
            if this_value > merged.get(key, 0):
                merged[key] = this_value
        self._clear_ancestor()
        return GCounter(merged)

    @override
    def merge_delta(self, that_delta: GCounter) -> GCounter:
        return self.merge(that_delta)

    @override
    def zero(self) -> GCounter:
        return GCounter.empty

    @override
    def reset_delta(self) -> GCounter:
        if self.delta is None:
            return self
        return self._assign_ancestor(GCounter(self.state))

    @property
    @override
    def delta_size(self) -> int:
        # Not in Pekko's GCounter, which leaves the size to the Replicator: 1 for a delta
        # that does not say.
        return 1

    @property
    @override
    def modified_by_nodes(self) -> frozenset[UniqueAddress]:
        return frozenset(self.state)

    @override
    def need_pruning_from(self, removed_node: UniqueAddress) -> bool:
        return removed_node in self.state

    @override
    def prune(self, removed_node: UniqueAddress, collapse_into: UniqueAddress) -> GCounter:
        if removed_node not in self.state:
            return self
        rest = {n: v for n, v in self.state.items() if n != removed_node}
        return GCounter(rest)._increment(collapse_into, self.state[removed_node])

    @override
    def pruning_cleanup(self, removed_node: UniqueAddress) -> GCounter:
        return GCounter({n: v for n, v in self.state.items() if n != removed_node})

    def __repr__(self) -> str:
        return f"GCounter({self.value})"

    @override
    def __eq__(self, o: object) -> bool:
        return isinstance(o, GCounter) and self.state == o.state

    @override
    def __hash__(self) -> int:
        return hash(frozenset(self.state.items()))


GCounter.empty = GCounter()


@remotable
@dataclass(frozen=True, eq=False)
class GCounterKey(Key[GCounter]):
    @override
    def with_id(self, new_id: str) -> GCounterKey:
        return GCounterKey(new_id)
