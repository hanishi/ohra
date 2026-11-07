# pyright: reportPrivateUsage=false
"""Pekko's PNCounter.scala: an 'Increment/Decrement Counter' CRDT, also called a 'PN-Counter'.

The increments (P) are tracked apart from the decrements (N), as two ``GCounter``s. Merge merges
the two. The value of the counter is the value of the P counter minus the value of the N counter.

Immutable: "modifying" methods return a new instance."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, ClassVar, override

from ...remote import UniqueAddress
from ...remote.serialization import remotable
from .gcounter import GCounter
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
@dataclass(frozen=True)
class PNCounter(
    DeltaReplicatedData["PNCounter"], ReplicatedDelta, ReplicatedDeltaSize, RemovedNodePruning
):
    empty: ClassVar[PNCounter]

    increments: GCounter = field(default_factory=lambda: GCounter.empty)
    decrements: GCounter = field(default_factory=lambda: GCounter.empty)

    @property
    def value(self) -> int:
        """Current total value of the counter."""
        return self.increments.value - self.decrements.value

    def increment(self, node: SelfUniqueAddress, n: int = 1) -> PNCounter:
        """Increment the counter with the delta ``n`` specified. If the delta is negative then it
        will decrement instead of increment."""
        return self._increment(node.unique_address, n)

    def decrement(self, node: SelfUniqueAddress, n: int = 1) -> PNCounter:
        """Decrement the counter with the delta ``n`` specified. If the delta is negative then it
        will increment instead of decrement."""
        return self._decrement(node.unique_address, n)

    def _increment(self, key: UniqueAddress, n: int = 1) -> PNCounter:
        """INTERNAL API"""
        return self._change(key, n)

    def _decrement(self, key: UniqueAddress, n: int = 1) -> PNCounter:
        """INTERNAL API"""
        return self._change(key, -n)

    def _change(self, key: UniqueAddress, n: int) -> PNCounter:
        """INTERNAL API"""
        if n > 0:
            return replace(self, increments=self.increments._increment(key, n))
        if n < 0:
            return replace(self, decrements=self.decrements._increment(key, -n))
        return self

    @override
    def merge(self, that: PNCounter) -> PNCounter:  # pyright: ignore[reportIncompatibleMethodOverride]
        return replace(
            self,
            increments=that.increments.merge(self.increments),
            decrements=that.decrements.merge(self.decrements),
        )

    @property
    @override
    def delta(self) -> PNCounter:  # pyright: ignore[reportIncompatibleVariableOverride]
        # Pekko's ``delta`` is a def, always a Some. Not a field: it is neither sent nor compared.
        return PNCounter(
            GCounter.empty if self.increments.delta is None else self.increments.delta,
            GCounter.empty if self.decrements.delta is None else self.decrements.delta,
        )

    @override
    def merge_delta(self, that_delta: PNCounter) -> PNCounter:
        return self.merge(that_delta)

    @override
    def zero(self) -> PNCounter:
        return PNCounter.empty

    @override
    def reset_delta(self) -> PNCounter:
        if self.increments.delta is None and self.decrements.delta is None:
            return self
        return PNCounter(self.increments.reset_delta(), self.decrements.reset_delta())

    @property
    @override
    def delta_size(self) -> int:
        # Not in Pekko's PNCounter, which leaves the size to the Replicator: 1 for a delta
        # that does not say.
        return 1

    @property
    @override
    def modified_by_nodes(self) -> frozenset[UniqueAddress]:
        return self.increments.modified_by_nodes | self.decrements.modified_by_nodes

    @override
    def need_pruning_from(self, removed_node: UniqueAddress) -> bool:
        return self.increments.need_pruning_from(
            removed_node
        ) or self.decrements.need_pruning_from(removed_node)

    @override
    def prune(self, removed_node: UniqueAddress, collapse_into: UniqueAddress) -> PNCounter:
        return replace(
            self,
            increments=self.increments.prune(removed_node, collapse_into),
            decrements=self.decrements.prune(removed_node, collapse_into),
        )

    @override
    def pruning_cleanup(self, removed_node: UniqueAddress) -> PNCounter:
        return replace(
            self,
            increments=self.increments.pruning_cleanup(removed_node),
            decrements=self.decrements.pruning_cleanup(removed_node),
        )

    def __repr__(self) -> str:
        return f"PNCounter({self.value})"


PNCounter.empty = PNCounter()


@remotable
@dataclass(frozen=True, eq=False)
class PNCounterKey(Key[PNCounter]):
    @override
    def with_id(self, new_id: str) -> PNCounterKey:
        return PNCounterKey(new_id)
