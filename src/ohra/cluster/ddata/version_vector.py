"""Pekko's VersionVector.scala: a Vector-based clock (counting clock), inspired by Lamport
logical clocks. Immutable: "modifying" methods return a new instance."""

from __future__ import annotations

import itertools
from abc import abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from operator import itemgetter
from typing import TYPE_CHECKING, ClassVar, cast, override

from ...remote import UniqueAddress
from ...remote.serialization import remotable
from . import estimated_size
from .replicated_data import RemovedNodePruning

if TYPE_CHECKING:
    from . import SelfUniqueAddress

type _Entry = tuple[UniqueAddress | None, int]


class VersionVector(RemovedNodePruning):
    empty: ClassVar[VersionVector]

    class Ordering(Enum):
        after = "after"
        before = "before"
        same = "same"
        concurrent = "concurrent"

    class Timestamp:
        """INTERNAL API"""

        ZERO = 0
        END_MARKER = -(2**63)
        counter = itertools.count(1)

    @staticmethod
    def create(versions: Mapping[UniqueAddress, int]) -> VersionVector:
        """Pekko's ``apply(TreeMap)`` and ``apply(List)``."""
        if not versions:
            return VersionVector.empty
        if len(versions) == 1:
            ((node, version),) = versions.items()
            return OneVersionVector(node, version)
        return ManyVersionVector(dict(versions))

    def increment(self, node: SelfUniqueAddress) -> VersionVector:
        """Increment the version for the node passed as argument. Returns a new VersionVector."""
        return self._increment(node.unique_address)

    @property
    @abstractmethod
    def is_empty(self) -> bool: ...

    @property
    @abstractmethod
    def size(self) -> int:
        """INTERNAL API"""

    @abstractmethod
    def _increment(self, node: UniqueAddress) -> VersionVector:
        """INTERNAL API"""

    @abstractmethod
    def version_at(self, node: UniqueAddress) -> int:
        """INTERNAL API"""

    @abstractmethod
    def contains(self, node: UniqueAddress) -> bool:
        """INTERNAL API"""

    @abstractmethod
    def _versions_iterator(self) -> Iterator[tuple[UniqueAddress, int]]:
        """INTERNAL API"""

    @property
    @abstractmethod
    def estimated_size(self) -> int:
        """INTERNAL API"""

    def is_concurrent(self, that: VersionVector) -> bool:
        """Pekko's ``<>``."""
        return self._compare_only_to(that, VersionVector.Ordering.concurrent) is (
            VersionVector.Ordering.concurrent
        )

    def __lt__(self, that: VersionVector) -> bool:
        return self._compare_only_to(that, VersionVector.Ordering.before) is (
            VersionVector.Ordering.before
        )

    def __gt__(self, that: VersionVector) -> bool:
        return self._compare_only_to(that, VersionVector.Ordering.after) is (
            VersionVector.Ordering.after
        )

    def __eq__(self, that: object) -> bool:
        """True if this VersionVector has the same history as ``that``."""
        if not isinstance(that, VersionVector):
            return NotImplemented
        return self._compare_only_to(that, VersionVector.Ordering.same) is (
            VersionVector.Ordering.same
        )

    __hash__ = None  # type: ignore[assignment]

    def _compare_only_to(
        self, that: VersionVector, order: VersionVector.Ordering
    ) -> VersionVector.Ordering:
        """Version vector comparison according to the semantics described by compare_to, with
        the ability to bail out early if we can't reach the Ordering that we are looking for.

        The ordering always starts with Same and can then go to Same, Before or After. If we're
        on After we can only go to After or Concurrent. If we're on Before we can only go to
        Before or Concurrent. If we go to Concurrent we exit the loop immediately.

        Asking for Concurrent gives a full comparison (Pekko's FullOrder marker)."""
        o = VersionVector.Ordering
        end_marker: _Entry = (None, VersionVector.Timestamp.END_MARKER)
        full_order = order is o.concurrent

        def next_or_else(it: Iterator[_Entry]) -> _Entry:
            return next(it, end_marker)

        if self is that:
            return o.same
        i1 = cast(Iterator[_Entry], self._versions_iterator())
        i2 = cast(Iterator[_Entry], that._versions_iterator())
        nt1, nt2, current = next_or_else(i1), next_or_else(i2), o.same
        while True:
            if not full_order and current is not o.same and current is not order:
                return current
            if nt1 is end_marker and nt2 is end_marker:
                return current
            # i1 is empty but i2 is not, so i1 can only be Before
            if nt1 is end_marker:
                return o.concurrent if current is o.after else o.before
            # i2 is empty but i1 is not, so i1 can only be After
            if nt2 is end_marker:
                return o.concurrent if current is o.before else o.after
            # compare the nodes
            a1, t1 = cast(tuple[UniqueAddress, int], nt1)
            a2, t2 = cast(tuple[UniqueAddress, int], nt2)
            nc = -1 if a1 < a2 else 1 if a2 < a1 else 0  # Pekko's compareTo
            if nc == 0:
                # both nodes exist, compare the timestamps
                if t1 == t2:
                    nt1, nt2 = next_or_else(i1), next_or_else(i2)
                elif t1 < t2:
                    # t1 is less than t2, so i1 can only be Before
                    if current is o.after:
                        return o.concurrent
                    nt1, nt2, current = next_or_else(i1), next_or_else(i2), o.before
                else:
                    # t2 is less than t1, so i1 can only be After
                    if current is o.before:
                        return o.concurrent
                    nt1, nt2, current = next_or_else(i1), next_or_else(i2), o.after
            elif nc < 0:
                # this node only exists in i1 so i1 can only be After
                if current is o.before:
                    return o.concurrent
                nt1, current = next_or_else(i1), o.after
            else:
                # this node only exists in i2 so i1 can only be Before
                if current is o.after:
                    return o.concurrent
                nt2, current = next_or_else(i2), o.before

    def compare_to(self, that: VersionVector) -> VersionVector.Ordering:
        """Compare two version vectors. The outcome is one of:

        1. SAME (==) iff for all i c1(i) == c2(i)
        2. BEFORE (<) iff for all i c1(i) <= c2(i) and there exists a j such that c1(j) < c2(j)
        3. AFTER (>) iff for all i c1(i) >= c2(i) and there exists a j such that c1(j) > c2(j)
        4. CONCURRENT (<>) otherwise."""
        return self._compare_only_to(that, VersionVector.Ordering.concurrent)

    @abstractmethod
    @override
    def merge(self, that: VersionVector) -> VersionVector:  # pyright: ignore[reportIncompatibleMethodOverride]
        """Merges this VersionVector with another VersionVector, i.e. merges its versioned
        history."""


@remotable
@dataclass(frozen=True, eq=False)
class OneVersionVector(VersionVector):
    node: UniqueAddress
    version: int

    @property
    @override
    def is_empty(self) -> bool:
        return False

    @property
    @override
    def size(self) -> int:
        return 1

    @override
    def _increment(self, node: UniqueAddress) -> VersionVector:
        v = next(VersionVector.Timestamp.counter)
        if node == self.node:
            return OneVersionVector(self.node, v)
        return ManyVersionVector({self.node: self.version, node: v})

    @override
    def version_at(self, node: UniqueAddress) -> int:
        return self.version if node == self.node else VersionVector.Timestamp.ZERO

    @override
    def contains(self, node: UniqueAddress) -> bool:
        return node == self.node

    @override
    def _versions_iterator(self) -> Iterator[tuple[UniqueAddress, int]]:
        return iter(((self.node, self.version),))

    @override
    def merge(self, that: VersionVector) -> VersionVector:
        if isinstance(that, OneVersionVector):
            if self.node == that.node:
                if self.version >= that.version:
                    return self
                return OneVersionVector(that.node, that.version)
            return ManyVersionVector({self.node: self.version, that.node: that.version})
        assert isinstance(that, ManyVersionVector)
        v2 = that.versions.get(self.node, VersionVector.Timestamp.ZERO)
        merged = (
            that.versions if v2 >= self.version else {**that.versions, self.node: self.version}
        )
        return VersionVector.create(merged)

    @property
    @override
    def modified_by_nodes(self) -> frozenset[UniqueAddress]:
        return frozenset((self.node,))

    @override
    def need_pruning_from(self, removed_node: UniqueAddress) -> bool:
        return self.node == removed_node

    @override
    def prune(self, removed_node: UniqueAddress, collapse_into: UniqueAddress) -> VersionVector:
        base = VersionVector.empty if self.node == removed_node else self
        return base._increment(collapse_into)

    @override
    def pruning_cleanup(self, removed_node: UniqueAddress) -> VersionVector:
        return VersionVector.empty if self.node == removed_node else self

    def __str__(self) -> str:
        return f"VersionVector({self.node} -> {self.version})"

    @property
    @override
    def estimated_size(self) -> int:
        return estimated_size.UNIQUE_ADDRESS + estimated_size.LONG_VALUE


@remotable
@dataclass(frozen=True, eq=False)
class ManyVersionVector(VersionVector):
    versions: dict[UniqueAddress, int] = field(default_factory=lambda: {})

    @property
    @override
    def is_empty(self) -> bool:
        return not self.versions

    @property
    @override
    def size(self) -> int:
        return len(self.versions)

    @override
    def _increment(self, node: UniqueAddress) -> VersionVector:
        v = next(VersionVector.Timestamp.counter)
        return VersionVector.create({**self.versions, node: v})

    @override
    def version_at(self, node: UniqueAddress) -> int:
        return self.versions.get(node, VersionVector.Timestamp.ZERO)

    @override
    def contains(self, node: UniqueAddress) -> bool:
        return node in self.versions

    @override
    def _versions_iterator(self) -> Iterator[tuple[UniqueAddress, int]]:
        # ponytail: sorts per compare, keep the dict sorted if profiling says so
        return iter(sorted(self.versions.items(), key=itemgetter(0)))

    @override
    def merge(self, that: VersionVector) -> VersionVector:
        if that.is_empty:
            return self
        if self.is_empty:
            return that
        if isinstance(that, ManyVersionVector):
            merged = dict(that.versions)
            for node, time in self.versions.items():
                if time > merged.get(node, VersionVector.Timestamp.ZERO):
                    merged[node] = time
            return VersionVector.create(merged)
        assert isinstance(that, OneVersionVector)
        v1 = self.versions.get(that.node, VersionVector.Timestamp.ZERO)
        merged = (
            self.versions if v1 >= that.version else {**self.versions, that.node: that.version}
        )
        return VersionVector.create(merged)

    @property
    @override
    def modified_by_nodes(self) -> frozenset[UniqueAddress]:
        return frozenset(self.versions)

    @override
    def need_pruning_from(self, removed_node: UniqueAddress) -> bool:
        return removed_node in self.versions

    @override
    def prune(self, removed_node: UniqueAddress, collapse_into: UniqueAddress) -> VersionVector:
        rest = {n: v for n, v in self.versions.items() if n != removed_node}
        return VersionVector.create(rest)._increment(collapse_into)

    @override
    def pruning_cleanup(self, removed_node: UniqueAddress) -> VersionVector:
        if removed_node in self.versions:
            return VersionVector.create(
                {n: v for n, v in self.versions.items() if n != removed_node}
            )
        return self

    def __str__(self) -> str:
        entries = sorted(self.versions.items(), key=itemgetter(0))
        return f"VersionVector({', '.join(f'{n} -> {v}' for n, v in entries)})"

    @property
    @override
    def estimated_size(self) -> int:
        return len(self.versions) * (estimated_size.UNIQUE_ADDRESS + estimated_size.LONG_VALUE)


VersionVector.empty = ManyVersionVector()
