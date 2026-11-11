"""Port of Pekko's ``SegmentedRecencyList`` (actor/.../util/SegmentedRecencyList.scala).

Pekko's ``DoubleLinkedList`` has no port: an ``OrderedDict`` per level (least recent first),
a ``dict`` from value to level, and, when there is a clock (Pekko's ``withOverallRecency``),
an ``OrderedDict`` from value to timestamp in overall recency order. Pekko demotes and removes
while it iterates; here the victims are collected first, which gives the same result.
"""

from collections import OrderedDict
from collections.abc import Hashable, Iterator, Sequence
from itertools import islice, takewhile
from typing import Self

from . import Clock


class SegmentedRecencyList[A: Hashable]:
    """SegmentedRecencyList.scala 23-186. ``SegmentedRecencyList(limits)`` is Scala's
    ``empty(limits)``, and with a ``clock`` it is ``withOverallRecency.empty(clock, limits)``."""

    def __init__(self, initial_limits: Sequence[int], clock: Clock | None = None) -> None:
        self._limits = tuple(initial_limits)
        self._total_limit = sum(self._limits)
        self._clock = clock
        self._levels = len(self._limits)
        self._lowest = 0
        self._highest = self._levels - 1
        self._segments: list[OrderedDict[A, None]] = [OrderedDict() for _ in self._limits]
        self._level_of: dict[A, int] = {}
        self._overall: OrderedDict[A, float] = OrderedDict()

    @property
    def size(self) -> int:
        """Scala 77."""
        return len(self._level_of)

    def size_of(self, level: int) -> int:
        """Scala 79."""
        return len(self._segments[level])

    def update(self, value: A) -> Self:
        """Scala 81-88: promote a known value, else insert it at the lowest level."""
        if value in self._level_of:
            self._promote(value)
        else:
            self._insert(value)
        return self

    def remove(self, value: A) -> Self:
        """Scala 90-95."""
        if value in self._level_of:
            self._remove_node(value)
        return self

    def contains(self, value: A) -> bool:
        """Scala 97."""
        return value in self._level_of

    def least_recent(self) -> A | None:
        """Scala 99-102: the least recent of the lowest level."""
        return next(iter(self._segments[self._lowest]), None)

    def least_to_most_recent_of(self, level: int) -> Iterator[A]:
        """Scala 104."""
        return iter(list(self._segments[level]))

    def remove_least_recent_over_limit(self) -> list[A]:
        """Scala 106-113: when over the total limit, demote the protected levels down to their
        limits, then remove the excess from the least recent of the lowest level."""
        if self.size > self._total_limit:
            self._adjust_protected_levels()
            excess = self.size - self._total_limit
            return self._remove_all(islice(self._segments[self._lowest], excess))
        return []

    def remove_least_recent(self) -> list[A]:
        """Scala 115-118."""
        return self._remove_all(islice(self._segments[self._lowest], 1))

    def remove_overall_least_recent_outside(self, duration: float) -> list[A]:
        """Scala 120-124: needs a clock, else ``RuntimeError`` (Scala's
        ``UnsupportedOperationException``)."""
        if self._clock is None:
            raise RuntimeError("Overall recency is not enabled")
        earliest = self._clock() - duration
        return self._remove_all(
            v for v, _ in takewhile(lambda e: e[1] < earliest, self._overall.items())
        )

    def update_limits(self, new_limits: Sequence[int]) -> None:
        """Scala 126-129."""
        self._limits = tuple(new_limits)
        self._total_limit = sum(self._limits)

    def _adjust_protected_levels(self) -> None:
        """Scala 131-132: ``highest until lowest by -1``."""
        for level in range(self._highest, self._lowest, -1):
            self._adjust(level)

    def _adjust(self, level: int) -> None:
        """Scala 134-137: demote the least recent of the level beyond its limit."""
        excess = self.size_of(level) - self._limits[level]
        if excess > 0:
            for value in list(islice(self._segments[level], excess)):
                self._demote(value, level)

    def _insert(self, value: A) -> None:
        """Scala 139-146."""
        self._append_to(self._lowest, value)
        if self._clock is not None:
            self._overall[value] = self._clock()

    def _promote(self, value: A) -> None:
        """Scala 148-161."""
        level = self._level_of[value]
        if level == self._highest:
            self._segments[level].move_to_end(value)
        else:
            del self._segments[level][value]
            self._append_to(level + 1, value)
            self._adjust(level + 1)
        if self._clock is not None:
            self._overall[value] = self._clock()
            self._overall.move_to_end(value)

    def _demote(self, value: A, level: int) -> None:
        """Scala 163-167: from a protected level to the end of the one below."""
        del self._segments[level][value]
        self._append_to(level - 1, value)

    def _append_to(self, level: int, value: A) -> None:
        """Scala 169-173."""
        self._level_of[value] = level
        self._segments[level][value] = None

    def _remove_all(self, victims: Iterator[A]) -> list[A]:
        removed = list(victims)
        for value in removed:
            self._remove_node(value)
        return removed

    def _remove_node(self, value: A) -> None:
        """Scala 180-186."""
        del self._segments[self._level_of.pop(value)][value]
        self._overall.pop(value, None)
