"""Port of Pekko's ``FrequencyList`` (actor/.../util/FrequencyList.scala).

Pekko's ``DoubleLinkedList`` has no port. The values with one priority are an ``OrderedDict``
(least recent first) in ``_buckets``, and the priorities in use are a sorted ``list`` kept with
``bisect``. Priorities are unique and only rise, so a sorted map gives the same order as Pekko's
``getFirstOrElsePrepend``, ``getNextOrElseInsert`` and ``findNextOrElseInsert``. Per value:
its accesses and priority; overall recency (with a clock) is an ``OrderedDict`` from value to
timestamp. Pekko removes while it iterates; here the victims are collected first, which gives
the same result. Under dynamic aging, only ``remove_least_frequent`` sets the age (Scala 262-265).
"""

from bisect import bisect_left, insort
from collections import OrderedDict
from collections.abc import Hashable, Iterator
from itertools import islice, takewhile
from typing import Self

from . import Clock

_NO_RECENCY = "Overall recency is not enabled for this FrequencyList"


class FrequencyList[A: Hashable]:
    """FrequencyList.scala 24-269. ``FrequencyList(dynamic_aging)`` is Scala's ``empty``, and
    with a ``clock`` it is ``withOverallRecency.empty``."""

    def __init__(self, dynamic_aging: bool = False, clock: Clock | None = None) -> None:
        self._dynamic_aging = dynamic_aging
        self._clock = clock
        self._priorities: list[int] = []  # ascending
        self._buckets: dict[int, OrderedDict[A, None]] = {}
        self._accesses: dict[A, int] = {}
        self._priority_of: dict[A, int] = {}
        self._overall: OrderedDict[A, float] = OrderedDict()
        self._age = 0

    @property
    def size(self) -> int:
        """Scala 82."""
        return len(self._priority_of)

    def update(self, value: A) -> Self:
        """Scala 84-101."""
        if value in self._priority_of:
            self._increase_frequency(value)
            if self._clock is not None:
                self._overall[value] = self._clock()
                self._overall.move_to_end(value)
        else:
            self._accesses[value] = 1
            self._add_to(1 + self._age if self._dynamic_aging else 1, value)
            if self._clock is not None:
                self._overall[value] = self._clock()
        return self

    def remove(self, value: A) -> Self:
        """Scala 103-108."""
        if value in self._priority_of:
            self._remove_node(value)
        return self

    def contains(self, value: A) -> bool:
        """Scala 110."""
        return value in self._priority_of

    def least_frequent(self) -> A | None:
        """Scala 112-119: the least recent of the lowest priority."""
        return next(self._forward(), None)

    def most_frequent(self) -> A | None:
        """Scala 121-128: the most recent of the highest priority."""
        return next(self._backward(), None)

    def least_to_most_frequent(self) -> Iterator[A]:
        """Scala 130."""
        return iter(list(self._forward()))

    def most_to_least_frequent(self) -> Iterator[A]:
        """Scala 132."""
        return iter(list(self._backward()))

    def remove_least_frequent(self, n: int = 1, skip: A | None = None) -> list[A]:
        """Scala 134-148: the first ``n`` of the least-to-most order but ``skip``; each removal
        sets the age under dynamic aging (``removeLeastFrequentNode``, 262-265)."""
        removed = list(islice((v for v in self._forward() if v != skip), n))
        for value in removed:
            if self._dynamic_aging:
                self._age = self._priority_of[value]
            self._remove_node(value)
        return removed

    def remove_most_frequent(self, n: int = 1, skip: A | None = None) -> list[A]:
        """Scala 150-164: the first ``n`` of the most-to-least order but ``skip``."""
        return self._remove_all(islice((v for v in self._backward() if v != skip), n))

    def overall_least_to_most_recent(self) -> Iterator[A]:
        """Scala 166."""
        return iter(list(self._overall))

    def overall_most_to_least_recent(self) -> Iterator[A]:
        """Scala 168."""
        return iter(list(reversed(self._overall)))

    def remove_overall_least_recent(self, n: int = 1) -> list[A]:
        """Scala 170-173: needs a clock, else ``RuntimeError`` (Scala's
        ``UnsupportedOperationException``), as do the other overall removals."""
        self._require_clock()
        return self._remove_all(islice(self._overall, n))

    def remove_overall_most_recent(self, n: int = 1) -> list[A]:
        """Scala 175-178."""
        self._require_clock()
        return self._remove_all(islice(reversed(self._overall), n))

    def remove_overall_least_recent_outside(self, duration: float) -> list[A]:
        """Scala 180-184."""
        clock = self._require_clock()
        earliest = clock() - duration
        return self._remove_all(
            v for v, _ in takewhile(lambda e: e[1] < earliest, self._overall.items())
        )

    def remove_overall_most_recent_within(self, duration: float) -> list[A]:
        """Scala 186-190."""
        clock = self._require_clock()
        latest = clock() - duration
        return self._remove_all(
            v for v, _ in takewhile(lambda e: e[1] > latest, reversed(self._overall.items()))
        )

    def _require_clock(self) -> Clock:
        if self._clock is None:
            raise RuntimeError(_NO_RECENCY)
        return self._clock

    def _increase_frequency(self, value: A) -> None:
        """Scala 219-241: one more access, to the next priority (plus the age under dynamic
        aging), found or inserted in order."""
        self._accesses[value] += 1
        priority = self._accesses[value] + (self._age if self._dynamic_aging else 0)
        self._remove_from_frequency(value)
        self._add_to(priority, value)

    def _add_to(self, priority: int, value: A) -> None:
        """Scala 192-217 and 243-246: the bucket for ``priority`` is found or inserted in
        order, and the value goes last in it."""
        bucket = self._buckets.get(priority)
        if bucket is None:
            bucket = self._buckets[priority] = OrderedDict()
            insort(self._priorities, priority)
        bucket[value] = None
        self._priority_of[value] = priority

    def _remove_from_frequency(self, value: A) -> None:
        """Scala 248-252: an emptied bucket goes."""
        priority = self._priority_of.pop(value)
        bucket = self._buckets[priority]
        del bucket[value]
        if not bucket:
            del self._buckets[priority]
            del self._priorities[bisect_left(self._priorities, priority)]

    def _remove_all(self, victims: Iterator[A]) -> list[A]:
        removed = list(victims)
        for value in removed:
            self._remove_node(value)
        return removed

    def _remove_node(self, value: A) -> None:
        """Scala 254-260."""
        self._remove_from_frequency(value)
        del self._accesses[value]
        self._overall.pop(value, None)

    def _forward(self) -> Iterator[A]:
        """Scala 267: ascending priority, least recent first."""
        for priority in self._priorities:
            yield from self._buckets[priority]

    def _backward(self) -> Iterator[A]:
        """Scala 269: descending priority, most recent first."""
        for priority in reversed(self._priorities):
            yield from reversed(self._buckets[priority])
