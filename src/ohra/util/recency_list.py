"""Port of Pekko's ``RecencyList`` (actor/.../util/RecencyList.scala).

Pekko's ``DoubleLinkedList`` has no port: one ``OrderedDict`` from value to timestamp, least
recent first, with ``move_to_end`` on update, gives the same order. Pekko removes while it
iterates (``take(n).map(removeNode)``); here the victims are collected first and then removed,
which gives the same result, since a removal never reorders what is left. Durations and
timestamps are seconds as floats.
"""

from collections import OrderedDict
from collections.abc import Hashable, Iterator
from itertools import islice, takewhile
from typing import Self

from . import Clock


class RecencyList[A: Hashable]:
    """RecencyList.scala 41-136. ``RecencyList(clock)`` is Scala's ``RecencyList.apply``."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._recency: OrderedDict[A, float] = OrderedDict()  # least recent first

    @property
    def size(self) -> int:
        """Scala 57."""
        return len(self._recency)

    def update(self, value: A) -> Self:
        """Scala 59-71: stamp the value and make it the most recent."""
        self._recency[value] = self._clock()
        self._recency.move_to_end(value)
        return self

    def remove(self, value: A) -> Self:
        """Scala 73-78."""
        self._recency.pop(value, None)
        return self

    def contains(self, value: A) -> bool:
        """Scala 80."""
        return value in self._recency

    def least_recent(self) -> A | None:
        """Scala 82-85."""
        return next(iter(self._recency), None)

    def most_recent(self) -> A | None:
        """Scala 87-90."""
        return next(reversed(self._recency), None)

    def least_to_most_recent(self) -> Iterator[A]:
        """Scala 92."""
        return iter(list(self._recency))

    def most_to_least_recent(self) -> Iterator[A]:
        """Scala 94."""
        return iter(list(reversed(self._recency)))

    def remove_least_recent(self, n: int = 1, skip: int = 0) -> list[A]:
        """Scala 96-106: ``slice(skip, skip + n)`` of the least-to-most order."""
        return self._remove_all(islice(self._recency, skip, skip + n))

    def remove_most_recent(self, n: int = 1, skip: int = 0) -> list[A]:
        """Scala 108-118: ``slice(skip, skip + n)`` of the most-to-least order."""
        return self._remove_all(islice(reversed(self._recency), skip, skip + n))

    def remove_least_recent_outside(self, duration: float) -> list[A]:
        """Scala 120-123: remove from the least recent while older than ``duration``."""
        earliest = self._clock() - duration
        return self._remove_all(
            v for v, _ in takewhile(lambda e: e[1] < earliest, self._recency.items())
        )

    def remove_most_recent_within(self, duration: float) -> list[A]:
        """Scala 125-128: remove from the most recent while newer than ``duration`` ago."""
        latest = self._clock() - duration
        return self._remove_all(
            v for v, _ in takewhile(lambda e: e[1] > latest, reversed(self._recency.items()))
        )

    def _remove_all(self, victims: Iterator[A]) -> list[A]:
        """Scala 130-135 ``removeNode``, after collecting the victims."""
        removed = list(victims)
        for value in removed:
            del self._recency[value]
        return removed
