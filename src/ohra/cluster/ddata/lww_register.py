"""Pekko's LWWRegister.scala: a 'Last Writer Wins Register' CRDT, also called a 'LWW-Register'.

It is described in the paper "A comprehensive study of Convergent and Commutative Replicated
Data Types".

Merge takes the register with highest timestamp. Note that this relies on synchronized clocks.
``LWWRegister`` should only be used when the choice of value is not important for concurrent
updates occurring within the clock skew.

Merge takes the register updated by the node with lowest address (``UniqueAddress`` is ordered)
if the timestamps are exactly the same.

Instead of using timestamps based on the wall clock it is possible to use a timestamp value
based on something else, for example an increasing version number from a database record that
is used for optimistic concurrency control.

The ``default_clock`` is using max value of the wall clock in milliseconds and
``current_timestamp + 1``. This means that the timestamp is increased for changes on the same
node that occurs within the same millisecond. For first-write-wins semantics use
``reverse_clock``.

Immutable: "modifying" methods return a new instance."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, override

from ...remote import UniqueAddress
from ...remote.serialization import remotable
from .key import Key
from .replicated_data import ReplicatedData

if TYPE_CHECKING:
    from . import SelfUniqueAddress

type Clock[A] = Callable[[int, A], int]
"""(current timestamp, value to set) -> next timestamp."""


def default_clock(current_timestamp: int, value: object) -> int:
    """Max of the wall clock in milliseconds and ``current_timestamp + 1``."""
    return max(current_timestamp + 1, time.time_ns() // 1_000_000)


def reverse_clock(current_timestamp: int, value: object) -> int:
    """For first-write-wins semantics: it counts backwards."""
    return min(-(time.time_ns() // 1_000_000), current_timestamp - 1)


@remotable
@dataclass(frozen=True)
class LWWRegister[A](ReplicatedData):
    node: UniqueAddress
    value: A
    timestamp: int

    @staticmethod
    def create[B](
        node: SelfUniqueAddress, initial_value: B, clock: Clock[B] = default_clock
    ) -> LWWRegister[B]:
        return LWWRegister._create(node.unique_address, initial_value, clock)

    @staticmethod
    def _create[B](node: UniqueAddress, initial_value: B, clock: Clock[B]) -> LWWRegister[B]:
        """INTERNAL API"""
        return LWWRegister(node, initial_value, clock(0, initial_value))

    def with_value(
        self, node: SelfUniqueAddress, value: A, clock: Clock[A] = default_clock
    ) -> LWWRegister[A]:
        """Change the value of the register."""
        return self._with_value(node.unique_address, value, clock)

    def _with_value(self, node: UniqueAddress, value: A, clock: Clock[A]) -> LWWRegister[A]:
        """INTERNAL API"""
        return LWWRegister(node, value, clock(self.timestamp, value))

    @property
    def updated_by(self) -> UniqueAddress:
        """The current ``value`` was set by this node."""
        return self.node

    @override
    def merge(self, that: LWWRegister[A]) -> LWWRegister[A]:  # pyright: ignore[reportIncompatibleMethodOverride]
        if that.timestamp > self.timestamp:
            return that
        if that.timestamp < self.timestamp:
            return self
        return that if that.node < self.node else self

    def __repr__(self) -> str:
        return f"LWWRegister({self.value})"


@remotable
@dataclass(frozen=True, eq=False)
class LWWRegisterKey[A](Key[LWWRegister[A]]):
    @override
    def with_id(self, new_id: str) -> LWWRegisterKey[A]:
        return LWWRegisterKey(new_id)
