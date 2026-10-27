"""Phi-accrual failure detection and the heartbeat ring, as Pekko's PhiAccrualFailureDetector,
DefaultFailureDetectorRegistry, HeartbeatNodeRing and ClusterHeartbeatSenderState. Times are
seconds."""

from __future__ import annotations

import math
import time
import zlib
from collections.abc import Callable
from dataclasses import dataclass, replace
from functools import cached_property

from ..remote import Address, UniqueAddress


@dataclass(frozen=True)
class HeartbeatHistory:
    """Mean and variance of the last ``max_sample_size`` heartbeat intervals, kept as running
    sums."""

    max_sample_size: int
    intervals: tuple[float, ...] = ()
    interval_sum: float = 0.0
    squared_interval_sum: float = 0.0

    def __post_init__(self) -> None:
        if self.max_sample_size < 1:
            raise ValueError(f"max_sample_size must be >= 1, got {self.max_sample_size}")

    @property
    def mean(self) -> float:
        return self.interval_sum / len(self.intervals)

    @property
    def variance(self) -> float:
        return self.squared_interval_sum / len(self.intervals) - self.mean * self.mean

    @property
    def std_deviation(self) -> float:
        return math.sqrt(max(self.variance, 0.0))

    def add(self, interval: float) -> HeartbeatHistory:
        if len(self.intervals) < self.max_sample_size:
            return replace(
                self,
                intervals=(*self.intervals, interval),
                interval_sum=self.interval_sum + interval,
                squared_interval_sum=self.squared_interval_sum + interval * interval,
            )
        return self._drop_oldest().add(interval)

    def _drop_oldest(self) -> HeartbeatHistory:
        head = self.intervals[0]
        return replace(
            self,
            intervals=self.intervals[1:],
            interval_sum=self.interval_sum - head,
            squared_interval_sum=self.squared_interval_sum - head * head,
        )


class PhiAccrualFailureDetector:
    """Hayashibara et al.'s phi accrual detector with Pekko's approximation of the normal CDF."""

    def __init__(
        self,
        threshold: float = 8.0,
        max_sample_size: int = 1000,
        min_std_deviation: float = 0.1,
        acceptable_heartbeat_pause: float = 3.0,
        first_heartbeat_estimate: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if threshold <= 0.0:
            raise ValueError("threshold must be > 0")
        if min_std_deviation <= 0.0:
            raise ValueError("min_std_deviation must be > 0")
        if acceptable_heartbeat_pause < 0.0:
            raise ValueError("acceptable_heartbeat_pause must be >= 0")
        if first_heartbeat_estimate <= 0.0:
            raise ValueError("first_heartbeat_estimate must be > 0")
        self.threshold = threshold
        self.min_std_deviation = min_std_deviation
        self.acceptable_heartbeat_pause = acceptable_heartbeat_pause
        self._clock = clock
        mean = first_heartbeat_estimate
        std = mean / 4
        self._first_heartbeat = HeartbeatHistory(max_sample_size).add(mean - std).add(mean + std)
        self._history = self._first_heartbeat
        self._timestamp: float | None = None

    @property
    def is_available(self) -> bool:
        return self._is_available(self._clock())

    def _is_available(self, timestamp: float) -> bool:
        return self._phi(timestamp) < self.threshold

    @property
    def is_monitoring(self) -> bool:
        return self._timestamp is not None

    def heartbeat(self) -> None:
        timestamp = self._clock()
        if self._timestamp is None:
            self._history = self._first_heartbeat
        elif self._is_available(timestamp):
            self._history = self._history.add(timestamp - self._timestamp)
        # else: the pause was too long to be a sample; keep the history as it was
        self._timestamp = timestamp

    @property
    def phi(self) -> float:
        return self._phi(self._clock())

    def _phi(self, timestamp: float) -> float:
        if self._timestamp is None:
            return 0.0  # a connection with zero heartbeats is treated as healthy
        time_diff = timestamp - self._timestamp
        mean = self._history.mean + self.acceptable_heartbeat_pause
        std = max(self._history.std_deviation, self.min_std_deviation)
        return phi(time_diff, mean, std)


def phi(time_diff: float, mean: float, std_deviation: float) -> float:
    """Pekko's logistic approximation of -log10(1 - CDF)."""
    y = (time_diff - mean) / std_deviation
    try:
        e = math.exp(-y * (1.5976 + 0.070566 * y * y))
    except OverflowError:  # the JVM's exp gives Infinity here: long before the mean, phi is 0
        e = math.inf
    if time_diff > mean:
        # the JVM's log10(0) is -Infinity
        return math.inf if e == 0.0 else -math.log10(e / (1.0 + e))
    return -math.log10(1.0 - 1.0 / (1.0 + e))


class FailureDetectorRegistry:
    """One detector per address, created by ``factory`` on the first heartbeat."""

    def __init__(self, factory: Callable[[], PhiAccrualFailureDetector]) -> None:
        self._factory = factory
        self._detectors: dict[Address, PhiAccrualFailureDetector] = {}

    def is_available(self, address: Address) -> bool:
        fd = self._detectors.get(address)
        return fd.is_available if fd is not None else True

    def is_monitoring(self, address: Address) -> bool:
        fd = self._detectors.get(address)
        return fd.is_monitoring if fd is not None else False

    def heartbeat(self, address: Address) -> None:
        fd = self._detectors.get(address)
        if fd is None:
            fd = self._detectors[address] = self._factory()
        fd.heartbeat()

    def remove(self, address: Address) -> None:
        self._detectors.pop(address, None)

    def reset(self) -> None:
        self._detectors.clear()


def _ring_hash(node: UniqueAddress) -> int:
    # ponytail: Pekko orders the ring by hashCode; Python's hash() is salted per process, and every
    # node must compute the same ring, so use a stable checksum instead.
    return zlib.crc32(f"{node.host}:{node.port}:{node.uid}".encode())


@dataclass(frozen=True)
class HeartbeatNodeRing:
    """Who heartbeats whom: each node monitors the next ``monitored_by_nr_of_members`` reachable
    nodes in a ring sorted by hash, plus up to that many unreachable nodes in between."""

    self_address: UniqueAddress
    nodes: frozenset[UniqueAddress]
    unreachable: frozenset[UniqueAddress]
    monitored_by_nr_of_members: int

    def __post_init__(self) -> None:
        if self.self_address not in self.nodes:
            raise ValueError(f"nodes must contain self_address {self.self_address}")

    @cached_property
    def node_ring(self) -> tuple[UniqueAddress, ...]:
        return tuple(
            sorted(self.nodes, key=lambda n: (_ring_hash(n), n.address.host, n.address.port))
        )

    @cached_property
    def my_receivers(self) -> frozenset[UniqueAddress]:
        return self.receivers(self.self_address)

    @property
    def _use_all_as_receivers(self) -> bool:
        return self.monitored_by_nr_of_members >= len(self.node_ring) - 1

    def receivers(self, sender: UniqueAddress) -> frozenset[UniqueAddress]:
        if self._use_all_as_receivers:
            return self.nodes - {sender}
        # Pick nodes after `sender` until n reachable ones have been selected. Intermediate
        # unreachable nodes, up to `monitored_by_nr_of_members` of them, are included too, so the
        # leader can keep removing downed nodes when many are shut down at once and nobody else is
        # watching them (Akka #16624).
        n = self.monitored_by_nr_of_members
        acc: set[UniqueAddress] = set()
        at = self.node_ring.index(sender)
        # the ring, starting after the sender
        for node in self.node_ring[at + 1 :] + self.node_ring[:at]:
            if n == 0:
                break
            if node in self.unreachable:
                if len(acc) < self.monitored_by_nr_of_members:
                    acc.add(node)  # include it, but do not count it
            else:
                acc.add(node)
                n -= 1
        return frozenset(acc)

    def add(self, node: UniqueAddress) -> HeartbeatNodeRing:
        return self if node in self.nodes else replace(self, nodes=self.nodes | {node})

    def remove(self, node: UniqueAddress) -> HeartbeatNodeRing:
        if node in self.nodes or node in self.unreachable:
            return replace(self, nodes=self.nodes - {node}, unreachable=self.unreachable - {node})
        return self


@dataclass(frozen=True)
class HeartbeatSenderState:
    """The sender's view: the ring, plus receivers that dropped out of it while unreachable and are
    still heartbeated until they answer or leave. The registry is shared and mutable, as
    Pekko's."""

    ring: HeartbeatNodeRing
    old_receivers_now_unreachable: frozenset[UniqueAddress]
    failure_detector: FailureDetectorRegistry

    @property
    def active_receivers(self) -> frozenset[UniqueAddress]:
        return self.ring.my_receivers | self.old_receivers_now_unreachable

    @property
    def self_address(self) -> UniqueAddress:
        return self.ring.self_address

    def init(
        self, nodes: frozenset[UniqueAddress], unreachable: frozenset[UniqueAddress]
    ) -> HeartbeatSenderState:
        return replace(
            self,
            ring=replace(self.ring, nodes=nodes | {self.self_address}, unreachable=unreachable),
        )

    def contains(self, node: UniqueAddress) -> bool:
        return node in self.ring.nodes

    def add_member(self, node: UniqueAddress) -> HeartbeatSenderState:
        return self._membership_change(self.ring.add(node))

    def remove_member(self, node: UniqueAddress) -> HeartbeatSenderState:
        new = self._membership_change(self.ring.remove(node))
        self.failure_detector.remove(node.address)
        if node in new.old_receivers_now_unreachable:
            return replace(
                new, old_receivers_now_unreachable=new.old_receivers_now_unreachable - {node}
            )
        return new

    def unreachable_member(self, node: UniqueAddress) -> HeartbeatSenderState:
        return self._membership_change(
            replace(self.ring, unreachable=self.ring.unreachable | {node})
        )

    def reachable_member(self, node: UniqueAddress) -> HeartbeatSenderState:
        return self._membership_change(
            replace(self.ring, unreachable=self.ring.unreachable - {node})
        )

    def _membership_change(self, new_ring: HeartbeatNodeRing) -> HeartbeatSenderState:
        removed = self.ring.my_receivers - new_ring.my_receivers
        adjusted = self.old_receivers_now_unreachable
        for a in removed:
            if self.failure_detector.is_available(a.address):
                self.failure_detector.remove(a.address)
            else:
                adjusted = adjusted | {a}
        return replace(self, ring=new_ring, old_receivers_now_unreachable=adjusted)

    def heartbeat_rsp(self, from_: UniqueAddress) -> HeartbeatSenderState:
        if from_ not in self.active_receivers:
            return self
        self.failure_detector.heartbeat(from_.address)
        if from_ in self.old_receivers_now_unreachable:
            # back from unreachable: ok to stop heartbeating to it
            if from_ not in self.ring.my_receivers:
                self.failure_detector.remove(from_.address)
            return replace(
                self, old_receivers_now_unreachable=self.old_receivers_now_unreachable - {from_}
            )
        return self
