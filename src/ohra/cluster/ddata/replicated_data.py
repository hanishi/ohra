"""The traits of Pekko's ReplicatedData.scala. The Java ``Abstract*`` classes are not ported."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Self

from ...remote import UniqueAddress


class ReplicatedData(ABC):
    """A data type whose replicas on different nodes can be merged, commutatively, associatively
    and idempotently."""

    @abstractmethod
    def merge(self, that: Self) -> Self: ...


class ReplicatedDelta(ReplicatedData):
    """The delta of a ``DeltaReplicatedData``: a small part of the state, merged like a state."""

    @abstractmethod
    def zero(self) -> DeltaReplicatedData[Any]:
        """The empty full state that this delta is merged into."""


class DeltaReplicatedData[D: ReplicatedDelta](ReplicatedData):
    """A ``ReplicatedData`` that can ship its changes as deltas.

    A subclass's ``delta`` is never sent and never compared: a ``transient()`` dataclass field,
    or a property computed from other fields (``PNCounter``'s)."""

    delta: D | None

    @abstractmethod
    def merge_delta(self, that_delta: D) -> Self: ...

    @abstractmethod
    def reset_delta(self) -> Self: ...


class RequiresCausalDeliveryOfDeltas(ReplicatedDelta):
    """A delta that must be delivered in order, without gaps."""


class ReplicatedDeltaSize(ABC):
    """A delta that knows its size, to bound how many are grouped in one message."""

    @property
    @abstractmethod
    def delta_size(self) -> int: ...


class RemovedNodePruning(ReplicatedData):
    """Data that records which nodes modified it, so a removed node's entries can be collapsed
    into a live node."""

    @property
    @abstractmethod
    def modified_by_nodes(self) -> frozenset[UniqueAddress]: ...

    @abstractmethod
    def need_pruning_from(self, removed_node: UniqueAddress) -> bool: ...

    @abstractmethod
    def prune(self, removed_node: UniqueAddress, collapse_into: UniqueAddress) -> Self: ...

    @abstractmethod
    def pruning_cleanup(self, removed_node: UniqueAddress) -> Self: ...
