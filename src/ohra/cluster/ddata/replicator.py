"""The Replicator's typed protocol, Pekko's ``ddata.typed.scaladsl.Replicator`` commands and
responses, nested in ``Replicator`` as ``EventStream``'s are.

The commands and responses stay on one node, so they are not ``@remotable``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .key import Key
from .replicated_data import ReplicatedData

if TYPE_CHECKING:
    from ...ref import ActorRef


class Replicator:
    # Consistency levels

    @dataclass(frozen=True)
    class ReadLocal:
        pass

    @dataclass(frozen=True)
    class ReadFrom:
        n: int
        timeout: float

    @dataclass(frozen=True)
    class ReadMajority:
        timeout: float
        min_cap: int = 0

    @dataclass(frozen=True)
    class ReadMajorityPlus:
        timeout: float
        additional: int
        min_cap: int = 0

    @dataclass(frozen=True)
    class ReadAll:
        timeout: float

    @dataclass(frozen=True)
    class WriteLocal:
        pass

    @dataclass(frozen=True)
    class WriteTo:
        n: int
        timeout: float

    @dataclass(frozen=True)
    class WriteMajority:
        timeout: float
        min_cap: int = 0

    @dataclass(frozen=True)
    class WriteMajorityPlus:
        timeout: float
        additional: int
        min_cap: int = 0

    @dataclass(frozen=True)
    class WriteAll:
        timeout: float

    type ReadConsistency = (
        Replicator.ReadLocal
        | Replicator.ReadFrom
        | Replicator.ReadMajority
        | Replicator.ReadMajorityPlus
        | Replicator.ReadAll
    )
    type WriteConsistency = (
        Replicator.WriteLocal
        | Replicator.WriteTo
        | Replicator.WriteMajority
        | Replicator.WriteMajorityPlus
        | Replicator.WriteAll
    )

    # Get

    @dataclass(frozen=True)
    class GetSuccess[A: ReplicatedData]:
        key: Key[A]
        data: A

        def get(self, key: Key[A]) -> A:
            return self.data

    @dataclass(frozen=True)
    class NotFound[A: ReplicatedData]:
        key: Key[A]

    @dataclass(frozen=True)
    class GetFailure[A: ReplicatedData]:
        key: Key[A]

    @dataclass(frozen=True)
    class GetDataDeleted[A: ReplicatedData]:
        key: Key[A]

    type GetResponse[A: ReplicatedData] = (
        Replicator.GetSuccess[A]
        | Replicator.NotFound[A]
        | Replicator.GetFailure[A]
        | Replicator.GetDataDeleted[A]
    )

    @dataclass(frozen=True)
    class Get[A: ReplicatedData]:
        key: Key[A]
        consistency: Replicator.ReadConsistency
        reply_to: ActorRef[Replicator.GetResponse[A]]

    # Update

    @dataclass(frozen=True)
    class UpdateSuccess[A: ReplicatedData]:
        key: Key[A]

    @dataclass(frozen=True)
    class UpdateTimeout[A: ReplicatedData]:
        key: Key[A]

    @dataclass(frozen=True)
    class UpdateDataDeleted[A: ReplicatedData]:
        key: Key[A]

    @dataclass(frozen=True)
    class ModifyFailure[A: ReplicatedData]:
        key: Key[A]
        error_message: str
        cause: BaseException

    @dataclass(frozen=True)
    class StoreFailure[A: ReplicatedData]:
        """Nothing produces it until durable storage comes."""

        key: Key[A]

    type UpdateResponse[A: ReplicatedData] = (
        Replicator.UpdateSuccess[A]
        | Replicator.UpdateTimeout[A]
        | Replicator.UpdateDataDeleted[A]
        | Replicator.ModifyFailure[A]
        | Replicator.StoreFailure[A]
    )

    @dataclass(frozen=True)
    class Update[A: ReplicatedData]:
        """``modify`` is applied to the current value, or to ``initial`` when there is none."""

        key: Key[A]
        initial: A
        write_consistency: Replicator.WriteConsistency
        reply_to: ActorRef[Replicator.UpdateResponse[A]]
        modify: Callable[[A], A]

    # Delete

    @dataclass(frozen=True)
    class DeleteSuccess[A: ReplicatedData]:
        key: Key[A]

    @dataclass(frozen=True)
    class DeleteFailure[A: ReplicatedData]:
        key: Key[A]

    @dataclass(frozen=True)
    class DataDeleted[A: ReplicatedData]:
        key: Key[A]

    type DeleteResponse[A: ReplicatedData] = (
        Replicator.DeleteSuccess[A] | Replicator.DeleteFailure[A] | Replicator.DataDeleted[A]
    )

    @dataclass(frozen=True)
    class Delete[A: ReplicatedData]:
        key: Key[A]
        consistency: Replicator.WriteConsistency
        reply_to: ActorRef[Replicator.DeleteResponse[A]]

    # Subscribe

    @dataclass(frozen=True)
    class Changed[A: ReplicatedData]:
        key: Key[A]
        data: A

        def get(self, key: Key[A]) -> A:
            return self.data

    @dataclass(frozen=True)
    class Deleted[A: ReplicatedData]:
        key: Key[A]

    @dataclass(frozen=True)
    class Expired[A: ReplicatedData]:
        key: Key[A]

    type SubscribeResponse[A: ReplicatedData] = (
        Replicator.Changed[A] | Replicator.Deleted[A] | Replicator.Expired[A]
    )

    @dataclass(frozen=True)
    class Subscribe[A: ReplicatedData]:
        """A key id ending in ``*`` is a wildcard for every key whose id starts with the prefix."""

        key: Key[A]
        subscriber: ActorRef[Replicator.SubscribeResponse[A]]

    @dataclass(frozen=True)
    class Unsubscribe[A: ReplicatedData]:
        key: Key[A]
        subscriber: ActorRef[Replicator.SubscribeResponse[A]]

    # Others

    @dataclass(frozen=True)
    class ReplicaCount:
        n: int

    @dataclass(frozen=True)
    class GetReplicaCount:
        reply_to: ActorRef[Replicator.ReplicaCount]

    @dataclass(frozen=True)
    class FlushChanges:
        pass

    @dataclass(frozen=True)
    class GetKeyIdsResult:
        key_ids: frozenset[str]

    @dataclass(frozen=True)
    class GetKeyIds:
        """Internal, for tests."""

        reply_to: ActorRef[Replicator.GetKeyIdsResult]

    type Command = (
        Replicator.Get[ReplicatedData]
        | Replicator.Update[ReplicatedData]
        | Replicator.Delete[ReplicatedData]
        | Replicator.Subscribe[ReplicatedData]
        | Replicator.Unsubscribe[ReplicatedData]
        | Replicator.GetReplicaCount
        | Replicator.FlushChanges
        | Replicator.GetKeyIds
    )
