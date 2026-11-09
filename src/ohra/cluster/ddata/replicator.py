"""Pekko's Replicator: its typed protocol, the ``ddata.typed.scaladsl.Replicator`` commands and
responses nested in ``Replicator`` as ``EventStream``'s are, and the actor, whose handlers port
classic ``Replicator.scala`` function for function.

The commands and responses stay on one node, so they are not ``@remotable``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ...behavior import Behavior, Behaviors
from .envelope import DELETED_ENVELOPE, DataEnvelope
from .key import Key
from .replicated_data import ReplicatedData
from .settings import ReplicatorSettings

if TYPE_CHECKING:
    from ...context import ActorContext
    from ...ref import ActorRef
    from ...remote import UniqueAddress


class Replicator:
    @staticmethod
    def behavior(settings: ReplicatorSettings) -> Behavior[Replicator.Command]:
        """The Replicator actor, as Pekko's ``Replicator.behavior(settings)``."""
        return _replicator(settings)

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
        Replicator.Get[Any]
        | Replicator.Update[Any]
        | Replicator.Delete[Any]
        | Replicator.Subscribe[Any]
        | Replicator.Unsubscribe[Any]
        | Replicator.GetReplicaCount
        | Replicator.FlushChanges
        | Replicator.GetKeyIds
    )


type _Digest = bytes
# Pekko's LazyDigest and DeletedDigest. The digests that gossip compares come with gossip.
_LAZY_DIGEST: _Digest = b"\x00"
_DELETED_DIGEST: _Digest = b""

type _Msg = Replicator.Command


def _replicator(settings: ReplicatorSettings) -> Behavior[_Msg]:
    @Behaviors.setup
    def setup(ctx: ActorContext[_Msg]) -> Behavior[_Msg]:
        # the other members that are Up or Leaving, empty until the Replicator tracks membership
        nodes: set[UniqueAddress] = set()
        data_entries: dict[str, tuple[DataEnvelope, _Digest]] = {}

        def receive_get(
            key: Key[Any],
            consistency: Replicator.ReadConsistency,
            reply_to: ActorRef[Replicator.GetResponse[Any]],
        ) -> None:
            local_value = get_data(key.id)
            ctx.log.debug("Received Get for key [%s].", key)
            if is_local_get(consistency):
                reply_to.tell(local_reply(key, local_value))
            # ponytail: Pekko starts a ReadAggregator here. With `nodes` empty it replies at once,
            # as below; the aggregator replaces this when the Replicator tracks membership.
            elif not_enough_nodes(consistency):
                reply_to.tell(Replicator.GetFailure(key))
            else:
                reply_to.tell(local_reply(key, local_value))

        def local_reply(
            key: Key[Any], local_value: DataEnvelope | None
        ) -> Replicator.GetResponse[Any]:
            match local_value:
                case None:
                    return Replicator.NotFound(key)
                case envelope if envelope.is_deleted:
                    return Replicator.GetDataDeleted(key)
                case envelope:
                    return Replicator.GetSuccess(key, envelope.data)

        def is_local_get(read_consistency: Replicator.ReadConsistency) -> bool:
            match read_consistency:
                case Replicator.ReadLocal():
                    return True
                case Replicator.ReadMajority() | Replicator.ReadAll():
                    return not nodes
                case _:
                    return False

        def receive_update(
            key: Key[Any],
            initial: Any,
            modify: Callable[[Any], Any],
            write_consistency: Replicator.WriteConsistency,
            reply_to: ActorRef[Replicator.UpdateResponse[Any]],
        ) -> None:
            local_value = get_data(key.id)
            try:
                match local_value:
                    case DataEnvelope() as envelope if envelope.is_deleted:
                        pass
                    case DataEnvelope(existing) as envelope:
                        envelope = envelope.merge(DataEnvelope(modify(existing)))
                    case None:
                        envelope = DataEnvelope(modify(initial))
            except Exception as e:  # Pekko's Try: NonFatal
                ctx.log.debug("Received Update for key [%s], failed: %s", key, e)
                reply_to.tell(Replicator.ModifyFailure(key, f"Update failed: {e}", e))
                return
            if envelope.is_deleted:
                ctx.log.debug("Received Update for deleted key [%s].", key)
                reply_to.tell(Replicator.UpdateDataDeleted(key))
                return
            ctx.log.debug("Received Update for key [%s].", key)
            set_data(key.id, envelope)
            if is_local_update(write_consistency):
                reply_to.tell(Replicator.UpdateSuccess(key))
            # ponytail: Pekko starts a WriteAggregator here, as in receive_get
            elif not_enough_nodes(write_consistency):
                reply_to.tell(Replicator.UpdateTimeout(key))
            else:
                reply_to.tell(Replicator.UpdateSuccess(key))

        def is_local_update(write_consistency: Replicator.WriteConsistency) -> bool:
            match write_consistency:
                case Replicator.WriteLocal():
                    return True
                case Replicator.WriteMajority() | Replicator.WriteAll():
                    return not nodes
                case _:
                    return False

        def not_enough_nodes(
            consistency: Replicator.ReadConsistency | Replicator.WriteConsistency,
        ) -> bool:
            """The aggregators' ``notEnoughNodes``: the local node counts as one replica."""
            match consistency:
                case Replicator.ReadFrom(n) | Replicator.WriteTo(n):
                    return n > len(nodes) + 1
                case _:
                    return False

        def receive_get_key_ids(reply_to: ActorRef[Replicator.GetKeyIdsResult]) -> None:
            keys = frozenset(
                k for k, (envelope, _) in data_entries.items() if not envelope.is_deleted
            )
            reply_to.tell(Replicator.GetKeyIdsResult(keys))

        def receive_delete(
            key: Key[Any],
            consistency: Replicator.WriteConsistency,
            reply_to: ActorRef[Replicator.DeleteResponse[Any]],
        ) -> None:
            match get_data(key.id):
                case DataEnvelope() as envelope if envelope.is_deleted:
                    reply_to.tell(Replicator.DataDeleted(key))  # already deleted
                case _:
                    set_data(key.id, DELETED_ENVELOPE)
                    if is_local_update(consistency):
                        reply_to.tell(Replicator.DeleteSuccess(key))
                    # ponytail: Pekko starts a WriteAggregator here, as in receive_get
                    elif not_enough_nodes(consistency):
                        reply_to.tell(Replicator.DeleteFailure(key))
                    else:
                        reply_to.tell(Replicator.DeleteSuccess(key))

        def set_data(key: str, envelope: DataEnvelope) -> DataEnvelope:
            dig = _DELETED_DIGEST if envelope.is_deleted else _LAZY_DIGEST
            data_entries[key] = (envelope, dig)
            return envelope

        def get_data(key: str) -> DataEnvelope | None:
            entry = data_entries.get(key)
            return None if entry is None else entry[0]

        def receive_get_replica_count(reply_to: ActorRef[Replicator.ReplicaCount]) -> None:
            reply_to.tell(Replicator.ReplicaCount(len(nodes) + 1))  # nodes excludes self

        @Behaviors.receive
        def normal_receive(ctx: ActorContext[_Msg], msg: _Msg) -> Behavior[_Msg]:
            match msg:
                case Replicator.Get(key, consistency, reply_to):
                    receive_get(key, consistency, reply_to)
                case Replicator.Update(key, initial, write_consistency, reply_to, modify):
                    receive_update(key, initial, modify, write_consistency, reply_to)
                case Replicator.GetKeyIds(reply_to):
                    receive_get_key_ids(reply_to)
                case Replicator.Delete(key, consistency, reply_to):
                    receive_delete(key, consistency, reply_to)
                case Replicator.GetReplicaCount(reply_to):
                    receive_get_replica_count(reply_to)
                case Replicator.Subscribe() | Replicator.Unsubscribe() | Replicator.FlushChanges():
                    return Behaviors.unhandled  # subscriptions are not ported yet
            return Behaviors.same

        return normal_receive

    return setup
