"""Pekko's Replicator: its typed protocol, the ``ddata.typed.scaladsl.Replicator`` commands and
responses nested in ``Replicator`` as ``EventStream``'s are, and the actor, whose handlers port
classic ``Replicator.scala`` function for function.

The Replicator tracks the cluster's members, reads and writes at a level other than local
through aggregator children, gossips digests to a random replica for anti-entropy, and prunes
the data of removed nodes, as the "CRDT Garbage" section of Pekko's Replicator Scaladoc
describes. Deltas and durable storage are not here.
The commands and responses stay on one node, so they are not ``@remotable``.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import time
import zlib
from bisect import insort
from collections.abc import Callable, Collection, Iterable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, cast

from ...behavior import Behavior, Behaviors
from ...cell import watched_ref
from ...remote.remoting import MAX_FRAME
from ...remote.serialization import Serialization, canonical_json
from ...signals import PostStop, PreRestart, Signal
from .envelope import (
    DELETED_ENVELOPE,
    DataEnvelope,
    Gossip,
    Read,
    ReadRepair,
    ReadRepairAck,
    ReadResult,
    Status,
    Write,
    WriteAck,
    WriteNack,
    send_to_replica,
)
from .key import Key
from .pruning_state import PruningInitialized, PruningPerformed
from .replicated_data import RemovedNodePruning, ReplicatedData
from .settings import ReplicatorSettings

if TYPE_CHECKING:
    from logging import Logger

    from ...context import ActorContext
    from ...ref import ActorRef
    from ...remote import Address, UniqueAddress
    from ..core import ClusterDomainEvent
    from ..gossip import Member


class Replicator:
    @staticmethod
    def behavior(settings: ReplicatorSettings) -> Behavior[Replicator.Command]:
        """The Replicator actor, as Pekko's ``Replicator.behavior(settings)``."""
        # Behavior has no narrow: the private ticks and terminations are not part of Command
        return cast(Behavior[Replicator.Command], _replicator(settings))

    # Consistency levels

    @dataclass(frozen=True)
    class ReadLocal:
        pass

    @dataclass(frozen=True)
    class ReadFrom:
        n: int
        timeout: float

        def __post_init__(self) -> None:
            # Pekko requires n >= 2; ohra runs n == 1 locally (appendix B)
            if self.n < 1:
                raise ValueError(f"ReadFrom n must be >= 1, was {self.n}")

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

        def __post_init__(self) -> None:
            # Pekko requires n >= 2; ohra runs n == 1 locally (appendix B)
            if self.n < 1:
                raise ValueError(f"WriteTo n must be >= 1, was {self.n}")

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


# Pekko's SHA-1 Digest, as lowercase hex. The sentinels can never be hex SHA-1.
type _Digest = str
_DELETED_DIGEST: _Digest = ""
_LAZY_DIGEST: _Digest = "lazy"
_NOT_FOUND_DIGEST: _Digest = "not-found"


def _digest(
    envelope: DataEnvelope, ref_path: Callable[[ActorRef[Any]], str]
) -> tuple[_Digest, int]:
    """The SHA-1 digest of the canonically encoded data, and the size of the encoded data. Pekko
    hashes its protobuf serialization, which sorts sets and maps: so does the canonical encoding,
    or equal data built in different orders would be gossiped forever."""
    if envelope.is_deleted:
        return _DELETED_DIGEST, 0
    body = canonical_json(Serialization.encode(envelope, ref_path, canonical=True)).encode()
    return hashlib.sha1(body).hexdigest(), len(body)


# Pekko's used timestamp, in seconds since the epoch. None is Pekko's 0L: not set. On the wire,
# in Status and Gossip, it is 0.0.
type _Timestamp = float


def _chunk_of(key: str, tot_chunks: int) -> int:
    """Pekko's ``math.abs(key.hashCode % totChunks)``. Python's str hash differs per process, so
    the chunk comes from the key's CRC-32, which every node computes alike."""
    return zlib.crc32(key.encode()) % tot_chunks


class _PayloadSizeAggregator:
    """Pekko's PayloadSizeAggregator: logs the encoded size of data larger than
    ``log_size_exceeding``, again only once it has grown by 10%, and keeps the largest size seen
    per key for sizing Gossip messages."""

    def __init__(self, log: Logger, log_size_exceeding: int, max_frame_size: int) -> None:
        self._log = log
        self._log_size_exceeding = log_size_exceeding
        self.max_frame_size = max_frame_size
        self._warn_size_exceeding = max_frame_size * 3 // 4
        self._max_payload_bytes: dict[str, int] = {}

    def update_payload_size(self, key: str, size: int) -> None:
        if size > 0:  # deleted has size 0
            existing = self._max_payload_bytes.get(key)
            if existing is None or size > existing:
                self._max_payload_bytes[key] = int(size * 1.1)  # 10% threshold until next log
                if size >= self._log_size_exceeding:
                    if size >= self._warn_size_exceeding:
                        self._log.warning(
                            "Distributed data size for [%s] is [%s] bytes. Close to max remote "
                            "message payload size.",
                            key,
                            size,
                        )
                    else:
                        self._log.info("Distributed data size for [%s] is [%s] bytes.", key, size)

    def get_max_size(self, key: str) -> int:
        return self._max_payload_bytes.get(key, 0)

    def remove(self, key: str) -> None:
        self._max_payload_bytes.pop(key, None)


@dataclass(frozen=True)
class _GossipTick:
    pass


@dataclass(frozen=True)
class _ClockTick:
    pass


@dataclass(frozen=True)
class _RemovedNodePruningTick:
    pass


@dataclass(frozen=True)
class _SubscriberTerminated:
    """A watched subscriber stopped: Pekko's Terminated, as ``watch_with`` delivers it."""

    ref: ActorRef[Any]


@dataclass(frozen=True)
class _ClusterEvent:
    """A member or reachability event, through the Replicator's message adapter."""

    event: ClusterDomainEvent


type _Msg = (
    Replicator.Command
    | Read
    | Write
    | ReadRepair
    | Status
    | Gossip
    | _GossipTick
    | _ClockTick
    | _RemovedNodePruningTick
    | _SubscriberTerminated
    | _ClusterEvent
)


def _age(m: Member) -> tuple[int, Address]:
    """Pekko's Member.ageOrdering, oldest first, as a sort key: Member.is_older_than."""
    return (m.up_number, m.address)


def _is_wildcard(key_id: str) -> bool:
    return key_id.endswith("*")


def _drop_wildcard(key_id: str) -> str:
    return key_id[:-1]


def _add_binding[K](m: dict[K, set[ActorRef[Any]]], key: K, ref: ActorRef[Any]) -> None:
    m.setdefault(key, set()).add(ref)


def _remove_binding[K](m: dict[K, set[ActorRef[Any]]], key: K, ref: ActorRef[Any]) -> None:
    """Scala's ``MultiMap.removeBinding``: the key goes with its last binding."""
    refs = m.get(key)
    if refs is not None:
        refs.discard(ref)
        if not refs:
            del m[key]


def _replicator(settings: ReplicatorSettings) -> Behavior[_Msg]:
    @Behaviors.setup
    def setup(ctx: ActorContext[_Msg]) -> Behavior[_Msg]:
        # imported here: cluster.core imports this package for ReplicatorSettings
        from ..core import (
            Cluster,
            MemberEvent,
            MemberExited,
            MemberJoined,
            MemberRemoved,
            MemberUp,
            MemberWeaklyUp,
            ReachabilityEvent,
            ReachableMember,
            Subscribe,
            UnreachableMember,
            Unsubscribe,
        )
        from ..gossip import leader_status_key

        # imported here: the aggregators import Replicator from this module
        from .aggregators import read_aggregator, write_aggregator

        cluster = Cluster(ctx.system)
        self_address = cluster.self_address
        self_unique_address = cluster.self_unique_address
        self_from_system_uid = self_unique_address.uid
        self_path = ctx.self.path.removeprefix(self_address.root)  # Pekko's replica: self.path
        scheduler = ctx.system.scheduler
        remoting = ctx.system.remoting
        assert remoting is not None  # a cluster node has remoting
        max_delta_elements = settings.max_delta_elements

        def now() -> float:
            """Pekko's System.currentTimeMillis, in seconds. It is the scheduler's clock, so
            ManualTime drives expiry, plus the gap from the loop's clock to wall time, read every
            time: the loop's monotonic clock stops while a container is frozen, and a gap taken
            once would leave this clock behind wall time after a resume. On the real scheduler
            it is wall time, and under ManualTime only virtual time moves it."""
            return scheduler.current_time + (time.time() - asyncio.get_running_loop().time())

        payload_size_aggregator = _PayloadSizeAggregator(
            ctx.log,
            settings.log_data_size_exceeding or 2**63,  # None: Pekko's Int.MaxValue
            MAX_FRAME,
        )

        # cluster nodes, without self, joining and weakly up ones
        nodes: set[UniqueAddress] = set()
        # the same members, oldest first; only kept when prefer_oldest is on
        members_by_age: list[Member] = []
        # weakly up, joining and exiting nodes, without self
        weakly_up_nodes: set[UniqueAddress] = set()
        joining_nodes: set[UniqueAddress] = set()
        exiting_nodes: set[UniqueAddress] = set()
        unreachable: set[UniqueAddress] = set()

        removed_nodes: dict[UniqueAddress, float] = {}
        # all members sorted with the leader first; one entry per unique address
        leader: list[Member] = []

        def is_leader() -> bool:
            return bool(leader) and leader[0].address == self_address and leader[0].status == "up"

        # for pruning, timeouts are based on a clock that is only increased when all nodes are
        # reachable. It runs on the scheduler's monotonic clock, which ManualTime drives.
        previous_clock_time = scheduler.current_time
        all_reachable_clock_time = 0.0

        # the chunk of the keys the next Status covers, when there are more than max_delta_elements
        status_count = 0
        status_tot_chunks = 0

        # Pekko's preStart. No ClusterShuttingDown in ohra: the node's own MemberRemoved stops
        # the Replicator. The subscription replays the current state, as InitialStateAsEvents.
        cluster_events = ctx.message_adapter(_ClusterEvent)
        cluster.subscriptions.tell(Subscribe(cluster_events, MemberEvent))
        cluster.subscriptions.tell(Subscribe(cluster_events, ReachabilityEvent))

        # Pekko's gossipTask, notifyTask, pruningTask and clockTask
        ctx.timers.start_timer_with_fixed_delay("gossip", _GossipTick(), settings.gossip_interval)
        ctx.timers.start_timer_with_fixed_delay(
            "notify", Replicator.FlushChanges(), settings.notify_subscribers_interval
        )
        if settings.pruning_interval > 0:  # Pekko's pruning-interval = off
            ctx.timers.start_timer_with_fixed_delay(
                "pruning", _RemovedNodePruningTick(), settings.pruning_interval
            )
        ctx.timers.start_timer_with_fixed_delay("clock", _ClockTick(), settings.gossip_interval)

        expiry_keys = settings.expire_keys_after_inactivity
        expiry_wildcards = {
            _drop_wildcard(k): v for k, v in expiry_keys.items() if _is_wildcard(k)
        }
        expiry_enabled = bool(expiry_keys)
        # updated on the gossip tick, as Pekko's, which avoids reading the clock on every access
        current_used_timestamp: _Timestamp | None = now() if expiry_enabled else None

        data_entries: dict[str, tuple[DataEnvelope, _Digest, _Timestamp | None]] = {}
        # keys that have changed: Changed, Deleted and Expired go to subscribers on FlushChanges
        changed: set[str] = set()

        subscribers: dict[str, set[ActorRef[Any]]] = {}
        new_subscribers: dict[str, set[ActorRef[Any]]] = {}
        subscription_keys: dict[str, Key[Any]] = {}
        wildcard_subscribers: dict[str, set[ActorRef[Any]]] = {}

        def nodes_for_read_write(exclude_exiting: bool) -> tuple[UniqueAddress, ...]:
            if exclude_exiting and exiting_nodes:
                if settings.prefer_oldest:
                    return tuple(
                        m.unique_address
                        for m in members_by_age
                        if m.unique_address not in exiting_nodes
                    )
                return tuple(sorted(nodes - exiting_nodes))
            if settings.prefer_oldest:
                return tuple(m.unique_address for m in members_by_age)
            return tuple(sorted(nodes))

        def is_known_node(node: UniqueAddress) -> bool:
            return (
                node in nodes
                or node in weakly_up_nodes
                or node in joining_nodes
                or node == self_unique_address
            )

        def all_nodes() -> set[UniqueAddress]:
            """Up and weakly up nodes, unreachable ones included; without joining ones and self."""
            return nodes | weakly_up_nodes

        def replica(node: UniqueAddress) -> ActorRef[Any]:
            """The Replicator on ``node``: this one's path at that node's address."""
            return ctx.system.resolve(node.root + self_path)

        def receive_get(
            key: Key[Any],
            consistency: Replicator.ReadConsistency,
            reply_to: ActorRef[Replicator.GetResponse[Any]],
        ) -> None:
            local_value = get_data(key.id)
            ctx.log.debug("Received Get for key [%s].", key)
            update_used_timestamp(key.id, current_used_timestamp)
            if is_local_get(consistency):
                reply_to.tell(local_reply(key, local_value))
            else:
                exclude_exiting = isinstance(
                    consistency, Replicator.ReadMajorityPlus | Replicator.ReadAll
                )
                ctx.spawn_anonymous(
                    read_aggregator(
                        key,
                        consistency,
                        reply_to,
                        nodes_for_read_write(exclude_exiting),
                        frozenset(unreachable),
                        local_value,
                        replica,
                        ctx.self,
                        self_unique_address,
                        shuffle=not settings.prefer_oldest,
                        now=now,
                    )
                )

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
                case Replicator.ReadLocal() | Replicator.ReadFrom(1):  # n == 1: appendix B
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
                        envelope = envelope.merge(DataEnvelope(modify(existing)), now())
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
            update_used_timestamp(key.id, current_used_timestamp)
            if is_local_update(write_consistency):
                reply_to.tell(Replicator.UpdateSuccess(key))
            else:
                start_write_aggregator(key, envelope, write_consistency, reply_to)

        def is_local_update(write_consistency: Replicator.WriteConsistency) -> bool:
            match write_consistency:
                case Replicator.WriteLocal() | Replicator.WriteTo(1):  # n == 1: appendix B
                    return True
                case Replicator.WriteMajority() | Replicator.WriteAll():
                    return not nodes
                case _:
                    return False

        def start_write_aggregator(
            key: Key[Any],
            envelope: DataEnvelope,
            consistency: Replicator.WriteConsistency,
            reply_to: ActorRef[Any],
        ) -> None:
            """The WriteAggregator that receiveUpdate and receiveDelete start, without deltas."""
            exclude_exiting = isinstance(
                consistency, Replicator.WriteMajorityPlus | Replicator.WriteAll
            )
            ctx.spawn_anonymous(
                write_aggregator(
                    key,
                    envelope,
                    consistency,
                    reply_to,
                    nodes_for_read_write(exclude_exiting),
                    frozenset(unreachable),
                    replica,
                    self_unique_address,
                    shuffle=not settings.prefer_oldest,
                )
            )

        def receive_read(key: str, reply_to: ActorRef[ReadResult]) -> None:
            send_to_replica(ctx.log, reply_to, ReadResult(get_data(key)), [key])

        def receive_write(
            key: str, envelope: DataEnvelope, reply_to: ActorRef[WriteAck | WriteNack]
        ) -> None:
            write_and_store(key, envelope, reply_to)

        def write_and_store(
            key: str, write_envelope: DataEnvelope, reply_to: ActorRef[WriteAck | WriteNack] | None
        ) -> None:
            """Without durable storage, the store is left out: a reply, if any, goes at once."""
            new_envelope = write(key, write_envelope)
            if reply_to is not None:
                reply_to.tell(WriteAck() if new_envelope is not None else WriteNack())

        def write(key: str, write_envelope: DataEnvelope) -> DataEnvelope | None:
            match get_data(key):
                case DataEnvelope() as envelope if envelope is write_envelope:
                    return envelope
                case DataEnvelope() as envelope if envelope.is_deleted:
                    return DELETED_ENVELOPE  # already deleted
                case DataEnvelope() as envelope:
                    try:
                        merged = envelope.merge(write_envelope, now()).add_seen(self_address)
                    except TypeError as e:  # Pekko's IllegalArgumentException
                        ctx.log.warning("Couldn't merge [%s], due to: %s", key, e)
                        return None
                    return set_data(key, merged)
                case None:
                    # no existing data for the key
                    return set_data(key, write_envelope.add_seen(self_address))

        def receive_read_repair(
            key: str, write_envelope: DataEnvelope, reply_to: ActorRef[ReadRepairAck]
        ) -> None:
            write_and_store(key, write_envelope, None)
            reply_to.tell(ReadRepairAck())

        def receive_get_key_ids(reply_to: ActorRef[Replicator.GetKeyIdsResult]) -> None:
            keys = frozenset(
                k for k, (envelope, _, _) in data_entries.items() if not envelope.is_deleted
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
                    update_used_timestamp(key.id, current_used_timestamp)
                    payload_size_aggregator.remove(key.id)
                    if is_local_update(consistency):
                        reply_to.tell(Replicator.DeleteSuccess(key))
                    else:
                        start_write_aggregator(key, DELETED_ENVELOPE, consistency, reply_to)

        def set_data(key: str, envelope: DataEnvelope) -> DataEnvelope:
            if has_subscriber(key) and key not in changed:
                old_digest = get_digest(key)
                dig, payload_size = digest(envelope)
                payload_size_aggregator.update_payload_size(key, payload_size)
                if dig != old_digest:
                    changed.add(key)  # notify subscribers, later
            elif envelope.is_deleted:
                dig = _DELETED_DIGEST
            else:
                dig = _LAZY_DIGEST
            data_entries[key] = (envelope, dig, get_used_timestamp(key))
            return envelope

        def get_digest(key: str) -> _Digest:
            match data_entries.get(key):
                case (envelope, digest_, used_timestamp) if digest_ == _LAZY_DIGEST:
                    d, size = digest(envelope)
                    payload_size_aggregator.update_payload_size(key, size)
                    data_entries[key] = (envelope, d, used_timestamp)
                    return d
                case (_, digest_, _):
                    return digest_
                case None:
                    return _NOT_FOUND_DIGEST

        def digest(envelope: DataEnvelope) -> tuple[_Digest, int]:
            return _digest(envelope, remoting.ref_path)

        def get_data(key: str) -> DataEnvelope | None:
            entry = data_entries.get(key)
            return None if entry is None else entry[0]

        def get_expiry_duration(key: str) -> float:
            """Pekko's getExpiryDuration: 0 means the key does not expire."""
            if not expiry_enabled:
                return 0.0
            if (d := expiry_keys.get(key)) is not None:
                return d
            return next((v for k, v in expiry_wildcards.items() if key.startswith(k)), 0.0)

        def get_used_timestamp(key: str) -> _Timestamp | None:
            entry = data_entries.get(key) if expiry_enabled else None
            return None if entry is None else entry[2]

        def is_expired(key: str, timestamp: _Timestamp | None, now: float) -> bool:
            if expiry_enabled and timestamp is not None:
                expiry_duration = get_expiry_duration(key)
                return expiry_duration != 0.0 and timestamp <= now - expiry_duration
            return False

        def update_used_timestamp(key: str, timestamp: _Timestamp | None) -> None:
            if expiry_enabled and timestamp is not None:
                entry = data_entries.get(key)
                if entry is not None:
                    existing_envelope, existing_digest, existing_timestamp = entry
                    if existing_timestamp is None or timestamp > existing_timestamp:
                        data_entries[key] = (existing_envelope, existing_digest, timestamp)

        def receive_flush_changes() -> None:
            def notify(
                key_id: str, subs: Iterable[ActorRef[Any]], send_expired_if_missing: bool
            ) -> None:
                key = subscription_keys.get(key_id)
                if key is None:
                    key = next(
                        (
                            r.with_id(key_id)
                            for k, r in subscription_keys.items()
                            if _is_wildcard(k) and key_id.startswith(_drop_wildcard(k))
                        ),
                        None,
                    )
                if key is None:
                    raise RuntimeError(
                        f"Subscription notification of [{key_id}], but no matching subscription "
                        f"key in [{', '.join(subscription_keys)}]"
                    )
                msg: Replicator.SubscribeResponse[Any]
                match get_data(key_id):
                    case DataEnvelope() as envelope:
                        msg = (
                            Replicator.Deleted(key)
                            if envelope.is_deleted
                            else Replicator.Changed(key, envelope.data)
                        )
                    case None if send_expired_if_missing:
                        msg = Replicator.Expired(key)
                    case None:
                        return
                for s in subs:
                    send_to_replica(ctx.log, s, msg, [key_id])

            nonlocal changed
            if subscribers or wildcard_subscribers:
                for key in changed:
                    if has_subscriber(key):
                        notify(key, get_subscribers(key), send_expired_if_missing=True)

            # Changed goes to new subscribers even when the key has not changed, i.e. the current
            # value. Expired is not sent to new subscribers as the first event.
            for key, subs in new_subscribers.items():
                if _is_wildcard(key):
                    prefix = _drop_wildcard(key)
                    for matching_key in [k for k in data_entries if k.startswith(prefix)]:
                        notify(matching_key, subs, send_expired_if_missing=False)
                    for s in subs:
                        _add_binding(wildcard_subscribers, prefix, s)
                else:
                    notify(key, subs, send_expired_if_missing=False)
                    for s in subs:
                        _add_binding(subscribers, key, s)
            new_subscribers.clear()

            changed = set()

        def receive_gossip_tick() -> None:
            cleanup_expired()
            # Pekko's fullStateGossipEnabled is always on: there are no deltas
            if (address := select_random_node(sorted(all_nodes()))) is not None:
                gossip_to(address)

        def gossip_to(address: UniqueAddress) -> None:
            nonlocal status_count, status_tot_chunks
            to = replica(address)
            to_system_uid = address.uid
            if len(data_entries) <= max_delta_elements:
                status = Status(
                    {
                        k: (get_digest(k), ts or 0.0)
                        for k, (_, _, ts) in list(data_entries.items())
                    },
                    0,
                    1,
                    to_system_uid,
                    self_from_system_uid,
                    ctx.self,
                )
                send_to_replica(ctx.log, to, status, status.digests)
            else:
                tot_chunks = len(data_entries) // max_delta_elements
                for _ in range(min(tot_chunks, 10)):
                    if tot_chunks == status_tot_chunks:
                        status_count += 1
                    else:
                        status_count = random.randrange(tot_chunks)
                        status_tot_chunks = tot_chunks
                    chunk = status_count % tot_chunks
                    status = Status(
                        {
                            k: (get_digest(k), ts or 0.0)
                            for k, (_, _, ts) in list(data_entries.items())
                            if _chunk_of(k, tot_chunks) == chunk
                        },
                        chunk,
                        tot_chunks,
                        to_system_uid,
                        self_from_system_uid,
                        ctx.self,
                    )
                    send_to_replica(ctx.log, to, status, status.digests)

        def select_random_node(addresses: list[UniqueAddress]) -> UniqueAddress | None:
            return random.choice(addresses) if addresses else None

        def receive_status(
            other_digests: dict[str, tuple[_Digest, float]],
            chunk: int,
            tot_chunks: int,
            from_system_uid: int,
            reply_to: ActorRef[Status | Gossip],
        ) -> None:
            ctx.log.debug(
                "Received gossip status from [%s], chunk [%s] of [%s] containing [%s].",
                reply_to.path,
                chunk + 1,
                tot_chunks,
                ", ".join(other_digests),
            )
            # update the used timestamp when needed
            if expiry_enabled:
                for key, (_, used_timestamp) in other_digests.items():
                    # if we don't have the key it will be updated with the full Gossip
                    update_used_timestamp(key, used_timestamp or None)

            def is_other_different(key: str, other_digest: _Digest) -> bool:
                d = get_digest(key)
                return d != _NOT_FOUND_DIGEST and d != other_digest

            other_different_keys = [
                key for key, (d, _) in other_digests.items() if is_other_different(key, d)
            ]
            other_keys = other_digests.keys()
            my_keys = (
                set(data_entries)
                if tot_chunks == 1
                else {k for k in data_entries if _chunk_of(k, tot_chunks) == chunk}
            )
            if expiry_enabled:
                now_ = now()
                other_missing_keys = [
                    k
                    for k in my_keys - other_keys
                    if not is_expired(k, get_used_timestamp(k), now_)
                ]
            else:
                other_missing_keys = list(my_keys - other_keys)
            keys = [*other_different_keys, *other_missing_keys][:max_delta_elements]
            if keys:
                ctx.log.debug(
                    "Sending gossip to [%s], containing [%s]", reply_to.path, ", ".join(keys)
                )
                send_back = bool(other_different_keys)
                for g in create_gossip_messages(keys, send_back, from_system_uid):
                    send_to_replica(ctx.log, reply_to, g, g.updated_data)
            my_missing_keys = other_keys - my_keys
            if my_missing_keys:
                ctx.log.debug(
                    "Sending gossip status to [%s], requesting missing [%s]",
                    reply_to.path,
                    ", ".join(my_missing_keys),
                )
                status = Status(
                    {k: (_NOT_FOUND_DIGEST, 0.0) for k in my_missing_keys},
                    chunk,
                    tot_chunks,
                    from_system_uid,
                    self_from_system_uid,
                    ctx.self,
                )
                send_to_replica(ctx.log, reply_to, status, status.digests)

        def create_gossip_messages(
            keys: Collection[str], send_back: bool, from_system_uid: int
        ) -> list[Gossip]:
            # The sizes don't have to be exact, rather error on too small messages.
            max_message_size = payload_size_aggregator.max_frame_size - 128

            messages: list[Gossip] = []
            collected_entries: dict[str, tuple[DataEnvelope, float]] = {}
            total = 0

            def add_gossip() -> None:
                if collected_entries:
                    messages.append(
                        Gossip(
                            dict(collected_entries),
                            send_back,
                            from_system_uid,
                            self_from_system_uid,
                            ctx.self,
                        )
                    )

            for key in keys:
                key_size = len(key) + 4
                data_size = payload_size_aggregator.get_max_size(key)
                if data_size == 0:
                    get_digest(key)  # trigger payload_size_aggregator update (LazyDigest)
                    data_size = payload_size_aggregator.get_max_size(key)
                data_envelope = get_data(key)
                assert data_envelope is not None  # keys are ones this node holds
                # Pekko adds the envelope's estimatedSizeWithoutData, its pruning markers; here
                # the data size is that of the whole encoded envelope, markers included
                envelope_size = 100
                entry_size = key_size + data_size + envelope_size
                entry = (data_envelope, get_used_timestamp(key) or 0.0)
                if total + entry_size <= max_message_size:
                    collected_entries[key] = entry
                    total += entry_size
                else:
                    add_gossip()
                    collected_entries.clear()
                    collected_entries[key] = entry
                    total = entry_size

            add_gossip()  # add remaining, if any
            ctx.log.debug(
                "Created [%s] Gossip messages from [%s] data entries.", len(messages), len(keys)
            )
            return messages

        def receive_gossip(
            updated_data: dict[str, tuple[DataEnvelope, float]],
            send_back: bool,
            from_system_uid: int,
            reply_to: ActorRef[Status | Gossip],
        ) -> None:
            ctx.log.debug(
                "Received gossip from [%s], containing [%s].",
                reply_to.path,
                ", ".join(updated_data),
            )
            reply_keys: set[str] = set()
            now_ = now() if expiry_enabled else 0.0
            for key, (envelope, used_timestamp) in updated_data.items():
                if not is_expired(key, used_timestamp or None, now_):
                    had_data = key in data_entries
                    write_and_store(key, envelope, None)
                    update_used_timestamp(key, used_timestamp or None)
                    if send_back and (d := get_data(key)) is not None and (had_data or d.pruning):
                        reply_keys.add(key)
            if send_back and reply_keys:
                for g in create_gossip_messages(reply_keys, False, from_system_uid):
                    send_to_replica(ctx.log, reply_to, g, g.updated_data)

        def receive_subscribe(key: Key[Any], subscriber: ActorRef[Any]) -> None:
            _add_binding(new_subscribers, key.id, subscriber)
            if key.id not in subscription_keys:
                subscription_keys[key.id] = key
            # a local actor, or a message adapter of one, whose actor is watched
            if (actor := watched_ref(subscriber)) is not None:
                ctx.watch_with(actor, _SubscriberTerminated(actor))  # once per actor

        def cleanup_expired() -> None:
            nonlocal current_used_timestamp
            if expiry_enabled:
                now_ = now()
                # no need to be more accurate than the gossip tick interval
                current_used_timestamp = now_
                expired_keys = [
                    key
                    # None when set by a Write the first time: such a key does not expire at once
                    for key, (_, _, used_timestamp) in data_entries.items()
                    if used_timestamp is not None
                    and (duration := get_expiry_duration(key)) != 0.0
                    and used_timestamp <= now_ - duration
                ]
                if expired_keys:
                    ctx.log.debug("Removing expired keys [%s]", ", ".join(expired_keys))
                    for key in expired_keys:
                        payload_size_aggregator.remove(key)
                        changed.add(key)  # notify subscribers, later
                        del data_entries[key]

        def receive_unsubscribe(key: Key[Any], subscriber: ActorRef[Any]) -> None:
            if _is_wildcard(key.id):
                _remove_binding(wildcard_subscribers, _drop_wildcard(key.id), subscriber)
            else:
                _remove_binding(subscribers, key.id, subscriber)
            _remove_binding(new_subscribers, key.id, subscriber)
            actor = watched_ref(subscriber)
            if actor is not None and not has_subscriber_actor(actor):
                ctx.unwatch(actor)
            if not has_subscriber(key.id) and key.id not in new_subscribers:
                subscription_keys.pop(key.id, None)

        def has_subscriber_actor(actor: ActorRef[Any]) -> bool:
            """Pekko's hasSubscriber(subscriber), for the actor a watch of each subscriber
            watches."""
            return any(
                watched_ref(s) == actor
                for m in (subscribers, wildcard_subscribers, new_subscribers)
                for subs in m.values()
                for s in subs
            )

        def has_subscriber(key_id: str) -> bool:
            return key_id in subscribers or any(key_id.startswith(k) for k in wildcard_subscribers)

        def get_subscribers(key_id: str) -> list[ActorRef[Any]]:
            """Pekko's getSubscribersIterator: the key's subscribers, then the first matching
            wildcard's."""
            none: set[ActorRef[Any]] = set()
            wildcard = next(
                (v for k, v in wildcard_subscribers.items() if key_id.startswith(k)), none
            )
            return [*subscribers.get(key_id, none), *wildcard]

        def receive_terminated(ref: ActorRef[Any]) -> None:
            def remove(m: dict[str, set[ActorRef[Any]]]) -> list[str]:
                keys = [k for k, subs in m.items() if any(watched_ref(s) == ref for s in subs)]
                for k in keys:
                    for s in [s for s in m[k] if watched_ref(s) == ref]:
                        _remove_binding(m, k, s)
                return keys

            keys1 = remove(subscribers)
            remove(wildcard_subscribers)
            keys3 = remove(new_subscribers)
            for key in keys1 + keys3:
                if not has_subscriber(key) and key not in new_subscribers:
                    subscription_keys.pop(key, None)

        def receive_member_joining(m: Member) -> None:
            if m.address != self_address:
                joining_nodes.add(m.unique_address)

        def receive_member_weakly_up(m: Member) -> None:
            if m.address != self_address:
                weakly_up_nodes.add(m.unique_address)
                joining_nodes.discard(m.unique_address)

        def receive_member_up(m: Member) -> None:
            if m not in leader:  # Pekko's TreeSet: the member is already there
                insort(leader, m, key=leader_status_key)
            if m.address != self_address:
                nodes.add(m.unique_address)
                weakly_up_nodes.discard(m.unique_address)
                joining_nodes.discard(m.unique_address)
                if settings.prefer_oldest and m not in members_by_age:
                    insort(members_by_age, m, key=_age)

        def receive_member_exiting(m: Member) -> None:
            if m.address != self_address:
                exiting_nodes.add(m.unique_address)

        def receive_member_removed(m: Member) -> Behavior[_Msg]:
            if m.address == self_address:
                return Behaviors.stopped()
            ctx.log.debug("adding removed node [%s] from MemberRemoved", m.unique_address)
            # remove by unique address: the ordering may have changed, as it is based on status
            if m in leader:  # members are equal by unique address
                leader.remove(m)
            nodes.discard(m.unique_address)
            weakly_up_nodes.discard(m.unique_address)
            joining_nodes.discard(m.unique_address)
            exiting_nodes.discard(m.unique_address)
            removed_nodes[m.unique_address] = all_reachable_clock_time
            unreachable.discard(m.unique_address)
            if settings.prefer_oldest and m in members_by_age:
                members_by_age.remove(m)
            # Pekko's deltaPropagationSelector.cleanupRemovedNode comes with deltas
            return Behaviors.same

        def receive_other_member_event(m: Member) -> None:
            # replace: the ordering may have changed, as it is based on status
            if m in leader:  # members are equal by unique address
                leader.remove(m)
            insort(leader, m, key=leader_status_key)

        def receive_unreachable(m: Member) -> None:
            unreachable.add(m.unique_address)

        def receive_reachable(m: Member) -> None:
            unreachable.discard(m.unique_address)

        def receive_clock_tick() -> None:
            nonlocal previous_clock_time, all_reachable_clock_time
            now_ = scheduler.current_time
            if not unreachable:
                all_reachable_clock_time += now_ - previous_clock_time
            previous_clock_time = now_

        def receive_removed_node_pruning_tick() -> None:
            # see the "CRDT Garbage" section of Pekko's Replicator Scaladoc for the process
            if not unreachable:
                if is_leader():
                    collect_removed_nodes()
                    init_removed_node_pruning()
                perform_removed_node_pruning()
                delete_obsolete_pruning_performed()

        def collect_removed_nodes() -> None:
            known_nodes = all_nodes() | removed_nodes.keys()
            new_removed_nodes: set[UniqueAddress] = set()
            for envelope, _, _ in data_entries.values():
                if isinstance(envelope.data, RemovedNodePruning):
                    new_removed_nodes |= {
                        n
                        for n in envelope.data.modified_by_nodes
                        if n != self_unique_address and n not in known_nodes
                    }
            for n in new_removed_nodes:
                ctx.log.debug("Adding removed node [%s] from data", n)
                removed_nodes[n] = all_reachable_clock_time

        def init_removed_node_pruning() -> None:
            # initiate pruning for removed nodes
            removed_set = {
                r
                for r, t in removed_nodes.items()
                if all_reachable_clock_time - t > settings.max_pruning_dissemination
            }
            if removed_set:
                # a snapshot, as Pekko iterates over its immutable dataEntries
                for key, (envelope, _, _) in list(data_entries.items()):
                    for removed in removed_set:

                        def init() -> None:
                            new_envelope = envelope.init_removed_node_pruning(
                                removed, self_unique_address
                            )
                            ctx.log.debug(
                                "Initiated pruning of [%s] for data key [%s] to [%s]",
                                removed,
                                key,
                                self_unique_address,
                            )
                            set_data(key, new_envelope)

                        # false for data that is not RemovedNodePruning, deleted data included
                        if envelope.need_pruning_from(removed):
                            match envelope.pruning.get(removed):
                                case None:
                                    init()
                                case PruningInitialized(owner) if owner != self_unique_address:
                                    init()
                                case _:
                                    pass  # already in progress

        def perform_removed_node_pruning() -> None:
            # perform pruning when all seen Init
            all_ = all_nodes()
            pruning_performed = PruningPerformed(now() + settings.pruning_marker_time_to_live)
            for key, (envelope, _, _) in list(data_entries.items()):
                if not isinstance(envelope.data, RemovedNodePruning):
                    continue  # deleted, or pruning not needed
                for removed, state in envelope.pruning.items():
                    match state:
                        case PruningInitialized(owner, seen) if owner == self_unique_address and (
                            not all_ or all(n.address in seen for n in all_)
                        ):
                            new_envelope = envelope.prune(removed, pruning_performed)
                            ctx.log.debug(
                                "Perform pruning of [%s] from [%s] to [%s]",
                                key,
                                removed,
                                self_unique_address,
                            )
                            set_data(key, new_envelope)
                        case _:
                            pass

        def delete_obsolete_pruning_performed() -> None:
            current_time = now()
            for key, (envelope, _, _) in list(data_entries.items()):
                if not isinstance(envelope.data, RemovedNodePruning):
                    continue  # deleted, or pruning not needed
                new_envelope = envelope
                for removed, p in envelope.pruning.items():
                    if isinstance(p, PruningPerformed) and p.is_obsolete(current_time):
                        ctx.log.debug(
                            "Removing obsolete pruning marker for [%s] in [%s]", removed, key
                        )
                        removed_nodes.pop(removed, None)
                        pruning = dict(new_envelope.pruning)
                        del pruning[removed]
                        new_envelope = replace(new_envelope, pruning=pruning)
                if new_envelope is not envelope:
                    set_data(key, new_envelope)

        def receive_get_replica_count(reply_to: ActorRef[Replicator.ReplicaCount]) -> None:
            reply_to.tell(Replicator.ReplicaCount(len(nodes) + 1))  # nodes excludes self

        @Behaviors.receive
        def normal_receive(ctx: ActorContext[_Msg], msg: _Msg) -> Behavior[_Msg]:
            match msg:
                case Read(_, from_node) | Write(_, _, from_node) if (
                    from_node is not None and not is_known_node(from_node)
                ):
                    # When restarting a node with same host:port it is possible that a Replicator
                    # on another node is sending messages to the restarted node even if it hasn't
                    # joined the same cluster. Therefore we check that the message was from a
                    # known cluster member.
                    ctx.log.info(
                        "Ignoring message [%s] from unknown node [%s]",
                        type(msg).__name__,
                        from_node,
                    )
                case Status(to_system_uid=uid) | Gossip(to_system_uid=uid) if (
                    uid != self_from_system_uid
                ):
                    # When restarting a node with same host:port it is possible that a Replicator
                    # on another node is sending messages to the restarted node even if it hasn't
                    # joined the same cluster. Therefore we check that the message was intended
                    # for this incarnation and otherwise it is discarded.
                    ctx.log.info(
                        "Ignoring message [%s] from [%s] intended for system uid [%s], self uid "
                        "is [%s]",
                        type(msg).__name__,
                        msg.reply_to.path,
                        uid,
                        self_from_system_uid,
                    )
                case Status(digests, chunk, tot_chunks, _, from_system_uid, reply_to):
                    receive_status(digests, chunk, tot_chunks, from_system_uid, reply_to)
                case Gossip(updated_data, send_back, _, from_system_uid, reply_to):
                    receive_gossip(updated_data, send_back, from_system_uid, reply_to)
                case Read(key, _, reply_to):
                    receive_read(key, reply_to)
                case Write(key, envelope, _, reply_to):
                    receive_write(key, envelope, reply_to)
                case Replicator.Get(key, consistency, reply_to):
                    receive_get(key, consistency, reply_to)
                case Replicator.Update(key, initial, write_consistency, reply_to, modify):
                    receive_update(key, initial, modify, write_consistency, reply_to)
                case ReadRepair(key, envelope, reply_to):
                    receive_read_repair(key, envelope, reply_to)
                case Replicator.GetKeyIds(reply_to):
                    receive_get_key_ids(reply_to)
                case Replicator.Delete(key, consistency, reply_to):
                    receive_delete(key, consistency, reply_to)
                case Replicator.GetReplicaCount(reply_to):
                    receive_get_replica_count(reply_to)
                case Replicator.FlushChanges():
                    receive_flush_changes()
                case _GossipTick():
                    receive_gossip_tick()
                case _ClockTick():
                    receive_clock_tick()
                case _RemovedNodePruningTick():
                    receive_removed_node_pruning_tick()
                case Replicator.Subscribe(key, subscriber):
                    receive_subscribe(key, subscriber)
                case Replicator.Unsubscribe(key, subscriber):
                    receive_unsubscribe(key, subscriber)
                case _SubscriberTerminated(ref):
                    receive_terminated(ref)
                case _ClusterEvent(MemberJoined(member=m)):
                    receive_member_joining(m)
                case _ClusterEvent(MemberWeaklyUp(member=m)):
                    receive_member_weakly_up(m)
                case _ClusterEvent(MemberUp(member=m)):
                    receive_member_up(m)
                case _ClusterEvent(MemberExited(member=m)):
                    receive_member_exiting(m)
                case _ClusterEvent(MemberRemoved(member=m)):
                    return receive_member_removed(m)
                case _ClusterEvent(MemberEvent(member=m)):
                    receive_other_member_event(m)
                case _ClusterEvent(UnreachableMember(member=m)):
                    receive_unreachable(m)
                case _ClusterEvent(ReachableMember(member=m)):
                    receive_reachable(m)
                case _ClusterEvent():
                    pass  # only member and reachability events are subscribed
            return Behaviors.same

        def on_signal(_ctx: ActorContext[_Msg], sig: Signal) -> Behavior[_Msg]:
            # Pekko's postStop, which its preRestart also runs
            if sig is PostStop or sig is PreRestart:
                cluster.subscriptions.tell(Unsubscribe(cluster_events))
            return Behaviors.same

        return normal_receive.receive_signal(on_signal)

    return setup
