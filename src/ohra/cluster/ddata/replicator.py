"""Pekko's Replicator: its typed protocol, the ``ddata.typed.scaladsl.Replicator`` commands and
responses nested in ``Replicator`` as ``EventStream``'s are, and the actor, whose handlers port
classic ``Replicator.scala`` function for function.

The Replicator tracks the cluster's members, reads and writes at a level other than local
through aggregator children, gossips digests to a random replica for anti-entropy, and prunes
the data of removed nodes, as the "CRDT Garbage" section of Pekko's Replicator Scaladoc
describes. Durable keys are loaded from the durable store before the Replicator serves.
Deltas are propagated on a tick and applied in sequence, as the "Delta-CRDT" section of the
Scaladoc describes.
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
from .delta_propagation_selector import DeltaPropagationSelector
from .envelope import (
    DELETED_ENVELOPE,
    NO_DELTA_PLACEHOLDER,
    DataEnvelope,
    Delta,
    DeltaNack,
    DeltaPropagation,
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
from .replicated_data import (
    DeltaReplicatedData,
    RemovedNodePruning,
    ReplicatedData,
    ReplicatedDelta,
    RequiresCausalDeliveryOfDeltas,
)
from .version_vector import VersionVector
from .durable_store import DurableDataEnvelope, DurableStore
from .settings import ReplicatorSettings, drop_wildcard, is_wildcard

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
        """The durable store could not store the entry, or, for a write to several nodes, the
        store's failure and the other nodes' nacks left too few acks."""

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
        Replicator.DeleteSuccess[A]
        | Replicator.DeleteFailure[A]
        | Replicator.DataDeleted[A]
        | Replicator.StoreFailure[A]
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

    @dataclass(frozen=True)
    class TestFullStateGossip:
        """Internal, for tests: Pekko's ``Replicator.Internal.TestFullStateGossip``. The delta
        specs turn full-state gossip off with it."""

        enabled: bool

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
    body = canonical_json(
        Serialization.encode(envelope.without_delta_versions(), ref_path, canonical=True)
    ).encode()
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
class _DeltaPropagationTick:
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


@dataclass(frozen=True)
class _DurableStoreTerminated:
    """The durable store stopped: Pekko's Terminated for it."""


type _Msg = (
    Replicator.Command
    | Read
    | Write
    | ReadRepair
    | Status
    | Gossip
    | _GossipTick
    | _DeltaPropagationTick
    | DeltaPropagation
    | Replicator.TestFullStateGossip
    | _ClockTick
    | _RemovedNodePruningTick
    | _SubscriberTerminated
    | _ClusterEvent
    | _DurableStoreTerminated
    | DurableStore.LoadData
    | DurableStore.LoadAllCompleted
)


def _age(m: Member) -> tuple[int, Address]:
    """Pekko's Member.ageOrdering, oldest first, as a sort key: Member.is_older_than."""
    return (m.up_number, m.address)


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

        # Pekko's durableStore, watched; its LoadAll is sent in preStart below
        durable_store_ref: ActorRef[DurableStore.Command] | None = None
        if settings.durable_keys:
            durable_store_ref = ctx.spawn(settings.durable_store(), "durableStore")
            ctx.watch_with(durable_store_ref, _DurableStoreTerminated())

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
        delta_crdt_enabled = settings.delta_crdt_enabled
        if delta_crdt_enabled:
            # Derive the deltaPropagationInterval from the gossipInterval.
            # Normally the delta is propagated to all nodes within the gossip tick, so that
            # full state gossip is not needed.
            delta_propagation_interval = max(settings.gossip_interval / 5, 0.2)
            ctx.timers.start_timer_with_fixed_delay(
                "deltaPropagation", _DeltaPropagationTick(), delta_propagation_interval
            )
        full_state_gossip_enabled = True

        expiry_keys = settings.expire_keys_after_inactivity
        expiry_wildcards = {drop_wildcard(k): v for k, v in expiry_keys.items() if is_wildcard(k)}
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

        def create_delta_propagation(
            deltas: dict[str, tuple[ReplicatedData, int, int]],
        ) -> DeltaPropagation:
            # Important to include the pruning state in the deltas. For example if the delta is
            # based on an entry that has been pruned but that has not yet been performed on the
            # target node.
            result: dict[str, Delta] = {}
            for key, (d, from_seq_nr, to_seq_nr) in deltas.items():
                if d is not NO_DELTA_PLACEHOLDER:
                    envelope = get_data(key)
                    result[key] = Delta(
                        DataEnvelope(d) if envelope is None else replace(envelope, data=d),
                        from_seq_nr,
                        to_seq_nr,
                    )
            return DeltaPropagation(self_unique_address, False, result, None)

        delta_propagation_selector = DeltaPropagationSelector(
            gossip_interval_divisor=5,
            # Replicator.allNodes is sorted
            all_nodes=lambda: sorted(all_nodes() - unreachable),
            max_delta_size=settings.max_delta_size,
            create_delta_propagation=create_delta_propagation,
        )

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

            def as_delta_data(d: Any) -> DeltaReplicatedData[Any] | None:
                if delta_crdt_enabled and isinstance(d, DeltaReplicatedData):
                    return cast("DeltaReplicatedData[Any]", d)
                return None

            def delta_or_placeholder(d: DeltaReplicatedData[Any]) -> ReplicatedData:
                return NO_DELTA_PLACEHOLDER if d.delta is None else d.delta

            # the delta of this update, None when the data is not a DeltaReplicatedData
            delta: ReplicatedData | None = None
            try:
                match local_value:
                    case DataEnvelope() as envelope if envelope.is_deleted:
                        pass
                    case DataEnvelope(existing) as envelope:
                        modified = modify(existing)
                        if (delta_data := as_delta_data(modified)) is not None:
                            envelope = envelope.merge(
                                DataEnvelope(delta_data.reset_delta()), now()
                            )
                            delta = delta_or_placeholder(delta_data)
                        else:
                            envelope = envelope.merge(DataEnvelope(modified), now())
                    case None:
                        modified = modify(initial)
                        if (delta_data := as_delta_data(modified)) is not None:
                            envelope = DataEnvelope(delta_data.reset_delta())
                            delta = delta_or_placeholder(delta_data)
                        else:
                            envelope = DataEnvelope(modified)
            except Exception as e:  # Pekko's Try: NonFatal
                ctx.log.debug("Received Update for key [%s], failed: %s", key, e)
                reply_to.tell(Replicator.ModifyFailure(key, f"Update failed: {e}", e))
                return
            if envelope.is_deleted:
                ctx.log.debug("Received Update for deleted key [%s].", key)
                reply_to.tell(Replicator.UpdateDataDeleted(key))
                return
            ctx.log.debug("Received Update for key [%s].", key)

            # handle the delta
            if delta is not None:
                delta_propagation_selector.update(key.id, delta)

            # note that it's important to do delta_propagation_selector.update before set_data,
            # so that the latest delta version is used
            new_envelope = set_data(key.id, envelope)

            update_used_timestamp(key.id, current_used_timestamp)
            durable = settings.is_durable(key.id)
            if is_local_update(write_consistency):
                if durable_store_ref is not None and durable:
                    durable_store_ref.tell(
                        DurableStore.Store(
                            key.id,
                            DurableDataEnvelope(new_envelope),
                            DurableStore.StoreReply(
                                Replicator.UpdateSuccess(key),
                                Replicator.StoreFailure(key),
                                reply_to,
                            ),
                        )
                    )
                else:
                    reply_to.tell(Replicator.UpdateSuccess(key))
            else:
                write_envelope = new_envelope
                write_delta: Delta | None = None
                if delta is NO_DELTA_PLACEHOLDER or delta is None:
                    pass
                elif isinstance(delta, RequiresCausalDeliveryOfDeltas):
                    v = delta_propagation_selector.current_version(key.id)
                    write_delta = Delta(replace(new_envelope, data=delta), v, v)
                else:
                    write_envelope = replace(new_envelope, data=delta)
                # When RequiresCausalDeliveryOfDeltas use deterministic order to so that sequence
                # numbers of subsequent updates are in sync on the destination nodes.
                # The order is also kept when prefer-oldest is enabled.
                shuffle = not (
                    settings.prefer_oldest
                    or (write_delta is not None and write_delta.requires_causal_delivery_of_deltas)
                )
                aggregator = start_write_aggregator(
                    key, write_envelope, write_consistency, reply_to, durable, shuffle, write_delta
                )
                if durable_store_ref is not None and durable:
                    durable_store_ref.tell(
                        DurableStore.Store(
                            key.id,
                            DurableDataEnvelope(new_envelope),
                            DurableStore.StoreReply(
                                Replicator.UpdateSuccess(key),
                                Replicator.StoreFailure(key),
                                aggregator,
                            ),
                        )
                    )

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
            durable: bool,
            shuffle: bool,
            delta: Delta | None = None,
        ) -> ActorRef[Any]:
            """The WriteAggregator that receiveUpdate and receiveDelete start."""
            exclude_exiting = isinstance(
                consistency, Replicator.WriteMajorityPlus | Replicator.WriteAll
            )
            aggregator = ctx.spawn_anonymous(
                write_aggregator(
                    key,
                    envelope,
                    consistency,
                    reply_to,
                    nodes_for_read_write(exclude_exiting),
                    frozenset(unreachable),
                    replica,
                    self_unique_address,
                    shuffle=shuffle,
                    durable=durable,
                    delta=delta,
                )
            )
            return cast("ActorRef[Any]", aggregator)  # the store's reply is not its Never

        def receive_read(key: str, reply_to: ActorRef[ReadResult]) -> None:
            send_to_replica(ctx.log, reply_to, ReadResult(get_data(key)), [key])

        def receive_write(
            key: str, envelope: DataEnvelope, reply_to: ActorRef[WriteAck | WriteNack]
        ) -> None:
            write_and_store(key, envelope, reply_to)

        def write_and_store(
            key: str, write_envelope: DataEnvelope, reply_to: ActorRef[WriteAck | WriteNack] | None
        ) -> None:
            new_envelope = write(key, write_envelope)
            if new_envelope is not None:
                if durable_store_ref is not None and settings.is_durable(key):
                    store_reply = (
                        None
                        if reply_to is None
                        else DurableStore.StoreReply(WriteAck(), WriteNack(), reply_to)
                    )
                    durable_store_ref.tell(
                        DurableStore.Store(key, DurableDataEnvelope(new_envelope), store_reply)
                    )
                elif reply_to is not None:
                    reply_to.tell(WriteAck())
            elif reply_to is not None:
                reply_to.tell(WriteNack())

        def write(key: str, write_envelope: DataEnvelope) -> DataEnvelope | None:
            match get_data(key):
                case DataEnvelope() as envelope if envelope is write_envelope:
                    return envelope
                case DataEnvelope() as envelope if envelope.is_deleted:
                    return DELETED_ENVELOPE  # already deleted
                case DataEnvelope() as envelope:
                    try:
                        # DataEnvelope will merge_delta when needed
                        merged = envelope.merge(write_envelope, now()).add_seen(self_address)
                    except TypeError as e:  # Pekko's IllegalArgumentException
                        ctx.log.warning("Couldn't merge [%s], due to: %s", key, e)
                        return None
                    return set_data(key, merged)
                case None:
                    # no existing data for the key
                    if isinstance(write_envelope.data, ReplicatedDelta):
                        z = write_envelope.data.zero()
                        write_envelope = replace(
                            write_envelope, data=z.merge_delta(write_envelope.data)
                        )
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
                    durable = settings.is_durable(key.id)
                    deleted = DurableDataEnvelope(DELETED_ENVELOPE)
                    success = Replicator.DeleteSuccess(key)
                    failure = Replicator.StoreFailure(key)
                    if is_local_update(consistency):
                        if durable_store_ref is not None and durable:
                            durable_store_ref.tell(
                                DurableStore.Store(
                                    key.id,
                                    deleted,
                                    DurableStore.StoreReply(success, failure, reply_to),
                                )
                            )
                        else:
                            reply_to.tell(success)
                    else:
                        aggregator = start_write_aggregator(
                            key,
                            DELETED_ENVELOPE,
                            consistency,
                            reply_to,
                            durable,
                            not settings.prefer_oldest,
                        )
                        if durable_store_ref is not None and durable:
                            durable_store_ref.tell(
                                DurableStore.Store(
                                    key.id,
                                    deleted,
                                    DurableStore.StoreReply(success, failure, aggregator),
                                )
                            )

        def set_data(key: str, envelope: DataEnvelope) -> DataEnvelope:
            new_envelope = envelope
            if delta_crdt_enabled:
                delta_versions = envelope.delta_versions
                curr_version = delta_propagation_selector.current_version(key)
                if curr_version != 0 and curr_version != delta_versions.version_at(
                    self_unique_address
                ):
                    new_envelope = replace(
                        envelope,
                        delta_versions=delta_versions.merge(
                            VersionVector.create({self_unique_address: curr_version})
                        ),
                    )
            if has_subscriber(key) and key not in changed:
                old_digest = get_digest(key)
                dig, payload_size = digest(new_envelope)
                payload_size_aggregator.update_payload_size(key, payload_size)
                if dig != old_digest:
                    changed.add(key)  # notify subscribers, later
            elif new_envelope.is_deleted:
                dig = _DELETED_DIGEST
            else:
                dig = _LAZY_DIGEST
            data_entries[key] = (new_envelope, dig, get_used_timestamp(key))
            if new_envelope.is_deleted:
                delta_propagation_selector.delete(key)
            return new_envelope

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
                            if is_wildcard(k) and key_id.startswith(drop_wildcard(k))
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
                if is_wildcard(key):
                    prefix = drop_wildcard(key)
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
            if (
                full_state_gossip_enabled
                and (address := select_random_node(sorted(all_nodes()))) is not None
            ):
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
                        delta_propagation_selector.delete(key)
                        payload_size_aggregator.remove(key)
                        changed.add(key)  # notify subscribers, later
                        del data_entries[key]
                    durable_expired_keys = frozenset(
                        k for k in expired_keys if settings.is_durable(k)
                    )
                    if durable_store_ref is not None and durable_expired_keys:
                        durable_store_ref.tell(DurableStore.Expire(durable_expired_keys))

        def receive_unsubscribe(key: Key[Any], subscriber: ActorRef[Any]) -> None:
            if is_wildcard(key.id):
                _remove_binding(wildcard_subscribers, drop_wildcard(key.id), subscriber)
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
            delta_propagation_selector.cleanup_removed_node(m.unique_address)
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
            durable_pruning_performed = PruningPerformed(
                now() + settings.durable_pruning_marker_time_to_live
            )
            for key, (envelope, _, _) in list(data_entries.items()):
                if not isinstance(envelope.data, RemovedNodePruning):
                    continue  # deleted, or pruning not needed
                for removed, state in envelope.pruning.items():
                    match state:
                        case PruningInitialized(owner, seen) if owner == self_unique_address and (
                            not all_ or all(n.address in seen for n in all_)
                        ):
                            durable = durable_store_ref is not None and settings.is_durable(key)
                            new_envelope = envelope.prune(
                                removed,
                                durable_pruning_performed if durable else pruning_performed,
                            )
                            ctx.log.debug(
                                "Perform pruning of [%s] from [%s] to [%s]",
                                key,
                                removed,
                                self_unique_address,
                            )
                            set_data(key, new_envelope)
                            if durable_store_ref is not None and (
                                new_envelope.data is not envelope.data and durable
                            ):
                                durable_store_ref.tell(
                                    DurableStore.Store(
                                        key, DurableDataEnvelope(new_envelope), None
                                    )
                                )
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

        def get_delta_seq_nr(key: str, from_node: UniqueAddress) -> int:
            entry = data_entries.get(key)
            return 0 if entry is None else entry[0].delta_versions.version_at(from_node)

        def is_node_removed(node: UniqueAddress, keys: Iterable[str]) -> bool:
            return node in removed_nodes or any(
                (entry := data_entries.get(key)) is not None and node in entry[0].pruning
                for key in keys
            )

        def receive_delta_propagation_tick() -> None:
            for (
                node,
                delta_propagation,
            ) in delta_propagation_selector.collect_propagations().items():
                # TODO split it to several DeltaPropagation if too many entries
                if delta_propagation.deltas:
                    send_to_replica(
                        ctx.log, replica(node), delta_propagation, delta_propagation.deltas
                    )
            if delta_propagation_selector.propagation_count % 5 == 0:
                delta_propagation_selector.cleanup_delta_entries()

        def receive_delta_propagation(
            from_node: UniqueAddress,
            reply: bool,
            deltas: dict[str, Delta],
            reply_to: ActorRef[WriteAck | WriteNack | DeltaNack] | None,
        ) -> None:
            if delta_crdt_enabled:
                try:
                    ctx.log.debug(
                        "Received DeltaPropagation from [%s], containing [%s].",
                        from_node.address,
                        ", ".join(
                            f"{key} {d.from_seq_nr}-{d.to_seq_nr}" for key, d in deltas.items()
                        ),
                    )
                    # a wire DeltaPropagation answers through its reply_to (the spec's D5)
                    ack_to = cast("ActorRef[WriteAck | WriteNack]", reply_to) if reply else None
                    if is_node_removed(from_node, deltas.keys()):
                        # Late message from a removed node.
                        # Drop it to avoid merging deltas that have been pruned on one side.
                        ctx.log.debug(
                            "Skipping DeltaPropagation from [%s] because that node has been "
                            "removed",
                            from_node.address,
                        )
                    else:
                        for key, delta in deltas.items():
                            envelope = delta.envelope
                            if isinstance(envelope.data, RequiresCausalDeliveryOfDeltas):
                                current_seq_nr = get_delta_seq_nr(key, from_node)
                                if current_seq_nr >= delta.to_seq_nr:
                                    ctx.log.debug(
                                        "Skipping DeltaPropagation from [%s] for [%s] because "
                                        "toSeqNr [%s] already handled [%s]",
                                        from_node.address,
                                        key,
                                        delta.to_seq_nr,
                                        current_seq_nr,
                                    )
                                    if ack_to is not None:
                                        ack_to.tell(WriteAck())
                                elif delta.from_seq_nr > current_seq_nr + 1:
                                    ctx.log.debug(
                                        "Skipping DeltaPropagation from [%s] for [%s] because "
                                        "missing deltas between [%s-%s]",
                                        from_node.address,
                                        key,
                                        current_seq_nr + 1,
                                        delta.from_seq_nr - 1,
                                    )
                                    if reply_to is not None and reply:
                                        reply_to.tell(DeltaNack())
                                else:
                                    ctx.log.debug(
                                        "Applying DeltaPropagation from [%s] for [%s] with "
                                        "sequence numbers [%s-%s], current was [%s]",
                                        from_node.address,
                                        key,
                                        delta.from_seq_nr,
                                        delta.to_seq_nr,
                                        current_seq_nr,
                                    )
                                    new_envelope = replace(
                                        envelope,
                                        delta_versions=VersionVector.create(
                                            {from_node: delta.to_seq_nr}
                                        ),
                                    )
                                    write_and_store(key, new_envelope, ack_to)
                            else:
                                # causal delivery of deltas not needed, just apply it
                                write_and_store(key, envelope, ack_to)
                except Exception as e:  # Pekko's NonFatal
                    # catching in case we need to support rolling upgrades that are
                    # mixing nodes with incompatible delta-CRDT types
                    ctx.log.warning(
                        "Couldn't process DeltaPropagation from [%s] due to %s", from_node, e
                    )
            elif reply and reply_to is not None:
                # !delta_crdt_enabled
                reply_to.tell(DeltaNack())

        def receive_test_full_state_gossip(enabled: bool) -> None:
            nonlocal full_state_gossip_enabled
            full_state_gossip_enabled = enabled

        def receive_get_replica_count(reply_to: ActorRef[Replicator.ReplicaCount]) -> None:
            reply_to.tell(Replicator.ReplicaCount(len(nodes) + 1))  # nodes excludes self

        def normal_receive(ctx: ActorContext[_Msg], msg: _Msg) -> Behavior[_Msg]:
            match msg:
                case Read(_, from_node) | Write(_, _, from_node) | DeltaPropagation(from_node) if (
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
                case _DeltaPropagationTick():
                    receive_delta_propagation_tick()
                case DeltaPropagation(from_node, reply, deltas, reply_to):
                    receive_delta_propagation(from_node, reply, deltas, reply_to)
                case Replicator.TestFullStateGossip(enabled):
                    receive_test_full_state_gossip(enabled)
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
                case _DurableStoreTerminated():
                    return receive_durable_store_terminated()
                case DurableStore.LoadData() | DurableStore.LoadAllCompleted():
                    pass  # only the load phase asks for them
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

        def receive_durable_store_terminated() -> Behavior[_Msg]:
            ctx.log.error("Stopping distributed-data Replicator because durable store terminated")
            return Behaviors.stopped()

        def on_signal(_ctx: ActorContext[_Msg], sig: Signal) -> Behavior[_Msg]:
            # Pekko's postStop, which its preRestart also runs
            if sig is PostStop or sig is PreRestart:
                cluster.subscriptions.tell(Unsubscribe(cluster_events))
            return Behaviors.same

        normal = Behaviors.receive(normal_receive).receive_signal(on_signal)
        if durable_store_ref is None:
            return normal

        # Pekko's load: commands wait in an internal buffer until the store has loaded
        start_time = time.monotonic()
        count = 0
        stash: list[_Msg] = []

        def load(ctx: ActorContext[_Msg], msg: _Msg) -> Behavior[_Msg]:
            nonlocal count
            match msg:
                case DurableStore.LoadData(data):
                    count += len(data)
                    for key, d in data.items():
                        new_envelope = write(key, d.data_envelope)
                        if new_envelope is not None and new_envelope is not d.data_envelope:
                            durable_store_ref.tell(
                                DurableStore.Store(key, DurableDataEnvelope(new_envelope), None)
                            )
                case DurableStore.LoadAllCompleted():
                    ctx.log.debug(
                        "Loading %s entries from durable store took %s ms, stashed %s",
                        count,
                        int((time.monotonic() - start_time) * 1000),
                        len(stash),
                    )
                    replay, stash[:] = stash[:], []
                    for stashed in replay:
                        result = normal_receive(ctx, stashed)
                        if result is not Behaviors.same:
                            return result  # a stop takes effect at once
                    ctx.self.tell(Replicator.FlushChanges())
                    return normal
                case Replicator.GetReplicaCount(reply_to):
                    # 0 until durable data has been loaded, used by test
                    reply_to.tell(Replicator.ReplicaCount(0))
                case (
                    _RemovedNodePruningTick()
                    | Replicator.FlushChanges()
                    | _GossipTick()
                    | _ClockTick()
                ):
                    # ignore scheduled ticks when loading durable data; Pekko stashes the clock
                    # tick, ohra drops it (the next tick after the load covers the time)
                    pass
                case Replicator.TestFullStateGossip(enabled):
                    receive_test_full_state_gossip(enabled)
                case Read() | Write() | Status() | Gossip():
                    # ignore gossip and replication when loading durable data
                    ctx.log.debug(
                        "ignoring message [%s] when loading durable data", type(msg).__name__
                    )
                case _DurableStoreTerminated():
                    return receive_durable_store_terminated()
                case _ClusterEvent():
                    result = normal_receive(ctx, msg)
                    if result is not Behaviors.same:
                        return result  # the node's own MemberRemoved stops the Replicator
                case _:
                    stash.append(msg)
            return Behaviors.same

        # Pekko's preStart sends LoadAll once the store exists
        durable_store_ref.tell(
            DurableStore.LoadAll(
                cast("ActorRef[DurableStore.LoadData | DurableStore.LoadAllCompleted]", ctx.self)
            )
        )
        return Behaviors.receive(load).receive_signal(on_signal)

    return setup
