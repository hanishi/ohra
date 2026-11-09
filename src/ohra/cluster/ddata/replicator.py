"""Pekko's Replicator: its typed protocol, the ``ddata.typed.scaladsl.Replicator`` commands and
responses nested in ``Replicator`` as ``EventStream``'s are, and the actor, whose handlers port
classic ``Replicator.scala`` function for function.

The commands and responses stay on one node, so they are not ``@remotable``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from ...behavior import Behavior, Behaviors
from ...cell import watched_ref
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

# Pekko's used timestamp, on the system scheduler's clock. None is Pekko's 0L: not set.
type _Timestamp = float


@dataclass(frozen=True)
class _GossipTick:
    pass


@dataclass(frozen=True)
class _SubscriberTerminated:
    """A watched subscriber stopped: Pekko's Terminated, as ``watch_with`` delivers it."""

    ref: ActorRef[Any]


type _Msg = Replicator.Command | _GossipTick | _SubscriberTerminated


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
        # the other members that are Up or Leaving, empty until the Replicator tracks membership
        nodes: set[UniqueAddress] = set()
        scheduler = ctx.system.scheduler

        # Pekko's gossipTask and notifyTask; its clockTask feeds only pruning, which is left out
        ctx.timers.start_timer_with_fixed_delay("gossip", _GossipTick(), settings.gossip_interval)
        ctx.timers.start_timer_with_fixed_delay(
            "notify", Replicator.FlushChanges(), settings.notify_subscribers_interval
        )

        expiry_keys = settings.expire_keys_after_inactivity
        expiry_wildcards = {
            _drop_wildcard(k): v for k, v in expiry_keys.items() if _is_wildcard(k)
        }
        expiry_enabled = bool(expiry_keys)
        # updated on the gossip tick, as Pekko's, which avoids reading the clock on every access
        current_used_timestamp: _Timestamp | None = (
            scheduler.current_time if expiry_enabled else None
        )

        data_entries: dict[str, tuple[DataEnvelope, _Digest, _Timestamp | None]] = {}
        # keys that have changed: Changed, Deleted and Expired go to subscribers on FlushChanges
        changed: set[str] = set()

        subscribers: dict[str, set[ActorRef[Any]]] = {}
        new_subscribers: dict[str, set[ActorRef[Any]]] = {}
        subscription_keys: dict[str, Key[Any]] = {}
        wildcard_subscribers: dict[str, set[ActorRef[Any]]] = {}

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
            update_used_timestamp(key.id, current_used_timestamp)
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
                    if is_local_update(consistency):
                        reply_to.tell(Replicator.DeleteSuccess(key))
                    # ponytail: Pekko starts a WriteAggregator here, as in receive_get
                    elif not_enough_nodes(consistency):
                        reply_to.tell(Replicator.DeleteFailure(key))
                    else:
                        reply_to.tell(Replicator.DeleteSuccess(key))

        def set_data(key: str, envelope: DataEnvelope) -> DataEnvelope:
            if has_subscriber(key) and key not in changed:
                # ponytail: Pekko compares the old and new SHA-1 digests here; envelope equality
                # stands in until gossip brings the digests
                if get_data(key) != envelope:
                    changed.add(key)  # notify subscribers, later
            dig = _DELETED_DIGEST if envelope.is_deleted else _LAZY_DIGEST
            data_entries[key] = (envelope, dig, get_used_timestamp(key))
            return envelope

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
                    s.tell(msg)

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
            # gossip to a random node comes with anti-entropy

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
                now = scheduler.current_time
                # no need to be more accurate than the gossip tick interval
                current_used_timestamp = now
                expired_keys = [
                    key
                    # None when set by a Write the first time: such a key does not expire at once
                    for key, (_, _, used_timestamp) in data_entries.items()
                    if used_timestamp is not None
                    and (duration := get_expiry_duration(key)) != 0.0
                    and used_timestamp <= now - duration
                ]
                if expired_keys:
                    ctx.log.debug("Removing expired keys [%s]", ", ".join(expired_keys))
                    for key in expired_keys:
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
                case Replicator.FlushChanges():
                    receive_flush_changes()
                case _GossipTick():
                    receive_gossip_tick()
                case Replicator.Subscribe(key, subscriber):
                    receive_subscribe(key, subscriber)
                case Replicator.Unsubscribe(key, subscriber):
                    receive_unsubscribe(key, subscriber)
                case _SubscriberTerminated(ref):
                    receive_terminated(ref)
            return Behaviors.same

        return normal_receive

    return setup
