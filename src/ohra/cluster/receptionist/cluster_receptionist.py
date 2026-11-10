"""The cluster receptionist, ported from Pekko's ClusterReceptionist.scala (55-613): service
registrations are replicated over several ``ORMultiMap`` ddata keys by a child Replicator.

Deadlines of tombstones are scheduler time, so ``ManualTime`` drives them.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, cast

from ...behavior import Behavior, Behaviors
from ...context import ActorContext
from ...receptionist import (
    Command,
    Deregister,
    Deregistered,
    Find,
    Listing,
    Register,
    Registered,
    ServiceKey,
    Subscribe,
)
from ...cell import watched_ref
from ...ref import ActorRef
from ...remote import RemoteRef, UniqueAddress
from ...system import ActorSystem
from ..core import (
    Cluster,
    ClusterDomainEvent,
    ClusterShuttingDown,
    MemberJoined,
    MemberRemoved,
    MemberUp,
    MemberWeaklyUp,
    ReachabilityEvent,
    ReachableMember,
    UnreachableMember,
)
from ..core import Subscribe as ClusterSubscribe
from ..ddata import ORMultiMap, Replicator, SelfUniqueAddress
from .registry import (
    EMPTY_OR_MULTI_MAP,
    DDataKey,
    Entry,
    ServiceRegistry,
    ShardedServiceRegistry,
)

NAME = "clusterReceptionist"
"""ClusterReceptionist.scala 61: ``override val name``."""

type Tombstones = Mapping[ActorRef[Any], frozenset[tuple[ServiceKey[Any], float]]]


@dataclass(frozen=True)
class State:
    """ClusterReceptionist.scala 94-188.

    ``registry`` is the last state seen from the replicator; ``services_per_actor`` is needed
    since an actor can implement several services; ``tombstones`` are local actors that were
    stopped and are not to be re-added; ``subscriptions`` are local and not replicated.
    Tombstone deadlines are scheduler times.
    """

    registry: ShardedServiceRegistry
    services_per_actor: Mapping[ActorRef[Any], frozenset[ServiceKey[Any]]] = field(
        default_factory=dict[ActorRef[Any], frozenset[ServiceKey[Any]]]
    )
    tombstones: Tombstones = field(
        default_factory=dict[ActorRef[Any], frozenset[tuple[ServiceKey[Any], float]]]
    )
    subscriptions: Mapping[ServiceKey[Any], frozenset[ActorRef[Any]]] = field(
        default_factory=dict[ServiceKey[Any], frozenset[ActorRef[Any]]]
    )

    def add_tombstone(self, actor: ActorRef[Any], deadline: float) -> State:
        """Tombstone all services the actor is registered for (Scala 101-106)."""
        state = self
        for key in self.services_per_actor.get(actor, frozenset()):
            state = state.add_tombstone_for(actor, key, deadline)
        return state

    def add_tombstone_for(
        self, actor: ActorRef[Any], key: ServiceKey[Any], deadline: float
    ) -> State:
        """Tombstone the one service the actor is registered for (Scala 108-111)."""
        mine = self.tombstones.get(actor, frozenset()) | {(key, deadline)}
        return replace(self, tombstones={**self.tombstones, actor: mine})

    def has_tombstone(self, key: ServiceKey[Any], actor: ActorRef[Any]) -> bool:
        return any(k == key for k, _ in self.tombstones.get(actor, frozenset()))

    def prune_tombstones(self, now: float) -> State:
        """Drop the tombstones whose deadline has passed (Scala 116-135); ``self`` if none."""
        if not self.tombstones:
            return self
        new: dict[ActorRef[Any], frozenset[tuple[ServiceKey[Any], float]]] = {}
        for actor, entries in self.tombstones.items():
            keep = frozenset(e for e in entries if e[1] > now)  # Deadline.hasTimeLeft
            if keep:
                new[actor] = keep
        if new == self.tombstones:
            return self
        return replace(self, tombstones=new)

    def active_actor_refs_for(
        self, key: ServiceKey[Any], self_unique_address: UniqueAddress
    ) -> tuple[frozenset[ActorRef[Any]], frozenset[ActorRef[Any]]]:
        """(reachable, all) (Scala 137-156)."""
        ddata_key = self.registry.ddata_key_for(key)
        entries = self.registry.registry_for(ddata_key).entries_for(key)
        reachable: set[ActorRef[Any]] = set()
        all_: set[ActorRef[Any]] = set()
        for entry in entries:
            entry_address = entry.unique_address(self_unique_address.address)
            if entry_address in self.registry.nodes and not self.has_tombstone(key, entry.ref):
                all_.add(entry.ref)
                if entry_address not in self.registry.unreachable:
                    reachable.add(entry.ref)
        return frozenset(reachable), frozenset(all_)

    def add_local_service(self, instance: ActorRef[Any], key: ServiceKey[Any]) -> State:
        """Scala 158-167: a re-registered service leaves the tombstones."""
        services = {
            **self.services_per_actor,
            instance: self.services_per_actor.get(instance, frozenset()) | {key},
        }
        mine = self.tombstones.get(instance, frozenset())
        tombstones = (
            self.tombstones
            if not mine
            else {**self.tombstones, instance: frozenset(t for t in mine if t[0] != key)}
        )
        return replace(self, services_per_actor=services, tombstones=tombstones)

    def remove_local_service(
        self, instance: ActorRef[Any], key: ServiceKey[Any], tombstone_deadline: float
    ) -> State:
        """Scala 169-182. Raises ``ValueError`` for an instance never registered, as Pekko's
        IllegalArgumentException."""
        keys = self.services_per_actor.get(instance)
        if keys is None:
            raise ValueError(
                f"Trying to remove {instance} for {key} but that has never been registered"
            )
        new_keys = keys - {key}
        services = {k: v for k, v in self.services_per_actor.items() if k != instance}
        if new_keys:
            services[instance] = new_keys
        return replace(
            self.add_tombstone_for(instance, key, tombstone_deadline), services_per_actor=services
        )

    def remove_subscriber(self, subscriber: ActorRef[Any]) -> State:
        """Scala 184: ``subscriptions.valueRemoved``."""
        subs = {k: v - {subscriber} for k, v in self.subscriptions.items()}
        return replace(self, subscriptions={k: v for k, v in subs.items() if v})


# --- internal commands (Scala 70-80) -------------------------------------------------------------


@dataclass(frozen=True)
class _ActorTerminated:
    """LocalServiceActorTerminated and SubscriberTerminated in one: ohra's ``watch_with`` keeps one
    message per target, and an actor can both register and subscribe."""

    ref: ActorRef[Any]


@dataclass(frozen=True)
class _NodeAdded:
    address: UniqueAddress


@dataclass(frozen=True)
class _NodeRemoved:
    address: UniqueAddress


@dataclass(frozen=True)
class _NodeUnreachable:
    address: UniqueAddress


@dataclass(frozen=True)
class _NodeReachable:
    address: UniqueAddress


@dataclass(frozen=True)
class _ChangeFromReplicator:
    key: DDataKey
    value: ORMultiMap[ServiceKey[Any], Entry]


@dataclass(frozen=True)
class _RemoveTick: ...


@dataclass(frozen=True)
class _PruneTombstonesTick: ...


type _Msg = (
    Command
    | _ActorTerminated
    | _NodeAdded
    | _NodeRemoved
    | _NodeUnreachable
    | _NodeReachable
    | _ChangeFromReplicator
    | _RemoveTick
    | _PruneTombstonesTick
)


def _owner(ref: ActorRef[Any]) -> ActorRef[Any]:
    """The actor a watch of ``ref`` watches (a message adapter's owner)."""
    return watched_ref(ref) or ref


def _entry_text(entry: Entry) -> str:
    return f"{entry.ref.path} @ {entry.system_uid}"


def _entries_text(entries: Iterable[Entry]) -> str:
    return ", ".join(_entry_text(e) for e in entries)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


class _Setup:
    """ClusterReceptionist.scala 190-207: what is captured once."""

    def __init__(self, ctx: ActorContext[Any]) -> None:
        self.cluster = Cluster(ctx.system)
        self.settings = self.cluster.settings.receptionist
        self.self_system_uid = self.cluster.self_unique_address.uid
        self.keep_tombstones_for = self.cluster.settings.prune_gossip_tombstones_after
        self.self_node = SelfUniqueAddress(self.cluster.self_unique_address)
        self.replicator: ActorRef[Replicator.Command] = ctx.spawn(
            Replicator.behavior(
                self.settings.effective_replicator_settings(self.cluster.settings)
            ),
            "replicator",
        )

    @property
    def self_unique_address(self) -> UniqueAddress:
        return self.cluster.self_unique_address

    def new_tombstone_deadline(self, ctx: ActorContext[Any]) -> float:
        return ctx.system.scheduler.current_time + self.keep_tombstones_for


def cluster_receptionist(system: ActorSystem[Any]) -> Behavior[Any]:
    """ClusterReceptionist.scala 209-270 (``behavior``): the receptionist actor, to be spawned as
    a system actor named ``NAME``."""

    @Behaviors.setup
    def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        s = _Setup(ctx)
        # include the self unique address so that it can be used locally before joining
        initial_registry = ShardedServiceRegistry.create(
            s.settings.distributed_key_count
        ).add_node(s.self_unique_address)

        # subscribe to changes from other nodes
        def from_replicator(msg: Any) -> _ChangeFromReplicator:
            if not isinstance(msg, Replicator.Changed):
                raise ValueError(f"Unexpected {msg}")
            changed = cast("Replicator.Changed[ORMultiMap[ServiceKey[Any], Entry]]", msg)
            return _ChangeFromReplicator(cast(DDataKey, changed.key), changed.data)

        replicator_adapter = ctx.message_adapter(from_replicator)
        for key in initial_registry.all_ddata_keys:
            s.replicator.tell(Replicator.Subscribe(key, replicator_adapter))

        # keep track of cluster members; remove entries when members are removed
        def from_cluster(event: ClusterDomainEvent) -> Any:
            match event:
                case MemberJoined(m) | MemberWeaklyUp(m) | MemberUp(m):
                    return _NodeAdded(m.unique_address)
                case MemberRemoved(m, _):
                    return _NodeRemoved(m.unique_address)
                case UnreachableMember(m):
                    return _NodeUnreachable(m.unique_address)
                case ReachableMember(m):
                    return _NodeReachable(m.unique_address)
                case ClusterShuttingDown():
                    return _NodeRemoved(s.self_unique_address)
                case _:
                    raise ValueError(f"Unexpected ClusterDomainEvent {event}. Please report bug.")

        cluster_adapter = ctx.message_adapter(from_cluster)
        for event_class in (
            MemberJoined,
            MemberWeaklyUp,
            MemberUp,
            MemberRemoved,
            ReachabilityEvent,
            ClusterShuttingDown,
        ):
            s.cluster.subscriptions.tell(ClusterSubscribe(cluster_adapter, event_class))

        # also periodic cleanup in case removal from ORMultiMap is skipped due to concurrent
        # update, which is possible for OR CRDTs
        ctx.timers.start_timer_with_fixed_delay(
            "remove", _RemoveTick(), s.settings.pruning_interval
        )
        # default tombstone keepalive is 24h and keeping the actorrefs around isn't very costly,
        # so don't prune often
        ctx.timers.start_timer_with_fixed_delay(
            "prune-tombstones", _PruneTombstonesTick(), s.keep_tombstones_for / 24
        )
        return _behavior(s, State(registry=initial_registry))

    return setup


def _behavior(s: _Setup, state: State) -> Behavior[Any]:
    """ClusterReceptionist.scala 272-613."""
    cluster, settings = s.cluster, s.settings

    @Behaviors.receive
    def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        log, self_address = ctx.log, cluster.self_address
        debug = log.isEnabledFor(logging.DEBUG)
        ignore = ctx.system.ignore_ref

        def update(
            ddata_key: DDataKey, modify: Callable[[ServiceRegistry], ServiceRegistry]
        ) -> None:
            s.replicator.tell(
                Replicator.Update(
                    ddata_key,
                    EMPTY_OR_MULTI_MAP,
                    settings.write_consistency,
                    ignore,
                    lambda registry: modify(ServiceRegistry(registry)).to_or_multi_map(),
                )
            )

        def is_leader() -> bool:
            return cluster.state.leader == self_address

        def nodes_removed(
            addresses: frozenset[UniqueAddress], only_remove_old_entries: bool
        ) -> None:
            """Scala 280-323."""
            now = _now_ms()
            older_than = settings.prune_removed_older_than * 1000
            removals: dict[ServiceKey[Any], set[Entry]] = {}
            for key, entries in state.registry.all_services():
                removed = {
                    e
                    for e in entries
                    if e.unique_address(s.self_unique_address.address) in addresses
                    # an entry added before MemberJoined is visible is not removed
                    and (not only_remove_old_entries or now - e.created_timestamp >= older_than)
                }
                if removed:
                    removals[key] = removed
            if not removals:
                return
            if debug:
                log.debug(
                    "ClusterReceptionist [%s] - Node(s) removed [%s], updating registry removing "
                    "entries: [%s]",
                    self_address,
                    ",".join(str(a) for a in addresses),
                    ",".join(
                        f"{k.id}->[{', '.join(_entry_text(e) for e in es)}]"
                        for k, es in removals.items()
                    ),
                )
            # shard changes over the ddata keys they belong to
            for ddata_key, removal in state.registry.entries_per_ddata_key(removals).items():
                update(
                    ddata_key,
                    lambda r, removal=removal: r.remove_all(removal, s.self_node),
                )

        def notify_subscribers(
            changed_keys: frozenset[ServiceKey[Any]],
            services_were_added_or_removed: bool,
            new_state: State,
        ) -> None:
            for key in changed_keys:
                subscribers = new_state.subscriptions.get(key)
                if subscribers:
                    reachable, all_ = new_state.active_actor_refs_for(key, s.self_unique_address)
                    listing = Listing(key, reachable, all_, services_were_added_or_removed)
                    for subscriber in subscribers:
                        subscriber.tell(listing)

        def listing_for(key: ServiceKey[Any]) -> Listing[Any]:
            reachable, all_ = state.active_actor_refs_for(key, s.self_unique_address)
            return Listing(key, reachable, all_, True)

        def is_local(ref: ActorRef[Any]) -> bool:
            return not isinstance(ref, RemoteRef)  # hasLocalScope

        def in_use(actor: ActorRef[Any], st: State) -> bool:
            return any(_owner(r) == actor for r in st.services_per_actor) or any(
                _owner(r) == actor for subs in st.subscriptions.values() for r in subs
            )

        match cast(_Msg, msg):
            case Register():
                key, instance, reply_to = msg.key, msg.service, msg.reply_to  # 347-368
                if is_local(instance):
                    entry = Entry(instance, s.self_system_uid, _now_ms())
                    if debug:
                        log.debug(
                            "ClusterReceptionist [%s] - Actor was registered: [%s] [%s]",
                            self_address,
                            key,
                            _entry_text(entry),
                        )
                    # actor already watched after one service key registration
                    if (actor := watched_ref(instance)) is not None:
                        ctx.watch_with(actor, _ActorTerminated(actor))
                    if reply_to is not None:
                        reply_to.tell(Registered(key, instance))
                    update(
                        state.registry.ddata_key_for(key),
                        lambda r: r.add_binding(key, entry, s.self_node),
                    )
                    return _behavior(s, state.add_local_service(instance, key))
                log.error(
                    "ClusterReceptionist [%s] - Register of non-local [%s] is not supported",
                    self_address,
                    instance.path,
                )
                return Behaviors.same

            case Deregister():
                key, instance, reply_to = msg.key, msg.service, msg.reply_to  # 370-397
                if is_local(instance):
                    entry = Entry(instance, s.self_system_uid)
                    if debug:
                        log.debug(
                            "ClusterReceptionist [%s] - Unregister actor: [%s] [%s]",
                            self_address,
                            key.id,
                            _entry_text(entry),
                        )
                    try:
                        new_state = state.remove_local_service(
                            instance, key, s.new_tombstone_deadline(ctx)
                        )
                    except ValueError as e:
                        # Pekko lets the IllegalArgumentException fail the receptionist; ohra logs
                        # it and changes nothing (appendix B)
                        log.error("ClusterReceptionist [%s] - %s", self_address, e)
                        return Behaviors.same
                    if (actor := watched_ref(instance)) is not None and not in_use(
                        actor, new_state
                    ):
                        # last service for the actor unregistered, stop watching
                        ctx.unwatch(actor)
                    if reply_to is not None:
                        reply_to.tell(Deregistered(key, instance))
                    update(
                        state.registry.ddata_key_for(key),
                        lambda r: r.remove_binding(key, entry, s.self_node),
                    )
                    # removals are tombstoned so they are not re-added by merging with other
                    # concurrent registrations for the same key
                    return _behavior(s, new_state)
                log.error(
                    "ClusterReceptionist [%s] - Unregistering non-local [%s] is not supported",
                    self_address,
                    instance.path,
                )
                return Behaviors.same

            case Find():
                key, reply_to = msg.key, msg.reply_to  # 399-402
                reply_to.tell(listing_for(key))
                return Behaviors.same

            case Subscribe():
                key, subscriber = msg.key, msg.subscriber
                if is_local(subscriber):
                    if (actor := watched_ref(subscriber)) is not None:
                        ctx.watch_with(actor, _ActorTerminated(actor))
                    # immediately reply with the initial listing to the new subscriber
                    subscriber.tell(listing_for(key))
                    subs = {
                        **state.subscriptions,
                        key: state.subscriptions.get(key, frozenset()) | {subscriber},
                    }
                    return _behavior(s, replace(state, subscriptions=subs))
                log.error(
                    "ClusterReceptionist [%s] - Subscriptions from non-local [%s] is not "
                    "supported",
                    self_address,
                    subscriber.path,
                )
                return Behaviors.same

            case _ActorTerminated(
                actor
            ):  # SubscriberTerminated 428, LocalServiceActorTerminated 431
                new_state = state
                for subscriber in {r for subs in state.subscriptions.values() for r in subs}:
                    if _owner(subscriber) == actor:
                        new_state = new_state.remove_subscriber(subscriber)
                # could be empty if there was a race between termination and unregistration
                for instance in [r for r in state.services_per_actor if _owner(r) == actor]:
                    keys = state.services_per_actor[instance]
                    entry = Entry(instance, s.self_system_uid)
                    if debug:
                        log.debug(
                            "ClusterReceptionist [%s] - Registered actor terminated: [%s] [%s]",
                            self_address,
                            ", ".join(k.id for k in keys),
                            _entry_text(entry),
                        )
                    for key in keys:
                        update(
                            state.registry.ddata_key_for(key),
                            lambda r, key=key, entry=entry: r.remove_binding(
                                key, entry, s.self_node
                            ),
                        )
                    # removals are tombstoned so they are not re-added by merging with other
                    # concurrent registrations for the same key
                    new_state = new_state.add_tombstone(instance, s.new_tombstone_deadline(ctx))
                return _behavior(s, new_state)

            case _ChangeFromReplicator(ddata_key, value):  # 453-499
                # every change will come back this way - this is where local notification happens
                new_registry = ServiceRegistry(value)
                changed_keys = frozenset(
                    state.registry.collect_changed_keys(ddata_key, new_registry)
                )
                new_state = replace(
                    state, registry=state.registry.with_service_registry(ddata_key, new_registry)
                )
                if not changed_keys:
                    return Behaviors.same
                if debug:
                    log.debug(
                        "ClusterReceptionist [%s] - Change from replicator: [%s], changes: [%s], "
                        "tombstones [%s]",
                        self_address,
                        new_registry.entries.entries,
                        ", ".join(
                            f"{k.id}->[{_entries_text(new_registry.entries_for(k))}]"
                            for k in changed_keys
                        ),
                        ", ".join(f"{a.path}->{t}" for a, t in state.tombstones.items()),
                    )
                notify_subscribers(changed_keys, True, new_state)
                for key in changed_keys:
                    # because of how ORMultiMap/ORSet works, an actor we removed can be
                    # re-introduced by a concurrent update; in that case we re-remove it
                    re_added = [
                        r for r in new_registry.actor_refs_for(key) if state.has_tombstone(key, r)
                    ]
                    if re_added:
                        if debug:
                            log.debug(
                                "ClusterReceptionist [%s] - Saw ActorRefs that were tomstoned "
                                "[%s], re-removing.",
                                self_address,
                                ", ".join(str(r.path) for r in re_added),
                            )

                        def re_remove(
                            r: ServiceRegistry,
                            key: ServiceKey[Any] = key,
                            re_added: list[ActorRef[Any]] = re_added,
                        ) -> ServiceRegistry:
                            for ref in re_added:
                                r = r.remove_binding(
                                    key, Entry(ref, s.self_system_uid), s.self_node
                                )
                            return r

                        update(ddata_key, re_remove)
                return _behavior(s, new_state)

            case _NodeAdded(address):  # 501-518
                if address in state.registry.nodes:
                    return Behaviors.same
                new_state = replace(state, registry=state.registry.add_node(address))
                keys_for_node = new_state.registry.keys_for(address, s.self_node)
                if keys_for_node:
                    log.debug(
                        "ClusterReceptionist [%s] - Node with registered services added [%s]",
                        self_address,
                        address,
                    )
                    notify_subscribers(keys_for_node, True, new_state)
                else:
                    log.debug("ClusterReceptionist [%s] - Node added [%s]", self_address, address)
                return _behavior(s, new_state)

            case _NodeRemoved(address):  # 520-550
                if address == s.self_unique_address:
                    log.debug("ClusterReceptionist [%s] - terminated/removed", self_address)
                    # If the self node is shutting down our own entries should have been removed
                    # via watch-Terminated or will be removed by other nodes. This point is
                    # anyway too late.
                    return Behaviors.stopped()
                if address in state.registry.nodes:
                    keys_for_node = state.registry.keys_for(address, s.self_node)
                    new_state = replace(state, registry=state.registry.remove_node(address))
                    if keys_for_node:
                        log.debug(
                            "ClusterReceptionist [%s] - Node with registered services removed "
                            "[%s]",
                            self_address,
                            address,
                        )
                        notify_subscribers(keys_for_node, True, new_state)
                    # Ok to update from several nodes but more efficient to try from one node.
                    if is_leader():
                        log.debug(
                            "ClusterReceptionist [%s] - Leader node observed removed node [%s]",
                            self_address,
                            address,
                        )
                        nodes_removed(frozenset({address}), False)
                    return _behavior(s, new_state)
                return Behaviors.same

            case _NodeUnreachable(address):  # 552-562
                keys_for_node = state.registry.keys_for(address, s.self_node)
                new_state = replace(state, registry=state.registry.add_unreachable(address))
                if keys_for_node:
                    log.debug(
                        "ClusterReceptionist [%s] - Node with registered services unreachable "
                        "[%s]",
                        self_address,
                        address,
                    )
                    notify_subscribers(keys_for_node, False, new_state)
                return _behavior(s, new_state)

            case _NodeReachable(address):  # 564-574
                keys_for_node = state.registry.keys_for(address, s.self_node)
                new_state = replace(state, registry=state.registry.remove_unreachable(address))
                if keys_for_node:
                    log.debug(
                        "ClusterReceptionist [%s] - Node with registered services reachable "
                        "again [%s]",
                        self_address,
                        address,
                    )
                    notify_subscribers(keys_for_node, False, new_state)
                return _behavior(s, new_state)

            case _RemoveTick():  # 576-592
                # ok to update from several nodes but more efficient to try from one node
                if is_leader():
                    all_addresses = state.registry.all_unique_addresses_in_state(
                        s.self_unique_address
                    )
                    not_in_cluster = all_addresses - state.registry.nodes
                    if not_in_cluster:
                        if debug:
                            log.debug(
                                "ClusterReceptionist [%s] - Leader node cleanup tick, removed "
                                "nodes: [%s]",
                                self_address,
                                ",".join(str(a) for a in not_in_cluster),
                            )
                        nodes_removed(not_in_cluster, True)
                return Behaviors.same

            case _PruneTombstonesTick():  # 594-600
                pruned = state.prune_tombstones(ctx.system.scheduler.current_time)
                if pruned is state:
                    return Behaviors.same
                log.debug("ClusterReceptionist [%s] - Pruning tombstones", self_address)
                return _behavior(s, pruned)

            case _:
                return Behaviors.unhandled

    return receive
