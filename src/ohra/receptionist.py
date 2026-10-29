"""Apache Pekko's Receptionist: actors register under a ServiceKey, others find or subscribe to the
set of refs under it. One actor per system at ``/system/receptionist``, as LocalReceptionist.scala.
On a cluster node it also replicates: each node owns one versioned row of bindings and pushes it to
the other members, in place of ClusterReceptionist's distributed data."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .behavior import Behavior, Behaviors
from .cluster.core import (
    ClusterDomainEvent,
    MemberEvent,
    MemberRemoved,
    ReachabilityEvent,
    ReachableMember,
    UnreachableMember,
)
from .cluster.core import Subscribe as ClusterSubscribe
from .cell import is_local_actor
from .context import ActorContext
from .ref import ActorRef, LocalRef
from .remote import Address, RemoteRef, UniqueAddress
from .remote.serialization import remotable

if TYPE_CHECKING:
    from .system import ActorSystem


@remotable
@dataclass(frozen=True)
class ServiceKey[T]:
    """A name for a kind of service; ``T`` is the service's message type and is checked statically
    only."""

    id: str


@remotable
@dataclass(frozen=True)
class Registered[T]:
    key: ServiceKey[T]
    service_instance: ActorRef[T]


@remotable
@dataclass(frozen=True)
class Deregistered[T]:
    key: ServiceKey[T]
    service_instance: ActorRef[T]


@remotable
@dataclass(frozen=True)
class Listing[T]:
    """``service_instances`` are the reachable ones; ``all_service_instances`` include those on
    unreachable nodes. ``services_were_added_or_removed`` is False when only reachability
    changed."""

    key: ServiceKey[T]
    service_instances: frozenset[ActorRef[T]]
    all_service_instances: frozenset[ActorRef[T]]
    services_were_added_or_removed: bool


@remotable
@dataclass(frozen=True)
class Register[T]:
    key: ServiceKey[T]
    service: ActorRef[T]
    reply_to: ActorRef[Registered[T]] | None = None


@remotable
@dataclass(frozen=True)
class Deregister[T]:
    key: ServiceKey[T]
    service: ActorRef[T]
    reply_to: ActorRef[Deregistered[T]] | None = None


@remotable
@dataclass(frozen=True)
class Find[T]:
    key: ServiceKey[T]
    reply_to: ActorRef[Listing[T]]


@remotable
@dataclass(frozen=True)
class Subscribe[T]:
    """The current Listing at once, then one per change of the key."""

    key: ServiceKey[T]
    subscriber: ActorRef[Listing[T]]


type Command = Register[Any] | Deregister[Any] | Find[Any] | Subscribe[Any]


@remotable
@dataclass(frozen=True)
class ReceptionistRow:
    """One node's bindings (key id, ref), replacing the receiver's copy when ``version`` is
    higher."""

    owner: UniqueAddress
    version: int
    bindings: tuple[tuple[str, ActorRef[Any]], ...]


@dataclass(frozen=True)
class _ServiceTerminated:
    ref: ActorRef[Any]


@dataclass(frozen=True)
class _SubscriberTerminated:
    ref: ActorRef[Any]


@dataclass(frozen=True)
class _ResendTick: ...


type _Msg = (
    Command
    | _ServiceTerminated
    | _SubscriberTerminated
    | ReceptionistRow
    | ClusterDomainEvent
    | _ResendTick
)


class Receptionist:
    """``Receptionist(system)``: the extension, started with the system as Pekko's. Send commands
    to ``ref``."""

    system: ActorSystem[Any]
    ref: ActorRef[Command]

    def __new__(cls, system: ActorSystem[Any]) -> Receptionist:
        if system.receptionist is None:
            self = object.__new__(cls)
            self.system = system
            cell = system._system_root.spawn_child(_receptionist(system), "receptionist", None)  # pyright: ignore[reportPrivateUsage]
            self.ref = cell.ref
            system.receptionist = self
        return system.receptionist


def _receptionist(system: ActorSystem[Any]) -> Behavior[_Msg]:
    def setup(ctx: ActorContext[_Msg]) -> Behavior[_Msg]:
        services_per_actor: dict[ActorRef[Any], set[ServiceKey[Any]]] = {}
        registry: dict[ServiceKey[Any], set[ActorRef[Any]]] = {}
        subscribers: dict[ServiceKey[Any], set[ActorRef[Any]]] = {}
        subscriptions_per_actor: dict[ActorRef[Any], set[ServiceKey[Any]]] = {}
        replication = _Replication(ctx, system) if system.cluster is not None else None

        def listing(
            key: ServiceKey[Any], services_were_added_or_removed: bool = True
        ) -> Listing[Any]:
            local = frozenset(registry.get(key, ()))
            if replication is None:
                return Listing(key, local, local, services_were_added_or_removed)
            reachable, all_ = replication.remote_instances(key.id)
            return Listing(key, local | reachable, local | all_, services_were_added_or_removed)

        def notify(
            keys: Iterable[ServiceKey[Any]], services_were_added_or_removed: bool = True
        ) -> None:
            for key in keys:  # one Listing per key, as Pekko's notifySubscribers
                if subs := subscribers.get(key):
                    changed = listing(key, services_were_added_or_removed)
                    for subscriber in subs:
                        subscriber.tell(changed)

        def local_change(keys: Iterable[ServiceKey[Any]]) -> None:
            keys = list(keys)
            if not keys:  # a service already gone: nothing to tell or publish
                return
            notify(keys)
            if replication is not None:
                replication.publish_own_row(registry)

        def remove_binding(key: ServiceKey[Any], service: ActorRef[Any]) -> None:
            keys = services_per_actor.get(service)
            if keys is not None:
                keys.discard(key)
                if not keys:
                    del services_per_actor[service]
            instances = registry.get(key)
            if instances is not None:
                instances.discard(service)
                if not instances:
                    del registry[key]

        def on_message(ctx: ActorContext[_Msg], msg: _Msg) -> Behavior[_Msg]:
            match msg:
                case Register(key, service, reply_to):
                    if not isinstance(service, LocalRef):
                        ctx.log.warning(
                            "Receptionist only accepts local actors for registration, ignoring "
                            "[%s] for [%s]",
                            service.path,
                            key.id,
                        )
                        return Behaviors.same
                    if service not in services_per_actor:
                        ctx.watch_with(service, _ServiceTerminated(service))
                    if reply_to is not None:
                        reply_to.tell(Registered(key, service))
                    services_per_actor.setdefault(service, set()).add(key)
                    registry.setdefault(key, set()).add(service)
                    local_change([key])
                case Deregister(key, service, reply_to):
                    keys = services_per_actor.get(service)
                    if keys:
                        if keys == {key}:
                            ctx.unwatch(service)
                        if reply_to is not None:
                            reply_to.tell(Deregistered(key, service))
                        remove_binding(key, service)
                        local_change([key])
                case Find(key, reply_to):
                    reply_to.tell(listing(key))
                case Subscribe(key, subscriber):
                    # as ClusterReceptionist: subscribers are local
                    if isinstance(subscriber, RemoteRef):
                        ctx.log.warning(
                            "Receptionist only accepts local subscribers, ignoring [%s] for [%s]",
                            subscriber.path,
                            key.id,
                        )
                        return Behaviors.same
                    if subscriber not in subscriptions_per_actor and is_local_actor(subscriber):
                        ctx.watch_with(subscriber, _SubscriberTerminated(subscriber))
                    subscriber.tell(listing(key))
                    subscriptions_per_actor.setdefault(subscriber, set()).add(key)
                    subscribers.setdefault(key, set()).add(subscriber)
                case _ServiceTerminated(ref):
                    keys = services_per_actor.pop(ref, set())
                    for key in keys:
                        remove_binding(key, ref)
                    local_change(keys)
                case _SubscriberTerminated(ref):
                    for key in subscriptions_per_actor.pop(ref, set()):
                        subscribers[key].discard(ref)
                        if not subscribers[key]:
                            del subscribers[key]
                case ReceptionistRow() | ClusterDomainEvent() | _ResendTick() if (
                    replication is not None
                ):
                    for key_ids, added_or_removed in replication.on_message(msg, registry):
                        notify((ServiceKey[Any](i) for i in key_ids), added_or_removed)
                case _:
                    pass
            return Behaviors.same

        return Behaviors.receive(on_message)

    return Behaviors.setup(setup)


class _Replication:
    """The cluster part: rows by owner, pushed to the other members and pruned by membership
    events."""

    def __init__(self, ctx: ActorContext[_Msg], system: ActorSystem[Any]) -> None:
        from .cluster.core import Cluster

        self.ctx = ctx
        self.system = system
        self.cluster = Cluster(system)
        self.self_address = self.cluster.self_unique_address
        self.rows: dict[UniqueAddress, tuple[int, frozenset[tuple[str, ActorRef[Any]]]]] = {}
        self.own_version = 0
        self.own_row = ReceptionistRow(self.self_address, 0, ())
        self.known: set[UniqueAddress] = set()
        self.cluster.subscriptions.tell(ClusterSubscribe(ctx.self, MemberEvent))
        self.cluster.subscriptions.tell(ClusterSubscribe(ctx.self, ReachabilityEvent))
        ctx.timers.start_timer_with_fixed_delay(
            "resend", _ResendTick(), self.cluster.settings.receptionist_resend_interval
        )

    # --- views -----------------------------------------------------------------------------------

    def members(self) -> frozenset[UniqueAddress]:
        return frozenset(m.unique_address for m in self.cluster.state.members)

    def unreachable(self) -> frozenset[UniqueAddress]:
        return frozenset(m.unique_address for m in self.cluster.state.unreachable)

    def remote_instances(
        self, key_id: str
    ) -> tuple[frozenset[ActorRef[Any]], frozenset[ActorRef[Any]]]:
        """(reachable, all) refs under the key from other members' rows."""
        members, unreachable = self.members(), self.unreachable()
        reachable: set[ActorRef[Any]] = set()
        all_: set[ActorRef[Any]] = set()
        for owner, (_, bindings) in self.rows.items():
            if owner == self.self_address or owner not in members:
                continue
            for binding_key, ref in bindings:
                if binding_key == key_id:
                    all_.add(ref)
                    if owner not in unreachable:
                        reachable.add(ref)
        return frozenset(reachable), frozenset(all_)

    # --- sending ---------------------------------------------------------------------------------

    def receptionist_at(self, address: Address) -> ActorRef[Any]:
        return self.system.resolve(f"{address.root}/system/receptionist")

    def publish_own_row(self, registry: dict[ServiceKey[Any], set[ActorRef[Any]]]) -> None:
        self.own_version += 1
        bindings = tuple((key.id, ref) for key, refs in registry.items() for ref in refs)
        self.own_row = ReceptionistRow(self.self_address, self.own_version, bindings)
        self.send_own_row_to(self.members() - self.unreachable())

    def send_own_row_to(self, nodes: Iterable[UniqueAddress]) -> None:
        for node in nodes:
            if node != self.self_address:
                self.receptionist_at(node.address).tell(self.own_row)

    # --- receiving -------------------------------------------------------------------------------

    def on_message(
        self,
        msg: ReceptionistRow | ClusterDomainEvent | _ResendTick,
        registry: dict[ServiceKey[Any], set[ActorRef[Any]]],
    ) -> list[tuple[set[str], bool]]:
        """Returns the key ids whose listings changed, each with the services_were_added_or_removed
        flag."""
        match msg:
            case ReceptionistRow(owner, version, bindings):
                if owner == self.self_address:
                    return []
                stored = self.rows.get(owner)
                if stored is not None and version <= stored[0]:
                    return []
                new = frozenset(bindings)
                self.rows[owner] = (version, new)
                if owner not in self.members():
                    return []  # hidden until the node is a member
                old: frozenset[tuple[str, ActorRef[Any]]] = (
                    stored[1] if stored is not None else frozenset()
                )
                return [({k for k, _ in old ^ new}, True)]
            case MemberRemoved(member=m):
                self.known.discard(m.unique_address)
                removed = self.rows.pop(m.unique_address, None)
                return [({k for k, _ in removed[1]}, True)] if removed is not None else []
            case MemberEvent(member=m):
                node = m.unique_address
                if node == self.self_address or node in self.known:
                    return []
                self.known.add(node)
                self.send_own_row_to([node])
                # a row that arrived before the member did becomes visible now
                row = self.rows.get(node)
                return (
                    [({k for k, _ in row[1]}, True)]
                    if row is not None and node in self.members()
                    else []
                )
            case UnreachableMember(member=m) | ReachableMember(member=m):
                row = self.rows.get(m.unique_address)
                return [({k for k, _ in row[1]}, False)] if row is not None else []
            case _ResendTick():
                self.send_own_row_to(self.members() - self.unreachable())
                return []
            case _:
                return []
