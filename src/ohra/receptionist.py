"""Apache Pekko's Receptionist: actors register under a ServiceKey, others find or subscribe to the
set of refs under it. ``Receptionist(system)`` runs LocalReceptionist.scala at
``/system/localReceptionist``, or on a cluster node the ClusterReceptionist at
``/system/clusterReceptionist`` (``ohra.cluster.receptionist``), as
ReceptionistImpl.scala picks."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .behavior import Behavior, Behaviors
from .cell import watched_ref
from .context import ActorContext
from .ref import ActorRef
from .remote import RemoteRef
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


@dataclass(frozen=True)
class _ActorTerminated:
    """A watched actor stopped: it ends its registrations and every subscription made through it
    or its message adapters. One watch per actor, as watch_with keeps one message per target."""

    ref: ActorRef[Any]


type _Msg = Command | _ActorTerminated


LOCAL_NAME = "localReceptionist"


class Receptionist:
    """``Receptionist(system)``: the extension, started with the system as Pekko's. Send commands
    to ``ref``."""

    system: ActorSystem[Any]
    ref: ActorRef[Command]

    def __new__(cls, system: ActorSystem[Any]) -> Receptionist:
        if system.receptionist is None:
            self = object.__new__(cls)
            self.system = system
            if system.cluster is None:
                behavior, name = local_receptionist(system), LOCAL_NAME
            else:  # not at module level: the cluster receptionist imports this module
                from .cluster.receptionist.cluster_receptionist import NAME, cluster_receptionist

                behavior, name = cluster_receptionist(system), NAME
            cell = system._system_root.spawn_child(behavior, name, None)  # pyright: ignore[reportPrivateUsage]
            self.ref = cell.ref
            system.receptionist = self
        return system.receptionist


def local_receptionist(system: ActorSystem[Any]) -> Behavior[Any]:
    """LocalReceptionist.scala 47: the receptionist of a system that is not clustered."""

    @Behaviors.setup
    def setup(ctx: ActorContext[_Msg]) -> Behavior[_Msg]:
        services_per_actor: dict[ActorRef[Any], set[ServiceKey[Any]]] = {}
        registry: dict[ServiceKey[Any], set[ActorRef[Any]]] = {}
        subscribers: dict[ServiceKey[Any], set[ActorRef[Any]]] = {}
        subscriptions_per_actor: dict[ActorRef[Any], set[ServiceKey[Any]]] = {}

        def listing(
            key: ServiceKey[Any], services_were_added_or_removed: bool = True
        ) -> Listing[Any]:
            local = frozenset(registry.get(key, ()))
            return Listing(key, local, local, services_were_added_or_removed)

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

        def in_use(actor: ActorRef[Any]) -> bool:
            return any(watched_ref(s) == actor for s in services_per_actor) or any(
                watched_ref(s) == actor for s in subscriptions_per_actor
            )

        @Behaviors.receive
        def on_message(ctx: ActorContext[_Msg], msg: _Msg) -> Behavior[_Msg]:
            match msg:
                case Register(key, service, reply_to):
                    # a local actor, or a message adapter of one, which watches its actor
                    if (actor := watched_ref(service)) is None:
                        ctx.log.warning(
                            "Receptionist only accepts local actors for registration, ignoring "
                            "[%s] for [%s]",
                            service.path,
                            key.id,
                        )
                        return Behaviors.same
                    ctx.watch_with(actor, _ActorTerminated(actor))  # once per actor
                    if reply_to is not None:
                        reply_to.tell(Registered(key, service))
                    services_per_actor.setdefault(service, set()).add(key)
                    registry.setdefault(key, set()).add(service)
                    local_change([key])
                case Deregister(key, service, reply_to):
                    keys = services_per_actor.get(service)
                    if keys:
                        if reply_to is not None:
                            reply_to.tell(Deregistered(key, service))
                        remove_binding(key, service)
                        if (actor := watched_ref(service)) is not None and not in_use(actor):
                            ctx.unwatch(actor)
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
                    if (actor := watched_ref(subscriber)) is not None:
                        ctx.watch_with(actor, _ActorTerminated(actor))  # once per actor
                    subscriber.tell(listing(key))
                    subscriptions_per_actor.setdefault(subscriber, set()).add(key)
                    subscribers.setdefault(key, set()).add(subscriber)
                case _ActorTerminated(ref):
                    for subscriber in [
                        s for s in subscriptions_per_actor if watched_ref(s) == ref
                    ]:
                        for key in subscriptions_per_actor.pop(subscriber):
                            subscribers[key].discard(subscriber)
                            if not subscribers[key]:
                                del subscribers[key]
                    left: set[ServiceKey[Any]] = set()
                    for service in [s for s in services_per_actor if watched_ref(s) == ref]:
                        service_keys = services_per_actor.pop(service)
                        left |= service_keys
                        for key in service_keys:
                            remove_binding(key, service)
                    local_change(left)
                case _:
                    pass
            return Behaviors.same

        return on_message

    return setup
