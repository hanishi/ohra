"""Pekko's cluster-typed ``DistributedData`` extension and ``ReplicatorMessageAdapter``."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from ...behavior import Behavior, Behaviors
from ...patterns import AskTimeoutError
from ...result import Failure, Success, Try
from ...supervision import SupervisorStrategy
from .key import Key
from .replicated_data import ReplicatedData
from .replicator import Replicator
from .settings import ReplicatorSettings

if TYPE_CHECKING:
    from ...context import ActorContext
    from ...ref import ActorRef
    from ...system import ActorSystem
    from . import SelfUniqueAddress

UNEXPECTED_ASK_TIMEOUT = 20.0
"""Pekko's ``replicator-message-adapter-unexpected-ask-timeout``, in seconds."""


class DistributedData:
    """``DistributedData(system)``: the extension. The first call starts the Replicator under
    ``/system``, with the settings in ``ClusterSettings.distributed_data``. A system that is not
    a cluster node raises RuntimeError."""

    replicator: ActorRef[Replicator.Command]
    self_unique_address: SelfUniqueAddress

    def __new__(cls, system: ActorSystem[Any]) -> DistributedData:
        if system.cluster is None:
            raise RuntimeError(
                f"system {system.name!r} has no cluster: pass cluster=ClusterSettings(...) to "
                "ActorSystem"
            )
        if system.distributed_data is None:
            # imported here: cluster.core imports this package for ReplicatorSettings, and this
            # package's __init__ imports this module before it defines SelfUniqueAddress
            from ..core import Cluster
            from . import SelfUniqueAddress

            cluster = Cluster(system)
            settings = cluster.settings.distributed_data
            # typed actors stop on failure, Pekko's classic Replicator restarts
            behavior = Behaviors.supervise(Replicator.behavior(settings)).on_failure(
                SupervisorStrategy.restart()
            )
            self = object.__new__(cls)
            name = ReplicatorSettings.name_for(system, None)
            self.replicator = system._system_root.spawn_child(behavior, name, None).ref  # pyright: ignore[reportPrivateUsage]
            self.self_unique_address = SelfUniqueAddress(cluster.self_unique_address)
            system.distributed_data = self
        return system.distributed_data

    @staticmethod
    def with_replicator_message_adapter[M, A: ReplicatedData](
        factory: Callable[[ReplicatorMessageAdapter[M, A]], Behavior[M]],
        unexpected_ask_timeout: float = UNEXPECTED_ASK_TIMEOUT,
    ) -> Behavior[M]:
        """A behavior that hands ``factory`` an adapter on this system's Replicator."""

        def setup(ctx: ActorContext[M]) -> Behavior[M]:
            replicator = DistributedData(ctx.system).replicator
            return factory(ReplicatorMessageAdapter(ctx, replicator, unexpected_ask_timeout))

        return Behaviors.setup(setup)


class _KeyedRequest[A: ReplicatedData, R](Protocol):
    """A Get, Update or Delete: what the adapter reads the key of when its ask times out."""

    @property
    def key(self) -> Key[A]: ...
    @property
    def reply_to(self) -> ActorRef[R]: ...


class ReplicatorMessageAdapter[M, A: ReplicatedData]:
    """Turns the Replicator's responses into the actor's own messages ``M``.

    The Replicator always replies within the request's own timeout, so ``unexpected_ask_timeout``
    is for cleanup only. When it passes, ``response_adapter`` gets ``GetFailure``,
    ``UpdateTimeout`` or ``DeleteFailure`` for the request's key. Pekko throws the timeout in the
    actor instead, but an exception in an adapt function here is only logged (appendix B).
    ``ask_replica_count`` has no failure reply, so its timeout is only logged."""

    def __init__(
        self,
        context: ActorContext[M],
        replicator: ActorRef[Replicator.Command],
        unexpected_ask_timeout: float,
    ) -> None:
        self._context = context
        self._replicator = replicator
        self._ask_timeout = unexpected_ask_timeout
        self._changed_message_adapters: dict[
            Key[A], ActorRef[Replicator.SubscribeResponse[A]]
        ] = {}

    def subscribe(
        self, key: Key[A], response_adapter: Callable[[Replicator.SubscribeResponse[A]], M]
    ) -> None:
        self.unsubscribe(key)  # in case it's called more than once per key
        reply_to = self._context.message_adapter(response_adapter)
        self._changed_message_adapters[key] = reply_to
        self._replicator.tell(Replicator.Subscribe(key, reply_to))

    def unsubscribe(self, key: Key[A]) -> None:
        subscriber = self._changed_message_adapters.get(key)
        if subscriber is not None:
            self._replicator.tell(Replicator.Unsubscribe(key, subscriber))

    def ask_update(
        self,
        create_request: Callable[[ActorRef[Replicator.UpdateResponse[A]]], Replicator.Update[A]],
        response_adapter: Callable[[Replicator.UpdateResponse[A]], M],
    ) -> None:
        self._ask(
            create_request,
            response_adapter,
            lambda key: response_adapter(Replicator.UpdateTimeout(key)),
        )

    def ask_get(
        self,
        create_request: Callable[[ActorRef[Replicator.GetResponse[A]]], Replicator.Get[A]],
        response_adapter: Callable[[Replicator.GetResponse[A]], M],
    ) -> None:
        self._ask(
            create_request,
            response_adapter,
            lambda key: response_adapter(Replicator.GetFailure(key)),
        )

    def ask_delete(
        self,
        create_request: Callable[[ActorRef[Replicator.DeleteResponse[A]]], Replicator.Delete[A]],
        response_adapter: Callable[[Replicator.DeleteResponse[A]], M],
    ) -> None:
        self._ask(
            create_request,
            response_adapter,
            lambda key: response_adapter(Replicator.DeleteFailure(key)),
        )

    def ask_replica_count(
        self,
        create_request: Callable[[ActorRef[Replicator.ReplicaCount]], Replicator.GetReplicaCount],
        response_adapter: Callable[[Replicator.ReplicaCount], M],
    ) -> None:
        def adapt(result: Try[Replicator.ReplicaCount]) -> M:
            match result:
                case Success(value):
                    return response_adapter(value)
                case Failure(exception):
                    raise exception  # the unexpected ask timeout, as Pekko

        self._context.ask(self._replicator, create_request, adapt, self._ask_timeout)

    def _ask[R](
        self,
        create_request: Callable[[ActorRef[R]], _KeyedRequest[A, R]],
        response_adapter: Callable[[R], M],
        timed_out: Callable[[Key[A]], M],
    ) -> None:
        sent: list[_KeyedRequest[A, R]] = []

        def make(reply_to: ActorRef[R]) -> _KeyedRequest[A, R]:
            sent.append(create_request(reply_to))
            return sent[0]

        def adapt(result: Try[R]) -> M:
            match result:
                case Success(value):
                    return response_adapter(value)
                case Failure(AskTimeoutError()):
                    return timed_out(sent[0].key)
                case Failure(exception):
                    raise exception

        self._context.ask(self._replicator, make, adapt, self._ask_timeout)
