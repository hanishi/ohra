"""ActorSystem: owns the guardian actor. It is itself an ActorRef to the guardian."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from .behavior import Behavior, Behaviors
from .cell import ActorCell
from .mailbox import MailboxSelector
from .patterns import Replyable, ask
from .ref import ActorRef, FunctionRef
from .remote.remoting import Remoting, UniqueAddress, resolve_local, unique_address

if TYPE_CHECKING:
    from .cluster.core import Cluster, ClusterSettings
    from .coordinated_shutdown import CoordinatedShutdownSettings
    from .cluster.sharding import ClusterSharding
    from .cluster.singleton import ClusterSingleton
    from .receptionist import Receptionist


class ActorSystem[T](ActorRef[T]):
    """Create inside a running event loop. Terminates when the guardian stops.

    ``async with ActorSystem(guardian, "name") as system:`` terminates on exit.
    """

    def __init__(
        self,
        guardian: Behavior[T],
        name: str = "system",
        mailbox: MailboxSelector | None = None,
        address: tuple[str, int] | None = None,
        cluster: ClusterSettings | None = None,
        shutdown: CoordinatedShutdownSettings | None = None,
    ) -> None:
        self.name = name
        self.log = logging.getLogger(f"pekko_py.system.{name}")
        self._dead_letters = logging.getLogger("pekko_py.dead_letters")
        if cluster is not None and address is None:
            raise ValueError("a cluster node needs an address=(host, port)")
        self.address: UniqueAddress | None = unique_address(*address, name) if address else None
        """Set when the system is reachable from other nodes; paths then read pekko://name@host:port/..."""
        self.remoting: Remoting | None = Remoting(self, self.address) if self.address else None
        self._shutdown: asyncio.Task[None] | None = None
        self._stop_requested = False  # set by _final_terminate; a guardian stopping on its own leaves it False
        root = self.address.root if self.address else f"pekko://{name}"
        self._root: ActorCell[T] = ActorCell(self, None, "user", f"{root}/user", guardian, mailbox or MailboxSelector())
        self.ignore_ref: ActorRef[Any] = FunctionRef(f"{root}/ignore", lambda _: None)
        """A ref that drops everything: pass it as reply_to when you do not want the reply."""
        self._system_root: ActorCell[Any] = ActorCell(self, None, "system", f"{root}/system", Behaviors.empty)
        """Pekko's /system guardian: hosts the receptionist and the cluster daemon."""
        self.receptionist: Receptionist | None = None
        """The receptionist extension, reached as ``Receptionist(system)``; started below, as Pekko's."""
        self.cluster: Cluster | None = None
        """The cluster extension, reached as ``Cluster(system)``."""
        self.cluster_singleton: ClusterSingleton | None = None
        """The singleton extension, reached as ``ClusterSingleton(system)``."""
        self.cluster_sharding: ClusterSharding | None = None
        """The cluster sharding extension, reached as ``ClusterSharding(system)``."""
        from .coordinated_shutdown import CoordinatedShutdown as _CoordinatedShutdown
        from .coordinated_shutdown import CoordinatedShutdownSettings as _CSSettings

        self.coordinated_shutdown = _CoordinatedShutdown._start(self, shutdown or _CSSettings())  # pyright: ignore[reportPrivateUsage]
        """Pekko's CoordinatedShutdown, reached as ``CoordinatedShutdown(system)``; before the cluster, which adds its tasks."""
        if cluster is not None:
            from .cluster.core import Cluster as _Cluster

            self.cluster = _Cluster._start(self, cluster)  # pyright: ignore[reportPrivateUsage]
        from .receptionist import Receptionist as _Receptionist

        _Receptionist(self)  # after the cluster, so a cluster node's receptionist replicates from the start
        # Shutdown order: user root, then system root, then remoting, however the user root went.
        self._root.done.add_done_callback(lambda _: self._user_root_stopped())
        self._system_root.done.add_done_callback(lambda _: self._close_remoting())
        loop = asyncio.get_running_loop()
        for sig in self.coordinated_shutdown.settings.run_on_signals:
            loop.add_signal_handler(sig, self._on_signal)
        if self.coordinated_shutdown.settings.run_on_signals:
            self._system_root.done.add_done_callback(lambda _: self._remove_signal_handlers())

    @property
    def path(self) -> str:
        return self._root.path

    def tell(self, msg: T) -> None:
        self._root.send_user(msg)

    async def offer(self, msg: T) -> None:
        await self._root.offer(msg)

    async def ask[Res](self, make: Callable[[ActorRef[Res]], Replyable[Res]], timeout: float) -> Res:
        return await ask(self, make, timeout)

    def resolve(self, path: str) -> ActorRef[Any]:
        """The ref at a path: a local actor, a temp ref, or a RemoteRef on another node (needs an address).
        Pekko's ActorRefResolver."""
        if self.remoting is not None:
            return self.remoting.resolve(path)
        return resolve_local(self, path)

    def terminate(self) -> None:
        """Run the coordinated shutdown, whose last phase stops the guardian and its subtree, then the system root;
        complete it with when_terminated. With ``run_by_actor_system_terminate=False`` the roots stop at once."""
        if self._system_root.done.done():
            return
        if self.coordinated_shutdown.settings.run_by_actor_system_terminate:
            from .coordinated_shutdown import ActorSystemTerminateReason

            self.coordinated_shutdown.run(ActorSystemTerminateReason())
        else:
            self._final_terminate()

    def _final_terminate(self) -> None:
        """Stop the guardian and its subtree, then the system root (Pekko's finalTerminate)."""
        self._stop_requested = True
        if self._root.done.done():  # the guardian already stopped on its own: only /system is left
            self._system_root.send_sys(_stop_msg())
        else:
            self._root.send_sys(_stop_msg())

    def _on_signal(self) -> None:
        from .coordinated_shutdown import JvmExitReason

        if not self._system_root.done.done():
            self.coordinated_shutdown.run(JvmExitReason())

    def _remove_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in self.coordinated_shutdown.settings.run_on_signals:
            loop.remove_signal_handler(sig)

    def _user_root_stopped(self) -> None:
        if self.remoting is not None:
            self.remoting.listening.set()  # a cluster daemon still waiting to listen must not block the stop
        if not self._stop_requested and self.coordinated_shutdown.settings.run_by_actor_system_terminate:
            # The guardian stopped on its own: Pekko Typed's guardian wrapper turns that into terminate(), so a cluster
            # node still leaves gracefully; the phases run after the user actors are gone.
            from .coordinated_shutdown import ActorSystemTerminateReason

            self.coordinated_shutdown.run(ActorSystemTerminateReason())
            return
        self._system_root.send_sys(_stop_msg())

    def _close_remoting(self) -> None:
        if self.remoting is not None and self._shutdown is None:
            self._shutdown = asyncio.get_running_loop().create_task(self.remoting.close())

    async def when_terminated(self) -> None:
        await asyncio.shield(self._root.done)
        await asyncio.shield(self._system_root.done)
        self._close_remoting()  # the done callback above may still be queued behind this very resumption
        if self._shutdown is not None:
            await asyncio.shield(self._shutdown)
        run = self.coordinated_shutdown._run  # pyright: ignore[reportPrivateUsage]
        if run is not None:
            await asyncio.shield(run)  # the phases after the stop are only the run's own bookkeeping; no task outlives us

    @property
    def is_terminated(self) -> bool:
        return self._root.done.done()

    def dead_letter(self, msg: object, recipient: ActorRef[Any]) -> None:
        self._dead_letters.debug("dead letter to %s: %r", recipient.path, msg)

    def unhandled(self, msg: object, recipient: ActorRef[Any]) -> None:
        self._dead_letters.debug("unhandled by %s: %r", recipient.path, msg)

    async def __aenter__(self) -> Self:
        if self.remoting is not None:
            try:
                await self.remoting.listen()
            except BaseException:  # the port is taken or the host is wrong: do not leave the guardian running
                self.terminate()
                await self.when_terminated()
                raise
        return self

    async def __aexit__(
        self, et: type[BaseException] | None, e: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.terminate()
        await self.when_terminated()


def _stop_msg() -> object:
    from .cell import _STOP  # pyright: ignore[reportPrivateUsage]

    return _STOP
