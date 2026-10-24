"""ActorSystem: owns the guardian actor. It is itself an ActorRef to the guardian."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from types import TracebackType
from typing import Any, Self

from .behavior import Behavior
from .cell import ActorCell
from .mailbox import MailboxSelector
from .patterns import Replyable, ask
from .ref import ActorRef, FunctionRef
from .remote import Remoting, UniqueAddress, resolve_local, unique_address


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
    ) -> None:
        self.name = name
        self.log = logging.getLogger(f"pekko_py.system.{name}")
        self._dead_letters = logging.getLogger("pekko_py.dead_letters")
        self.address: UniqueAddress | None = unique_address(*address, name) if address else None
        """Set when the system is reachable from other nodes; paths then read pekko://name@host:port/..."""
        self.remoting: Remoting | None = Remoting(self, self.address) if self.address else None
        self._shutdown: asyncio.Task[None] | None = None
        root = self.address.root if self.address else f"pekko://{name}"
        self._root: ActorCell[T] = ActorCell(self, None, "user", f"{root}/user", guardian, mailbox or MailboxSelector())
        self.ignore_ref: ActorRef[Any] = FunctionRef(f"{root}/ignore", lambda _: None)
        """A ref that drops everything: pass it as reply_to when you do not want the reply."""
        if self.remoting is not None:  # remoting closes once the guardian is gone, however it went
            self._root.done.add_done_callback(lambda _: self._close_remoting())

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
        """Stop the guardian and its subtree. Complete the shutdown with when_terminated."""
        self._root.send_sys(_stop_msg())

    def _close_remoting(self) -> None:
        assert self.remoting is not None
        self._shutdown = asyncio.get_running_loop().create_task(self.remoting.close())

    async def when_terminated(self) -> None:
        await asyncio.shield(self._root.done)
        if self._shutdown is not None:
            await asyncio.shield(self._shutdown)

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
