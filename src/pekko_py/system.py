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


class ActorSystem[T](ActorRef[T]):
    """Create inside a running event loop. Terminates when the guardian stops.

    ``async with ActorSystem(guardian, "name") as system:`` terminates on exit.
    """

    def __init__(
        self, guardian: Behavior[T], name: str = "system", mailbox: MailboxSelector | None = None
    ) -> None:
        self.name = name
        self.log = logging.getLogger(f"pekko_py.system.{name}")
        self._dead_letters = logging.getLogger("pekko_py.dead_letters")
        self._root: ActorCell[T] = ActorCell(
            self, None, "user", f"pekko://{name}/user", guardian, mailbox or MailboxSelector()
        )
        self.ignore_ref: ActorRef[Any] = FunctionRef(f"pekko://{name}/ignore", lambda _: None)
        """A ref that drops everything: pass it as reply_to when you do not want the reply."""

    @property
    def path(self) -> str:
        return self._root.path

    def tell(self, msg: T) -> None:
        self._root.send_user(msg)

    async def offer(self, msg: T) -> None:
        await self._root.offer(msg)

    async def ask[Res](self, make: Callable[[ActorRef[Res]], Replyable[Res]], timeout: float) -> Res:
        return await ask(self, make, timeout)

    def terminate(self) -> None:
        self._root.send_sys(_stop_msg())

    async def when_terminated(self) -> None:
        await asyncio.shield(self._root.done)

    @property
    def is_terminated(self) -> bool:
        return self._root.done.done()

    def dead_letter(self, msg: object, recipient: ActorRef[Any]) -> None:
        self._dead_letters.debug("dead letter to %s: %r", recipient.path, msg)

    def unhandled(self, msg: object, recipient: ActorRef[Any]) -> None:
        self._dead_letters.debug("unhandled by %s: %r", recipient.path, msg)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, et: type[BaseException] | None, e: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.terminate()
        await self.when_terminated()


def _stop_msg() -> object:
    from .cell import _STOP  # pyright: ignore[reportPrivateUsage]

    return _STOP
