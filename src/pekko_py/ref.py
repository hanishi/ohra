"""Actor references. ``ActorRef[T]`` is contravariant: a ref accepting a base
message type can be used where a ref to a subtype is expected."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .cell import ActorCell

_dead_letters = logging.getLogger("pekko_py.dead_letters")


class ActorRef[T](ABC):
    __slots__ = ("__weakref__",)  # refs can be weakly held, e.g. by remoting's temp table

    @property
    @abstractmethod
    def path(self) -> str: ...

    @abstractmethod
    def tell(self, msg: T) -> None: ...

    async def offer(self, msg: T) -> None:
        """Like tell, but on a full bounded mailbox wait for room instead of dropping or raising.
        Backpressure for producers that can await; tell never waits."""
        self.tell(msg)

    def __repr__(self) -> str:
        return f"ActorRef({self.path})"


class LocalRef[T](ActorRef[T]):
    __slots__ = ("_cell",)

    def __init__(self, cell: ActorCell[T]) -> None:
        self._cell = cell

    @property
    def path(self) -> str:
        return self._cell.path

    def tell(self, msg: T) -> None:
        self._cell.send_user(msg)

    async def offer(self, msg: T) -> None:
        await self._cell.offer(msg)


class PromiseRef[T](ActorRef[T]):
    """Temporary ref used by ask: the first message completes the future, later ones are dead
    letters."""

    __slots__ = ("_future", "_path")

    def __init__(self, future: asyncio.Future[T], path: str) -> None:
        self._future = future
        self._path = path

    @property
    def path(self) -> str:
        return self._path

    def tell(self, msg: T) -> None:
        if self._future.done():
            _dead_letters.debug(
                "dead letter to %s (ask already completed or timed out): %r", self._path, msg
            )
        else:
            self._future.set_result(msg)


class FunctionRef[T](ActorRef[T]):
    """A ref that runs a callback on tell. Used for message adapters, which name the actor they
    belong to in ``owner`` so that watching the adapter watches that actor."""

    __slots__ = ("_fn", "_path", "owner")

    def __init__(
        self, path: str, fn: Callable[[T], None], owner: ActorCell[Any] | None = None
    ) -> None:
        self._path = path
        self._fn = fn
        self.owner = owner

    @property
    def path(self) -> str:
        return self._path

    def tell(self, msg: T) -> None:
        self._fn(msg)
