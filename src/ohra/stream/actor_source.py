"""``ActorSource.actor_ref``, a port of Apache Pekko's ``ActorSource.actorRef`` (stream-typed).

ohra has no streams, so the source and its materialized ref are one object: actors tell its
``ref``, and one consumer iterates it with ``async for``. Elements are buffered up to
``buffer_size`` and the overflow strategy decides what a full buffer does. A message matched by
``completion_matcher`` ends the iteration once the buffer is drained; one matched by
``failure_matcher`` raises its exception in the loop at once, ahead of buffered elements. Once the
iteration has ended or failed, or the source is cancelled (``cancel()``, ``aclose()``, or a
consumer cancelled while it waits), tells are dead letters. A ``break`` doesn't cancel it."""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections import deque
from collections.abc import Callable
from enum import Enum
from typing import Self

from ..ref import ActorRef

_ids = itertools.count()
_dead_letters = logging.getLogger("ohra.dead_letters")
_log = logging.getLogger(__name__)


class OverflowStrategy(Enum):
    """What a full buffer does with a new element. ``backpressure`` is refused by
    ``ActorSource.actor_ref``, as in Pekko: a tell can't wait."""

    drop_head = "drop_head"
    drop_tail = "drop_tail"
    drop_buffer = "drop_buffer"
    fail = "fail"
    backpressure = "backpressure"


class BufferOverflowError(Exception):
    """Raised in the loop when the buffer overflows under ``OverflowStrategy.fail``."""


class _SourceRef[T](ActorRef[T]):
    __slots__ = ("_path", "_source")

    def __init__(self, source: ActorSource[T], path: str) -> None:
        self._source = source
        self._path = path

    @property
    def path(self) -> str:
        return self._path

    def tell(self, msg: T) -> None:
        self._source._receive(msg)  # pyright: ignore[reportPrivateUsage]


class ActorSource[T]:
    """A ref for actors and an async iterator for one consumer. Build it with ``actor_ref``."""

    def __init__(
        self,
        completion_matcher: Callable[[T], bool],
        failure_matcher: Callable[[T], BaseException | None],
        buffer_size: int,
        overflow_strategy: OverflowStrategy,
    ) -> None:
        self._completion_matcher = completion_matcher
        self._failure_matcher = failure_matcher
        self._buffer_size = buffer_size
        self._overflow_strategy = overflow_strategy
        self._buffer: deque[T] = deque()
        self._completing = False
        self._failure: BaseException | None = None
        self._stopped = False
        self._waiter: asyncio.Future[None] | None = None
        self._ref = _SourceRef(self, f"/temp/$source-{next(_ids)}")

    @staticmethod
    def actor_ref[M](
        completion_matcher: Callable[[M], bool],
        failure_matcher: Callable[[M], BaseException | None],
        buffer_size: int,
        overflow_strategy: OverflowStrategy,
    ) -> ActorSource[M]:
        """An outbox that actors tell and a consumer iterates. With ``buffer_size=0`` an
        element is dropped unless the consumer is already waiting for one."""
        if buffer_size < 0:
            raise ValueError("bufferSize must be greater than or equal to 0")
        if overflow_strategy is OverflowStrategy.backpressure:
            raise ValueError("Backpressure overflowStrategy not supported")
        return ActorSource(completion_matcher, failure_matcher, buffer_size, overflow_strategy)

    @property
    def ref(self) -> ActorRef[T]:
        return self._ref

    def cancel(self) -> None:
        """Pekko's cancel from downstream: the buffer is discarded, the loop ends, and later
        tells are dead letters."""
        self._stopped = True
        self._buffer.clear()
        self._wake()

    async def aclose(self) -> None:
        self.cancel()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        """The next element. A ``break`` leaves the rest for a later loop; a consumer cancelled
        while it waits cancels the source."""
        while True:
            if self._failure is not None:
                failure, self._failure = self._failure, None
                raise failure
            if self._buffer:
                return self._buffer.popleft()
            if self._completing or self._stopped:
                raise StopAsyncIteration
            if self._waiter is not None:
                raise RuntimeError("an ActorSource has one consumer")
            self._waiter = asyncio.get_running_loop().create_future()
            try:
                await self._waiter
            except asyncio.CancelledError:
                self.cancel()
                raise
            finally:
                self._waiter = None

    def _receive(self, msg: T) -> None:
        if self._stopped or self._completing and not self._buffer:
            _dead_letters.debug("dead letter to %s (source ended): %r", self._ref.path, msg)
            return
        try:  # a matcher that raises fails the source, as in Pekko, not the actor telling it
            failure = self._failure_matcher(msg)
            completes = failure is None and self._completion_matcher(msg)
        except Exception as e:
            self._fail(e)
            return
        if failure is not None:
            self._fail(failure)
        elif completes:
            self._completing = True
            self._wake()
        elif self._completing:
            _log.warning(
                "Dropping element because the source is completing, only draining already "
                "buffered elements: [%r] (pending: [%d]) in %s",
                msg,
                len(self._buffer),
                self._ref.path,
            )
        elif self._buffer_size == 0:
            if self._waiter is None:
                _log.debug(
                    "Dropping element because there is no demand and no buffer: [%r] in %s",
                    msg,
                    self._ref.path,
                )
            else:
                self._buffer.append(msg)
                self._wake()
        elif len(self._buffer) < self._buffer_size:
            self._buffer.append(msg)
            self._wake()
        else:
            self._overflow(msg)

    def _overflow(self, msg: T) -> None:
        match self._overflow_strategy:
            case OverflowStrategy.drop_head:
                _log.debug("Dropping the head element because the buffer is full in %s", self.ref)
                self._buffer.popleft()
            case OverflowStrategy.drop_tail:
                _log.debug("Dropping the tail element because the buffer is full in %s", self.ref)
                self._buffer.pop()
            case OverflowStrategy.drop_buffer:
                _log.debug(
                    "Dropping all the buffered elements because the buffer is full in %s", self.ref
                )
                self._buffer.clear()
            case OverflowStrategy.fail | OverflowStrategy.backpressure:
                self._fail(
                    BufferOverflowError(
                        f"Buffer overflow (max capacity was: {self._buffer_size})!"
                    )
                )
                return
        self._buffer.append(msg)
        self._wake()

    def _fail(self, failure: BaseException) -> None:
        self._failure = failure
        self._stopped = True
        self._buffer.clear()
        self._wake()

    def _wake(self) -> None:
        waiter, self._waiter = self._waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
