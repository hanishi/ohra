"""``ActorSource.actor_ref``, a port of Apache Pekko's ``ActorSource.actorRef`` (stream-typed),
after its ``ActorRefSource`` stage.

A ``Source`` whose materialized value is a ref that actors tell; each run materializes a new one.
Elements are buffered up to ``buffer_size`` and the overflow strategy decides what a full buffer
does. A message matched by ``completion_matcher`` completes the stream once the buffer is drained;
one matched by ``failure_matcher`` fails it at once, ahead of buffered elements. Once the stream
has completed, failed or been cancelled, tells are dead letters."""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections import deque
from collections.abc import Callable
from enum import Enum
from typing import TYPE_CHECKING, Self

from ..ref import ActorRef
from .dsl import Source

if TYPE_CHECKING:
    from .dsl import _Ctx  # pyright: ignore[reportPrivateUsage]

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
    """Fails the stream when the buffer overflows under ``OverflowStrategy.fail``."""


class _SourceRef[T](ActorRef[T]):
    __slots__ = ("_path", "_source")

    def __init__(self, source: _ActorRefSource[T], path: str) -> None:
        self._source = source
        self._path = path

    @property
    def path(self) -> str:
        return self._path

    def tell(self, msg: T) -> None:
        self._source._receive(msg)  # pyright: ignore[reportPrivateUsage]


class ActorSource:
    """Pekko's stream-typed ``ActorSource``."""

    @staticmethod
    def actor_ref[T](
        completion_matcher: Callable[[T], bool],
        failure_matcher: Callable[[T], BaseException | None],
        buffer_size: int,
        overflow_strategy: OverflowStrategy,
    ) -> Source[T, ActorRef[T]]:
        """A source that actors feed by telling its materialized ref. With ``buffer_size=0`` an
        element is dropped unless downstream is already pulling."""
        if buffer_size < 0:
            raise ValueError("bufferSize must be greater than or equal to 0")
        if overflow_strategy is OverflowStrategy.backpressure:
            raise ValueError("Backpressure overflowStrategy not supported")

        def build(ctx: _Ctx) -> tuple[_ActorRefSource[T], ActorRef[T]]:
            stage = _ActorRefSource(
                completion_matcher, failure_matcher, buffer_size, overflow_strategy, ctx
            )
            return stage, stage.ref

        return Source(build)


class _ActorRefSource[T]:
    """One run's stage: the ref's buffer, and the iterator downstream pulls. ``aclose()`` is a
    plain method, so a stream cancelled before its first pull still detaches the ref."""

    def __init__(
        self,
        completion_matcher: Callable[[T], bool],
        failure_matcher: Callable[[T], BaseException | None],
        buffer_size: int,
        overflow_strategy: OverflowStrategy,
        ctx: _Ctx,
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
        self._failure_listeners: list[Callable[[BaseException], None]] = []
        self.ref: ActorRef[T] = _SourceRef(self, f"/temp/$source-{next(_ids)}")
        ctx.on_abort(self.cancel)  # the run stopped under the stream, perhaps before a pull

    def cancel(self) -> None:
        """Pekko's cancel from downstream: the buffer is discarded and later tells are dead
        letters."""
        self._stopped = True
        self._buffer.clear()
        self._wake()

    async def aclose(self) -> None:
        self.cancel()

    def on_failure(self, listener: Callable[[BaseException], None]) -> None:
        """Pekko fails downstream at once, pulled or not; here a failure travels with the next
        pull, so the stage right below (``watch_termination``) listens for it here too."""
        self._failure_listeners.append(listener)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        while True:
            if self._failure is not None:
                failure, self._failure = self._failure, None
                raise failure
            if self._buffer:
                return self._buffer.popleft()
            if self._completing or self._stopped:
                raise StopAsyncIteration
            self._waiter = asyncio.get_running_loop().create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None

    def _receive(self, msg: T) -> None:
        if self._stopped or self._completing and not self._buffer:
            _dead_letters.debug("dead letter to %s (source ended): %r", self.ref.path, msg)
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
                self.ref.path,
            )
        elif self._buffer_size == 0:
            if self._waiter is None:
                _log.debug(
                    "Dropping element because there is no demand and no buffer: [%r] in %s",
                    msg,
                    self.ref.path,
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
        for listener in self._failure_listeners:
            listener(failure)

    def _wake(self) -> None:
        waiter, self._waiter = self._waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
