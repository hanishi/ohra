"""Apache Pekko's scaladsl ``Source``, ``Flow``, ``Sink``, ``RunnableGraph`` and ``Keep``, as
typed blueprints over async iterators.

A stage is an immutable value holding a build function; each run calls it again, so a blueprint
can run any number of times. Demand is the pull: the sink calls ``__anext__``, and each stage
pulls from the one above. A failure goes downstream as the exception, and a cancel goes upstream
as ``aclose()``. The whole graph runs fused in one task, the one ``Materializer`` drives."""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
)
from typing import TYPE_CHECKING, Any, Final, Generic, Never, TypeVar, final

from . import ops
from .ops import Stage, filter_op, map_op

if TYPE_CHECKING:
    from ..scheduler import Cancellable
    from ..system import ActorSystem
    from .actor_source import OverflowStrategy
    from .queue import SinkQueue, SourceQueue


@final
class NotUsed:
    """Pekko's ``NotUsed``: the materialized value of a stage that has none worth keeping."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "NOT_USED"


@final
class Done:
    """Pekko's ``Done``: a completion that carries no value."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "DONE"


NOT_USED: Final = NotUsed()
DONE: Final = Done()


class AbruptStageTerminationError(Exception):
    """Pekko's ``AbruptStageTerminationException``: the stream's actor stopped while the stream
    ran, for example because the actor system terminated."""


# Pekko's pekko.stream.materializer.sync-processing-limit: a source that never suspends lets the
# event loop run after this many elements, so an endless stream can still be terminated.
_SYNC_PROCESSING_LIMIT = 1000


class _Ctx:
    """One per run: what a stage may need while it is built, and whether the run was aborted.

    A run is aborted when the stream's actor stops under it (``terminate()``), as opposed to a
    stage cancelling upstream. The sink drive calls ``abort()`` when it is cancelled; the
    materializer calls it when the drive ends without ever having started. Stages that own
    materialized state (queues, futures) register a callback with ``on_abort`` at build time that
    settles it. Callbacks run once and must be idempotent with the stage's normal settling."""

    __slots__ = ("_callbacks", "aborted", "system")

    def __init__(self, system: ActorSystem[Any]) -> None:
        self.system = system
        self.aborted = False
        self._callbacks: list[Callable[[], object]] = []

    def on_abort(self, callback: Callable[[], object]) -> None:
        if self.aborted:  # registered late: the run is already aborted
            callback()
        else:
            self._callbacks.append(callback)

    def abort(self) -> None:
        self.aborted = True
        callbacks, self._callbacks = self._callbacks, []
        for callback in callbacks:
            callback()


# Declared, not inferred, against the 3.12 generics rule: PEP 695 auto-variance across these
# mutually referencing classes depends on pyright's evaluation order. Pekko declares them so:
# Source[+Out, +Mat], Flow[-In, +Out, +Mat], Sink[-In, +Mat], RunnableGraph[+Mat].
_Out_co = TypeVar("_Out_co", covariant=True)
_In_contra = TypeVar("_In_contra", contravariant=True)
_Mat_co = TypeVar("_Mat_co", covariant=True)

type _SourceBuild[Out, Mat] = Callable[[_Ctx], tuple[AsyncIterator[Out], Mat]]
type _FlowBuild[In, Out, Mat] = Callable[[AsyncIterator[In], _Ctx], tuple[AsyncIterator[Out], Mat]]
type _SinkBuild[In, Mat] = Callable[
    [AsyncIterator[In], _Ctx], tuple[Coroutine[Any, Any, None], Mat]
]
type _GraphBuild[Mat] = Callable[[_Ctx], tuple[Coroutine[Any, Any, None], Mat]]


class Keep:
    """How ``via_mat`` and ``to_mat`` combine two materialized values."""

    @staticmethod
    def left[L](left: L, right: object) -> L:
        return left

    @staticmethod
    def right[R](left: object, right: R) -> R:
        return right

    @staticmethod
    def both[L, R](left: L, right: R) -> tuple[L, R]:
        return left, right

    @staticmethod
    def none(left: object, right: object) -> NotUsed:
        return NOT_USED


class Source(Generic[_Out_co, _Mat_co]):
    """A stage with one output: Pekko's ``Source[Out, Mat]``."""

    def __init__(self, build: _SourceBuild[_Out_co, _Mat_co]) -> None:
        self._build: Final = build

    # --- factories ---------------------------------------------------------------------

    @staticmethod
    def single[T](element: T) -> Source[T, NotUsed]:
        return Source.from_iterable((element,))

    @staticmethod
    def from_iterable[T](iterable: Iterable[T]) -> Source[T, NotUsed]:
        """Iterates ``iterable`` on each run, as Pekko's ``Source(iterable)``: a collection runs
        afresh every time, while a one-shot iterator is consumed by the first run."""
        return Source(lambda _: (_iterate(iterable), NOT_USED))

    @staticmethod
    def from_async_iterable[T](factory: Callable[[], AsyncIterable[T]]) -> Source[T, NotUsed]:
        """Calls ``factory`` on the first pull of each run, as Pekko's ``Source.fromIterator``,
        and closes the iterator when the stream is cancelled."""
        return Source(lambda _: (_iterate_async(factory), NOT_USED))

    @staticmethod
    def empty() -> Source[Never, NotUsed]:
        return Source(lambda _: (_iterate(()), NOT_USED))

    @staticmethod
    def failed(exc: Exception) -> Source[Never, NotUsed]:
        return Source(lambda _: (_fail(exc), NOT_USED))

    @staticmethod
    def future[T](aw: Awaitable[T]) -> Source[T, NotUsed]:
        """Emits the result of ``aw``, or fails with its exception. A coroutine can be awaited
        once, so a ``Source.future`` of one runs once; pass a future to run it more often."""
        return Source(lambda _: (_await(aw), NOT_USED))

    @staticmethod
    def repeat[T](element: T) -> Source[T, NotUsed]:
        return Source.from_iterable(itertools.repeat(element))

    @staticmethod
    def queue[T](
        buffer_size: int, overflow_strategy: OverflowStrategy
    ) -> Source[T, SourceQueue[T]]:
        """A source fed from outside: offer elements to the materialized ``SourceQueue``."""
        from .queue import source_queue_build

        if buffer_size < 0:
            raise ValueError("Buffer size must be greater than or equal to 0")
        return Source(lambda ctx: source_queue_build(buffer_size, overflow_strategy, ctx))

    # --- combining -----------------------------------------------------------------------

    def _via_op[O](
        self, op: Callable[[AsyncIterator[_Out_co], _Ctx], AsyncIterator[O]]
    ) -> Source[O, _Mat_co]:
        """The stage every operator method delegates to."""
        build = self._build

        def via(ctx: _Ctx) -> tuple[AsyncIterator[O], _Mat_co]:
            up, mat = build(ctx)
            return Stage(op(up, ctx), up), mat

        return Source(via)

    def via[O](self, flow: Flow[_Out_co, O, Any]) -> Source[O, _Mat_co]:
        return self.via_mat(flow, Keep.left)

    def via_mat[O, M2, M3](
        self, flow: Flow[_Out_co, O, M2], combine: Callable[[_Mat_co, M2], M3]
    ) -> Source[O, M3]:
        build, flow_build = self._build, flow._build  # pyright: ignore[reportPrivateUsage]

        def via(ctx: _Ctx) -> tuple[AsyncIterator[O], M3]:
            up, mat = build(ctx)
            out, flow_mat = flow_build(up, ctx)
            return out, combine(mat, flow_mat)

        return Source(via)

    def to(self, sink: Sink[_Out_co, Any]) -> RunnableGraph[_Mat_co]:
        return self.to_mat(sink, Keep.left)

    def to_mat[M2, M3](
        self, sink: Sink[_Out_co, M2], combine: Callable[[_Mat_co, M2], M3]
    ) -> RunnableGraph[M3]:
        build, sink_build = self._build, sink._build  # pyright: ignore[reportPrivateUsage]

        def graph(ctx: _Ctx) -> tuple[Coroutine[Any, Any, None], M3]:
            up, mat = build(ctx)
            drive, sink_mat = sink_build(up, ctx)
            try:
                return drive, combine(mat, sink_mat)
            except BaseException:
                drive.close()  # never started: it must not warn "never awaited"
                raise

        return RunnableGraph(graph)

    def map_materialized_value[M2](self, f: Callable[[_Mat_co], M2]) -> Source[_Out_co, M2]:
        build = self._build

        def mapped(ctx: _Ctx) -> tuple[AsyncIterator[_Out_co], M2]:
            out, mat = build(ctx)
            return out, f(mat)

        return Source(mapped)

    def run_with[M2](self, sink: Sink[_Out_co, M2], system: ActorSystem[Any]) -> M2:
        return self.to_mat(sink, Keep.right).run(system)

    def run_foreach(
        self, f: Callable[[_Out_co], None], system: ActorSystem[Any]
    ) -> asyncio.Future[Done]:
        return self.run_with(Sink.foreach(f), system)

    def run_fold[S](
        self, zero: S, f: Callable[[S, _Out_co], S], system: ActorSystem[Any]
    ) -> asyncio.Future[S]:
        return self.run_with(Sink.fold(zero, f), system)

    def run_reduce[T](
        self: Source[T, Any], f: Callable[[T, T], T], system: ActorSystem[Any]
    ) -> asyncio.Future[T]:
        # self typed, as Pekko's runReduce[U >: Out]: a covariant Out cannot type f's parameters
        return self.run_with(Sink.reduce(f), system)

    @staticmethod
    def tick[T](initial_delay: float, interval: float, tick: T) -> Source[T, Cancellable]:
        """Emits ``tick`` every ``interval`` after ``initial_delay``, dropping ticks that find no
        demand. Cancel the materialized ``Cancellable`` to complete the stream."""
        if interval <= 0:
            raise ValueError("interval must be > 0")
        return Source(lambda ctx: (t := ops.TickSource(ctx, initial_delay, interval, tick), t))

    # --- operators -----------------------------------------------------------------------

    def map[O](self, f: Callable[[_Out_co], O]) -> Source[O, _Mat_co]:
        return self._via_op(map_op(f))

    def map_async[O](
        self, parallelism: int, f: Callable[[_Out_co], Awaitable[O]]
    ) -> Source[O, _Mat_co]:
        """At most ``parallelism`` calls of ``f`` in flight; results in input order."""
        return self._via_op(ops.map_async_op(parallelism, f, True))

    def map_async_unordered[O](
        self, parallelism: int, f: Callable[[_Out_co], Awaitable[O]]
    ) -> Source[O, _Mat_co]:
        """As ``map_async``, emitting results in completion order."""
        return self._via_op(ops.map_async_op(parallelism, f, False))

    def filter(self, p: Callable[[_Out_co], bool]) -> Source[_Out_co, _Mat_co]:
        return self._via_op(filter_op(p))

    def filter_not(self, p: Callable[[_Out_co], bool]) -> Source[_Out_co, _Mat_co]:
        return self._via_op(ops.filter_not_op(p))

    def collect[O](self, pf: Callable[[_Out_co], O | None]) -> Source[O, _Mat_co]:
        """Python's stand-in for a partial function: ``None`` skips the element."""
        return self._via_op(ops.collect_op(pf))

    def map_concat[O](self, f: Callable[[_Out_co], Iterable[O]]) -> Source[O, _Mat_co]:
        return self._via_op(ops.map_concat_op(f))

    def take(self, n: int) -> Source[_Out_co, _Mat_co]:
        return self._via_op(ops.take_op(n))

    def take_while(
        self, p: Callable[[_Out_co], bool], inclusive: bool = False
    ) -> Source[_Out_co, _Mat_co]:
        return self._via_op(ops.take_while_op(p, inclusive))

    def drop(self, n: int) -> Source[_Out_co, _Mat_co]:
        return self._via_op(ops.drop_op(n))

    def drop_while(self, p: Callable[[_Out_co], bool]) -> Source[_Out_co, _Mat_co]:
        return self._via_op(ops.drop_while_op(p))

    def grouped(self, n: int) -> Source[list[_Out_co], _Mat_co]:
        return self._via_op(ops.grouped_op(n))

    def grouped_within(self, n: int, d: float) -> Source[list[_Out_co], _Mat_co]:
        """Groups of up to ``n`` elements, closed after ``d`` seconds if not full; never empty."""
        return self._via_op(ops.grouped_within_op(n, d))

    def throttle(
        self, elements: int, per: float, maximum_burst: int | None = None
    ) -> Source[_Out_co, _Mat_co]:
        """At most ``elements`` per ``per`` seconds, bursting up to ``maximum_burst`` (default:
        Pekko's automatic burst). Shaping mode: excess elements wait."""
        return self._via_op(ops.throttle_op(elements, per, maximum_burst))

    def scan[S](self, zero: S, f: Callable[[S, _Out_co], S]) -> Source[S, _Mat_co]:
        return self._via_op(ops.scan_op(zero, f))

    def fold[S](self, zero: S, f: Callable[[S, _Out_co], S]) -> Source[S, _Mat_co]:
        return self._via_op(ops.fold_op(zero, f))

    def recover[T, M](self: Source[T, M], f: Callable[[Exception], T | None]) -> Source[T, M]:
        """``None`` rethrows, as Pekko's unmatched partial function."""
        return self._via_op(ops.recover_op(f))

    def recover_with_retries[T, M](
        self: Source[T, M], attempts: int, f: Callable[[Exception], Source[T, Any] | None]
    ) -> Source[T, M]:
        """Switch to the source ``f`` returns, at most ``attempts`` times (negative: no limit)."""
        return self._via_op(ops.recover_with_retries_op(attempts, f))

    def merge[U](
        self, other: Source[U, Any], eager_complete: bool = False
    ) -> Source[_Out_co | U, _Mat_co]:
        """Elements of both, as they arrive. Completes when both complete, or when either does
        with ``eager_complete``. The union is Pekko's ``merge[U >: Out]``."""
        return self._via_op(ops.merge_op(other, eager_complete))

    def zip[O](self, other: Source[O, Any]) -> Source[tuple[_Out_co, O], _Mat_co]:
        """Pairs of one element from each; completes when either completes."""
        return self.zip_with(other, lambda a, b: (a, b))

    def zip_with[O, R](
        self, other: Source[O, Any], f: Callable[[_Out_co, O], R]
    ) -> Source[R, _Mat_co]:
        """``f`` of one element from each; completes when either completes."""
        return self._via_op(ops.zip_with_op(other, f))

    def concat[U](self, other: Source[U, Any]) -> Source[_Out_co | U, _Mat_co]:
        """This source's elements, then ``other``'s, pulled once this one completes."""
        return self._via_op(ops.concat_op(other))

    def watch_termination[M2](
        self, combine: Callable[[_Mat_co, asyncio.Future[Done]], M2]
    ) -> Source[_Out_co, M2]:
        build = self._build

        def watch(ctx: _Ctx) -> tuple[AsyncIterator[_Out_co], M2]:
            up, mat = build(ctx)
            fut = ops.new_done_future()
            return ops.watch_stage(up, ctx, fut), combine(mat, fut)

        return Source(watch)


class Flow(Generic[_In_contra, _Out_co, _Mat_co]):
    """A stage with one input and one output: Pekko's ``Flow[In, Out, Mat]``."""

    def __init__(self, build: _FlowBuild[_In_contra, _Out_co, _Mat_co]) -> None:
        self._build: Final = build

    @staticmethod
    def of[T](t: type[T]) -> Flow[T, T, NotUsed]:
        """The identity flow of ``t``s, to start a ``Flow`` from: Pekko javadsl's
        ``Flow.of(Integer.class)``."""
        return Flow[T, T, NotUsed](lambda up, _: (up, NOT_USED))

    def _via_op[O](
        self, op: Callable[[AsyncIterator[_Out_co], _Ctx], AsyncIterator[O]]
    ) -> Flow[_In_contra, O, _Mat_co]:
        """The stage every operator method delegates to."""
        build = self._build

        def via(
            upstream: AsyncIterator[_In_contra], ctx: _Ctx
        ) -> tuple[AsyncIterator[O], _Mat_co]:
            up, mat = build(upstream, ctx)
            return Stage(op(up, ctx), up), mat

        return Flow(via)

    def via[O](self, flow: Flow[_Out_co, O, Any]) -> Flow[_In_contra, O, _Mat_co]:
        return self.via_mat(flow, Keep.left)

    def via_mat[O, M2, M3](
        self, flow: Flow[_Out_co, O, M2], combine: Callable[[_Mat_co, M2], M3]
    ) -> Flow[_In_contra, O, M3]:
        build, flow_build = self._build, flow._build

        def via(upstream: AsyncIterator[_In_contra], ctx: _Ctx) -> tuple[AsyncIterator[O], M3]:
            up, mat = build(upstream, ctx)
            out, flow_mat = flow_build(up, ctx)
            return out, combine(mat, flow_mat)

        return Flow(via)

    def to(self, sink: Sink[_Out_co, Any]) -> Sink[_In_contra, _Mat_co]:
        return self.to_mat(sink, Keep.left)

    def to_mat[M2, M3](
        self, sink: Sink[_Out_co, M2], combine: Callable[[_Mat_co, M2], M3]
    ) -> Sink[_In_contra, M3]:
        build, sink_build = self._build, sink._build  # pyright: ignore[reportPrivateUsage]

        def to(
            upstream: AsyncIterator[_In_contra], ctx: _Ctx
        ) -> tuple[Coroutine[Any, Any, None], M3]:
            up, mat = build(upstream, ctx)
            drive, sink_mat = sink_build(up, ctx)
            try:
                return drive, combine(mat, sink_mat)
            except BaseException:
                drive.close()  # never started: it must not warn "never awaited"
                raise

        return Sink(to)

    def map_materialized_value[M2](
        self, f: Callable[[_Mat_co], M2]
    ) -> Flow[_In_contra, _Out_co, M2]:
        build = self._build

        def mapped(
            upstream: AsyncIterator[_In_contra], ctx: _Ctx
        ) -> tuple[AsyncIterator[_Out_co], M2]:
            out, mat = build(upstream, ctx)
            return out, f(mat)

        return Flow(mapped)

    def map[O](self, f: Callable[[_Out_co], O]) -> Flow[_In_contra, O, _Mat_co]:
        return self._via_op(map_op(f))

    def map_async[O](
        self, parallelism: int, f: Callable[[_Out_co], Awaitable[O]]
    ) -> Flow[_In_contra, O, _Mat_co]:
        """At most ``parallelism`` calls of ``f`` in flight; results in input order."""
        return self._via_op(ops.map_async_op(parallelism, f, True))

    def map_async_unordered[O](
        self, parallelism: int, f: Callable[[_Out_co], Awaitable[O]]
    ) -> Flow[_In_contra, O, _Mat_co]:
        """As ``map_async``, emitting results in completion order."""
        return self._via_op(ops.map_async_op(parallelism, f, False))

    def filter(self, p: Callable[[_Out_co], bool]) -> Flow[_In_contra, _Out_co, _Mat_co]:
        return self._via_op(filter_op(p))

    def filter_not(self, p: Callable[[_Out_co], bool]) -> Flow[_In_contra, _Out_co, _Mat_co]:
        return self._via_op(ops.filter_not_op(p))

    def collect[O](self, pf: Callable[[_Out_co], O | None]) -> Flow[_In_contra, O, _Mat_co]:
        """Python's stand-in for a partial function: ``None`` skips the element."""
        return self._via_op(ops.collect_op(pf))

    def map_concat[O](self, f: Callable[[_Out_co], Iterable[O]]) -> Flow[_In_contra, O, _Mat_co]:
        return self._via_op(ops.map_concat_op(f))

    def take(self, n: int) -> Flow[_In_contra, _Out_co, _Mat_co]:
        return self._via_op(ops.take_op(n))

    def take_while(
        self, p: Callable[[_Out_co], bool], inclusive: bool = False
    ) -> Flow[_In_contra, _Out_co, _Mat_co]:
        return self._via_op(ops.take_while_op(p, inclusive))

    def drop(self, n: int) -> Flow[_In_contra, _Out_co, _Mat_co]:
        return self._via_op(ops.drop_op(n))

    def drop_while(self, p: Callable[[_Out_co], bool]) -> Flow[_In_contra, _Out_co, _Mat_co]:
        return self._via_op(ops.drop_while_op(p))

    def grouped(self, n: int) -> Flow[_In_contra, list[_Out_co], _Mat_co]:
        return self._via_op(ops.grouped_op(n))

    def grouped_within(self, n: int, d: float) -> Flow[_In_contra, list[_Out_co], _Mat_co]:
        """Groups of up to ``n`` elements, closed after ``d`` seconds if not full; never empty."""
        return self._via_op(ops.grouped_within_op(n, d))

    def throttle(
        self, elements: int, per: float, maximum_burst: int | None = None
    ) -> Flow[_In_contra, _Out_co, _Mat_co]:
        """At most ``elements`` per ``per`` seconds, bursting up to ``maximum_burst`` (default:
        Pekko's automatic burst). Shaping mode: excess elements wait."""
        return self._via_op(ops.throttle_op(elements, per, maximum_burst))

    def scan[S](self, zero: S, f: Callable[[S, _Out_co], S]) -> Flow[_In_contra, S, _Mat_co]:
        return self._via_op(ops.scan_op(zero, f))

    def fold[S](self, zero: S, f: Callable[[S, _Out_co], S]) -> Flow[_In_contra, S, _Mat_co]:
        return self._via_op(ops.fold_op(zero, f))

    def recover[I, T, M](self: Flow[I, T, M], f: Callable[[Exception], T | None]) -> Flow[I, T, M]:
        """``None`` rethrows, as Pekko's unmatched partial function."""
        return self._via_op(ops.recover_op(f))

    def recover_with_retries[I, T, M](
        self: Flow[I, T, M], attempts: int, f: Callable[[Exception], Source[T, Any] | None]
    ) -> Flow[I, T, M]:
        """Switch to the source ``f`` returns, at most ``attempts`` times (negative: no limit)."""
        return self._via_op(ops.recover_with_retries_op(attempts, f))

    def merge[U](
        self, other: Source[U, Any], eager_complete: bool = False
    ) -> Flow[_In_contra, _Out_co | U, _Mat_co]:
        """Elements of both, as they arrive. Completes when both complete, or when either does
        with ``eager_complete``. The union is Pekko's ``merge[U >: Out]``."""
        return self._via_op(ops.merge_op(other, eager_complete))

    def zip[O](self, other: Source[O, Any]) -> Flow[_In_contra, tuple[_Out_co, O], _Mat_co]:
        """Pairs of one element from each; completes when either completes."""
        return self.zip_with(other, lambda a, b: (a, b))

    def zip_with[O, R](
        self, other: Source[O, Any], f: Callable[[_Out_co, O], R]
    ) -> Flow[_In_contra, R, _Mat_co]:
        """``f`` of one element from each; completes when either completes."""
        return self._via_op(ops.zip_with_op(other, f))

    def concat[U](self, other: Source[U, Any]) -> Flow[_In_contra, _Out_co | U, _Mat_co]:
        """This flow's elements, then ``other``'s, pulled once this one completes."""
        return self._via_op(ops.concat_op(other))

    def watch_termination[M2](
        self, combine: Callable[[_Mat_co, asyncio.Future[Done]], M2]
    ) -> Flow[_In_contra, _Out_co, M2]:
        build = self._build

        def watch(
            upstream: AsyncIterator[_In_contra], ctx: _Ctx
        ) -> tuple[AsyncIterator[_Out_co], M2]:
            up, mat = build(upstream, ctx)
            fut = ops.new_done_future()
            return ops.watch_stage(up, ctx, fut), combine(mat, fut)

        return Flow(watch)


class Sink(Generic[_In_contra, _Mat_co]):
    """A stage with one input: Pekko's ``Sink[In, Mat]``. Its build returns the coroutine that
    drives the stream by pulling, and settles the materialized value itself. On cancellation
    (the stream's actor stopped) it fails what it has not settled with
    ``AbruptStageTerminationError``."""

    def __init__(self, build: _SinkBuild[_In_contra, _Mat_co]) -> None:
        self._build: Final = build

    @staticmethod
    def foreach[T](f: Callable[[T], None]) -> Sink[T, asyncio.Future[Done]]:
        async def consume(up: AsyncIterator[T]) -> Done:
            async for x in up:
                f(x)
            return DONE

        return _future_sink(consume)

    @staticmethod
    def seq[T]() -> Sink[T, asyncio.Future[list[T]]]:
        async def consume(up: AsyncIterator[T]) -> list[T]:
            return [x async for x in up]

        return _future_sink(consume)

    @staticmethod
    def fold[T, S](zero: S, f: Callable[[S, T], S]) -> Sink[T, asyncio.Future[S]]:
        async def consume(up: AsyncIterator[T]) -> S:
            acc = zero
            async for x in up:
                acc = f(acc, x)
            return acc

        return _future_sink(consume)

    @staticmethod
    def reduce[T](f: Callable[[T, T], T]) -> Sink[T, asyncio.Future[T]]:
        async def consume(up: AsyncIterator[T]) -> T:
            try:
                acc = await anext(up)
            except StopAsyncIteration:
                raise LookupError("reduce over empty stream") from None
            async for x in up:
                acc = f(acc, x)
            return acc

        return _future_sink(consume)

    @staticmethod
    def head[T]() -> Sink[T, asyncio.Future[T]]:
        """The first element; then it cancels upstream. ``LookupError`` (Pekko's
        ``NoSuchElementException``) for an empty stream."""

        async def consume(up: AsyncIterator[T]) -> T:
            async for x in up:
                return x
            raise LookupError("head of empty stream")

        return _future_sink(consume)

    @staticmethod
    def head_option[T]() -> Sink[T, asyncio.Future[T | None]]:
        async def consume(up: AsyncIterator[T]) -> T | None:
            async for x in up:
                return x
            return None

        return _future_sink(consume)

    @staticmethod
    def last[T]() -> Sink[T, asyncio.Future[T]]:
        """The last element. ``LookupError`` for an empty stream."""

        async def consume(up: AsyncIterator[T]) -> T:
            try:
                last = await anext(up)
            except StopAsyncIteration:
                raise LookupError("last of empty stream") from None
            async for x in up:
                last = x
            return last

        return _future_sink(consume)

    @staticmethod
    def last_option[T]() -> Sink[T, asyncio.Future[T | None]]:
        async def consume(up: AsyncIterator[T]) -> T | None:
            last: T | None = None
            async for x in up:
                last = x
            return last

        return _future_sink(consume)

    @staticmethod
    def queue[T]() -> Sink[T, SinkQueue[T]]:
        """Pull the stream's elements from outside, on demand, from the materialized
        ``SinkQueue``."""
        from .queue import sink_queue_build

        return Sink(lambda up, ctx: sink_queue_build(up, ctx))

    @staticmethod
    def ignore() -> Sink[Any, asyncio.Future[Done]]:
        async def consume(up: AsyncIterator[Any]) -> Done:
            async for _ in up:
                pass
            return DONE

        return _future_sink(consume)

    @staticmethod
    def cancelled() -> Sink[Any, NotUsed]:
        """Cancels upstream at once, pulling nothing."""

        def build(
            upstream: AsyncIterator[Any], ctx: _Ctx
        ) -> tuple[Coroutine[Any, Any, None], NotUsed]:
            async def nothing(_: AsyncIterator[Any]) -> None:
                pass

            async def drive() -> None:
                await ops.run_drive(ctx, upstream, nothing)

            return drive(), NOT_USED

        return Sink(build)


class RunnableGraph(Generic[_Mat_co]):
    """A source connected to a sink: Pekko's ``RunnableGraph[Mat]``."""

    def __init__(self, build: _GraphBuild[_Mat_co]) -> None:
        self._build: Final = build

    def run(self, system: ActorSystem[Any]) -> _Mat_co:
        """Start the stream as a child of the system's stream supervisor; return its
        materialized value."""
        from .materializer import Materializer

        return Materializer(system).materialize(self)


def _future_sink[T, R](
    consume: Callable[[AsyncIterator[T]], Coroutine[Any, Any, R]],
) -> Sink[T, asyncio.Future[R]]:
    """A sink materializing a future that ``consume`` settles; it cancels upstream when
    ``consume`` returns early."""

    def build(
        upstream: AsyncIterator[T], ctx: _Ctx
    ) -> tuple[Coroutine[Any, Any, None], asyncio.Future[R]]:
        fut: asyncio.Future[R] = asyncio.get_running_loop().create_future()
        # a failed future that Keep dropped must not log "exception was never retrieved"
        fut.add_done_callback(lambda f: f.cancelled() or f.exception())

        def abort() -> None:
            if not fut.done():
                fut.set_exception(AbruptStageTerminationError("stream terminated abruptly"))

        ctx.on_abort(abort)

        async def drive() -> None:
            try:
                result = await ops.run_drive(ctx, upstream, consume)
            except Exception as e:
                if not fut.done():
                    fut.set_exception(e)
            else:
                if not fut.done():
                    fut.set_result(result)

        return drive(), fut

    return Sink(build)


async def _iterate[T](iterable: Iterable[T]) -> AsyncGenerator[T, None]:
    for n, x in enumerate(iterable, 1):
        yield x
        if n % _SYNC_PROCESSING_LIMIT == 0:
            await asyncio.sleep(0)


async def _iterate_async[T](factory: Callable[[], AsyncIterable[T]]) -> AsyncGenerator[T, None]:
    it = aiter(factory())
    try:
        n = 0
        async for x in it:
            yield x
            n += 1
            if n % _SYNC_PROCESSING_LIMIT == 0:
                await asyncio.sleep(0)
    finally:
        aclose = getattr(it, "aclose", None)  # an iterator the user wrote may have none
        if aclose is not None:
            await aclose()


async def _fail(exc: Exception) -> AsyncGenerator[Never, None]:
    raise exc
    yield  # makes this an async generator


async def _await[T](aw: Awaitable[T]) -> AsyncGenerator[T, None]:
    # a future is the caller's: cancelling the stream must not cancel it
    yield await (asyncio.shield(aw) if isinstance(aw, asyncio.Future) else aw)
