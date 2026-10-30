"""Operators shared by ``Source`` and ``Flow``. Each takes the upstream iterator and the run's
context and returns the downstream iterator, an async generator that holds its upstream in
``contextlib.aclosing``, so closing it cancels upstream at once."""

from __future__ import annotations

import contextlib
import asyncio
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from ..scheduler import Cancellable
    from .dsl import Done, Source, _Ctx  # pyright: ignore[reportPrivateUsage]


def closing[I: AsyncIterator[Any]](it: I) -> contextlib.AbstractAsyncContextManager[I]:
    """``contextlib.aclosing`` for a stage's upstream. Every iterator in a stream has ``aclose``
    (sources and operators are async generators, or a ``Stage``), though ``AsyncIterator``
    doesn't declare it."""
    return contextlib.aclosing(cast(Any, it))


async def run_drive[I, R](
    ctx: _Ctx, upstream: AsyncIterator[I], body: Callable[[AsyncIterator[I]], Awaitable[R]]
) -> R:
    """The shared shape of every sink drive: ``body`` pulls ``upstream`` in a child task, and the
    drive waits for it inside ``closing(upstream)``. When the drive task is cancelled (the run's
    actor stopped) it calls ``ctx.abort()`` first, so that the stages unwinding under the cancel
    see the run aborted, and only then cancels the child and waits for it, so that upstream is
    closed with no task suspended inside it. Waiting with ``asyncio.wait`` keeps the cancel from
    reaching the child before the abort."""
    async with closing(upstream) as up:
        child = asyncio.ensure_future(body(up))
        try:
            await asyncio.wait((child,))
        except asyncio.CancelledError:
            ctx.abort()
            raise
        finally:
            if not child.done():
                child.cancel()
                await asyncio.gather(child, return_exceptions=True)
        return child.result()


class Stage[T]:
    """An operator's iterator, as ``_via_op`` hands it downstream. Closing it closes the
    operator, then its upstream: an async generator closed before its first pull never enters
    its ``aclosing``, so on its own it would leave a source's materialized resources (a queue,
    an actor) open when a stream is cancelled before it pulls (``Sink.cancelled``)."""

    __slots__ = ("_gen", "_on_close", "_upstream")

    def __init__(
        self,
        gen: AsyncIterator[T],
        upstream: AsyncIterator[Any],
        on_close: Callable[[], None] | None = None,
    ) -> None:
        self._gen = gen
        self._upstream = upstream
        self._on_close = on_close  # runs even if the operator never started

    def __aiter__(self) -> Stage[T]:
        return self

    def __anext__(self) -> Awaitable[T]:
        return self._gen.__anext__()

    async def aclose(self) -> None:
        try:
            async with closing(self._upstream), closing(self._gen):  # exits the operator first
                pass
        finally:
            if self._on_close is not None:
                self._on_close()


def map_op[I, O](f: Callable[[I], O]) -> Callable[[AsyncIterator[I], _Ctx], AsyncIterator[O]]:
    async def run(upstream: AsyncIterator[I], _ctx: _Ctx) -> AsyncGenerator[O, None]:
        async with closing(upstream) as up:
            async for x in up:
                yield f(x)

    return run


def filter_op[T](p: Callable[[T], bool]) -> Callable[[AsyncIterator[T], _Ctx], AsyncIterator[T]]:
    async def run(upstream: AsyncIterator[T], _ctx: _Ctx) -> AsyncGenerator[T, None]:
        async with closing(upstream) as up:
            async for x in up:
                if p(x):
                    yield x

    return run


type Op[I, O] = Callable[[AsyncIterator[I], _Ctx], AsyncIterator[O]]


def filter_not_op[T](p: Callable[[T], bool]) -> Op[T, T]:
    return filter_op(lambda x: not p(x))


def collect_op[I, O](pf: Callable[[I], O | None]) -> Op[I, O]:
    async def run(upstream: AsyncIterator[I], _ctx: _Ctx) -> AsyncGenerator[O, None]:
        async with closing(upstream) as up:
            async for x in up:
                y = pf(x)
                if y is not None:
                    yield y

    return run


def map_concat_op[I, O](f: Callable[[I], Iterable[O]]) -> Op[I, O]:
    async def run(upstream: AsyncIterator[I], _ctx: _Ctx) -> AsyncGenerator[O, None]:
        async with closing(upstream) as up:
            async for x in up:
                for y in f(x):
                    yield y

    return run


def take_op[T](n: int) -> Op[T, T]:
    async def run(upstream: AsyncIterator[T], _ctx: _Ctx) -> AsyncGenerator[T, None]:
        async with closing(upstream) as up:
            if n <= 0:
                return
            count = 0
            async for x in up:
                yield x
                count += 1
                if count >= n:
                    return  # without pulling again; aclosing cancels upstream

    return run


def take_while_op[T](p: Callable[[T], bool], inclusive: bool) -> Op[T, T]:
    async def run(upstream: AsyncIterator[T], _ctx: _Ctx) -> AsyncGenerator[T, None]:
        async with closing(upstream) as up:
            async for x in up:
                if p(x):
                    yield x
                else:
                    if inclusive:
                        yield x
                    return

    return run


def drop_op[T](n: int) -> Op[T, T]:
    async def run(upstream: AsyncIterator[T], _ctx: _Ctx) -> AsyncGenerator[T, None]:
        async with closing(upstream) as up:
            left = n
            async for x in up:
                if left > 0:
                    left -= 1
                else:
                    yield x

    return run


def drop_while_op[T](p: Callable[[T], bool]) -> Op[T, T]:
    async def run(upstream: AsyncIterator[T], _ctx: _Ctx) -> AsyncGenerator[T, None]:
        async with closing(upstream) as up:
            dropping = True
            async for x in up:
                if dropping and p(x):
                    continue
                dropping = False
                yield x

    return run


def grouped_op[T](n: int) -> Op[T, list[T]]:
    async def run(upstream: AsyncIterator[T], _ctx: _Ctx) -> AsyncGenerator[list[T], None]:
        async with closing(upstream) as up:
            buf: list[T] = []
            async for x in up:
                buf.append(x)
                if len(buf) == n:
                    out, buf = buf, []
                    yield out
            if buf:
                yield buf

    return run


def scan_op[I, S](zero: S, f: Callable[[S, I], S]) -> Op[I, S]:
    async def run(upstream: AsyncIterator[I], _ctx: _Ctx) -> AsyncGenerator[S, None]:
        async with closing(upstream) as up:
            acc = zero
            yield acc
            async for x in up:
                acc = f(acc, x)
                yield acc

    return run


def fold_op[I, S](zero: S, f: Callable[[S, I], S]) -> Op[I, S]:
    async def run(upstream: AsyncIterator[I], _ctx: _Ctx) -> AsyncGenerator[S, None]:
        async with closing(upstream) as up:
            acc = zero
            async for x in up:
                acc = f(acc, x)
            yield acc

    return run


def recover_op[T](f: Callable[[Exception], T | None]) -> Op[T, T]:
    async def run(upstream: AsyncIterator[T], _ctx: _Ctx) -> AsyncGenerator[T, None]:
        async with closing(upstream) as up:
            while True:
                try:
                    x = await anext(up)
                except StopAsyncIteration:
                    return
                except Exception as e:
                    last = f(e)
                    if last is None:
                        raise
                    yield last  # the recovery element, then completion
                    return
                yield x

    return run


def recover_with_retries_op[T](
    attempts: int, f: Callable[[Exception], Source[T, Any] | None]
) -> Op[T, T]:
    async def run(upstream: AsyncIterator[T], ctx: _Ctx) -> AsyncGenerator[T, None]:
        current: AsyncIterator[T] = upstream
        attempt = 0
        while True:
            try:
                async with closing(current) as up:
                    async for x in up:
                        yield x
                return
            except Exception as e:
                if attempts >= 0 and attempt >= attempts:
                    raise
                source = f(e)
                if source is None:
                    raise
                attempt += 1
                current, _ = source._build(ctx)  # pyright: ignore[reportPrivateUsage]

    return run


def watch_stage[T](up: AsyncIterator[T], ctx: _Ctx, fut: asyncio.Future[Done]) -> Stage[T]:
    """Settles ``fut`` with ``DONE`` on completion and on downstream cancel, with the upstream
    exception on failure, and with ``AbruptStageTerminationError`` when the run is aborted
    (``terminate()``). A ``CancelledError`` passing through is not settled here: it is the
    downstream cancelling a pull (``DONE``, from the close) or the run's abort."""
    from .dsl import DONE, AbruptStageTerminationError

    def settle(error: BaseException | None) -> None:
        if fut.done():
            return
        if error is not None:
            fut.set_exception(error)
        elif ctx.aborted:
            fut.set_exception(AbruptStageTerminationError("stream terminated abruptly"))
        else:
            fut.set_result(DONE)

    ctx.on_abort(lambda: settle(None))

    async def run() -> AsyncGenerator[T, None]:
        try:
            async with closing(up) as u:
                async for x in u:
                    yield x
        except Exception as e:
            settle(e)
            raise
        settle(None)

    return Stage(run(), up, lambda: settle(None))  # the close: downstream cancel, even unstarted


def new_done_future() -> asyncio.Future[Done]:
    fut: asyncio.Future[Done] = asyncio.get_running_loop().create_future()
    # a failure nobody retrieved must not log "exception was never retrieved"
    fut.add_done_callback(lambda f: f.cancelled() or f.exception())
    return fut


def map_async_op[I, O](
    parallelism: int, f: Callable[[I], Awaitable[O]], ordered: bool
) -> Op[I, O]:
    """After Pekko's ``MapAsync`` (``ordered``) and ``MapAsyncUnordered``: pulls upstream only
    while fewer than ``parallelism`` calls are in flight, fails with the first failure, and
    cancels what is still running when the stage ends for any reason."""
    if parallelism < 1:
        raise ValueError("parallelism must be at least 1")

    async def run(upstream: AsyncIterator[I], _ctx: _Ctx) -> AsyncGenerator[O, None]:
        pending: deque[asyncio.Future[O]] = deque()
        pull: asyncio.Future[I] | None = None
        done = False
        async with closing(upstream) as up:
            try:
                while True:
                    if not done and pull is None and len(pending) < parallelism:
                        pull = asyncio.ensure_future(anext(up))
                    if pull is None and not pending:
                        return
                    waiting: set[asyncio.Future[Any]] = set(pending)
                    if pull is not None:
                        waiting.add(pull)
                    await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
                    for t in pending:
                        if t.done() and not t.cancelled() and t.exception() is not None:
                            raise cast(BaseException, t.exception())
                    if pull is not None and pull.done():
                        got, pull = pull, None
                        try:
                            pending.append(asyncio.ensure_future(f(got.result())))
                        except StopAsyncIteration:
                            done = True
                        continue
                    if ordered:
                        while pending and pending[0].done():
                            yield pending.popleft().result()
                    else:
                        for t in [t for t in pending if t.done()]:
                            pending.remove(t)
                            yield t.result()
            finally:
                # before the upstream closes: a pull still suspended inside it would make
                # its aclose() fail with "already running"
                tasks: list[asyncio.Future[Any]] = [*pending]
                if pull is not None:
                    tasks.append(pull)
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    return run


def _timer(ctx: _Ctx, delay: float) -> tuple[asyncio.Future[None], Cancellable]:
    """A future the system scheduler resolves after ``delay``, so ``ManualTime`` controls it."""
    fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def fire() -> None:
        if not fut.done():
            fut.set_result(None)

    return fut, ctx.system.scheduler.schedule_once(delay, fire)


def throttle_op[T](elements: int, per: float, maximum_burst: int | None) -> Op[T, T]:
    """After Pekko's ``Throttle`` in shaping mode, with ``NanoTimeTokenBucket`` on the scheduler's
    clock: an element that finds no token waits for the delay the bucket names."""
    if elements <= 0:
        raise ValueError("elements must be > 0")
    if per <= 0:
        raise ValueError("per time must be > 0")
    between = per / elements
    if maximum_burst is None:  # Pekko's AutomaticMaximumBurst
        capacity = max(1, int(0.1 / between))
    elif maximum_burst < 0:
        raise ValueError("maximum_burst must be >= 0")
    else:
        capacity = maximum_burst

    async def run(upstream: AsyncIterator[T], ctx: _Ctx) -> AsyncGenerator[T, None]:
        scheduler = ctx.system.scheduler
        available = float(capacity)
        last_update = scheduler.current_time
        timer: Cancellable | None = None
        try:
            async with closing(upstream) as up:
                async for x in up:
                    # TokenBucket.offer(1)
                    now = scheduler.current_time
                    arrived = int((now - last_update) / between + 1e-9)
                    if arrived > 0:
                        last_update += arrived * between
                    available = min(available + arrived, float(capacity))
                    if available >= 1:
                        available -= 1
                    else:
                        delay = (1 - available) * between - (now - last_update)
                        available = 0
                        last_update = now + delay
                        fut, timer = _timer(ctx, delay)
                        await fut
                    yield x
        finally:
            if timer is not None:
                timer.cancel()

    return run


def grouped_within_op[T](n: int, d: float) -> Op[T, list[T]]:
    """After Pekko's ``GroupedWeightedWithin`` with every weight 1: a group closes at ``n``
    elements, or when the timer fires with elements buffered. The timer restarts after each group
    and runs on while the buffer is empty, so no group is ever empty. Upstream is pulled in a
    task that races the timer."""
    if n <= 0:
        raise ValueError("max number must be > 0")
    if d <= 0:
        raise ValueError("interval must be > 0")

    async def run(upstream: AsyncIterator[T], ctx: _Ctx) -> AsyncGenerator[list[T], None]:
        buf: list[T] = []
        pull: asyncio.Future[T] | None = None
        timer, cancellable = _timer(ctx, d)
        async with closing(upstream) as up:
            try:
                while True:
                    if pull is None:
                        pull = asyncio.ensure_future(anext(up))
                    await asyncio.wait((pull, timer), return_when=asyncio.FIRST_COMPLETED)
                    if pull.done():
                        got, pull = pull, None
                        try:
                            buf.append(got.result())
                        except StopAsyncIteration:
                            break
                        if len(buf) == n:
                            cancellable.cancel()
                            group, buf = buf, []
                            timer, cancellable = _timer(ctx, d)
                            yield group
                    else:  # the timer fired
                        timer, cancellable = _timer(ctx, d)
                        if buf:
                            group, buf = buf, []
                            yield group
            finally:
                # before the upstream closes: a pull still suspended inside it would make
                # its aclose() fail with "already running"
                cancellable.cancel()
                if pull is not None:
                    pull.cancel()
                    await asyncio.gather(pull, return_exceptions=True)
            if buf:
                yield buf

    return run


class TickSource[T]:
    """After Pekko's ``TickSource``: the iterator and the materialized ``Cancellable`` in one.
    The scheduler job starts when the stream is materialized; a tick that finds no pull waiting
    is dropped, and cancelling completes the stream."""

    def __init__(self, ctx: _Ctx, initial_delay: float, interval: float, tick: T) -> None:
        self._tick = tick
        self._waiter: asyncio.Future[None] | None = None
        self._stopped = False
        self._job = ctx.system.scheduler.schedule_at_fixed_rate(
            initial_delay, interval, self._fire
        )

    def _fire(self) -> None:
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_result(None)

    def cancel(self) -> bool:
        if self._stopped:
            return False
        self._stopped = True
        self._job.cancel()
        self._fire()
        return True

    @property
    def is_cancelled(self) -> bool:
        return self._stopped

    def __aiter__(self) -> TickSource[T]:
        return self

    async def __anext__(self) -> T:
        if self._stopped:
            raise StopAsyncIteration
        self._waiter = asyncio.get_running_loop().create_future()
        try:
            await self._waiter
        finally:
            self._waiter = None
        if self._stopped:
            raise StopAsyncIteration
        return self._tick

    async def aclose(self) -> None:
        self.cancel()
