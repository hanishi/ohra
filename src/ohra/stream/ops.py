"""Operators shared by ``Source`` and ``Flow``. Each takes the upstream iterator and the run's
context and returns the downstream iterator, an async generator that holds its upstream in
``contextlib.aclosing``, so closing it cancels upstream at once."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from .dsl import _Ctx  # pyright: ignore[reportPrivateUsage]


def closing[I: AsyncIterator[Any]](it: I) -> contextlib.AbstractAsyncContextManager[I]:
    """``contextlib.aclosing`` for a stage's upstream. Every iterator in a stream has ``aclose``
    (sources and operators are async generators, or a ``Stage``), though ``AsyncIterator``
    doesn't declare it."""
    return contextlib.aclosing(cast(Any, it))


class Stage[T]:
    """An operator's iterator, as ``_via_op`` hands it downstream. Closing it closes the
    operator, then its upstream: an async generator closed before its first pull never enters
    its ``aclosing``, so on its own it would leave a source's materialized resources (a queue,
    an actor) open when a stream is cancelled before it pulls (``Sink.cancelled``)."""

    __slots__ = ("_gen", "_upstream")

    def __init__(self, gen: AsyncIterator[T], upstream: AsyncIterator[Any]) -> None:
        self._gen = gen
        self._upstream = upstream

    def __aiter__(self) -> Stage[T]:
        return self

    def __anext__(self) -> Awaitable[T]:
        return self._gen.__anext__()

    async def aclose(self) -> None:
        async with closing(self._upstream), closing(self._gen):  # exits the operator first
            pass


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
