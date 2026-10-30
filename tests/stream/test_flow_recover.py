"""recover and recover_with_retries, after Apache Pekko's FlowRecoverSpec and
FlowRecoverWithSpec."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest

from ohra.stream import Flow, Sink, Source
from ohra.testkit import ActorTestKit


async def _boom_after(*xs: int) -> AsyncGenerator[int, None]:
    for x in xs:
        yield x
    raise ValueError("boom")


def boom_after(*xs: int) -> Source[int, object]:
    return Source.from_async_iterable(lambda: _boom_after(*xs))


async def run[T](src: Source[T, object]) -> list[T]:
    async with ActorTestKit() as kit:
        return await src.run_with(Sink.seq(), kit.system)


async def test_recover_emits_last_element_and_completes():
    assert await run(boom_after(1).recover(lambda _: -1)) == [1, -1]


async def test_recover_passes_through_without_failure():
    assert await run(Source.from_iterable([1, 2]).recover(lambda _: -1)) == [1, 2]


async def test_recover_rethrows_when_none():
    with pytest.raises(ValueError, match="boom"):
        await run(boom_after(1).recover(lambda _: None))


async def test_recover_does_not_catch_downstream_failure():
    with pytest.raises(KeyError):
        await run(boom_after(1).recover(lambda _: -1).map(lambda _: {}[1]))


async def test_recover_sees_the_exception():
    seen: list[Exception] = []

    def f(e: Exception) -> int:
        seen.append(e)
        return 0

    await run(boom_after().recover(f))
    assert [str(e) for e in seen] == ["boom"]


async def test_recover_on_flow():
    flow = Flow.of(int).map(lambda x: 10 // x).recover(lambda _: -1)
    assert await run(Source.from_iterable([5, 0, 1]).via(flow)) == [2, -1]


async def test_recover_with_retries_switches_to_the_new_source():
    src = boom_after(1).recover_with_retries(3, lambda _: Source.from_iterable([8, 9]))
    assert await run(src) == [1, 8, 9]


async def test_recover_with_retries_gives_up_after_attempts():
    src = boom_after(1).recover_with_retries(1, lambda _: boom_after(2))
    async with ActorTestKit() as kit:
        out: list[int] = []
        with pytest.raises(ValueError, match="boom"):
            await src.run_foreach(out.append, kit.system)
        assert out == [1, 2]


async def test_recover_with_retries_retries_until_a_source_succeeds():
    calls: list[int] = []

    def f(_: Exception) -> Source[int, object]:
        calls.append(1)
        return boom_after(len(calls)) if len(calls) < 3 else Source.single(99)

    assert await run(boom_after(0).recover_with_retries(5, f)) == [0, 1, 2, 99]


async def test_recover_with_retries_rethrows_when_none():
    with pytest.raises(ValueError, match="boom"):
        await run(boom_after(1).recover_with_retries(3, lambda _: None))


async def test_recover_with_retries_unlimited_with_negative_attempts():
    n = 0

    def f(_: Exception) -> Source[int, object]:
        nonlocal n
        n += 1
        return boom_after(n) if n < 5 else Source.empty()

    assert await run(boom_after().recover_with_retries(-1, f)) == [1, 2, 3, 4]


async def test_recover_with_retries_on_flow():
    flow = Flow.of(int).map(lambda x: 10 // x).recover_with_retries(1, lambda _: Source.single(0))
    assert await run(Source.from_iterable([5, 0, 1]).via(flow)) == [2, 0]
