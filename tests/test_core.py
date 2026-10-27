"""Behaviors, typed refs, ask, pipe_to_self, message adapters."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from pekko_py import (
    ActorContext,
    ActorRef,
    ActorSystem,
    AskTimeoutError,
    Behavior,
    Behaviors,
    Failure,
    PostStop,
    Signal,
    Success,
    Try,
    ask,
)

# --- a counter in the functional style ---------------------------------------------


@dataclass(frozen=True)
class Increment:
    by: int


@dataclass(frozen=True)
class GetTotal:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Stop:
    pass


type Command = Increment | GetTotal | Stop


def counter(total: int = 0) -> Behavior[Command]:
    def on_message(msg: Command) -> Behavior[Command]:
        match msg:
            case Increment(by):
                return counter(total + by)
            case GetTotal(reply_to):
                reply_to.tell(total)
                return Behaviors.same
            case Stop():
                return Behaviors.stopped()

    return Behaviors.receive_message(on_message)


async def test_state_lives_in_the_behavior():
    async with ActorSystem(counter(), "t") as system:
        system.tell(Increment(5))
        system.tell(Increment(10))
        assert await system.ask(GetTotal, timeout=1) == 15


async def test_setup_runs_once_per_incarnation():
    calls: list[str] = []

    def guardian(ctx: ActorContext[Command]) -> Behavior[Command]:
        calls.append(ctx.self.path)
        return counter()

    async with ActorSystem(Behaviors.setup(guardian), "t") as system:
        system.tell(Increment(1))
        assert await system.ask(GetTotal, timeout=1) == 1
    assert calls == ["pekko://t/user"]


async def test_async_handler_keeps_messages_serialized():
    """The point of actors under asyncio: no interleaving across awaits."""
    log: list[str] = []

    async def on_message(msg: str) -> Behavior[str]:
        log.append(f"start {msg}")
        await asyncio.sleep(0.01)
        log.append(f"end {msg}")
        return Behaviors.same

    async with ActorSystem(Behaviors.receive_message(on_message), "t") as system:
        system.tell("a")
        system.tell("b")
        await asyncio.sleep(0.05)
    assert log == ["start a", "end a", "start b", "end b"]


async def test_stopped_delivers_post_stop_and_dead_letters_later_messages():
    events: list[str] = []

    def on_signal(_ctx: ActorContext[Command], sig: Signal) -> Behavior[Command]:
        if sig is PostStop:
            events.append("post-stop")
        return Behaviors.same

    def on_message(_ctx: ActorContext[Command], msg: Command) -> Behavior[Command]:
        events.append(type(msg).__name__)
        return (
            Behaviors.stopped(lambda: events.append("callback"))
            if isinstance(msg, Stop)
            else Behaviors.same
        )

    system = ActorSystem(Behaviors.receive(on_message).receive_signal(on_signal), "t")
    system.tell(Increment(1))
    system.tell(Stop())
    system.tell(Increment(2))
    await system.when_terminated()
    assert events == ["Increment", "Stop", "post-stop", "callback"]


# --- ask ---------------------------------------------------------------------------


async def test_ask_timeout_and_late_reply_is_dropped():
    held: list[ActorRef[int]] = []

    def on_message(msg: GetTotal) -> Behavior[GetTotal]:
        held.append(msg.reply_to)  # never answers in time
        return Behaviors.same

    async with ActorSystem(Behaviors.receive_message(on_message), "t") as system:
        with pytest.raises(AskTimeoutError):
            await ask(system, GetTotal, timeout=0.02)
        held[0].tell(42)  # late reply: must not raise or leave anything pending


# --- pipe_to_self, ctx.ask, message adapters ----------------------------------------


@dataclass(frozen=True)
class Fetch:
    url: str
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class _Fetched:
    result: Try[str]
    reply_to: ActorRef[str]


type FetcherCmd = Fetch | _Fetched


async def fake_http(url: str) -> str:
    await asyncio.sleep(0.01)
    if "bad" in url:
        raise ConnectionError(url)
    return f"body of {url}"


def fetcher() -> Behavior[FetcherCmd]:
    def on_message(ctx: ActorContext[FetcherCmd], msg: FetcherCmd) -> Behavior[FetcherCmd]:
        match msg:
            case Fetch(url, reply_to):
                ctx.pipe_to_self(fake_http(url), lambda r: _Fetched(r, reply_to))
            case _Fetched(Success(body), reply_to):
                reply_to.tell(body)
            case _Fetched(Failure(e), reply_to):
                reply_to.tell(f"error: {e!r}")
        return Behaviors.same

    return Behaviors.receive(on_message)


async def test_pipe_to_self_success_and_failure():
    async with ActorSystem(fetcher(), "t") as system:
        ok = await system.ask(lambda r: Fetch("good", r), timeout=1)
        bad = await system.ask(lambda r: Fetch("bad", r), timeout=1)
    assert ok == "body of good"
    assert bad == "error: ConnectionError('bad')"


async def test_pipe_to_self_is_cancelled_when_actor_stops():
    started = asyncio.Event()

    async def forever() -> str:
        started.set()
        await asyncio.sleep(3600)
        return "never"

    def guardian(ctx: ActorContext[str]) -> Behavior[str]:
        ctx.pipe_to_self(forever(), lambda r: "done")
        return Behaviors.ignore

    system = ActorSystem(Behaviors.setup(guardian), "t")
    await started.wait()
    system.terminate()
    await system.when_terminated()  # the autouse fixture checks the pipe task is gone


@dataclass(frozen=True)
class Report:
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class _GotTotal:
    total: int


type ReporterCmd = Report | _GotTotal


async def test_ctx_ask_and_message_adapter():
    def reporter(ctx: ActorContext[ReporterCmd]) -> Behavior[ReporterCmd]:
        c = ctx.spawn(counter(), "counter")
        c.tell(Increment(7))
        totals = ctx.message_adapter(_GotTotal)  # ActorRef[int] -> _GotTotal
        waiting: list[ActorRef[str]] = []

        def on_message(msg: ReporterCmd) -> Behavior[ReporterCmd]:
            match msg:
                case Report(reply_to):
                    waiting.append(reply_to)
                    c.tell(GetTotal(totals))
                case _GotTotal(total):
                    waiting.pop(0).tell(f"total={total}")
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    async with ActorSystem(Behaviors.setup(reporter), "t") as system:
        assert await system.ask(Report, timeout=1) == "total=7"

    def asker(ctx: ActorContext[ReporterCmd]) -> Behavior[ReporterCmd]:
        c = ctx.spawn(counter(3), "counter")
        waiting: list[ActorRef[str]] = []

        def on_message(msg: ReporterCmd) -> Behavior[ReporterCmd]:
            match msg:
                case Report(reply_to):
                    waiting.append(reply_to)
                    ctx.ask(
                        c,
                        GetTotal,
                        lambda r: _GotTotal(r.value if isinstance(r, Success) else -1),
                        timeout=1,
                    )
                case _GotTotal(total):
                    waiting.pop(0).tell(f"total={total}")
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    async with ActorSystem(Behaviors.setup(asker), "t2") as system:
        assert await system.ask(Report, timeout=1) == "total=3"
