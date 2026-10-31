"""Behaviors, typed refs, ask, pipe_to_self, message adapters."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass

import pytest

from ohra import (
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
    assert calls == ["ohra://t/user"]


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


@dataclass(frozen=True)
class _Answer:
    text: str
    reply_to: ActorRef[str]


type AnswerCmd = Fetch | _Answer


async def test_pipe_to_self_as_a_decorator():
    def on_message(ctx: ActorContext[AnswerCmd], msg: AnswerCmd) -> Behavior[AnswerCmd]:
        match msg:
            case Fetch(url, reply_to):

                @ctx.pipe_to_self(fake_http(url))
                def _(result: Try[str]) -> AnswerCmd:
                    match result:
                        case Success(body):
                            return _Answer(body, reply_to)
                        case Failure(e):
                            return _Answer(f"error: {e!r}", reply_to)

            case _Answer(text, reply_to):
                reply_to.tell(text)
        return Behaviors.same

    async with ActorSystem(Behaviors.receive(on_message), "t") as system:
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


async def test_pipe_to_self_closes_work_the_actor_stopped_before_it_started():
    async def work() -> str:
        return "never"

    coro = work()

    def guardian(ctx: ActorContext[str]) -> Behavior[str]:
        ctx.pipe_to_self(coro, lambda r: "done")
        return Behaviors.stopped()

    async with ActorSystem(Behaviors.setup(guardian), "t") as system:
        await system.when_terminated()
    assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED


async def test_pipe_to_self_from_post_stop_closes_the_work_at_once():
    # the actor is stopping: nothing it starts now could be cancelled with the rest of its work
    async def work() -> str:
        await asyncio.sleep(3600)
        return "never"

    coro = work()

    def on_signal(ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
        if sig is PostStop:
            ctx.pipe_to_self(coro, lambda r: "done")
        return Behaviors.same

    behavior = Behaviors.receive_message(lambda _m: Behaviors.same).receive_signal(on_signal)
    async with ActorSystem(behavior, "t") as system:
        system.terminate()
        await system.when_terminated()
        assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED


@dataclass(frozen=True)
class Asked:
    reply_to: ActorRef[str]


async def test_ctx_ask_sends_its_request_before_a_later_tell():
    # Pekko's ctx.ask sends the request before it returns; only the reply is piped back
    seen: list[str] = []
    done = asyncio.Event()

    @Behaviors.receive_message
    def target(msg: Asked | str) -> Behavior[Asked | str]:
        match msg:
            case Asked(reply_to):
                seen.append("asked")
                reply_to.tell("ok")
            case _:
                seen.append(msg)
                done.set()
        return Behaviors.same

    def guardian(ctx: ActorContext[str]) -> Behavior[str]:
        t = ctx.spawn(target, "target")
        ctx.ask(t, Asked, lambda r: "answered", timeout=1)
        t.tell("told after")
        return Behaviors.ignore

    async with ActorSystem(Behaviors.setup(guardian), "t"):
        await done.wait()
    assert seen == ["asked", "told after"]


async def test_ctx_ask_fails_the_handler_when_the_request_cannot_be_made():
    failures: list[BaseException] = []

    def bad_request(_reply_to: ActorRef[str]) -> Asked:
        raise ValueError("bad request")

    def guardian(ctx: ActorContext[str]) -> Behavior[str]:
        try:
            ctx.ask(ctx.self, bad_request, lambda r: "answered", timeout=1)
        except ValueError as e:
            failures.append(e)
        return Behaviors.stopped()

    async with ActorSystem(Behaviors.setup(guardian), "t") as system:
        await system.when_terminated()
    assert [str(e) for e in failures] == ["bad request"]


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


async def test_ctx_ask_as_a_decorator():
    def asker(ctx: ActorContext[ReporterCmd]) -> Behavior[ReporterCmd]:
        c = ctx.spawn(counter(5), "counter")
        waiting: list[ActorRef[str]] = []

        def on_message(msg: ReporterCmd) -> Behavior[ReporterCmd]:
            match msg:
                case Report(reply_to):
                    waiting.append(reply_to)

                    @ctx.ask(c, GetTotal, timeout=1)
                    def _(result: Try[int]) -> ReporterCmd:
                        match result:
                            case Success(total):
                                return _GotTotal(total)
                            case Failure():
                                return _GotTotal(-1)

                case _GotTotal(total):
                    waiting.pop(0).tell(f"total={total}")
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    async with ActorSystem(Behaviors.setup(asker), "t") as system:
        assert await system.ask(Report, timeout=1) == "total=5"
