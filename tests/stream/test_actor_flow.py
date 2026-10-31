"""ActorFlow.ask, after Apache Pekko's ActorFlowSpec."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest

from ohra import ActorRef, AskTimeoutError, Behavior, Behaviors
from ohra.stream import ActorFlow, Sink, Source, WatchedActorTerminatedError
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


@dataclass(frozen=True)
class Asking:
    s: str
    reply_to: ActorRef[str]


@Behaviors.receive_message
def replier(msg: Asking) -> Behavior[Asking]:
    if msg.s == "TERMINATE":
        return Behaviors.stopped()
    msg.reply_to.tell(msg.s + "!!!")
    return Behaviors.same


async def test_actor_flow_ask_replies_in_order():
    async with ActorTestKit() as kit:
        ref = kit.spawn(replier)
        ask = ActorFlow.ask(ref, Asking, timeout=1.0)
        out = Source.from_iterable(range(1, 51)).map(str).via(ask).run_with(Sink.seq(), kit.system)
        assert await out == [f"{i}!!!" for i in range(1, 51)]


async def test_actor_flow_ask_timeout_fails_stream():
    async with ActorTestKit(manual_time=True) as kit:
        time = ManualTime(kit.system)
        probe: TestProbe[Asking] = kit.create_test_probe()  # never replies
        ask = ActorFlow.ask(probe.ref, Asking, timeout=1.0, parallelism=4)
        out = Source.from_iterable(["nope"]).via(ask).run_with(Sink.seq(), kit.system)
        await probe.expect_message_type(Asking)
        await time.time_passes(1.0)
        with pytest.raises(AskTimeoutError):
            await out


async def test_actor_flow_ask_fails_when_target_terminates():
    suspended = asyncio.Event()

    async def never() -> AsyncGenerator[str, None]:
        suspended.set()
        await asyncio.Event().wait()  # never set: Pekko's Source.maybe
        yield ""

    async with ActorTestKit() as kit:
        ref = kit.spawn(replier)
        ask = ActorFlow.ask(ref, Asking, timeout=3.0)
        done = Source.from_async_iterable(never).via(ask).run_with(Sink.ignore(), kit.system)
        await suspended.wait()
        nobody: TestProbe[str] = kit.create_test_probe()
        ref.tell(Asking("TERMINATE", nobody.ref))
        with pytest.raises(WatchedActorTerminatedError, match=r"Actor watched by \[ask\(\)\]"):
            await asyncio.wait_for(done, 1)


async def test_actor_flow_ask_fails_when_target_terminates_with_asks_in_flight():
    async with ActorTestKit() as kit:
        probe: TestProbe[Asking] = kit.create_test_probe()  # never replies
        ask = ActorFlow.ask(probe.ref, Asking, timeout=60.0)
        done = Source.from_iterable(["a", "b", "c"]).via(ask).run_with(Sink.ignore(), kit.system)
        await probe.expect_message_type(Asking)
        await probe.expect_message_type(Asking)  # parallelism 2: both in flight, none pulled
        probe.stop()
        with pytest.raises(WatchedActorTerminatedError):
            await asyncio.wait_for(done, 1)
