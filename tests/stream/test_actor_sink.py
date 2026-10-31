"""ActorSink.actor_ref and actor_ref_with_backpressure, after Apache Pekko's ActorSourceSinkSpec
("ActorSink should")."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass

from ohra import ActorRef
from ohra.stream import DONE, AbruptStageTerminationError, ActorSink, Keep, Source
from ohra.testkit import ActorTestKit, TestProbe


async def test_actor_sink_sends_elements_then_complete():
    async with ActorTestKit() as kit:
        probe: TestProbe[object] = kit.create_test_probe()
        sink = ActorSink.actor_ref(probe.ref, "done", lambda e: f"FAILED: {e}")
        Source.from_iterable([1, 2]).run_with(sink, kit.system)
        await probe.expect_message(1)
        await probe.expect_message(2)
        await probe.expect_message("done")


async def test_actor_sink_sends_failure_message():
    async with ActorTestKit() as kit:
        probe: TestProbe[object] = kit.create_test_probe()
        sink = ActorSink.actor_ref(probe.ref, "done", lambda e: f"FAILED: {e}")
        Source.failed(RuntimeError("boom")).run_with(sink, kit.system)
        await probe.expect_message("FAILED: boom")


async def test_actor_sink_sends_failure_message_when_the_stream_is_aborted():
    suspended = asyncio.Event()

    async def never() -> AsyncGenerator[int, None]:
        suspended.set()
        await asyncio.Event().wait()  # never set: suspended mid-pull
        yield 0

    failures: list[BaseException] = []
    async with ActorTestKit() as kit:
        probe: TestProbe[object] = kit.create_test_probe()
        sink = ActorSink.actor_ref(probe.ref, "done", failures.append)
        Source.from_async_iterable(never).run_with(sink, kit.system)
        await suspended.wait()
        kit.system.terminate()
        await kit.system.when_terminated()
    assert len(failures) == 1
    assert isinstance(failures[0], AbruptStageTerminationError)


async def test_actor_sink_cancels_upstream_when_its_target_stops():
    suspended = asyncio.Event()
    log: list[str] = []

    async def never() -> AsyncGenerator[int, None]:
        try:
            suspended.set()
            await asyncio.Event().wait()  # never set: suspended mid-pull
            yield 0
        finally:
            log.append("closed")

    async with ActorTestKit() as kit:
        probe: TestProbe[object] = kit.create_test_probe()
        sink = ActorSink.actor_ref(probe.ref, "done", lambda e: e)
        done = (
            Source.from_async_iterable(never)
            .watch_termination(Keep.right)
            .to(sink)
            .run(kit.system)
        )
        await suspended.wait()
        probe.stop()
        assert await asyncio.wait_for(done, 1) is DONE
        assert log == ["closed"]


@dataclass(frozen=True)
class Init:
    ack_to: ActorRef[str]


@dataclass(frozen=True)
class Msg:
    ack_to: ActorRef[str]
    msg: int


@dataclass(frozen=True)
class Failed:
    error: BaseException


type AckProto = Init | Msg | Failed | str


async def test_actor_sink_with_backpressure_waits_for_ack():
    async with ActorTestKit() as kit:
        probe: TestProbe[AckProto] = kit.create_test_probe()
        sink = ActorSink.actor_ref_with_backpressure(
            probe.ref, Msg, Init, "ack", "complete", Failed
        )
        Source.from_iterable([1]).run_with(sink, kit.system)
        init = await probe.expect_message_type(Init)
        await probe.expect_no_message(0.05)  # no element before the first ack
        init.ack_to.tell("ack")
        msg = await probe.expect_message_type(Msg)
        assert msg.msg == 1
        msg.ack_to.tell("ack")
        await probe.expect_message("complete")


async def test_actor_sink_with_backpressure_ignores_other_messages_to_its_ref():
    async with ActorTestKit() as kit:
        probe: TestProbe[AckProto] = kit.create_test_probe()
        sink = ActorSink.actor_ref_with_backpressure(
            probe.ref, Msg, Init, "ack", "complete", Failed
        )
        Source.from_iterable([1, 2]).run_with(sink, kit.system)
        init = await probe.expect_message_type(Init)
        init.ack_to.tell("not an ack")
        await probe.expect_no_message(0.05)
        init.ack_to.tell("ack")
        assert (await probe.expect_message_type(Msg)).msg == 1
        init.ack_to.tell("ack")
        assert (await probe.expect_message_type(Msg)).msg == 2
        init.ack_to.tell("ack")
        await probe.expect_message("complete")


async def test_actor_sink_with_backpressure_sends_failure_message():
    async with ActorTestKit() as kit:
        probe: TestProbe[AckProto] = kit.create_test_probe()
        sink = ActorSink.actor_ref_with_backpressure(
            probe.ref, Msg, Init, "ack", "complete", Failed
        )
        Source.failed(RuntimeError("boom")).run_with(sink, kit.system)
        await probe.expect_message_type(Init)
        failed = await probe.expect_message_type(Failed)
        assert str(failed.error) == "boom"
