"""StatusReply, Done and ask_with_status, after Apache Pekko's StatusReplySpec, the
"askWithStatus pattern" cases of AskSpec and the askWithStatus cases of ActorContextAskSpec."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, cast

import pytest

from ohra import (
    DONE,
    ActorContext,
    ActorRef,
    AskTimeoutError,
    Behavior,
    Behaviors,
    Done,
    Failure,
    StatusReply,
    Success,
    Try,
    ask_with_status,
)
from ohra.remote.serialization import Serialization
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


class TestException(Exception):  # noqa: N818 (Pekko's TestException)
    __test__ = False


@dataclass(frozen=True)
class Request:
    reply_to: ActorRef[StatusReply[str]]


def _round_trip(v: object) -> object:
    data = Serialization.encode(v, lambda r: r.path)
    return Serialization.decode(json.loads(json.dumps(data)), lambda _p: _never())


def _never() -> Any:
    raise AssertionError("resolver called")


# --- StatusReplySpec ---------------------------------------------------------------------------


def _any(v: object) -> object:
    return v


def test_pattern_match_success():
    # like in a classic actor receive: the reply is seen as an object
    match _any(StatusReply.success("woho!")):
        case StatusReply(success_value=int()):
            pytest.fail("matched an int")
        case StatusReply(success_value="woho!", error_message=None):
            pass
        case _:
            pytest.fail("no match")


def test_pattern_match_success_ack():
    assert StatusReply.ACK == StatusReply.success(DONE)
    assert StatusReply.ACK.value is DONE


def test_pattern_match_error_with_text():
    reply = StatusReply.error("boho!")
    assert reply.is_error and not reply.is_success
    assert str(reply) == "Error(boho!)"


def test_pattern_match_error_with_exception():
    reply = StatusReply.error(TestException("boho!"))
    assert reply.is_error
    assert str(reply) == "Error(boho!)"


def test_the_value_of_an_error_with_text_raises_error_message():
    with pytest.raises(StatusReply.ErrorMessage) as raised:
        StatusReply.error("boho").value
    assert str(raised.value) == "boho"
    assert raised.value == StatusReply.ErrorMessage("boho")


def test_an_error_with_an_exception_keeps_it_locally():
    e = TestException("boo")
    reply = StatusReply.error(e)
    assert reply.exception is e
    with pytest.raises(TestException):
        reply.value


def test_the_exception_of_a_success_is_a_value_error():
    with pytest.raises(ValueError, match="Expected reply to be a failure, but was a success"):
        StatusReply.success(1).exception


def test_str_of_a_success():
    assert str(StatusReply.success("woho")) == "Success(woho)"


def test_create_from_try_success():
    reply = StatusReply.from_try(Success("woho"))
    assert reply.is_success
    assert reply.value == "woho"


def test_create_from_try_failure_with_message():
    reply: StatusReply[str] = StatusReply.from_try(Failure(TestException("boo")))
    assert reply.is_error
    assert str(reply.exception) == "boo"
    assert isinstance(reply.exception, StatusReply.ErrorMessage)


def test_create_from_try_failure_keeping_exception():
    e = TestException("boo")
    reply: StatusReply[str] = StatusReply.from_try_keep_exception(Failure(e))
    assert reply.is_error
    assert reply.exception is e


def test_create_from_try_success_via_from_try_keep_exception():
    reply = StatusReply.from_try_keep_exception(Success("woho"))
    assert reply.is_success
    assert reply.value == "woho"


def test_create_from_try_failure_with_an_empty_message_uses_the_class_name():
    reply: StatusReply[str] = StatusReply.from_try(Failure(RuntimeError()))
    assert str(reply.exception) == "RuntimeError"


def test_an_error_reply_crosses_the_wire_as_its_message():
    back = _round_trip(StatusReply.error(KeyError("k")))
    assert isinstance(back, StatusReply)
    reply = cast(StatusReply[Any], back)
    assert reply.is_error
    assert reply.exception == StatusReply.ErrorMessage("'k'")


def test_a_success_reply_crosses_the_wire():
    assert _round_trip(StatusReply.success("woho")) == StatusReply.success("woho")
    assert _round_trip(StatusReply.ACK) == StatusReply.ACK


def test_done_crosses_the_wire_as_done():
    assert _round_trip(DONE) == DONE
    assert isinstance(_round_trip(DONE), Done)


# --- AskSpec "askWithStatus pattern" -----------------------------------------------------------


async def test_ask_with_status_unwraps_a_successful_response():
    async with ActorTestKit() as testkit:
        probe: TestProbe[Request] = testkit.create_test_probe()
        result = asyncio.create_task(ask_with_status(probe.ref, Request, 3))
        (await probe.receive_message()).reply_to.tell(StatusReply.success("goodie"))
        assert await result == "goodie"


async def test_ask_with_status_fails_for_a_fail_response_with_text():
    async with ActorTestKit() as testkit:
        probe: TestProbe[Request] = testkit.create_test_probe()
        result = asyncio.create_task(ask_with_status(probe.ref, Request, 3))
        (await probe.receive_message()).reply_to.tell(StatusReply.error("boom"))
        with pytest.raises(StatusReply.ErrorMessage, match="^boom$"):
            await result


async def test_ask_with_status_fails_for_a_fail_response_with_custom_exception():
    async with ActorTestKit() as testkit:
        probe: TestProbe[Request] = testkit.create_test_probe()
        result = asyncio.create_task(ask_with_status(probe.ref, Request, 3))
        (await probe.receive_message()).reply_to.tell(StatusReply.error(TestException("boom")))
        with pytest.raises(TestException, match="^boom$"):
            await result


async def test_ask_with_status_fails_for_a_reply_that_is_not_a_status_reply():
    async with ActorTestKit() as testkit:
        probe: TestProbe[Request] = testkit.create_test_probe()
        result = asyncio.create_task(ask_with_status(probe.ref, Request, 3))
        reply_to: ActorRef[Any] = (await probe.receive_message()).reply_to
        reply_to.tell("woho")
        with pytest.raises(ValueError, match="Unexpected status reply success value: 'woho'"):
            await result


async def test_ask_with_status_times_out():
    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        silent = testkit.spawn(Behaviors.ignore)
        result = asyncio.create_task(
            ask_with_status(silent, Request, 5, scheduler=testkit.scheduler)
        )
        await asyncio.sleep(0.05)
        await manual_time.time_passes(5)
        with pytest.raises(AskTimeoutError):
            await result


# --- ActorContextAskSpec askWithStatus ---------------------------------------------------------


def _asker(target: ActorRef[Request], probe: ActorRef[str]) -> Behavior[str]:
    @Behaviors.setup
    def setup(ctx: Any) -> Behavior[str]:
        def adapt(result: Try[str]) -> str:
            match result:
                case Success(v):
                    return f"got {v}"
                case Failure(e):
                    return f"got error: {type(e).__name__}, {e}"

        ctx.ask_with_status(target, Request, adapt, timeout=3)

        @Behaviors.receive_message
        def on_message(msg: str) -> Behavior[str]:
            probe.tell(msg)
            return Behaviors.same

        return on_message

    return setup


async def test_ctx_ask_with_status_unwraps_a_success():
    async with ActorTestKit() as testkit:
        target: TestProbe[Request] = testkit.create_test_probe()
        probe: TestProbe[str] = testkit.create_test_probe()
        testkit.spawn(_asker(target.ref, probe.ref))
        (await target.receive_message()).reply_to.tell(StatusReply.success("pong"))
        await probe.expect_message("got pong")


async def test_ctx_ask_with_status_unwraps_an_error_message():
    async with ActorTestKit() as testkit:
        target: TestProbe[Request] = testkit.create_test_probe()
        probe: TestProbe[str] = testkit.create_test_probe()
        testkit.spawn(_asker(target.ref, probe.ref))
        (await target.receive_message()).reply_to.tell(StatusReply.error("boho"))
        await probe.expect_message("got error: ErrorMessage, boho")


async def test_ctx_ask_with_status_unwraps_a_custom_exception():
    async with ActorTestKit() as testkit:
        target: TestProbe[Request] = testkit.create_test_probe()
        probe: TestProbe[str] = testkit.create_test_probe()
        testkit.spawn(_asker(target.ref, probe.ref))
        (await target.receive_message()).reply_to.tell(StatusReply.error(TestException("boho")))
        await probe.expect_message("got error: TestException, boho")


async def test_ctx_ask_with_status_ends_its_ask_when_the_actor_stops(
    caplog: pytest.LogCaptureFixture,
):
    """As ctx.ask: an actor that stops before the reply cancels the ask, so the reply is a dead
    letter and no timeout is left running."""

    def stops_after_asking(target: ActorRef[Request]) -> Behavior[str]:
        @Behaviors.setup
        def setup(ctx: ActorContext[str]) -> Behavior[str]:
            ctx.ask_with_status(target, Request, lambda _r: "answered", timeout=3)
            return Behaviors.stopped()

        return setup

    async with ActorTestKit() as testkit:
        target: TestProbe[Request] = testkit.create_test_probe()
        watcher: TestProbe[Any] = testkit.create_test_probe()
        asker = testkit.spawn(stops_after_asking(target.ref))
        await watcher.expect_terminated(asker)
        request = await target.receive_message()
        with caplog.at_level(logging.DEBUG, logger="ohra.dead_letters"):
            request.reply_to.tell(StatusReply.success("late"))
        assert any("ask already completed" in r.getMessage() for r in caplog.records)


def test_done_and_status_reply_leave_the_plain_manifests_to_the_users_messages():
    """A user's own ``@remotable`` Done or StatusReply registers under its class name, so the
    core's go on the wire under ``ohra.`` names, as sharding's go under ``Sharding.``."""
    assert Serialization.encode(DONE, lambda r: r.path) == {"$type": "ohra.Done"}
    encoded = Serialization.encode(StatusReply.ACK, lambda r: r.path)
    assert isinstance(encoded, dict) and encoded["$type"] == "ohra.StatusReply"
