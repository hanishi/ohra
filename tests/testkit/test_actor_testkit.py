"""ActorTestKit, after Apache Pekko's ActorTestKitSpec."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from ohra import ActorContext, ActorRef, Behavior, Behaviors, Signal, PostStop, ask
from ohra.testkit import ActorTestKit, TestKitSettings


@dataclass(frozen=True)
class Echo:
    text: str
    reply_to: ActorRef[str]


def echo() -> Behavior[Echo]:
    @Behaviors.receive_message
    def on_message(msg: Echo) -> Behavior[Echo]:
        msg.reply_to.tell(msg.text)
        return Behaviors.same

    return on_message


def on_stop(events: list[str], delay: float = 0.0) -> Behavior[str]:
    async def on_signal(_ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
        if sig is PostStop:
            await asyncio.sleep(delay)
            events.append("stopped")
        return Behaviors.same

    return Behaviors.receive_signal(on_signal)


async def test_spawned_actor_answers():
    async with ActorTestKit() as testkit:
        ref = testkit.spawn(echo(), "echo")
        assert ref.path.endswith("/user/echo")
        assert await ask(ref, lambda r: Echo("hi", r), timeout=1) == "hi"


async def test_spawn_without_a_name():
    async with ActorTestKit() as testkit:
        a = testkit.spawn(echo())
        b = testkit.spawn(echo())
        assert a.path != b.path


async def test_stop_waits_until_stopped():
    events: list[str] = []
    async with ActorTestKit() as testkit:
        ref = testkit.spawn(on_stop(events))
        await testkit.stop(ref)
        assert events == ["stopped"]


async def test_stop_times_out():
    events: list[str] = []
    async with ActorTestKit() as testkit:
        ref = testkit.spawn(on_stop(events, delay=0.2))
        with pytest.raises(AssertionError, match=r"timeout \(0.05s\) during stop\(\) waiting"):
            await testkit.stop(ref, max=0.05)


async def test_system_is_terminated_after_the_block():
    async with ActorTestKit() as testkit:
        pass
    assert testkit.system.is_terminated


async def test_name_comes_from_the_calling_test():
    async with ActorTestKit() as testkit:
        assert testkit.system.name == "test_name_comes_from_the_calling_test"


async def test_body_error_is_not_masked_by_shutdown():
    events: list[str] = []
    with pytest.raises(ValueError, match="boom"):
        async with ActorTestKit(settings=TestKitSettings(system_shutdown_default=0.01)) as tk:
            tk.spawn(on_stop(events, delay=0.2))
            raise ValueError("boom")
    await tk.system.when_terminated()  # left running past the timeout, as Pekko leaves it
    assert events == ["stopped"]


async def test_shutdown_timeout_raises():
    events: list[str] = []
    with pytest.raises(RuntimeError, match=r"Failed to stop \[.*\] within \[0.01s\]"):
        async with ActorTestKit(settings=TestKitSettings(system_shutdown_default=0.01)) as tk:
            tk.spawn(on_stop(events, delay=0.2))
    await tk.system.when_terminated()  # it was left running, as Pekko leaves it
    assert events == ["stopped"]


def stuck(never: asyncio.Future[None]) -> Behavior[str]:
    async def on_message(_msg: str) -> Behavior[str]:
        await never
        return Behaviors.same

    return Behaviors.receive_message(on_message)


async def test_shutdown_of_a_stuck_actor_raises_instead_of_hanging():
    never: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    tk = ActorTestKit(settings=TestKitSettings(system_shutdown_default=0.05))
    with pytest.raises(RuntimeError, match="Failed to stop"):
        async with asyncio.timeout(2), tk:
            tk.spawn(stuck(never)).tell("go")
            await asyncio.sleep(0.01)
    never.set_result(None)
    await tk.system.when_terminated()


async def test_body_error_surfaces_when_an_actor_is_stuck():
    never: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    tk = ActorTestKit(settings=TestKitSettings(system_shutdown_default=0.05))
    with pytest.raises(ValueError, match="boom"):
        async with asyncio.timeout(2), tk:
            tk.spawn(stuck(never)).tell("go")
            await asyncio.sleep(0.01)
            raise ValueError("boom")
    never.set_result(None)
    await tk.system.when_terminated()


def test_settings_dilate():
    assert TestKitSettings(timefactor=2).single_expect_default_dilated == 6.0
    assert TestKitSettings(timefactor=2).dilated(0.5) == 1.0
