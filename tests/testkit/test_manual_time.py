"""ManualTime, after Apache Pekko's ManualTimerExampleSpec."""

from __future__ import annotations

import asyncio
import gc
import time
from dataclasses import dataclass

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    AskTimeoutError,
    Behavior,
    Behaviors,
    SupervisorStrategy,
    ask,
)
from ohra.testkit import ActorTestKit, ManualTime, TestProbe


@dataclass(frozen=True)
class Tick:
    n: int = 0


@dataclass(frozen=True)
class Tock:
    pass


@dataclass(frozen=True)
class CancelIt:
    pass


@dataclass(frozen=True)
class IsActive:
    reply_to: ActorRef[bool]


type Cmd = Tick | Tock | CancelIt | IsActive


def single(delay: float, probe: ActorRef[Tock]) -> Behavior[Cmd]:
    """Starts a single timer; tells the probe when it fires; CancelIt cancels it."""

    def setup(ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
        ctx.timers.start_single_timer("k", Tick(), delay)

        def on_message(_ctx: ActorContext[Cmd], msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Tick():
                    probe.tell(Tock())
                case CancelIt():
                    ctx.timers.cancel("k")
                case IsActive(reply_to):
                    reply_to.tell(ctx.timers.is_timer_active("k"))
                case Tock():
                    ctx.timers.start_single_timer("k", Tick(), delay)
            return Behaviors.same

        return Behaviors.receive(on_message)

    return Behaviors.setup(setup)


async def test_single_timer():
    t0 = time.monotonic()
    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        probe: TestProbe[Tock] = testkit.create_test_probe()
        testkit.spawn(single(10, probe.ref))
        await manual_time.expect_no_message_for(9.9, probe)
        await manual_time.time_passes(0.1)
        await probe.expect_message(Tock())
    assert time.monotonic() - t0 < 1


async def test_repeating_timer_fires_every_interval():
    def ticker(probe: ActorRef[Tick]) -> Behavior[Tick]:
        def setup(ctx: ActorContext[Tick]) -> Behavior[Tick]:
            ctx.timers.start_timer_with_fixed_delay("k", Tick(), 0.1)

            @Behaviors.receive_message
            def on_message(msg: Tick) -> Behavior[Tick]:
                probe.tell(msg)
                return Behaviors.same

            return on_message

        return Behaviors.setup(setup)

    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        probe: TestProbe[Tick] = testkit.create_test_probe()
        testkit.spawn(ticker(probe.ref))
        await manual_time.time_passes(1.0)
        assert len(await probe.receive_messages(10)) == 10
        await probe.expect_no_message(0)


async def test_cancelled_timer_message_is_dropped_under_manual_time():
    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        probe: TestProbe[Tock] = testkit.create_test_probe()
        ref = testkit.spawn(single(1, probe.ref))
        await manual_time.time_passes(0)  # the actor has started its timer
        # CancelIt is in the mailbox ahead of the timer's message: the actor cancels first
        ref.tell(CancelIt())
        manual_time.scheduler.time_passes(1)  # fires without letting the actor run
        await manual_time.expect_no_message_for(1, probe)


async def test_chain_of_timers_in_one_window():
    def chain(probe: ActorRef[Tick]) -> Behavior[Tick]:
        def setup(ctx: ActorContext[Tick]) -> Behavior[Tick]:
            ctx.timers.start_single_timer("k", Tick(1), 1)

            @Behaviors.receive_message
            def on_message(msg: Tick) -> Behavior[Tick]:
                probe.tell(msg)
                if msg.n == 1:
                    ctx.timers.start_single_timer("k", Tick(2), 1)
                return Behaviors.same

            return on_message

        return Behaviors.setup(setup)

    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        probe: TestProbe[Tick] = testkit.create_test_probe()
        testkit.spawn(chain(probe.ref))
        await manual_time.time_passes(2)
        assert await probe.receive_messages(2, max=0) == [Tick(1), Tick(2)]


async def test_zero_delay_timer_under_manual_time():
    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        probe: TestProbe[Tock] = testkit.create_test_probe()
        active: TestProbe[bool] = testkit.create_test_probe()
        ref = testkit.spawn(single(0, probe.ref))
        await probe.expect_message(Tock())
        ref.tell(IsActive(active.ref))
        await active.expect_message(False)
        ref.tell(Tock())  # starts "k" again, at 0
        await probe.expect_message(Tock())
        await manual_time.expect_no_message_for(1, probe)


async def test_ask_times_out_only_when_time_passes():
    @dataclass(frozen=True)
    class Ping:
        reply_to: ActorRef[int]

    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        silent = testkit.spawn(Behaviors.ignore)
        task = asyncio.create_task(ask(silent, Ping, 5, scheduler=testkit.scheduler))
        await asyncio.sleep(0.05)
        assert not task.done()
        await manual_time.time_passes(5)
        with pytest.raises(AskTimeoutError):
            await task


async def test_ctx_ask_times_out_after_its_timeout_from_the_call():
    @dataclass(frozen=True)
    class Ping:
        reply_to: ActorRef[int]

    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        probe: TestProbe[str] = testkit.create_test_probe()
        silent = testkit.spawn(Behaviors.ignore)

        def asker(ctx: ActorContext[str]) -> Behavior[str]:
            ctx.ask(silent, Ping, lambda r: type(r).__name__, timeout=5)
            return Behaviors.receive_message(lambda m: (probe.ref.tell(m), Behaviors.same)[1])

        testkit.spawn(Behaviors.setup(asker))
        await manual_time.time_passes(5)
        await probe.expect_message("Failure", max=0.5)


async def test_an_ask_that_failed_before_the_actor_stopped_logs_nothing():
    # a zero timeout fails the reply future inside ctx.ask; the actor stops in the same step,
    # so its pipe task never starts and nobody would read that failure
    @dataclass(frozen=True)
    class Ping:
        reply_to: ActorRef[int]

    errors: list[str] = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _, context: errors.append(str(context["message"])))
    try:
        async with ActorTestKit(manual_time=True) as testkit:
            silent = testkit.spawn(Behaviors.ignore)

            def asker(ctx: ActorContext[str]) -> Behavior[str]:
                ctx.ask(silent, Ping, lambda r: "answered", timeout=0)
                return Behaviors.stopped()

            ref = testkit.spawn(Behaviors.setup(asker))
            probe: TestProbe[str] = testkit.create_test_probe()
            await probe.expect_terminated(ref)
        gc.collect()
    finally:
        loop.set_exception_handler(None)
    assert errors == []


async def test_backoff_restart_under_manual_time():
    def failing(probe: ActorRef[str]) -> Behavior[str]:
        def setup(_ctx: ActorContext[str]) -> Behavior[str]:
            probe.tell("started")

            @Behaviors.receive_message
            def on_message(msg: str) -> Behavior[str]:
                raise RuntimeError(msg)

            return on_message

        return Behaviors.supervise(Behaviors.setup(setup)).on_failure(
            SupervisorStrategy.restart_with_backoff(3, 10, random_factor=0)
        )

    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        probe: TestProbe[str] = testkit.create_test_probe()
        ref = testkit.spawn(failing(probe.ref))
        await probe.expect_message("started")
        ref.tell("boom")
        await manual_time.expect_no_message_for(2.9, probe)
        await manual_time.time_passes(0.1)
        await probe.expect_message("started", max=0)


async def test_time_passes_does_not_wait_forever_for_a_busy_actor():
    never: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    async def stuck(_msg: str) -> Behavior[str]:
        await never
        return Behaviors.same

    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        testkit.spawn(Behaviors.receive_message(stuck)).tell("go")
        t0 = time.monotonic()
        await manual_time.time_passes(1)
        assert time.monotonic() - t0 < 0.5
        never.set_result(None)


async def test_manual_time_on_a_default_system_raises():
    async with ActorTestKit() as testkit:
        with pytest.raises(
            ValueError,
            match="^ActorSystem not configured with explicitly triggered scheduler.*"
            "make sure to pass manual_time=True to ActorTestKit or "
            r"scheduler=ExplicitlyTriggeredScheduler\(\) to ActorSystem$",
        ):
            ManualTime(testkit.system)


async def test_ask_cancels_its_timeout_when_the_request_cannot_be_made():
    @dataclass(frozen=True)
    class Ping:
        reply_to: ActorRef[int]

    def bad_request(_reply_to: ActorRef[int]) -> Ping:
        raise ValueError("bad request")

    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        with pytest.raises(ValueError, match="bad request"):
            await ask(testkit.system, bad_request, 5, scheduler=testkit.scheduler)
        assert manual_time.scheduler.next_due(float("inf")) is None
