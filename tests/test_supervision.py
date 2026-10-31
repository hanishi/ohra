"""Supervision."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    PreRestart,
    Signal,
    SupervisorStrategy,
    Terminated,
)
from ohra.supervision import SupervisorStrategy as S
from ohra.testkit import ActorTestKit, ManualTime, TestProbe

from .util import eventually


@dataclass(frozen=True)
class Add:
    n: int


@dataclass(frozen=True)
class Get:
    reply_to: ActorRef[int]


type Cmd = Add | Get


def fragile(events: list[str], total: int = 0) -> Behavior[Cmd]:
    """Fails on negative numbers."""

    def on_message(msg: Cmd) -> Behavior[Cmd]:
        match msg:
            case Add(n) if n < 0:
                raise ValueError(n)
            case Add(n):
                return fragile(events, total + n)
            case Get(reply_to):
                reply_to.tell(total)
                return Behaviors.same

    def on_signal(_ctx: ActorContext[Cmd], sig: Signal) -> Behavior[Cmd]:
        if sig is PreRestart:
            events.append(f"pre-restart at {total}")
        return Behaviors.same

    return Behaviors.receive_message(on_message).receive_signal(on_signal)


def started(events: list[str]) -> Behavior[Cmd]:
    def setup(_ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
        events.append("setup")
        return fragile(events)

    return Behaviors.setup(setup)


async def test_resume_keeps_state():
    events: list[str] = []
    b = Behaviors.supervise(started(events)).on_failure(SupervisorStrategy.resume())
    async with ActorSystem(b, "t") as system:
        system.tell(Add(5))
        system.tell(Add(-1))
        system.tell(Add(2))
        assert await system.ask(Get, timeout=1) == 7
    assert events == ["setup"]


async def test_restart_reruns_setup_and_signals_pre_restart():
    events: list[str] = []
    b = Behaviors.supervise(started(events)).on_failure(SupervisorStrategy.restart())
    async with ActorSystem(b, "t") as system:
        system.tell(Add(5))
        system.tell(Add(-1))
        system.tell(Add(2))
        assert await system.ask(Get, timeout=1) == 2
    assert events == ["setup", "pre-restart at 5", "setup"]


async def test_restart_limit_then_stop():
    events: list[str] = []
    b = Behaviors.supervise(started(events)).on_failure(S.restart().with_limit(2, within=10))
    system = ActorSystem(b, "t")
    for _ in range(3):
        system.tell(Add(-1))
    await asyncio.wait_for(system.when_terminated(), 1)
    assert events.count("setup") == 3


async def test_exception_type_filter():
    events: list[str] = []
    b = Behaviors.supervise(started(events)).on_failure(S.restart(), exc_type=KeyError)
    system = ActorSystem(b, "t")
    system.tell(Add(-1))  # ValueError is not KeyError -> default: stop
    await asyncio.wait_for(system.when_terminated(), 1)
    assert events == ["setup"]


async def test_restart_stops_children():
    stopped: list[str] = []

    def parent(ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
        ctx.spawn(Behaviors.ignore, "kid")  # same name each incarnation

        def on_message(msg: Cmd) -> Behavior[Cmd]:
            if isinstance(msg, Add) and msg.n < 0:
                raise ValueError
            if isinstance(msg, Get):
                msg.reply_to.tell(len(ctx.children))
            return Behaviors.same

        stopped.append(f"setup with {len(ctx.children)} child")
        return Behaviors.receive_message(on_message)

    b = Behaviors.supervise(Behaviors.setup(parent)).on_failure(S.restart())
    async with ActorSystem(b, "t") as system:
        system.tell(Add(-1))
        assert await system.ask(Get, timeout=1) == 1
    # if the old child survived, re-spawning "kid" in setup would have raised
    assert stopped == ["setup with 1 child", "setup with 1 child"]


async def test_backoff_stashes_messages_and_replays_after_restart():
    events: list[str] = []
    b = Behaviors.supervise(started(events)).on_failure(S.restart_with_backoff(0.02, 0.1, 0.0))
    async with ActorSystem(b, "t") as system:
        system.tell(Add(-1))  # fails -> backoff 20ms
        system.tell(Add(3))  # stashed
        system.tell(Add(4))  # stashed
        t0 = asyncio.get_running_loop().time()
        assert await system.ask(Get, timeout=1) == 7
        assert asyncio.get_running_loop().time() - t0 >= 0.015
    assert events == ["setup", "pre-restart at 0", "setup"]


async def test_backoff_grows_and_survives_failing_setup():
    attempts: list[float] = []
    loop = asyncio.get_running_loop()

    def flaky_setup(_ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
        attempts.append(loop.time())
        if len(attempts) < 4:
            raise ConnectionError("db not ready")
        return fragile([])

    b = Behaviors.supervise(Behaviors.setup(flaky_setup)).on_failure(
        S.restart_with_backoff(0.01, 1.0, 0.0)
    )
    async with ActorSystem(b, "t") as system:
        system.tell(Add(9))
        assert await system.ask(Get, timeout=2) == 9
    gaps = [b - a for a, b in zip(attempts, attempts[1:])]
    assert len(attempts) == 4
    assert gaps[0] < gaps[1] < gaps[2]  # 10ms, 20ms, 40ms


async def test_unsupervised_failure_stops_actor():
    events: list[str] = []
    system = ActorSystem(started(events), "t")
    system.tell(Add(-1))
    await asyncio.wait_for(system.when_terminated(), 1)
    await eventually(lambda: system.is_terminated)


@dataclass(frozen=True)
class StopChild:
    after_pending: bool


def parent_of(child: Behavior[Cmd], refs: TestProbe[ActorRef[Cmd]]) -> Behavior[StopChild]:
    """Spawns ``child``, hands its ref to ``refs``, and stops it on ``StopChild``."""

    @Behaviors.setup
    def setup(ctx: ActorContext[StopChild]) -> Behavior[StopChild]:
        kid = ctx.spawn(child, "kid")
        refs.ref.tell(kid)

        @Behaviors.receive_message
        def on_message(msg: StopChild) -> Behavior[StopChild]:
            ctx.stop(kid, after_pending=msg.after_pending)
            return Behaviors.same

        return on_message

    return setup


async def test_an_after_pending_stop_during_a_backoff_runs_the_stashed_messages_first():
    # Pekko's RestartSupervisor stashes the stop (a PoisonPill signal) behind them
    async with ActorTestKit() as testkit:
        events: list[str] = []
        refs: TestProbe[ActorRef[Cmd]] = testkit.create_test_probe()
        totals: TestProbe[int] = testkit.create_test_probe()
        child = Behaviors.supervise(started(events)).on_failure(
            S.restart_with_backoff(0.05, 0.05, 0.0)
        )
        parent = testkit.spawn(parent_of(child, refs))
        kid = await refs.receive_message()
        kid.tell(Add(-1))  # fails: backing off
        kid.tell(Add(3))  # stashed
        kid.tell(Get(totals.ref))  # stashed
        parent.tell(StopChild(after_pending=True))  # passivation
        assert await totals.receive_message() == 3
        await totals.expect_terminated(kid)


async def test_a_stop_during_a_backoff_dead_letters_the_stashed_messages():
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        events: list[str] = []
        dead = testkit.create_dead_letter_probe()
        refs: TestProbe[ActorRef[Cmd]] = testkit.create_test_probe()
        child = Behaviors.supervise(started(events)).on_failure(
            S.restart_with_backoff(10, 10, 0.0)
        )
        parent = testkit.spawn(parent_of(child, refs))
        kid = await refs.receive_message()
        kid.tell(Add(-1))  # fails: backing off for 10 s
        await eventually(lambda: events == ["setup", "pre-restart at 0"])
        kid.tell(Add(3))
        await manual.time_passes(0.001)  # settles first: the child has stashed it
        parent.tell(StopChild(after_pending=False))
        letter = await dead.receive_message()
        assert letter.message == Add(3)


async def test_a_failure_out_of_restarts_during_the_replay_dead_letters_the_rest():
    async with ActorTestKit() as testkit:
        events: list[str] = []
        dead = testkit.create_dead_letter_probe()
        refs: TestProbe[ActorRef[Cmd]] = testkit.create_test_probe()
        child = Behaviors.supervise(started(events)).on_failure(
            S.restart_with_backoff(0.01, 0.01, 0.0).with_max_restarts(1)
        )
        testkit.spawn(parent_of(child, refs))
        kid = await refs.receive_message()
        kid.tell(Add(-1))  # fails: backing off
        kid.tell(Add(-2))  # replayed first after the restart, and fails out of restarts
        kid.tell(Add(3))
        await refs.expect_terminated(kid)
        assert (await dead.receive_message()).message == Add(3)


async def test_a_restart_failing_with_an_unsupervised_error_dead_letters_the_stash():
    starts: list[int] = []

    def setup(_ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
        starts.append(len(starts))
        if len(starts) == 2:
            raise OSError("down at the restart")
        return fragile([])

    async with ActorTestKit() as testkit:
        dead = testkit.create_dead_letter_probe()
        refs: TestProbe[ActorRef[Cmd]] = testkit.create_test_probe()
        child = Behaviors.supervise(Behaviors.setup(setup)).on_failure(
            S.restart_with_backoff(0.01, 0.01, 0.0), ValueError
        )
        testkit.spawn(parent_of(child, refs))
        kid = await refs.receive_message()
        kid.tell(Add(-1))  # fails: backing off
        kid.tell(Add(3))
        await refs.expect_terminated(kid)
        assert (await dead.receive_message()).message == Add(3)


@dataclass(frozen=True)
class WatchPeer:
    peer: ActorRef[object]


def watcher(events: list[str], gone: TestProbe[ActorRef[object]]) -> Behavior[Cmd | WatchPeer]:
    """Counts like ``started``, watches a peer on ``WatchPeer``, and tells ``gone`` when one
    terminates."""

    def setup(ctx: ActorContext[Cmd | WatchPeer]) -> Behavior[Cmd | WatchPeer]:
        events.append("setup")
        return counting(ctx, 0)

    def counting(ctx: ActorContext[Cmd | WatchPeer], total: int) -> Behavior[Cmd | WatchPeer]:
        def on_message(msg: Cmd | WatchPeer) -> Behavior[Cmd | WatchPeer]:
            match msg:
                case WatchPeer(peer):
                    ctx.watch(peer)
                case Add(n) if n < 0:
                    raise ValueError(n)
                case Add(n):
                    return counting(ctx, total + n)
                case Get(reply_to):
                    reply_to.tell(total)
            return Behaviors.same

        def on_signal(
            _ctx: ActorContext[Cmd | WatchPeer], sig: Signal
        ) -> Behavior[Cmd | WatchPeer]:
            if sig is PreRestart:
                events.append("pre-restart")
            elif isinstance(sig, Terminated):
                gone.ref.tell(sig.ref)
            return Behaviors.same

        return Behaviors.receive_message(on_message).receive_signal(on_signal)

    return Behaviors.setup(setup)


async def test_a_terminated_during_a_backoff_is_delivered_after_the_restart():
    # Pekko's RestartSupervisor stashes signals with the messages while it restarts
    async with ActorTestKit(manual_time=True) as testkit:
        manual = ManualTime(testkit.system)
        events: list[str] = []
        gone: TestProbe[ActorRef[object]] = testkit.create_test_probe()
        totals: TestProbe[int] = testkit.create_test_probe()
        peer = testkit.spawn(Behaviors.ignore)
        ref = testkit.spawn(
            Behaviors.supervise(watcher(events, gone)).on_failure(
                S.restart_with_backoff(1, 1, 0.0)
            )
        )
        ref.tell(WatchPeer(peer))
        ref.tell(Add(-1))  # fails: backing off
        ref.tell(Add(3))
        await eventually(lambda: events == ["setup", "pre-restart"])
        await testkit.stop(peer)  # its Terminated arrives while backing off
        ref.tell(Get(totals.ref))
        await gone.expect_no_message(0.05)
        await manual.time_passes(1)  # the restart
        assert await gone.receive_message() == peer
        assert await totals.receive_message() == 3


async def test_a_stop_during_a_backoff_dead_letters_the_messages_but_not_a_terminated():
    async with ActorTestKit(manual_time=True) as testkit:
        events: list[str] = []
        dead = testkit.create_dead_letter_probe()
        gone: TestProbe[ActorRef[object]] = testkit.create_test_probe()
        peer = testkit.spawn(Behaviors.ignore)
        ref = testkit.spawn(
            Behaviors.supervise(watcher(events, gone)).on_failure(
                S.restart_with_backoff(1, 1, 0.0)
            )
        )
        ref.tell(WatchPeer(peer))
        ref.tell(Add(-1))  # fails: backing off
        ref.tell(Add(3))
        await eventually(lambda: events == ["setup", "pre-restart"])
        await testkit.stop(peer)  # its Terminated is stashed behind Add(3)
        await testkit.stop(ref)
        assert (await dead.receive_message()).message == Add(3)
        await dead.expect_no_message(0.05)


async def test_a_zero_delay_backoff_under_manual_time_leaves_no_timer_behind():
    # ManualTime runs a zero delay inside schedule_once, before the handle could be stored
    async with ActorTestKit(manual_time=True) as testkit:
        events: list[str] = []
        ref = testkit.spawn(
            Behaviors.supervise(started(events)).on_failure(S.restart_with_backoff(0, 0, 0.0))
        )
        ref.tell(Add(-1))
        totals: TestProbe[int] = testkit.create_test_probe()
        ref.tell(Get(totals.ref))
        assert await totals.receive_message() == 0  # restarted
        cell = ref._cell  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
        assert not cell._timers  # pyright: ignore[reportUnknownMemberType]


async def test_a_supervisor_returned_for_the_same_exception_replaces_the_current_one():
    """Pekko's SupervisionSpec, "replace supervision when new returned behavior catches same
    exception": the returned stop supervisor is the only one left, so the failure stops the actor
    instead of reaching the restart around it."""

    def switched(msg: str) -> Behavior[str]:
        raise ValueError(msg)

    def first(msg: str) -> Behavior[str]:
        if msg == "switch":
            return Behaviors.supervise(Behaviors.receive_message(switched)).on_failure(S.stop())
        return Behaviors.same

    async with ActorTestKit() as testkit:
        ref = testkit.spawn(
            Behaviors.supervise(Behaviors.receive_message(first)).on_failure(S.restart())
        )
        probe: TestProbe[str] = testkit.create_test_probe()
        ref.tell("switch")
        ref.tell("boom")
        await probe.expect_terminated(ref)


async def test_a_supervisor_nested_for_the_same_exception_keeps_only_the_inner_one():
    """Pekko's InterceptorImpl.preStart deduplicates the started stack: the inner stop supervisor
    hides the outer restart for the same exception."""

    def fails(msg: str) -> Behavior[str]:
        raise ValueError(msg)

    inner = Behaviors.supervise(Behaviors.receive_message(fails)).on_failure(S.stop())
    async with ActorTestKit() as testkit:
        ref = testkit.spawn(Behaviors.supervise(inner).on_failure(S.restart()))
        probe: TestProbe[str] = testkit.create_test_probe()
        ref.tell("boom")
        await probe.expect_terminated(ref)


async def test_a_supervisor_nested_for_another_exception_stays():
    """Pekko's SupervisionSpec, "support nesting to handle different exceptions"."""

    def fails(msg: str) -> Behavior[str]:
        raise ValueError(msg)

    def counted(events: list[str]) -> Behavior[str]:
        def setup(_ctx: ActorContext[str]) -> Behavior[str]:
            events.append("setup")
            return Behaviors.supervise(Behaviors.receive_message(fails)).on_failure(
                S.stop(), exc_type=KeyError
            )

        return Behaviors.setup(setup)

    events: list[str] = []
    async with ActorTestKit() as testkit:
        ref = testkit.spawn(Behaviors.supervise(counted(events)).on_failure(S.restart()))
        ref.tell("boom")  # a ValueError: past the KeyError stop, to the restart
        await eventually(lambda: events == ["setup", "setup"])


def test_reset_after_defaults_to_the_mean_of_min_and_max_backoff():
    """Pekko's SupervisionSpec, "default resetBackoffAfter to average of min and max backoff"."""
    assert S.restart_with_backoff(1.0, 3.0, 0).reset_backoff_after == 2.0
    assert S.restart_with_backoff(1.0, 3.0, 0).with_reset_after(0.5).reset_backoff_after == 0.5


async def test_the_backoff_count_resets_once_the_restarted_actor_has_run_reset_after():
    """Pekko's SupervisionSpec, "reset exponential backoff count after reset timeout", with the
    default reset_after of (1 + 10) / 2 seconds."""
    async with ActorTestKit(manual_time=True) as testkit:
        manual_time = ManualTime(testkit.system)
        events: list[str] = []
        ref = testkit.spawn(
            Behaviors.supervise(started(events)).on_failure(S.restart_with_backoff(1, 10, 0.0))
        )
        totals: TestProbe[int] = testkit.create_test_probe()
        ref.tell(Add(-1))  # backs off min_backoff
        ref.tell(Get(totals.ref))
        await manual_time.time_passes(1)
        await totals.expect_message(0, max=0)
        await manual_time.time_passes(6)  # up for longer than reset_after
        ref.tell(Add(-1))
        ref.tell(Get(totals.ref))
        await manual_time.time_passes(1)  # min_backoff again, not twice it
        await totals.expect_message(0, max=0)


async def test_a_restart_that_fails_at_once_never_resets_the_backoff_count(
    caplog: pytest.LogCaptureFixture,
):
    """Pekko's RestartSupervisor schedules its ResetRestartCount when a restart completes, so the
    backoff delay itself is not healthy time: a setup that always fails keeps doubling the delay,
    even past reset_after."""
    attempts: list[int] = []

    def setup(_ctx: ActorContext[str]) -> Behavior[str]:
        attempts.append(len(attempts))
        raise ConnectionError("not ready")

    strategy = S.restart_with_backoff(0.01, 5.0, 0.0).with_reset_after(0.015)
    with caplog.at_level(logging.WARNING, logger="ohra.actor"):
        async with ActorTestKit() as testkit:
            testkit.spawn(Behaviors.supervise(Behaviors.setup(setup)).on_failure(strategy))
            await eventually(lambda: len(attempts) >= 5)
    delays = [
        r.getMessage().split("restarting in ")[1].split("s ")[0]
        for r in caplog.records
        if "restarting in" in r.getMessage()
    ]
    assert delays[:4] == ["0.010", "0.020", "0.040", "0.080"]
