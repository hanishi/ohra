"""Supervision."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    PreRestart,
    Signal,
    SupervisorStrategy,
)
from ohra.supervision import SupervisorStrategy as S
from ohra.testkit import ActorTestKit, TestProbe

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
    async with ActorTestKit() as testkit:
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
        for _ in range(3):  # let the child stash it
            await asyncio.sleep(0)
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
