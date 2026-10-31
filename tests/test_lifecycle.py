"""Children, stop, DeathWatch."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ChildFailed,
    PostStop,
    Signal,
    SpawnProtocol,
    Terminated,
)
from ohra.testkit import ActorTestKit, TestProbe

from .util import eventually


def recorder(name: str, events: list[str], children: tuple[str, ...] = ()) -> Behavior[str]:
    def setup(ctx: ActorContext[str]) -> Behavior[str]:
        for c in children:
            ctx.spawn(recorder(f"{name}/{c}", events), c)

        def on_signal(_ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
            if sig is PostStop:
                events.append(f"stopped {name}")
            return Behaviors.same

        return Behaviors.receive(lambda _c, m: Behaviors.same).receive_signal(on_signal)

    return Behaviors.setup(setup)


async def test_stopping_parent_stops_descendants_first():
    events: list[str] = []

    def guardian(ctx: ActorContext[str]) -> Behavior[str]:
        ctx.spawn(recorder("a", events, ("x", "y")), "a")
        return Behaviors.ignore

    system = ActorSystem(Behaviors.setup(guardian), "t")
    system.terminate()
    await system.when_terminated()
    assert events[-1] == "stopped a"
    assert set(events[:-1]) == {"stopped a/x", "stopped a/y"}


@dataclass(frozen=True)
class Spawn:
    pass


@dataclass(frozen=True)
class StopChild:
    pass


@dataclass(frozen=True)
class Events:
    reply_to: ActorRef[list[str]]


type ParentCmd = Spawn | StopChild | Events


async def test_watch_delivers_terminated_and_ctx_stop_drops_pending():
    events: list[str] = []

    def parent(ctx: ActorContext[ParentCmd]) -> Behavior[ParentCmd]:
        child: list[ActorRef[str]] = []

        def on_message(msg: ParentCmd) -> Behavior[ParentCmd]:
            match msg:
                case Spawn():
                    c = ctx.spawn(
                        Behaviors.receive_message(lambda m: events.append(m) or Behaviors.same),
                        "child",
                    )
                    ctx.watch(c)
                    child.append(c)
                case StopChild():
                    ctx.stop(child[0])
                    child[0].tell("never delivered")
                case Events(reply_to):
                    reply_to.tell(list(events))
            return Behaviors.same

        def on_signal(_ctx: ActorContext[ParentCmd], sig: Signal) -> Behavior[ParentCmd]:
            if isinstance(sig, Terminated):
                events.append(f"terminated {sig.ref.path.rsplit('/', 1)[-1]}")
            return Behaviors.same

        return Behaviors.receive_message(on_message).receive_signal(on_signal)

    async with ActorSystem(Behaviors.setup(parent), "t") as system:
        system.tell(Spawn())
        system.tell(StopChild())
        await system.ask(Events, timeout=1)
        assert await system.ask(Events, timeout=1) == ["terminated child"]


async def test_child_failure_gives_child_failed_and_unhandled_terminated_is_a_death_pact():
    seen: list[Signal] = []

    def boom(_m: str) -> Behavior[str]:
        raise ValueError("boom")

    def handles_it(ctx: ActorContext[str]) -> Behavior[str]:
        c = ctx.spawn(Behaviors.receive_message(boom), "c")
        ctx.watch(c)
        c.tell("go")

        def on_signal(_ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
            seen.append(sig)
            return Behaviors.same

        return Behaviors.receive(lambda _c, _m: Behaviors.same).receive_signal(on_signal)

    async with ActorSystem(Behaviors.setup(handles_it), "t"):
        await eventually(lambda: any(isinstance(s, ChildFailed) for s in seen))
    failed = [s for s in seen if isinstance(s, ChildFailed)]
    assert isinstance(failed[0].cause, ValueError)

    def ignores_it(ctx: ActorContext[str]) -> Behavior[str]:
        c = ctx.spawn(Behaviors.receive_message(boom), "c")
        ctx.watch(c)
        c.tell("go")
        return Behaviors.ignore  # no signal handler -> DeathPactError -> guardian stops

    system = ActorSystem(Behaviors.setup(ignores_it), "t2")
    await asyncio.wait_for(system.when_terminated(), 1)


async def test_watch_with_and_watching_an_already_dead_actor():
    got: list[str] = []

    def guardian(ctx: ActorContext[str]) -> Behavior[str]:
        c = ctx.spawn(Behaviors.stopped(), "dies-immediately")

        def on_message(m: str) -> Behavior[str]:
            got.append(m)
            if m == "first-gone":
                ctx.watch_with(c, "watched-dead")  # already terminated: still notified
            return Behaviors.same

        ctx.watch_with(c, "first-gone")
        return Behaviors.receive_message(on_message)

    async with ActorSystem(Behaviors.setup(guardian), "t"):
        await eventually(lambda: len(got) == 2)
    assert got == ["first-gone", "watched-dead"]


async def test_spawn_protocol_spawns_on_request_with_unique_names():
    events: list[str] = []
    async with ActorSystem(SpawnProtocol.apply(), "t") as system:
        a = await system.ask(
            lambda r: SpawnProtocol.Spawn(recorder("a", events), "worker", r), timeout=1
        )
        b = await system.ask(
            lambda r: SpawnProtocol.Spawn(recorder("b", events), "worker", r), timeout=1
        )
        c = await system.ask(
            lambda r: SpawnProtocol.Spawn(recorder("c", events), "", r), timeout=1
        )
        assert [r.path for r in (a, b, c)] == [
            "ohra://t/user/worker",
            "ohra://t/user/worker-1",
            "ohra://t/user/$0",
        ]
    assert sorted(events) == ["stopped a", "stopped b", "stopped c"]


async def test_stopping_an_already_stopped_child_does_nothing():
    """Pekko's ActorContextSpec, "not have problems stopping already stopped child"."""
    async with ActorTestKit() as testkit:
        acks: TestProbe[str] = testkit.create_test_probe()

        def parent(ctx: ActorContext[str]) -> Behavior[str]:
            child = ctx.spawn(Behaviors.ignore, "child")
            ctx.watch(child)

            def on_message(_msg: str) -> Behavior[str]:
                ctx.stop(child)
                acks.ref.tell("stopped")
                return Behaviors.same

            def on_signal(_ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
                if isinstance(sig, Terminated):
                    acks.ref.tell("terminated")
                return Behaviors.same

            return Behaviors.receive_message(on_message).receive_signal(on_signal)

        ref = testkit.spawn(Behaviors.setup(parent))
        ref.tell("stop")
        await acks.expect_message("stopped")
        await acks.expect_message("terminated")
        ref.tell("stop again")
        await acks.expect_message("stopped")


async def test_stopping_an_actor_that_is_not_a_child_raises():
    async with ActorTestKit() as testkit:
        outcomes: TestProbe[str] = testkit.create_test_probe()
        stranger = testkit.spawn(Behaviors.ignore)

        def on_message(ctx: ActorContext[ActorRef[Any]], ref: ActorRef[Any]) -> Behavior[Any]:
            try:
                ctx.stop(ref)
            except ValueError:
                outcomes.ref.tell("ValueError")
            return Behaviors.same

        ref: ActorRef[ActorRef[Any]] = testkit.spawn(Behaviors.receive(on_message))
        ref.tell(stranger)
        await outcomes.expect_message("ValueError")
        ref.tell(ref)
        await outcomes.expect_message("ValueError")


async def test_watch_with_the_same_message_twice_is_one_watch():
    """Pekko's WatchSpec, "allow idempotent invocations of watchWith with matching msgs"."""
    async with ActorTestKit() as testkit:
        got: TestProbe[str] = testkit.create_test_probe()

        def watcher(ctx: ActorContext[str]) -> Behavior[str]:
            child = ctx.spawn(Behaviors.ignore, "child")
            ctx.watch_with(child, "gone")
            ctx.watch_with(child, "gone")
            ctx.stop(child)

            def on_message(msg: str) -> Behavior[str]:
                got.ref.tell(msg)
                return Behaviors.same

            return Behaviors.receive_message(on_message)

        testkit.spawn(Behaviors.setup(watcher))
        await got.expect_message("gone")
        await got.expect_no_message(0.05)


@pytest.mark.parametrize(
    ("first", "second"),
    [("gone", "other"), (None, "gone"), ("gone", None)],
    ids=["watch_with-then-watch_with", "watch-then-watch_with", "watch_with-then-watch"],
)
async def test_watching_again_with_another_message_fails_the_watcher(
    first: str | None, second: str | None
):
    """Pekko's WatchSpec, "fail when watchWitch is used after watchWith with different
    termination message", and the two cases with a plain watch: unwatch first to change it."""

    def watch(ctx: ActorContext[str], ref: ActorRef[Any], msg: str | None) -> None:
        if msg is None:
            ctx.watch(ref)
        else:
            ctx.watch_with(ref, msg)

    async with ActorTestKit() as testkit:
        target = testkit.spawn(Behaviors.ignore)

        def on_message(ctx: ActorContext[str], _msg: str) -> Behavior[str]:
            watch(ctx, target, first)
            watch(ctx, target, second)
            return Behaviors.same

        ref = testkit.spawn(Behaviors.receive(on_message))
        probe: TestProbe[str] = testkit.create_test_probe()
        ref.tell("watch twice")
        await probe.expect_terminated(ref)


async def test_unwatch_then_watch_with_another_message():
    """Pekko's WatchSpec, "allow watch message definition after watch using unwatch"."""
    async with ActorTestKit() as testkit:
        got: TestProbe[str] = testkit.create_test_probe()

        def watcher(ctx: ActorContext[str]) -> Behavior[str]:
            child = ctx.spawn(Behaviors.ignore, "child")
            ctx.watch_with(child, "first")
            ctx.unwatch(child)
            ctx.watch_with(child, "second")
            ctx.stop(child)

            def on_message(msg: str) -> Behavior[str]:
                got.ref.tell(msg)
                return Behaviors.same

            return Behaviors.receive_message(on_message)

        testkit.spawn(Behaviors.setup(watcher))
        await got.expect_message("second")
