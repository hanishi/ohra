"""CoordinatedShutdown on a plain system: Pekko's phases, timeouts that continue, run once, the
signal hook."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from dataclasses import dataclass

import pytest

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    ActorSystemTerminateReason,
    Behavior,
    Behaviors,
    CoordinatedShutdown,
    CoordinatedShutdownSettings,
    JvmExitReason,
    PostStop,
    Reason,
    Signal,
)
from ohra.coordinated_shutdown import PHASES

FAST = CoordinatedShutdownSettings(
    default_phase_timeout=0.2, phase_timeouts={"actor-system-terminate": 0.5}
)


def test_phase_list_and_timeouts():
    names = [p.name for p in PHASES]
    assert (
        names[:2] == ["before-service-unbind", "service-unbind"]
        and names[-1] == "actor-system-terminate"
    )
    timeouts = {p.name: p.timeout for p in PHASES}
    assert timeouts["cluster-exiting"] == 10.0 and timeouts["service-stop"] == 5.0
    with pytest.raises(ValueError):
        CoordinatedShutdownSettings(default_phase_timeout=0)
    with pytest.raises(ValueError):
        CoordinatedShutdownSettings(phase_timeouts={"no-such-phase": 1.0})


async def test_phases_run_in_order_and_tasks_in_a_phase_concurrently():
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=FAST)
    cs = CoordinatedShutdown(system)
    log: list[str] = []

    async def slow(name: str) -> None:
        log.append(f"{name}-start")
        await asyncio.sleep(0.05)
        log.append(f"{name}-end")

    cs.add_task("service-unbind", "a", lambda: slow("unbind-a"))
    cs.add_task("service-unbind", "b", lambda: slow("unbind-b"))
    cs.add_task("service-stop", "c", lambda: slow("stop"))
    await cs.run(ActorSystemTerminateReason())
    await system.when_terminated()
    # a and b overlapped, c after both
    assert log[:2] == ["unbind-a-start", "unbind-b-start"] and log[4] == "stop-start"
    assert cs.shutdown_reason == ActorSystemTerminateReason()


async def test_a_failing_task_and_a_hanging_task_do_not_stop_the_run(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.WARNING, logger="ohra.coordinated_shutdown")
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=FAST)
    cs = CoordinatedShutdown(system)

    async def failing() -> None:
        raise RuntimeError("boom")

    async def hanging() -> None:
        await asyncio.sleep(60)

    cs.add_task("service-unbind", "failing", failing)
    cs.add_task("service-stop", "hanging", hanging)
    began = asyncio.get_running_loop().time()
    await cs.run(ActorSystemTerminateReason())
    await system.when_terminated()
    # the hanging task cost exactly its phase timeout
    assert 0.2 <= asyncio.get_running_loop().time() - began < 1.0
    assert any("failing" in r.message for r in caplog.records) and any(
        "timed out" in r.message for r in caplog.records
    )


async def test_a_hanging_task_delays_only_its_phase():
    settings = CoordinatedShutdownSettings(
        default_phase_timeout=5.0, phase_timeouts={"service-stop": 0.1}
    )
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=settings)

    async def hanging() -> None:
        await asyncio.sleep(60)

    CoordinatedShutdown(system).add_task("service-stop", "hanging", hanging)
    began = asyncio.get_running_loop().time()
    system.terminate()
    await system.when_terminated()
    assert asyncio.get_running_loop().time() - began < 1.0


async def test_run_is_once_and_terminate_twice_joins_the_same_run():
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=FAST)
    cs = CoordinatedShutdown(system)
    first = cs.run(ActorSystemTerminateReason())
    assert cs.run(JvmExitReason()) is first and cs.shutdown_reason == ActorSystemTerminateReason()
    system.terminate()
    system.terminate()  # no error, no second run
    await system.when_terminated()


async def test_unknown_phase_and_empty_name_raise():
    async with ActorSystem(Behaviors.ignore, "cs", shutdown=FAST) as system:
        with pytest.raises(ValueError):
            CoordinatedShutdown(system).add_task("nope", "x", lambda: asyncio.sleep(0))
        with pytest.raises(ValueError):
            CoordinatedShutdown(system).add_task("service-stop", "", lambda: asyncio.sleep(0))


async def test_actor_termination_task_waits_for_the_actor():
    stopped: list[str] = []
    refs: list[ActorRef[str]] = []

    def worker() -> Behavior[str]:
        def on_message(msg: str) -> Behavior[str]:
            return Behaviors.stopped() if msg == "stop" else Behaviors.same

        def on_signal(_ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
            if sig is PostStop:
                stopped.append("worker")
            return Behaviors.same

        return Behaviors.receive_message(on_message).receive_signal(on_signal)

    def guardian(ctx: ActorContext[str]) -> Behavior[str]:
        refs.append(ctx.spawn(worker(), "w"))
        return Behaviors.ignore

    system = ActorSystem(Behaviors.setup(guardian), "cs", shutdown=FAST)
    await asyncio.sleep(0)  # the guardian's setup has run
    cs = CoordinatedShutdown(system)
    cs.add_actor_termination_task("service-stop", "w", refs[0], "stop")
    seen_before_terminate: list[list[str]] = []

    async def check() -> None:
        seen_before_terminate.append(list(stopped))

    cs.add_task("before-actor-system-terminate", "check", check)
    system.terminate()
    await system.when_terminated()
    # the worker had stopped before the later phase ran
    assert seen_before_terminate == [["worker"]]


async def test_terminate_runs_a_task_before_the_guardian_stops():
    order: list[str] = []

    def guardian() -> Behavior[str]:
        def on_signal(_ctx: ActorContext[str], sig: Signal) -> Behavior[str]:
            if sig is PostStop:
                order.append("guardian-stopped")
            return Behaviors.same

        return Behaviors.receive_message(lambda _m: Behaviors.same).receive_signal(on_signal)

    system = ActorSystem(guardian(), "cs", shutdown=FAST)

    async def task() -> None:
        order.append("task")

    CoordinatedShutdown(system).add_task("before-actor-system-terminate", "t", task)
    system.terminate()
    await system.when_terminated()
    assert order == ["task", "guardian-stopped"]


async def test_run_by_actor_system_terminate_off_stops_directly():
    ran: list[str] = []
    system = ActorSystem(
        Behaviors.ignore,
        "cs",
        shutdown=CoordinatedShutdownSettings(run_by_actor_system_terminate=False),
    )

    async def task() -> None:
        ran.append("x")

    CoordinatedShutdown(system).add_task("service-stop", "t", task)
    system.terminate()
    await system.when_terminated()
    assert ran == [] and CoordinatedShutdown(system).shutdown_reason is None


async def test_signal_runs_the_shutdown_and_handler_is_idempotent_and_removed():
    settings = CoordinatedShutdownSettings(
        default_phase_timeout=0.2, run_on_signals=(signal.SIGUSR1,)
    )
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=settings)
    os.kill(os.getpid(), signal.SIGUSR1)
    os.kill(os.getpid(), signal.SIGUSR1)  # twice: the second joins the run
    await asyncio.wait_for(system.when_terminated(), timeout=5)
    assert CoordinatedShutdown(system).shutdown_reason == JvmExitReason()
    # already removed
    assert asyncio.get_running_loop().remove_signal_handler(signal.SIGUSR1) is False


async def test_timeout_arithmetic():
    settings = CoordinatedShutdownSettings(
        default_phase_timeout=1.0, phase_timeouts={"cluster-exiting": 2.0}
    )
    async with ActorSystem(Behaviors.ignore, "cs", shutdown=settings) as system:
        cs = CoordinatedShutdown(system)
        assert (
            cs.timeout("service-stop") == 1.0
            and cs.timeout("cluster-exiting") == 2.0
            and cs.timeout("actor-system-terminate") == 10.0
        )
        assert cs.total_timeout == 9 * 1.0 + 2.0 + 10.0 + 10.0


@dataclass(frozen=True)
class MyReason(Reason): ...


async def test_user_reason():
    system = ActorSystem(Behaviors.ignore, "cs", shutdown=FAST)
    await CoordinatedShutdown(system).run(MyReason())
    await system.when_terminated()
    assert CoordinatedShutdown(system).shutdown_reason == MyReason()
