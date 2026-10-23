"""Benchmark: numpy work in threads scales across cores, pure Python does not,
and the event loop stays responsive either way.

    uv run --extra numpy python examples/compute.py
"""
import asyncio
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import median

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pekko_py import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    StashBuffer,
    Success,
    Try,
)

WORKERS = min(8, os.cpu_count() or 1)


def numpy_job(seed: int) -> float:
    # sort is single-threaded and releases the GIL (matmul would already use all cores via BLAS)
    return float(np.sort(np.random.default_rng(seed).random(2_000_000))[-1])


def python_job(seed: int) -> float:
    return float(sum((i * seed) % 7 for i in range(400_000)))


# --- worker: compute in a thread, stash meanwhile --------------------------------------


@dataclass(frozen=True)
class Compute:
    job: str
    seed: int
    reply_to: ActorRef[float]


@dataclass(frozen=True)
class Done:
    result: Try[float]


type Cmd = Compute | Done
JOBS = {"numpy": numpy_job, "python": python_job}


def worker() -> Behavior[Cmd]:
    def with_stash(buffer: StashBuffer[Cmd]) -> Behavior[Cmd]:
        def idle(ctx: ActorContext[Cmd], msg: Cmd) -> Behavior[Cmd]:
            match msg:
                case Compute(job, seed, reply_to):
                    ctx.pipe_to_self(asyncio.to_thread(JOBS[job], seed), Done)
                    return computing(reply_to)
                case Done():
                    return Behaviors.same

        def computing(reply_to: ActorRef[float]) -> Behavior[Cmd]:
            def on_message(msg: Cmd) -> Behavior[Cmd]:
                match msg:
                    case Done(Success(value)):
                        reply_to.tell(value)
                        return buffer.unstash_all(Behaviors.receive(idle))
                    case Done(_):
                        reply_to.tell(float("nan"))
                        return buffer.unstash_all(Behaviors.receive(idle))
                    case _:
                        buffer.stash(msg)
                        return Behaviors.same

            return Behaviors.receive_message(on_message)

        return Behaviors.receive(idle)

    return Behaviors.with_stash(1000, with_stash)


# --- latency probe: a timer ticking every 5ms records how late it fires ----------------


@dataclass(frozen=True)
class Tick:
    pass


@dataclass(frozen=True)
class Report:
    reply_to: ActorRef[list[float]]


def probe(period: float = 0.005) -> Behavior[Tick | Report]:
    def setup(ctx: ActorContext[Tick | Report]) -> Behavior[Tick | Report]:
        ctx.timers.start_timer_with_fixed_delay("tick", Tick(), period)
        loop = asyncio.get_running_loop()

        def ticking(last: float, lateness: list[float]) -> Behavior[Tick | Report]:
            def on_message(msg: Tick | Report) -> Behavior[Tick | Report]:
                match msg:
                    case Tick():
                        now = loop.time()
                        return ticking(now, [*lateness, (now - last) - period])
                    case Report(reply_to):
                        reply_to.tell(lateness)
                        ctx.timers.start_timer_with_fixed_delay("tick", Tick(), period)  # drops a queued stale Tick
                        return ticking(loop.time(), [])

            return Behaviors.receive_message(on_message)

        return ticking(loop.time(), [])

    return Behaviors.setup(setup)


# --- driver --------------------------------------------------------------------------------


@dataclass(frozen=True)
class Run:
    job: str
    reply_to: ActorRef[None]


@dataclass(frozen=True)
class Collected:
    value: float


def guardian(ctx: ActorContext[Run | Collected]) -> Behavior[Run | Collected]:
    workers = [ctx.spawn(worker(), f"w{i}") for i in range(WORKERS)]
    collector = ctx.message_adapter(Collected)

    def idle(msg: Run | Collected) -> Behavior[Run | Collected]:
        match msg:
            case Run(job, reply_to):
                for i, w in enumerate(workers):
                    w.tell(Compute(job, i, collector))
                return collecting(reply_to, WORKERS)
            case Collected():
                return Behaviors.same

    def collecting(reply_to: ActorRef[None], remaining: int) -> Behavior[Run | Collected]:
        def on_message(msg: Run | Collected) -> Behavior[Run | Collected]:
            match msg:
                case Collected() if remaining == 1:
                    reply_to.tell(None)
                    return Behaviors.receive_message(idle)
                case Collected():
                    return collecting(reply_to, remaining - 1)
                case Run():
                    raise RuntimeError("one run at a time")

        return Behaviors.receive_message(on_message)

    return Behaviors.receive_message(idle)


async def timed_parallel(system: ActorSystem[Run | Collected], job: str) -> float:
    t0 = time.perf_counter()
    await system.ask(lambda r: Run(job, r), timeout=60)
    return time.perf_counter() - t0


def timed_sequential(job: str) -> float:
    t0 = time.perf_counter()
    for i in range(WORKERS):
        JOBS[job](i)
    return time.perf_counter() - t0


async def main() -> None:
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    print(f"python {sys.version.split()[0]}  GIL {'on' if gil else 'off'}  workers {WORKERS}  cores {os.cpu_count()}")
    print("job      sequential  parallel  speedup  5ms-timer lateness meanwhile (median / max)")
    async with ActorSystem(Behaviors.setup(guardian), "bench") as system, ActorSystem(probe(), "probe") as p:
        await p.ask(Report, timeout=1)  # reset the probe
        await asyncio.sleep(0.2)
        late = await p.ask(Report, timeout=1)
        print(f"idle                                     {median(late) * 1000:6.1f}ms / {max(late) * 1000:6.1f}ms")
        for job in ("numpy", "python"):
            JOBS[job](0)  # warm up
            seq = timed_sequential(job)
            await p.ask(Report, timeout=1)
            par = await timed_parallel(system, job)
            late = await p.ask(Report, timeout=1)
            print(
                f"{job:7}  {seq:9.2f}s  {par:7.2f}s  {seq / par:6.1f}x  "
                f"{median(late) * 1000:6.1f}ms / {max(late) * 1000:6.1f}ms  ({len(late)} ticks)"
            )


asyncio.run(main())
