"""Message throughput and latency of the runtime itself: tell into a busy actor, through a
pool, ping-pong between two actors, and ask from outside. The numbers in the README come
from this file.

    uv run python examples/benchmark.py
"""

import asyncio
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ohra import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, Routers

N = 200_000


@dataclass(frozen=True)
class Done:
    reply_to: ActorRef[int]


def counter(total: int = 0) -> Behavior[int | Done]:
    """The functional style: a new behavior per message."""

    @Behaviors.receive_message
    def on_message(msg: int | Done) -> Behavior[int | Done]:
        if isinstance(msg, Done):
            msg.reply_to.tell(total)
            return Behaviors.same
        return counter(total + msg)

    return on_message


def counter_in_place() -> Behavior[int | Done]:
    """State in a per-actor cell, handler returns same: nothing allocated per message."""

    @Behaviors.setup
    def setup(_ctx: ActorContext[int | Done]) -> Behavior[int | Done]:
        total = [0]

        @Behaviors.receive_message
        def on_message(msg: int | Done) -> Behavior[int | Done]:
            if isinstance(msg, Done):
                msg.reply_to.tell(total[0])
            else:
                total[0] += msg
            return Behaviors.same

        return on_message

    return setup


def row(name: str, count: int, seconds: float, unit: str) -> None:
    rate = count / seconds
    rate_s = f"{rate / 1e6:.2f}M/s" if rate >= 1e6 else f"{rate / 1e3:.0f}k/s"
    print(f"{name:36} {rate_s:>9}   {seconds / count * 1e6:6.2f} µs per {unit}")


async def tell_throughput(name: str, b: Behavior[int | Done]) -> None:
    async with ActorSystem(b, "t") as system:
        t0 = time.perf_counter()
        for _ in range(N):
            system.tell(1)
        assert await system.ask(Done, timeout=30) == N
        row(name, N, time.perf_counter() - t0, "message")


async def pool_throughput() -> None:
    async with ActorSystem(Routers.pool(8, counter_in_place()), "r") as system:
        t0 = time.perf_counter()
        for _ in range(N):
            system.tell(1)
        totals = await asyncio.gather(*(system.ask(Done, timeout=30) for _ in range(8)))
        assert sum(totals) == N
        row("tell through a pool of 8", N, time.perf_counter() - t0, "message")


@dataclass(frozen=True)
class Ping:
    n: int
    reply_to: ActorRef["Pong"]


@dataclass(frozen=True)
class Pong:
    n: int


async def ping_pong() -> None:
    rounds = 50_000
    done: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def ponger() -> Behavior[Ping]:
        @Behaviors.receive_message
        def on_message(msg: Ping) -> Behavior[Ping]:
            msg.reply_to.tell(Pong(msg.n))
            return Behaviors.same

        return on_message

    @Behaviors.setup
    def pinger(ctx: ActorContext[Pong]) -> Behavior[Pong]:
        other = ctx.spawn(ponger(), "ponger")
        other.tell(Ping(0, ctx.self))

        @Behaviors.receive_message
        def on_message(msg: Pong) -> Behavior[Pong]:
            if msg.n + 1 == rounds:
                done.set_result(None)
            else:
                other.tell(Ping(msg.n + 1, ctx.self))
            return Behaviors.same

        return on_message

    t0 = time.perf_counter()
    async with ActorSystem(pinger, "pp"):
        await done
    row("ping-pong between two actors", rounds, time.perf_counter() - t0, "round trip")


async def ask_latency() -> None:
    n = 20_000
    async with ActorSystem(counter_in_place(), "a") as system:
        t0 = time.perf_counter()
        for _ in range(n):
            await system.ask(Done, timeout=5)
        row("ask from outside, one at a time", n, time.perf_counter() - t0, "ask")


async def spawn_cost() -> None:
    n = 20_000

    @Behaviors.setup
    def guardian(ctx: ActorContext[Done]) -> Behavior[Done]:
        for i in range(n):
            ctx.spawn(Behaviors.ignore, str(i))
        return Behaviors.receive_message(lambda m: Behaviors.same)

    t0 = time.perf_counter()
    async with ActorSystem(guardian, "s"):
        pass
    row("spawn and stop an actor", n, time.perf_counter() - t0, "actor")


async def main() -> None:
    print(f"python {sys.version.split()[0]}, {N} messages\n")
    await tell_throughput("tell, handler returns same", counter_in_place())
    await tell_throughput("tell, handler returns a new behavior", counter())
    await pool_throughput()
    await ping_pong()
    await ask_latency()
    await spawn_cost()


asyncio.run(main())
