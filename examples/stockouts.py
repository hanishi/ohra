"""Benchmark: a loop numpy cannot vectorize, run four ways in actors. Which products lose
the most sales to stockouts? Each order changes the stock, and a reorder arrives
LEAD_TIME orders after it is placed, so every step depends on the one before. Pure
Python in a thread, pure Python in a process with shared memory, numba in a thread, and
numba with nogil=True in a thread; one analyst, then eight at once, with the worst delay
of the event loop meanwhile.

    uv run --extra numba python examples/stockouts.py
"""

import asyncio
import os
import sys
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import numpy.typing as npt
from numba import njit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ohra import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, Success, Try
from ohra.ext.shm import SharedArray

N_ORDERS = 20_000_000
N_PRODUCTS = 1000
LEAD_TIME = 5_000  # orders between placing a reorder and its arrival
ANALYSTS = min(8, os.cpu_count() or 1)

type Ints = npt.NDArray[np.int64]
type Run = Callable[[], Awaitable[list[int]]]


def lost_sales(orders: Ints, restock: Ints) -> Ints:
    """Simulates the stock of every product, order by order: a sale when there is stock, a
    lost sale when there is none, and a reorder of restock[p] when it falls to a quarter
    of that. Returns the lost sales per product."""
    stock = restock.copy()
    arrives = np.full(len(restock), -1, np.int64)
    lost = np.zeros(len(restock), np.int64)
    for t in range(len(orders)):
        p = orders[t]
        if arrives[p] >= 0 and t >= arrives[p]:  # the reorder came in
            stock[p] += restock[p]
            arrives[p] = -1
        if stock[p] > 0:
            stock[p] -= 1
        else:
            lost[p] += 1
        if stock[p] <= restock[p] // 4 and arrives[p] < 0:  # running low: reorder
            arrives[p] = t + LEAD_TIME
    return lost


def lost_sales_py(orders: Ints, restock: Ints) -> list[int]:
    """The same simulation on Python lists, the fastest pure-Python form."""
    refill: list[int] = restock.tolist()
    stock = refill.copy()
    arrives = [-1] * len(stock)
    lost = [0] * len(stock)
    ids: memoryview[int] = memoryview(orders)  # Python ints one at a time, not a 20M list
    for t, p in enumerate(ids):
        if arrives[p] >= 0 and t >= arrives[p]:
            stock[p] += refill[p]
            arrives[p] = -1
        if stock[p] > 0:
            stock[p] -= 1
        else:
            lost[p] += 1
        if stock[p] <= refill[p] // 4 and arrives[p] < 0:
            arrives[p] = t + LEAD_TIME
    return worst_three(np.array(lost))


lost_sales_holding_gil = njit(lost_sales)
lost_sales_nogil = njit(nogil=True)(lost_sales)


def worst_three(lost: Ints) -> list[int]:
    return [int(p) for p in np.argsort(-lost, kind="stable")[:3]]


def numba_holding_gil(orders: Ints, restock: Ints) -> list[int]:
    return worst_three(lost_sales_holding_gil(orders, restock))


def numba_nogil(orders: Ints, restock: Ints) -> list[int]:
    return worst_three(lost_sales_nogil(orders, restock))


def py_in(orders: SharedArray, restock: Ints) -> list[int]:
    with orders.open() as shared:
        return lost_sales_py(shared, restock)


@dataclass(frozen=True)
class WorstStockouts:
    reply_to: ActorRef[list[int]]


@dataclass(frozen=True)
class Computed:
    result: Try[list[int]]
    reply_to: ActorRef[list[int]]


type AnalystCmd = WorstStockouts | Computed


def analyst(run: Run) -> Behavior[AnalystCmd]:
    @Behaviors.receive
    def on_message(ctx: ActorContext[AnalystCmd], msg: AnalystCmd) -> Behavior[AnalystCmd]:
        match msg:
            case WorstStockouts(reply_to):
                ctx.pipe_to_self(run(), lambda result: Computed(result, reply_to))
            case Computed(Success(worst), reply_to):
                reply_to.tell(worst)
            case Computed():
                pass  # it failed: the ask times out
        return Behaviors.same

    return on_message


@dataclass(frozen=True)
class AskAnalyst:
    index: int
    reply_to: ActorRef[list[int]]


def team(run: Run) -> Behavior[AskAnalyst]:
    """A guardian with ANALYSTS analysts, passing each request to the one it names."""

    @Behaviors.setup
    def start(ctx: ActorContext[AskAnalyst]) -> Behavior[AskAnalyst]:
        analysts = [ctx.spawn(analyst(run), f"analyst-{i}") for i in range(ANALYSTS)]

        @Behaviors.receive_message
        def on_message(msg: AskAnalyst) -> Behavior[AskAnalyst]:
            analysts[msg.index].tell(WorstStockouts(msg.reply_to))
            return Behaviors.same

        return on_message

    return start


async def worst_lag(stop: asyncio.Event) -> float:
    """The longest a 1 ms sleep overslept: how long the event loop could not run."""
    worst = 0.0
    while not stop.is_set():
        before = time.perf_counter()
        await asyncio.sleep(0.001)
        worst = max(worst, time.perf_counter() - before - 0.001)
    return worst


async def measure(name: str, run: Run) -> list[int]:
    line = f"{name:<30}"
    worst: list[int] = []
    async with ActorSystem(team(run), "stockouts") as system:
        for n in (1, ANALYSTS):
            stop = asyncio.Event()
            probe = asyncio.create_task(worst_lag(stop))
            start = time.perf_counter()
            replies = await asyncio.gather(
                *(system.ask(lambda r, i=i: AskAnalyst(i, r), timeout=600) for i in range(n))
            )
            elapsed = time.perf_counter() - start
            stop.set()
            lag = await probe
            worst = replies[0]
            line += f"{elapsed:6.2f}s (lag {lag * 1000:5.0f} ms)  "
    print(line, worst, flush=True)
    return worst


async def main() -> None:
    rng = np.random.default_rng(0)
    popularity = 1.0 / np.arange(1, N_PRODUCTS + 1)
    share = popularity / popularity.sum()
    orders = rng.choice(N_PRODUCTS, size=N_ORDERS, p=share)
    # each reorder brings about what sells while the next one is on its way, give or take half
    expected = LEAD_TIME * share * rng.uniform(0.5, 1.5, N_PRODUCTS)
    restock = np.maximum(1, expected).astype(np.int64)
    orders.flags.writeable = False
    restock.flags.writeable = False

    start = time.perf_counter()
    numba_holding_gil(orders[:10], restock)
    numba_nogil(orders[:10], restock)
    print(f"python {sys.version.split()[0]}, {N_ORDERS:,} orders, {ANALYSTS} analysts")
    print(f"numba compiled both versions in {time.perf_counter() - start:.2f}s\n")
    print(f"{'':<30}{'one analyst':<26}{f'{ANALYSTS} at once':<26}worst three")

    loop = asyncio.get_running_loop()
    shared = SharedArray.create(orders)
    try:
        with ProcessPoolExecutor(ANALYSTS) as pool:
            await asyncio.gather(*(loop.run_in_executor(pool, int) for _ in range(ANALYSTS)))
            runs: dict[str, Run] = {
                "pure Python, thread": lambda: asyncio.to_thread(lost_sales_py, orders, restock),
                "pure Python, process + shm": lambda: loop.run_in_executor(
                    pool, py_in, shared, restock
                ),
                "numba, thread": lambda: asyncio.to_thread(numba_holding_gil, orders, restock),
                "numba nogil=True, thread": lambda: asyncio.to_thread(
                    numba_nogil, orders, restock
                ),
            }
            results = [await measure(name, run) for name, run in runs.items()]
    finally:
        shared.unlink()
    print("\nall four agree:", all(r == results[0] for r in results))


if __name__ == "__main__":
    asyncio.run(main())
