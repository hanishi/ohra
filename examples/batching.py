"""Benchmark: throughput and p99 latency of the batching behavior across batch sizes
and wait times. The batch handler does a numpy update plus a fixed per-batch cost
(an await, standing in for a round trip to a database or a GPU).

    uv run --extra numpy python examples/batching.py
"""
import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pekko_py import ActorSystem, batching

N_MESSAGES = 5_000
N_ARMS = 1_000
PER_BATCH_COST = 0.0002  # seconds


@dataclass(frozen=True, slots=True)
class Reward:
    arm: int
    value: float
    sent: float


async def run(max_n: int, max_wait: float, rate: float | None) -> tuple[float, float, int]:
    """rate=None: send as fast as possible. Otherwise send ``rate`` messages per second in 1ms bursts."""
    loop = asyncio.get_running_loop()
    totals = np.zeros(N_ARMS)
    latencies: list[float] = []
    n_batches = 0

    async def handle(batch: list[Reward]) -> None:
        nonlocal n_batches
        np.add.at(totals, [r.arm for r in batch], [r.value for r in batch])
        await asyncio.sleep(PER_BATCH_COST)
        now = loop.time()
        latencies.extend(now - r.sent for r in batch)
        n_batches += 1

    rng = np.random.default_rng(0)
    arms = rng.integers(0, N_ARMS, N_MESSAGES)
    async with ActorSystem(batching(max_n, max_wait, handle), "bench") as system:
        t0 = loop.time()
        burst = 50 if rate is None else max(1, int(rate / 1000))
        for i in range(N_MESSAGES):
            system.tell(Reward(int(arms[i]), 1.0, loop.time()))
            if i % burst == burst - 1:
                await asyncio.sleep(0 if rate is None else 0.001)
        while len(latencies) < N_MESSAGES:
            await asyncio.sleep(0.001)
        elapsed = loop.time() - t0
    assert totals.sum() == N_MESSAGES
    return N_MESSAGES / elapsed, float(np.percentile(latencies, 99)) * 1000, n_batches


async def table(rate: float | None) -> None:
    print("saturated producer" if rate is None else f"producer paced at {rate:.0f}/s")
    print("max_n  max_wait   throughput   p99 latency  batches")
    for max_n in (1, 10, 100, 1000):
        for max_wait in (0.001, 0.010):
            got, p99, batches = await run(max_n, max_wait, rate)
            print(f"{max_n:5}  {max_wait * 1000:5.0f}ms  {got:9.0f}/s  {p99:9.1f}ms  {batches:7}")
    print()


async def main() -> None:
    print(f"{N_MESSAGES} messages, {PER_BATCH_COST * 1000:.1f}ms fixed cost per batch\n")
    await table(None)
    await table(20_000)


asyncio.run(main())
