"""Benchmark: 100k single-arm actors versus 16 shard actors behind a hashing router.

Shards keep arm state as struct-of-arrays and apply reward batches with np.add.at.
Thompson sampling is one vectorized draw per shard instead of one ask per arm.

    uv run --extra numpy python examples/population.py
"""

import asyncio
import random
import sys
import tracemalloc
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ohra import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Routers,
    ask,
    batching,
)

N_ARMS = 100_000
N_SHARDS = 16
N_REWARDS = 200_000


@dataclass(frozen=True, slots=True)
class Reward:
    arm: int
    value: float


@dataclass(frozen=True, slots=True)
class Sample:
    shard: int  # routing key for the shard pool; ignored by single-arm actors
    reply_to: ActorRef[tuple[int, float]]  # (best arm, its draw)


type Cmd = Reward | Sample


# --- one actor per arm ---------------------------------------------------------------------


def arm(arm_id: int, applied: list[int], count: int = 0, total: float = 0.0) -> Behavior[Cmd]:
    @Behaviors.receive_message
    def on_message(msg: Cmd) -> Behavior[Cmd]:
        match msg:
            case Reward(_, value):
                applied[0] += 1
                return arm(arm_id, applied, count + 1, total + value)
            case Sample(_, reply_to):
                mean = total / count if count else 0.0
                reply_to.tell((arm_id, random.gauss(mean, 1 / (count + 1) ** 0.5)))
                return Behaviors.same

    return on_message


def population(arms: list[ActorRef[Cmd]], applied: list[int]) -> Behavior[Reward]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Reward]) -> Behavior[Reward]:
        arms.extend(ctx.spawn(arm(i, applied), str(i)) for i in range(N_ARMS))

        @Behaviors.receive_message
        def route(msg: Reward) -> Behavior[Reward]:
            arms[msg.arm].tell(msg)
            return Behaviors.same

        return route

    return setup


# --- shards: struct of arrays, batched updates -----------------------------------------------


def shard(applied: list[int]) -> Behavior[Cmd]:
    @Behaviors.setup
    # per-actor arrays: the pool spawns this value 16 times
    def setup(_ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
        size = -(-N_ARMS // N_SHARDS)
        counts = np.zeros(size)
        totals = np.zeros(size)
        rng = np.random.default_rng()

        def handle(batch: list[Cmd]) -> None:
            rewards = [m for m in batch if isinstance(m, Reward)]
            if rewards:
                local = [m.arm // N_SHARDS for m in rewards]
                np.add.at(counts, local, 1)
                np.add.at(totals, local, [m.value for m in rewards])
                applied[0] += len(rewards)
            for m in batch:
                if isinstance(m, Sample):
                    means = np.divide(totals, counts, out=np.zeros(size), where=counts > 0)
                    draws = rng.normal(means, 1 / np.sqrt(counts + 1))
                    best = int(draws.argmax())
                    m.reply_to.tell((best * N_SHARDS + m.shard, float(draws[best])))

        return batching(1000, 0.001, handle)

    return setup


def shard_pool(applied: list[int]) -> Behavior[Cmd]:
    def key(m: Cmd) -> int:
        # hash(int) is the int, so shard k gets key k
        return m.arm if isinstance(m, Reward) else m.shard

    return Routers.pool(N_SHARDS, shard(applied)).with_consistent_hashing_routing(key)


# --- driver --------------------------------------------------------------------------------------


async def settle() -> None:
    for _ in range(3):
        await asyncio.sleep(0)


async def measure[M](
    name: str,
    build: Callable[[list[int]], Behavior[M]],
    send: Callable[[ActorSystem[M], Reward], None],
    sample: Callable[[ActorSystem[M]], Awaitable[int]],
) -> None:
    loop = asyncio.get_running_loop()
    applied = [0]
    tracemalloc.start()
    base = tracemalloc.get_traced_memory()[0]
    t0 = loop.time()
    async with ActorSystem(build(applied), name) as system:
        await settle()
        spawn_s = loop.time() - t0
        mem_mb = (tracemalloc.get_traced_memory()[0] - base) / 1e6
        tracemalloc.stop()

        rewards = np.random.default_rng(0).integers(0, N_ARMS, N_REWARDS)
        t0 = loop.time()
        for i, a in enumerate(rewards.tolist()):
            send(system, Reward(int(a), 1.0))
            if i % 1000 == 999:
                await asyncio.sleep(0)
        while applied[0] < N_REWARDS:
            await asyncio.sleep(0.001)
        rate = N_REWARDS / (loop.time() - t0)

        t0 = loop.time()
        best = await sample(system)
        sample_ms = (loop.time() - t0) * 1000
    print(
        f"{name:8} spawn {spawn_s:5.2f}s  memory {mem_mb:7.1f}MB  updates {rate:9.0f}/s  sample "
        f"{sample_ms:8.1f}ms  best arm {best}"
    )


async def main() -> None:
    print(f"{N_ARMS} arms, {N_REWARDS} rewards, {N_SHARDS} shards\n")

    arms: list[ActorRef[Cmd]] = []

    def best_of(draws: list[tuple[int, float]]) -> int:
        return max(draws, key=lambda d: d[1])[0]

    def sample_from(shard_idx: int) -> Callable[[ActorRef[tuple[int, float]]], Sample]:
        return lambda reply_to: Sample(shard_idx, reply_to)

    async def sample_arms(_system: ActorSystem[Reward]) -> int:
        return best_of(await asyncio.gather(*(ask(a, sample_from(0), timeout=30) for a in arms)))

    await measure(
        "per-arm", lambda applied: population(arms, applied), lambda s, m: s.tell(m), sample_arms
    )

    async def sample_shards(system: ActorSystem[Cmd]) -> int:
        return best_of(
            await asyncio.gather(
                *(system.ask(sample_from(k), timeout=30) for k in range(N_SHARDS))
            )
        )

    await measure("shards", shard_pool, lambda s, m: s.tell(m), sample_shards)


asyncio.run(main())
