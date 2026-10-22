"""Thompson sampling over a population of bandit arms, sharded across actors.

Each shard keeps Beta posteriors for its arms as two numpy arrays and answers a
proposal request with one vectorized draw. The agent asks every shard, pulls the best
candidate on the environment, and routes the reward back to the owning shard. A timer
reports progress while it runs.

    uv run --extra numpy python examples/bandit.py
"""
import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pekko_py import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Routers,
    Success,
    TimerScheduler,
    Try,
)

N_ARMS = 200
N_SHARDS = 4
STEPS = 10_000


# --- shards: Beta posteriors as struct-of-arrays -----------------------------------------


@dataclass(frozen=True)
class Observe:
    arm: int
    reward: int


@dataclass(frozen=True)
class Propose:
    shard: int
    reply_to: ActorRef[tuple[int, float]]  # (global arm id, sampled mean)


type ShardCmd = Observe | Propose


def shard() -> Behavior[ShardCmd]:
    # The pool spawns this one behavior value N_SHARDS times. State created inside setup
    # is per actor; state created out here would be shared by every routee.
    def setup(_ctx: ActorContext[ShardCmd]) -> Behavior[ShardCmd]:
        size = -(-N_ARMS // N_SHARDS)
        successes = np.ones(size)  # Beta(1, 1) prior
        failures = np.ones(size)
        rng = np.random.default_rng()

        def on_message(msg: ShardCmd) -> Behavior[ShardCmd]:
            match msg:
                case Observe(arm, reward):
                    local = arm // N_SHARDS
                    if reward:
                        successes[local] += 1
                    else:
                        failures[local] += 1
                case Propose(k, reply_to):
                    draws = rng.beta(successes, failures)  # one draw per arm, in C
                    best = int(draws.argmax())
                    reply_to.tell((best * N_SHARDS + k, float(draws[best])))
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)


def shard_pool() -> Behavior[ShardCmd]:
    def key(m: ShardCmd) -> int:
        return m.arm if isinstance(m, Observe) else m.shard  # hash(int) == int: shard k owns key k

    return Routers.pool(N_SHARDS, shard()).with_consistent_hashing_routing(key)


# --- environment: Bernoulli rewards with hidden means ------------------------------------


@dataclass(frozen=True)
class Pull:
    arm: int
    reply_to: ActorRef[int]


def environment(means: np.ndarray) -> Behavior[Pull]:
    rng = np.random.default_rng(1)

    def on_message(msg: Pull) -> Behavior[Pull]:
        msg.reply_to.tell(int(rng.random() < means[msg.arm]))
        return Behaviors.same

    return Behaviors.receive_message(on_message)


# --- agent: one step = ask all shards, pull the best, observe ----------------------------


@dataclass(frozen=True)
class Run:
    steps: int
    reply_to: ActorRef["Summary"]


@dataclass(frozen=True)
class Summary:
    steps: int
    regret: float
    pulls_of_best: int


@dataclass(frozen=True)
class Proposal:
    result: Try[tuple[int, float]]


@dataclass(frozen=True)
class Rewarded:
    result: Try[int]


@dataclass(frozen=True)
class Report:
    pass


type AgentCmd = Run | Proposal | Rewarded | Report


@dataclass(frozen=True)
class Progress:
    step: int = 0
    regret: float = 0.0
    pulls_of_best: int = 0


def agent(means: np.ndarray) -> Behavior[AgentCmd]:
    best_arm, best_mean = int(means.argmax()), float(means.max())

    def with_timers(timers: TimerScheduler[AgentCmd]) -> Behavior[AgentCmd]:
        def setup(ctx: ActorContext[AgentCmd]) -> Behavior[AgentCmd]:
            shards = ctx.spawn(shard_pool(), "shards")
            env = ctx.spawn(environment(means), "env")

            def idle(msg: AgentCmd) -> Behavior[AgentCmd]:
                match msg:
                    case Run(steps, reply_to):
                        timers.start_timer_with_fixed_delay("report", Report(), 0.5)
                        return propose(steps, reply_to, Progress())
                    case _:
                        return Behaviors.same

            def propose(steps: int, reply_to: ActorRef[Summary], p: Progress) -> Behavior[AgentCmd]:
                for k in range(N_SHARDS):
                    ctx.ask(shards, lambda r, k=k: Propose(k, r), Proposal, timeout=1)
                return collecting(steps, reply_to, p, ())

            def collecting(
                steps: int, reply_to: ActorRef[Summary], p: Progress, got: tuple[tuple[int, float], ...]
            ) -> Behavior[AgentCmd]:
                def on_message(msg: AgentCmd) -> Behavior[AgentCmd]:
                    match msg:
                        case Proposal(Success(candidate)):
                            got2 = (*got, candidate)
                            if len(got2) < N_SHARDS:
                                return collecting(steps, reply_to, p, got2)
                            arm = max(got2, key=lambda c: c[1])[0]
                            ctx.ask(env, lambda r: Pull(arm, r), Rewarded, timeout=1)
                            return pulling(steps, reply_to, p, arm)
                        case Proposal(_):
                            raise RuntimeError("a shard did not answer")
                        case Report():
                            print(f"step {p.step:5}  regret {p.regret:7.2f}")
                            return Behaviors.same
                        case _:
                            return Behaviors.same

                return Behaviors.receive_message(on_message)

            def pulling(steps: int, reply_to: ActorRef[Summary], p: Progress, arm: int) -> Behavior[AgentCmd]:
                def on_message(msg: AgentCmd) -> Behavior[AgentCmd]:
                    match msg:
                        case Rewarded(Success(reward)):
                            shards.tell(Observe(arm, reward))
                            p2 = Progress(
                                p.step + 1,
                                p.regret + best_mean - float(means[arm]),
                                p.pulls_of_best + (arm == best_arm),
                            )
                            if p2.step == steps:
                                timers.cancel("report")
                                reply_to.tell(Summary(p2.step, p2.regret, p2.pulls_of_best))
                                return Behaviors.receive_message(idle)
                            return propose(steps, reply_to, p2)
                        case Rewarded(_):
                            raise RuntimeError("the environment did not answer")
                        case Report():
                            print(f"step {p.step:5}  regret {p.regret:7.2f}")
                            return Behaviors.same
                        case _:
                            return Behaviors.same

                return Behaviors.receive_message(on_message)

            return Behaviors.receive_message(idle)

        return Behaviors.setup(setup)

    return Behaviors.with_timers(with_timers)


async def main() -> None:
    means = np.random.default_rng(0).beta(2, 8, N_ARMS)  # most arms poor, a few good
    print(f"{N_ARMS} arms in {N_SHARDS} shards, best mean {means.max():.3f}, {STEPS} steps\n")
    async with ActorSystem(agent(means), "bandit") as system:
        s = await system.ask(lambda r: Run(STEPS, r), timeout=60)
    random_regret = STEPS * (means.max() - means.mean())
    print(f"\ndone: regret {s.regret:.1f} (uniform random play would be about {random_regret:.0f}), "
          f"best arm pulled {s.pulls_of_best} of {s.steps} times")


asyncio.run(main())
