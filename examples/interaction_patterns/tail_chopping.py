"""Latency tail chopping: send the request to one replica, and after a short wait send it to
the next too; the first reply wins. Trades extra load for a shorter latency tail
(Pekko interaction patterns)."""

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import random
from collections.abc import Callable

from ohra import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, TimerScheduler

# --- the pattern ----------------------------------------------------------------------


@dataclass(frozen=True)
class _RequestTimeout:
    pass


@dataclass(frozen=True)
class _FinalTimeout:
    pass


@dataclass(frozen=True)
class _WrappedReply[R]:
    reply: R


type Command[R] = _RequestTimeout | _FinalTimeout | _WrappedReply[R]


def tail_chopping[Reply](
    # (attempt, reply_to) -> False when out of replicas
    send_request: Callable[[int, ActorRef[Reply]], bool],
    next_request_after: float,
    reply_to: ActorRef[Reply],
    final_timeout: float,
    timeout_reply: Reply,
) -> Behavior[Command[Reply]]:
    @Behaviors.with_timers
    def with_timers(timers: TimerScheduler[Command[Reply]]) -> Behavior[Command[Reply]]:
        @Behaviors.setup
        def setup(ctx: ActorContext[Command[Reply]]) -> Behavior[Command[Reply]]:
            reply_adapter = ctx.message_adapter(_WrappedReply[Reply])

            def send_next(attempt: int) -> Behavior[Command[Reply]]:
                if send_request(attempt, reply_adapter):
                    timers.start_single_timer("request", _RequestTimeout(), next_request_after)
                else:
                    timers.start_single_timer("final", _FinalTimeout(), final_timeout)
                return waiting(attempt)

            def waiting(attempt: int) -> Behavior[Command[Reply]]:
                @Behaviors.receive_message
                def on_message(msg: Command[Reply]) -> Behavior[Command[Reply]]:
                    match msg:
                        case _WrappedReply(reply):
                            reply_to.tell(reply)
                            return Behaviors.stopped()
                        case _RequestTimeout():
                            return send_next(attempt + 1)
                        case _FinalTimeout():
                            reply_to.tell(timeout_reply)
                            return Behaviors.stopped()

                return on_message

            return send_next(1)

        return setup

    return with_timers


# --- usage: three replicas with a long latency tail -----------------------------------------


@dataclass(frozen=True)
class Lookup:
    key: str
    reply_to: ActorRef[str]


def replica(name: str) -> Behavior[Lookup]:
    @Behaviors.receive_message
    async def on_message(msg: Lookup) -> Behavior[Lookup]:
        await asyncio.sleep(random.choice([0.01, 0.01, 0.01, 0.5]))  # one in four is slow
        msg.reply_to.tell(f"{msg.key} from {name}")
        return Behaviors.same

    return on_message


@dataclass(frozen=True)
class Find:
    key: str
    reply_to: ActorRef[str]


@Behaviors.setup
def app(ctx: ActorContext[Find]) -> Behavior[Find]:
    replicas = [ctx.spawn(replica(f"r{i}"), f"r{i}") for i in range(3)]

    @Behaviors.receive_message
    def on_message(msg: Find) -> Behavior[Find]:
        def send_request(attempt: int, reply_to: ActorRef[str]) -> bool:
            if attempt > len(replicas):
                return False
            replicas[attempt - 1].tell(Lookup(msg.key, reply_to))
            return True

        ctx.spawn_anonymous(tail_chopping(send_request, 0.05, msg.reply_to, 1.0, "timed out"))
        return Behaviors.same

    return on_message


async def main() -> None:
    random.seed(3)
    async with ActorSystem(app, "tail") as system:
        loop = asyncio.get_running_loop()
        for key in ("a", "b", "c", "d"):
            t0 = loop.time()
            answer = await system.ask(lambda r: Find(key, r), timeout=2)
            print(f"{answer:12}  in {(loop.time() - t0) * 1000:4.0f}ms")


asyncio.run(main())
