"""A client for a flaky database: backoff supervision retries a failing setup, a stash
holds queries until connected, a timer sends heartbeats, and ctx.ask talks to the
database actor with a timeout.

    uv run python examples/database_client.py
"""

import asyncio
import logging
import random
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pekko_py import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Failure,
    StashBuffer,
    Success,
    SupervisorStrategy,
    TimerScheduler,
    Try,
)

# --- the database: answers queries, and is only up after a while ------------------------


@dataclass(frozen=True)
class Query:
    sql: str
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[bool]


def database(up_after: float) -> Behavior[Query | Ping]:
    @Behaviors.setup
    def setup(ctx: ActorContext[Query | Ping]) -> Behavior[Query | Ping]:
        started = asyncio.get_running_loop().time()

        @Behaviors.receive_message
        async def on_message(msg: Query | Ping) -> Behavior[Query | Ping]:
            up = asyncio.get_running_loop().time() - started > up_after
            match msg:
                case Ping(reply_to):
                    reply_to.tell(up)
                case Query(sql, reply_to):
                    await asyncio.sleep(random.uniform(0.01, 0.05))
                    reply_to.tell(f"rows for {sql!r}")
            return Behaviors.same

        return on_message

    return setup


# --- the client ------------------------------------------------------------------------


@dataclass(frozen=True)
class Run:
    sql: str
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class Connected:
    up: Try[bool]


@dataclass(frozen=True)
class Heartbeat:
    pass


@dataclass(frozen=True)
class Answer:
    result: Try[str]
    reply_to: ActorRef[str]


type Cmd = Run | Connected | Heartbeat | Answer


def client(db: ActorRef[Query | Ping]) -> Behavior[Cmd]:
    def supervised(timers: TimerScheduler[Cmd], buffer: StashBuffer[Cmd]) -> Behavior[Cmd]:
        @Behaviors.setup
        def setup(ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
            ctx.log.info("connecting")
            ctx.ask(db, Ping, Connected, timeout=0.2)
            return connecting(ctx)

        def connecting(ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
            @Behaviors.receive_message
            def on_message(msg: Cmd) -> Behavior[Cmd]:
                match msg:
                    case Connected(Success(True)):
                        ctx.log.info("connected, replaying %d stashed queries", buffer.size)
                        timers.start_timer_with_fixed_delay("hb", Heartbeat(), 0.3)
                        return buffer.unstash_all(ready(ctx))
                    case Connected(Success(False)):
                        # supervisor: retry with backoff
                        raise ConnectionError("database not ready")
                    case Connected(Failure(e)):
                        raise ConnectionError(str(e))
                    case Run():
                        buffer.stash(msg)
                    case _:
                        pass
                return Behaviors.same

            return on_message

        def ready(ctx: ActorContext[Cmd]) -> Behavior[Cmd]:
            @Behaviors.receive_message
            def on_message(msg: Cmd) -> Behavior[Cmd]:
                match msg:
                    case Run(sql, reply_to):
                        ctx.ask(
                            db,
                            lambda r: Query(sql, r),
                            lambda res: Answer(res, reply_to),
                            timeout=1,
                        )
                    case Answer(result, reply_to):
                        reply_to.tell(
                            result.value
                            if isinstance(result, Success)
                            else f"error: {result.exception}"
                        )
                    case Heartbeat():
                        ctx.log.info("heartbeat")
                    case Connected():
                        pass
                return Behaviors.same

            return on_message

        # Only the part inside supervise is restarted. The stash and timers sit outside, so
        # queries stashed before a failed connection attempt survive the restart.
        return Behaviors.supervise(setup).on_failure(
            SupervisorStrategy.restart_with_backoff(0.1, 1.0), exc_type=ConnectionError
        )

    return Behaviors.with_timers(
        lambda timers: Behaviors.with_stash(100, lambda buffer: supervised(timers, buffer))
    )


@Behaviors.setup
def app(ctx: ActorContext[Run]) -> Behavior[Run]:
    db = ctx.spawn(database(up_after=0.35), "db")
    c = ctx.spawn(client(db), "client")

    @Behaviors.receive_message
    def forward(msg: Run) -> Behavior[Run]:
        c.tell(msg)
        return Behaviors.same

    return forward


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(relativeCreated)6.0fms %(name)s %(message)s")
    async with ActorSystem(app, "app") as system:
        asks = (
            system.ask(lambda r, sql=sql: Run(sql, r), timeout=5)
            for sql in ("select 1", "select 2", "select 3")
        )
        print(*await asyncio.gather(*asks), sep="\n")
        await asyncio.sleep(0.7)  # a couple of heartbeats


asyncio.run(main())
