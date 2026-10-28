"""Scheduling messages to self: a timer sends a message later, or periodically. The Buncher
collects messages until a size or a deadline is reached (Pekko interaction patterns)."""

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from ohra import ActorRef, ActorSystem, Behavior, Behaviors, TimerScheduler


@dataclass(frozen=True)
class ExcitingMessage:
    message: str


@dataclass(frozen=True)
class _Timeout:
    pass


type Command = ExcitingMessage | _Timeout


@dataclass(frozen=True)
class Batch:
    messages: tuple[ExcitingMessage, ...]


def buncher(target: ActorRef[Batch], after: float, max_size: int) -> Behavior[Command]:
    @Behaviors.with_timers
    def with_timers(timers: TimerScheduler[Command]) -> Behavior[Command]:
        @Behaviors.receive_message
        def idle(msg: Command) -> Behavior[Command]:
            match msg:
                case ExcitingMessage():
                    timers.start_single_timer("timeout", _Timeout(), after)
                    return active((msg,))
                case _Timeout():
                    return Behaviors.same

        def active(buffer: tuple[ExcitingMessage, ...]) -> Behavior[Command]:
            @Behaviors.receive_message
            def on_message(msg: Command) -> Behavior[Command]:
                match msg:
                    case _Timeout():
                        target.tell(Batch(buffer))
                        return idle
                    case ExcitingMessage():
                        new_buffer = (*buffer, msg)
                        if len(new_buffer) == max_size:
                            timers.cancel("timeout")
                            target.tell(Batch(new_buffer))
                            return idle
                        return active(new_buffer)

            return on_message

        return idle

    return with_timers


# --- schedule periodically: the same scheduler, a fixed-delay timer -------------------------


@dataclass(frozen=True)
class Tick:
    pass


def ticker(period: float) -> Behavior[Tick]:
    @Behaviors.with_timers
    def with_timers(timers: TimerScheduler[Tick]) -> Behavior[Tick]:
        timers.start_timer_with_fixed_delay("tick", Tick(), period)

        @Behaviors.receive_message
        def on_tick(_: Tick) -> Behavior[Tick]:
            print("tick")
            return Behaviors.same

        return on_tick

    return with_timers


async def main() -> None:
    received: list[Batch] = []

    @Behaviors.receive_message
    def collect(batch: Batch) -> Behavior[Batch]:
        received.append(batch)
        return Behaviors.same

    sink = collect
    async with (
        ActorSystem(sink, "sink") as sink_system,
        ActorSystem(buncher(sink_system, 0.1, 10), "buncher") as b,
    ):
        b.tell(ExcitingMessage("one"))
        b.tell(ExcitingMessage("two"))
        await asyncio.sleep(0.05)
        print("after 50ms:", received)  # nothing yet
        await asyncio.sleep(0.1)
        print("after 150ms:", received)  # one batch of two

    async with ActorSystem(ticker(0.05), "ticker"):
        await asyncio.sleep(0.18)  # three ticks, then the timer dies with the actor


asyncio.run(main())
