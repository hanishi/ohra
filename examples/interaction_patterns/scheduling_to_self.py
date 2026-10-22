"""Scheduling messages to self: a timer sends a message later, or periodically. The Buncher
collects messages until a size or a deadline is reached (Pekko interaction patterns)."""
import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pekko_py import ActorRef, ActorSystem, Behavior, Behaviors, TimerScheduler


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
    def with_timers(timers: TimerScheduler[Command]) -> Behavior[Command]:
        def idle(msg: Command) -> Behavior[Command]:
            match msg:
                case ExcitingMessage():
                    timers.start_single_timer("timeout", _Timeout(), after)
                    return active((msg,))
                case _Timeout():
                    return Behaviors.same

        def active(buffer: tuple[ExcitingMessage, ...]) -> Behavior[Command]:
            def on_message(msg: Command) -> Behavior[Command]:
                match msg:
                    case _Timeout():
                        target.tell(Batch(buffer))
                        return Behaviors.receive_message(idle)
                    case ExcitingMessage():
                        new_buffer = (*buffer, msg)
                        if len(new_buffer) == max_size:
                            timers.cancel("timeout")
                            target.tell(Batch(new_buffer))
                            return Behaviors.receive_message(idle)
                        return active(new_buffer)

            return Behaviors.receive_message(on_message)

        return Behaviors.receive_message(idle)

    return Behaviors.with_timers(with_timers)


# --- schedule periodically: the same scheduler, a fixed-delay timer -------------------------


@dataclass(frozen=True)
class Tick:
    pass


def ticker(period: float) -> Behavior[Tick]:
    def with_timers(timers: TimerScheduler[Tick]) -> Behavior[Tick]:
        timers.start_timer_with_fixed_delay("tick", Tick(), period)
        return Behaviors.receive_message(lambda _: (print("tick"), Behaviors.same)[1])

    return Behaviors.with_timers(with_timers)


async def main() -> None:
    received: list[Batch] = []
    sink = Behaviors.receive_message(lambda b: (received.append(b), Behaviors.same)[1])
    async with ActorSystem(sink, "sink") as sink_system, ActorSystem(buncher(sink_system, 0.1, 10), "buncher") as b:
        b.tell(ExcitingMessage("one"))
        b.tell(ExcitingMessage("two"))
        await asyncio.sleep(0.05)
        print("after 50ms:", received)  # nothing yet
        await asyncio.sleep(0.1)
        print("after 150ms:", received)  # one batch of two

    async with ActorSystem(ticker(0.05), "ticker"):
        await asyncio.sleep(0.18)  # three ticks, then the timer dies with the actor


asyncio.run(main())
