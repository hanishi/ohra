import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pekko_py import ActorRef, ActorSystem, Behavior, Behaviors, SupervisorStrategy


@dataclass(frozen=True)
class Increment:
    by: int


@dataclass(frozen=True)
class GetTotal:
    reply_to: ActorRef[int]


type Command = Increment | GetTotal


def counter(total: int = 0) -> Behavior[Command]:
    def on_message(msg: Command) -> Behavior[Command]:
        match msg:
            case Increment(by) if by < 0:
                raise ValueError("negative increment")
            case Increment(by):
                return counter(total + by)
            case GetTotal(reply_to):
                reply_to.tell(total)
                return Behaviors.same

    return Behaviors.receive_message(on_message)


async def main() -> None:
    guardian = Behaviors.supervise(counter()).on_failure(SupervisorStrategy.resume())
    async with ActorSystem(guardian, "demo") as system:
        system.tell(Increment(5))
        system.tell(Increment(-1))  # fails, resumed: state kept
        system.tell(Increment(2))
        print("total:", await system.ask(GetTotal, timeout=1))


asyncio.run(main())
