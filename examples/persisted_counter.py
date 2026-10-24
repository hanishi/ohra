"""A counter whose total survives a restart of the actor and of the process.

The state is the factory's parameter, as in counter.py. Behaviors.persisted wraps the factory so that
returning counter(total + by) writes the new total before it is installed and before the deferred
reply leaves. recover(id, empty) loads the total in one read.

    uv run python examples/persisted_counter.py          # run it twice: the total carries over
"""
import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pekko_py import ActorRef, ActorSystem, Behavior, Behaviors, SqliteStore, SupervisorStrategy

DB = Path(os.environ.get("COUNTER_DB", Path(__file__).with_name("counter.db")))


@dataclass(frozen=True)
class Increment:
    by: int
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class GetTotal:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Crash:
    pass


type Command = Increment | GetTotal | Crash

store = SqliteStore[int](DB)


@Behaviors.persisted(store)
def counter(total: int) -> Behavior[Command]:
    @Behaviors.receive_message
    def on_message(msg: Command) -> Behavior[Command]:
        match msg:
            case Increment(by, reply_to):
                return counter(total + by).then_reply(reply_to, total + by)  # written, then replied
            case GetTotal(reply_to):
                reply_to.tell(total)  # a read: nothing written
                return Behaviors.same
            case Crash():
                raise RuntimeError("crash")

    return on_message


# restart re-runs recover, so the actor comes back with the committed total, not an in-memory one
durable = Behaviors.supervise(counter.recover("demo", empty=0)).on_failure(SupervisorStrategy.restart())


async def main() -> None:
    async with ActorSystem(durable, "first") as system:
        start = await system.ask(GetTotal, timeout=1)
        print(f"recovered total {start} from {DB.name}")
        for by in (1, 2, 3):
            print("incremented to", await system.ask(lambda r: Increment(by, r), timeout=1))
        system.tell(Crash())
        print("after a crash and restart:", await system.ask(GetTotal, timeout=1))

    async with ActorSystem(durable, "second") as system:  # a new system on the same store, as after a process restart
        print("a new actor system sees:", await system.ask(GetTotal, timeout=1))
    print("run again and the total keeps growing")


if __name__ == "__main__":
    asyncio.run(main())
