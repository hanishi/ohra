"""Fire and forget: tell, with no reply expected (Pekko interaction patterns)."""
import asyncio
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pekko_py import ActorContext, ActorSystem, Behavior, Behaviors


@dataclass(frozen=True)
class PrintMe:
    message: str


def printer() -> Behavior[PrintMe]:
    def on_message(ctx: ActorContext[PrintMe], msg: PrintMe) -> Behavior[PrintMe]:
        ctx.log.info(msg.message)
        return Behaviors.same

    return Behaviors.receive(on_message)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")
    async with ActorSystem(printer(), "fire-and-forget") as system:
        # the system is also the ref to the top-level actor
        system.tell(PrintMe("message 1"))
        system.tell(PrintMe("not message 2"))
        await asyncio.sleep(0.01)


asyncio.run(main())
