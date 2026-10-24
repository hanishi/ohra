"""Ignoring replies: when a protocol requires a reply_to but you do not want the reply, pass
system.ignore_ref (Pekko interaction patterns)."""
import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pekko_py import ActorRef, ActorSystem, Behavior, Behaviors


@dataclass(frozen=True)
class Request:
    query: str
    reply_to: ActorRef[str]


def cookie_fabric() -> Behavior[Request]:
    @Behaviors.receive_message
    def on_message(msg: Request) -> Behavior[Request]:
        print("fabric: baking for", msg.query)
        msg.reply_to.tell(f"cookies for {msg.query}")  # goes nowhere
        return Behaviors.same

    return on_message


async def main() -> None:
    async with ActorSystem(cookie_fabric(), "cookies") as system:
        system.tell(Request("don't send cookies back", system.ignore_ref))
        await asyncio.sleep(0.01)


asyncio.run(main())
