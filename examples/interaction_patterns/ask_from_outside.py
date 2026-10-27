"""Request-response with ask from outside an actor: await the reply in plain async code
(Pekko interaction patterns)."""

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from ohra import ActorRef, ActorSystem, AskTimeoutError, Behavior, Behaviors


@dataclass(frozen=True)
class GiveMeCookies:
    count: int
    reply_to: ActorRef["Reply"]


@dataclass(frozen=True)
class Cookies:
    count: int


@dataclass(frozen=True)
class InvalidRequest:
    reason: str


type Reply = Cookies | InvalidRequest


def cookie_fabric() -> Behavior[GiveMeCookies]:
    @Behaviors.receive_message
    def on_message(msg: GiveMeCookies) -> Behavior[GiveMeCookies]:
        if msg.count >= 5:
            msg.reply_to.tell(InvalidRequest("Too many cookies."))
        else:
            msg.reply_to.tell(Cookies(msg.count))
        return Behaviors.same

    return on_message


async def main() -> None:
    async with ActorSystem(cookie_fabric(), "cookies") as system:
        for count in (3, 7):
            try:
                match await system.ask(lambda r: GiveMeCookies(count, r), timeout=3):
                    case Cookies(n):
                        print(f"Yay, {n} cookies!")
                    case InvalidRequest(reason):
                        print(f"No cookies for me. {reason}")
            except AskTimeoutError as e:
                print(f"Boo! didn't get cookies: {e}")


asyncio.run(main())
