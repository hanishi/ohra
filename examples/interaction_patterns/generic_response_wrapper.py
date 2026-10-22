"""Generic response wrapper: instead of a Reply union per protocol, reply with Try[Cookies],
so the asking side handles success and failure uniformly. Pekko uses StatusReply for this;
pekko-py's Success / Failure already have that shape (Pekko interaction patterns)."""
import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pekko_py import ActorRef, ActorSystem, Behavior, Behaviors, Failure, Success, Try


@dataclass(frozen=True)
class GiveMeCookies:
    count: int
    reply_to: ActorRef[Try["Cookies"]]


@dataclass(frozen=True)
class Cookies:
    count: int


def cookie_fabric() -> Behavior[GiveMeCookies]:
    def on_message(msg: GiveMeCookies) -> Behavior[GiveMeCookies]:
        if msg.count >= 5:
            msg.reply_to.tell(Failure(ValueError("Too many cookies.")))
        else:
            msg.reply_to.tell(Success(Cookies(msg.count)))
        return Behaviors.same

    return Behaviors.receive_message(on_message)


async def main() -> None:
    async with ActorSystem(cookie_fabric(), "cookies") as system:
        for count in (3, 7):
            match await system.ask(lambda r: GiveMeCookies(count, r), timeout=3):
                case Success(Cookies(n)):
                    print(f"Yay, {n} cookies!")
                case Failure(e):
                    print(f"No cookies for me. {e}")


asyncio.run(main())
