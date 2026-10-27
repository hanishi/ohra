"""Request-response: the request carries the ref to reply to (Pekko interaction patterns)."""

import asyncio
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from ohra import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors


@dataclass(frozen=True)
class Request:
    query: str
    reply_to: ActorRef["Response"]


@dataclass(frozen=True)
class Response:
    result: str


def cookie_fabric() -> Behavior[Request]:
    @Behaviors.receive_message
    def on_message(msg: Request) -> Behavior[Request]:
        msg.reply_to.tell(Response(f"Here are the cookies for [{msg.query}]!"))
        return Behaviors.same

    return on_message


def customer(fabric: ActorRef[Request]) -> Behavior[Response]:
    # inside an actor, ctx.self is the natural reply_to when the reply is part of our protocol
    @Behaviors.setup
    def setup(ctx: ActorContext[Response]) -> Behavior[Response]:
        fabric.tell(Request("give me cookies", ctx.self))

        @Behaviors.receive_message
        def on_message(msg: Response) -> Behavior[Response]:
            ctx.log.info(msg.result)
            return Behaviors.stopped()

        return on_message

    return setup


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")

    @Behaviors.setup
    def app(ctx: ActorContext[None]) -> Behavior[None]:
        fabric = ctx.spawn(cookie_fabric(), "fabric")
        ctx.spawn(customer(fabric), "customer")
        return Behaviors.ignore

    async with ActorSystem(app, "request-response"):
        await asyncio.sleep(0.01)


asyncio.run(main())
