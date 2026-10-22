"""Request-response with ask between two actors: ctx.ask turns the reply (or timeout) into
a message of our own protocol (Pekko interaction patterns)."""
import asyncio
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pekko_py import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, Failure, Success, Try


@dataclass(frozen=True)
class OpenThePodBayDoorsPlease:
    reply_to: ActorRef["HalResponse"]


@dataclass(frozen=True)
class HalResponse:
    message: str


def hal() -> Behavior[OpenThePodBayDoorsPlease]:
    def on_message(msg: OpenThePodBayDoorsPlease) -> Behavior[OpenThePodBayDoorsPlease]:
        msg.reply_to.tell(HalResponse("I'm sorry, Dave. I'm afraid I can't do that."))
        return Behaviors.same

    return Behaviors.receive_message(on_message)


@dataclass(frozen=True)
class _AdaptedResponse:  # internal to Dave
    message: str


def dave(hal_ref: ActorRef[OpenThePodBayDoorsPlease]) -> Behavior[_AdaptedResponse]:
    def setup(ctx: ActorContext[_AdaptedResponse]) -> Behavior[_AdaptedResponse]:
        def adapt(r: Try[HalResponse]) -> _AdaptedResponse:
            match r:
                case Success(HalResponse(message)):
                    return _AdaptedResponse(message)
                case Failure(_):
                    return _AdaptedResponse("Request failed")

        ctx.ask(hal_ref, OpenThePodBayDoorsPlease, adapt, timeout=3)

        # request context can be closed over; the adapt function runs inside this actor
        request_id = 1

        def adapt_with_id(r: Try[HalResponse]) -> _AdaptedResponse:
            return _AdaptedResponse(f"{request_id}: {adapt(r).message}")

        ctx.ask(hal_ref, OpenThePodBayDoorsPlease, adapt_with_id, timeout=3)

        def on_message(msg: _AdaptedResponse) -> Behavior[_AdaptedResponse]:
            ctx.log.info("Got response from hal: %s", msg.message)
            return Behaviors.same

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(setup)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")

    def app(ctx: ActorContext[None]) -> Behavior[None]:
        ctx.spawn(dave(ctx.spawn(hal(), "hal")), "dave")
        return Behaviors.ignore

    async with ActorSystem(Behaviors.setup(app), "ask"):
        await asyncio.sleep(0.01)


asyncio.run(main())
