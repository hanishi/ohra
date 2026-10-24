"""The Aggregator from Pekko's interaction patterns: a short-lived actor that sends
requests to several actors with different protocols, collects their replies through
one message adapter, and delivers one aggregate (or a partial one on timeout).

Pekko's version uses a receive timeout, which resets on every reply. This one uses a
single timer for the whole aggregation, which is the deadline a caller usually means.

    uv run python examples/interaction_patterns/aggregator.py
"""
import asyncio
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pekko_py import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, TimerScheduler

# --- the pattern ----------------------------------------------------------------------


@dataclass(frozen=True)
class _Reply[R]:
    reply: R


@dataclass(frozen=True)
class _Timeout:
    pass


type Command[R] = _Reply[R] | _Timeout


def aggregator[Reply, Aggregate](
    send_requests: Callable[[ActorRef[Reply]], None],
    expected_replies: int,
    reply_to: ActorRef[Aggregate],
    aggregate: Callable[[list[Reply]], Aggregate],
    timeout: float,
) -> Behavior[Command[Reply]]:
    def with_timers(timers: TimerScheduler[Command[Reply]]) -> Behavior[Command[Reply]]:
        @Behaviors.setup
        def setup(ctx: ActorContext[Command[Reply]]) -> Behavior[Command[Reply]]:
            timers.start_single_timer("deadline", _Timeout(), timeout)
            send_requests(ctx.message_adapter(_Reply[Reply]))
            return collecting([])

        def collecting(replies: list[Reply]) -> Behavior[Command[Reply]]:
            @Behaviors.receive_message
            def on_message(msg: Command[Reply]) -> Behavior[Command[Reply]]:
                match msg:
                    case _Reply(reply):
                        collected = [*replies, reply]
                        if len(collected) < expected_replies:
                            return collecting(collected)
                    case _Timeout():
                        collected = replies
                reply_to.tell(aggregate(collected))
                return Behaviors.stopped()

            return on_message

        return setup

    return Behaviors.with_timers(with_timers)


# --- usage: two hotels with different protocols, and one that is too slow ------------------


@dataclass(frozen=True)
class RequestQuote:
    reply_to: ActorRef["Quote"]


@dataclass(frozen=True)
class Quote:
    hotel: str
    price: float


@dataclass(frozen=True)
class RequestPrice:
    reply_to: ActorRef["Price"]


@dataclass(frozen=True)
class Price:
    hotel: str
    cents: int


def hotel1(name: str, price: float, delay: float = 0.0) -> Behavior[RequestQuote]:
    @Behaviors.receive_message
    async def on_message(msg: RequestQuote) -> Behavior[RequestQuote]:
        await asyncio.sleep(delay)
        msg.reply_to.tell(Quote(name, price))
        return Behaviors.same

    return on_message


def hotel2(name: str, cents: int) -> Behavior[RequestPrice]:
    @Behaviors.receive_message
    def on_message(msg: RequestPrice) -> Behavior[RequestPrice]:
        msg.reply_to.tell(Price(name, cents))
        return Behaviors.same

    return on_message


@dataclass(frozen=True)
class AggregatedQuotes:
    quotes: list[Quote]  # cheapest first; may be shorter than the number of hotels


@dataclass(frozen=True)
class FindCheapest:
    pass


type CustomerCmd = FindCheapest | AggregatedQuotes


def customer(
    a: ActorRef[RequestQuote], b: ActorRef[RequestPrice], c: ActorRef[RequestQuote]
) -> Behavior[CustomerCmd]:
    @Behaviors.receive
    def on_message(ctx: ActorContext[CustomerCmd], msg: CustomerCmd) -> Behavior[CustomerCmd]:
        match msg:
            case FindCheapest():

                def send_requests(reply_to: ActorRef[Quote | Price]) -> None:
                    a.tell(RequestQuote(reply_to))  # ActorRef[Quote | Price] narrows to ActorRef[Quote]
                    b.tell(RequestPrice(reply_to))
                    c.tell(RequestQuote(reply_to))

                def aggregate(replies: list[Quote | Price]) -> AggregatedQuotes:
                    quotes = [r if isinstance(r, Quote) else Quote(r.hotel, r.cents / 100) for r in replies]
                    return AggregatedQuotes(sorted(quotes, key=lambda q: q.price))

                ctx.spawn_anonymous(aggregator(send_requests, 3, ctx.self, aggregate, timeout=0.2))
            case AggregatedQuotes(quotes):
                ctx.log.info("%d of 3 answered in time; cheapest: %s", len(quotes), quotes[0] if quotes else "none")
        return Behaviors.same

    return on_message


@Behaviors.setup
def app(ctx: ActorContext[FindCheapest]) -> Behavior[FindCheapest]:
    a = ctx.spawn(hotel1("Grand", 120.0), "grand")
    b = ctx.spawn(hotel2("Budget", 7950), "budget")
    c = ctx.spawn(hotel1("Slow", 50.0, delay=1.0), "slow")  # misses the deadline
    cust = ctx.spawn(customer(a, b, c), "customer")

    @Behaviors.receive_message
    def forward(msg: FindCheapest) -> Behavior[FindCheapest]:
        cust.tell(msg)
        return Behaviors.same

    return forward


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")
    async with ActorSystem(app, "quotes") as system:
        system.tell(FindCheapest())
        await asyncio.sleep(0.3)


asyncio.run(main())
