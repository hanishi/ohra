import asyncio
from dataclasses import dataclass

from ohra import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors
from ohra.stream import (
    ActorFlow,
    ActorSink,
    ActorSource,
    OverflowStrategy,
    RunnableGraph,
    Source,
)


@dataclass(frozen=True)
class GetPrice:
    sku: str
    reply_to: ActorRef[int]


def pricing(prices: dict[str, int]) -> Behavior[GetPrice]:
    @Behaviors.receive_message
    def on_message(msg: GetPrice) -> Behavior[GetPrice]:
        msg.reply_to.tell(prices.get(msg.sku, 0))
        return Behaviors.same

    return on_message


@dataclass(frozen=True)
class Charge:
    amount: int


@dataclass(frozen=True)
class Closed:
    pass


@dataclass(frozen=True)
class Broken:
    cause: BaseException


type LedgerCmd = Charge | Closed | Broken


def ledger(total: int, report_to: ActorRef[int]) -> Behavior[LedgerCmd]:
    """Adds up charges; reports the total when the till closes."""

    @Behaviors.receive_message
    def on_message(msg: LedgerCmd) -> Behavior[LedgerCmd]:
        match msg:
            case Charge(amount):
                return ledger(total + amount, report_to)
            case Closed():
                report_to.tell(total)
                return Behaviors.stopped()
            case Broken(cause):
                raise cause

    return on_message


@dataclass(frozen=True)
class Scan:
    sku: str


@dataclass(frozen=True)
class CloseTill:
    pass


type TillMsg = Scan | CloseTill


def till(
    prices: ActorRef[GetPrice], book: ActorRef[LedgerCmd]
) -> RunnableGraph[ActorRef[TillMsg]]:
    """A stream from the actors that scan, through the pricing actor, into the ledger."""
    scans: Source[TillMsg, ActorRef[TillMsg]] = ActorSource.actor_ref(
        completion_matcher=lambda msg: isinstance(msg, CloseTill),
        failure_matcher=lambda _: None,
        buffer_size=64,
        overflow_strategy=OverflowStrategy.fail,
    )
    return (
        scans.collect(lambda msg: msg.sku if isinstance(msg, Scan) else None)
        .via(ActorFlow.ask(prices, GetPrice, timeout=1.0))
        .map(Charge)
        .to(ActorSink.actor_ref(book, Closed(), Broken))
    )


@dataclass(frozen=True)
class Checkout:
    skus: tuple[str, ...]
    reply_to: ActorRef[int]


def shop(price_list: dict[str, int]) -> Behavior[Checkout]:
    """Opens a till for each checkout; the checkout's ledger replies with the total."""

    def setup(ctx: ActorContext[Checkout]) -> Behavior[Checkout]:
        prices = ctx.spawn(pricing(price_list), "pricing")

        @Behaviors.receive_message
        def on_message(msg: Checkout) -> Behavior[Checkout]:
            book = ctx.spawn_anonymous(ledger(0, msg.reply_to))
            scanner = till(prices, book).run(ctx.system)
            for sku in msg.skus:
                scanner.tell(Scan(sku))
            scanner.tell(CloseTill())
            return Behaviors.same

        return on_message

    return Behaviors.setup(setup)


async def main() -> None:
    async with ActorSystem(shop({"book": 1200, "pen": 150}), "shop") as system:
        total = await system.ask(lambda r: Checkout(("book", "pen", "pen"), r), timeout=1)
        print(total)  # 1500


if __name__ == "__main__":
    asyncio.run(main())
