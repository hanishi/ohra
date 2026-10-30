import asyncio
from collections.abc import Iterable

from ohra import ActorSystem, Behaviors
from ohra.stream import NotUsed, Sink, Source

PRICES = {"book": 1200, "pen": 150, "mug": 800}


async def look_up_price(sku: str) -> int:
    await asyncio.sleep(0.01)  # a call to the pricing service
    return PRICES[sku]


def invoices(skus: Iterable[str]) -> Source[list[int], NotUsed]:
    """The prices of the known SKUs, two to an invoice."""
    return (
        Source.from_iterable(skus)
        .filter(lambda sku: sku in PRICES)
        .map_async(4, look_up_price)  # up to 4 lookups at once, results in order
        .grouped(2)
    )


async def main() -> None:
    async with ActorSystem(Behaviors.empty, "pipeline") as system:
        blueprint = invoices(["book", "pen", "lamp", "mug"])
        print(await blueprint.run_with(Sink.seq(), system))  # [[1200, 150], [800]]
        total = blueprint.map(sum).run_fold(0, lambda acc, n: acc + n, system)
        print(await total)  # 2150


if __name__ == "__main__":
    asyncio.run(main())
