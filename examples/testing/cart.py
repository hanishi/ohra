import asyncio
from dataclasses import dataclass

from ohra import ActorRef, ActorSystem, Behavior, Behaviors


@dataclass(frozen=True)
class AddItem:
    sku: str
    qty: int
    reply_to: ActorRef[dict[str, int]]


@dataclass(frozen=True)
class GetCart:
    reply_to: ActorRef[dict[str, int]]


type CartCmd = AddItem | GetCart


async def check_stock(sku: str) -> None:
    await asyncio.sleep(0.01)  # another service


def cart(items: dict[str, int]) -> Behavior[CartCmd]:
    @Behaviors.receive_message
    async def on_message(msg: CartCmd) -> Behavior[CartCmd]:
        match msg:
            case AddItem(sku, qty, reply_to):
                await check_stock(sku)  # the next message waits
                updated = {**items, sku: items.get(sku, 0) + qty}
                reply_to.tell(updated)
                return cart(updated)
            case GetCart(reply_to):
                reply_to.tell(items)
                return Behaviors.same

    return on_message


async def main() -> None:
    async with ActorSystem(cart({}), "shop") as system:
        await asyncio.gather(
            system.ask(lambda r: AddItem("book", 1, r), timeout=1),
            system.ask(lambda r: AddItem("pen", 2, r), timeout=1),
        )
        print(await system.ask(GetCart, timeout=1))  # {'book': 1, 'pen': 2}


if __name__ == "__main__":
    asyncio.run(main())
