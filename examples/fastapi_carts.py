"""A FastAPI service backed by actors: one cart actor per cart id, spawned on demand by a
manager and stopped after a period of inactivity. Request handlers ask the manager and
await the reply; the actor system lives for the lifetime of the app.

    uv run --extra web uvicorn examples.fastapi_carts:app --port 8000

    curl -X POST localhost:8000/carts/alice/items -H 'content-type: application/json' -d '{"sku": "book", "qty": 2}'
    curl localhost:8000/carts/alice
    curl -X DELETE localhost:8000/carts/alice
"""
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pekko_py import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Signal,
    Terminated,
)
from pekko_py.fastapi import Actors

IDLE_TIMEOUT = 30.0  # seconds without a request before a cart actor stops

# --- cart protocol -------------------------------------------------------------------


@dataclass(frozen=True)
class AddItem:
    sku: str
    qty: int
    reply_to: ActorRef[dict[str, int]]


@dataclass(frozen=True)
class GetCart:
    reply_to: ActorRef[dict[str, int]]


@dataclass(frozen=True)
class Checkout:
    reply_to: ActorRef[dict[str, int]]


@dataclass(frozen=True)
class _Idle:
    pass


type CartCmd = AddItem | GetCart | Checkout | _Idle


def cart() -> Behavior[CartCmd]:
    def setup(ctx: ActorContext[CartCmd]) -> Behavior[CartCmd]:
        def active(items: dict[str, int]) -> Behavior[CartCmd]:
            ctx.timers.start_single_timer("idle", _Idle(), IDLE_TIMEOUT)  # restarted on every request

            def on_message(msg: CartCmd) -> Behavior[CartCmd]:
                match msg:
                    case AddItem(sku, qty, reply_to):
                        updated = {**items, sku: items.get(sku, 0) + qty}
                        reply_to.tell(updated)
                        return active(updated)
                    case GetCart(reply_to):
                        reply_to.tell(items)
                        return active(items)
                    case Checkout(reply_to):
                        reply_to.tell(items)
                        return Behaviors.stopped()
                    case _Idle():
                        return Behaviors.stopped()

            return Behaviors.receive_message(on_message)

        return active({})

    return Behaviors.setup(setup)


# --- manager: routes by cart id, spawns on demand, forgets stopped carts ------------------


type CartRequest = AddItem | GetCart | Checkout


@dataclass(frozen=True)
class ForCart:
    cart_id: str
    cmd: CartRequest
    create: bool = True  # False: reply "no such cart" instead of spawning

    @property
    def reply_to(self) -> ActorRef[dict[str, int]]:  # an envelope is askable if it exposes the inner reply_to
        return self.cmd.reply_to


@dataclass(frozen=True)
class CartCount:
    reply_to: ActorRef[int]


type ManagerCmd = ForCart | CartCount


def manager() -> Behavior[ManagerCmd]:
    def on_message(ctx: ActorContext[ManagerCmd], msg: ManagerCmd) -> Behavior[ManagerCmd]:
        match msg:
            case ForCart(cart_id, cmd, create):
                existing = ctx.child(cart_id)
                if existing is not None:
                    existing.tell(cmd)
                elif create:
                    child = ctx.spawn(cart(), cart_id)
                    ctx.watch(child)
                    child.tell(cmd)
                else:
                    cmd.reply_to.tell({})
            case CartCount(reply_to):
                reply_to.tell(len(ctx.children))
        return Behaviors.same

    def on_signal(ctx: ActorContext[ManagerCmd], sig: Signal) -> Behavior[ManagerCmd]:
        if isinstance(sig, Terminated):
            ctx.log.info("cart %s stopped", sig.ref.path.rsplit("/", 1)[-1])
            return Behaviors.same
        return Behaviors.unhandled

    return Behaviors.receive(on_message).receive_signal(on_signal)


# --- HTTP ------------------------------------------------------------------------------------


actors = Actors(manager(), "carts")
app = FastAPI(title="carts", lifespan=actors.lifespan)



type Reply = ActorRef[dict[str, int]]


def for_cart(cart_id: str, build: Callable[[Reply], CartRequest], create: bool = True) -> Callable[[Reply], ForCart]:
    """ask needs a function from reply ref to message; this composes the cart command into ForCart."""
    return lambda reply_to: ForCart(cart_id, build(reply_to), create)


class Item(BaseModel):
    sku: str
    qty: int = 1


@app.post("/carts/{cart_id}/items")
async def add_item(cart_id: str, item: Item, system: ActorSystem[ManagerCmd] = actors.depends()) -> dict[str, Any]:
    items = await system.ask(for_cart(cart_id, lambda r: AddItem(item.sku, item.qty, r)), timeout=2)
    return {"cart": cart_id, "items": items}


@app.get("/carts/{cart_id}")
async def get_cart(cart_id: str, system: ActorSystem[ManagerCmd] = actors.depends()) -> dict[str, Any]:
    items = await system.ask(for_cart(cart_id, GetCart, create=False), timeout=2)
    if not items:
        raise HTTPException(404, "no such cart")
    return {"cart": cart_id, "items": items}


@app.delete("/carts/{cart_id}")
async def checkout(cart_id: str, system: ActorSystem[ManagerCmd] = actors.depends()) -> dict[str, Any]:
    items = await system.ask(for_cart(cart_id, Checkout, create=False), timeout=2)
    if not items:
        raise HTTPException(404, "no such cart")
    return {"cart": cart_id, "checked_out": items}


@app.get("/carts")
async def count(system: ActorSystem[ManagerCmd] = actors.depends()) -> dict[str, int]:
    return {"active_carts": await system.ask(CartCount, timeout=2)}
