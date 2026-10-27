# 9. FastAPI, numpy and shared memory

## The problem

Most Python services are web apps, and the web framework already runs the event loop. The actor system has to start with the app and stop with it, and every route has to reach it. Some work, such as numpy on a large array, also takes long enough to block that loop, and while it does, every actor and every request waits.

## The idea

`ohra.ext.fastapi.Actors` ties an actor system to a FastAPI app. The system starts when the app starts and shuts down, through the coordinated shutdown of [chapter 2](02-actor-system.md), when the app stops, and each route receives it as a dependency. Work that blocks goes to a thread, and its result comes back to the actor as a message, so the event loop keeps running in the meantime. Work that belongs in another process gets its large arrays through shared memory, as a small handle instead of a copy.

## A first example

Chapter 1's cart behind two routes. It needs the `web` extra (`pip install -e '.[web]'`):

```python
from fastapi import FastAPI

from cart import AddItem, CartCmd, GetCart, cart
from ohra import ActorSystem
from ohra.ext.fastapi import Actors

actors = Actors(cart({}), "shop")  # the actor system lives as long as the app
app = FastAPI(lifespan=actors.lifespan)


@app.post("/items/{sku}")
async def add_item(
    sku: str, qty: int = 1, system: ActorSystem[CartCmd] = actors.depends()
) -> dict[str, int]:
    return await system.ask(lambda r: AddItem(sku, qty, r), timeout=1)


@app.get("/cart")
async def get_cart(system: ActorSystem[CartCmd] = actors.depends()) -> dict[str, int]:
    return await system.ask(GetCart, timeout=1)
```

Start it in one terminal, and call it from another:

```
$ uvicorn web_cart:app
```

```
$ curl -X POST localhost:8000/items/book
{"book":1}
$ curl -X POST 'localhost:8000/items/pen?qty=2'
{"book":1,"pen":2}
$ curl localhost:8000/cart
{"book":1,"pen":2}
```

- **`Actors(cart({}), "shop")`** takes the same arguments as `ActorSystem`.
- **`lifespan=actors.lifespan`** lets FastAPI start the system and stop it.
- **`actors.depends()`** hands each route the system, with its type: `system.ask(GetCart, ...)` returns a `dict[str, int]`, and pyright checks that against the route's return type.
- **Each request is a message.** Two requests at the same moment still reach the cart one at a time, as in chapter 1.
- **A cart that does not answer within the timeout** turns into a 504 response.

## A second example: numpy in a thread

An analyst that finds the three best-selling products in 30 million orders. It needs the `numpy` extra (`pip install -e '.[numpy]'`). Save it as `top_sellers.py`, since the third example imports from it:

```python
import asyncio
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

from ohra import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, Success, Try

type Orders = npt.NDArray[np.int64]  # one product id per sale


def top_sellers(orders: Orders) -> list[int]:
    sales_per_product = np.bincount(orders)
    return [int(p) for p in np.argsort(sales_per_product)[::-1][:3]]


@dataclass(frozen=True)
class TopSellers:
    reply_to: ActorRef[list[int]]


@dataclass(frozen=True)
class CountSales:
    reply_to: ActorRef[int]


@dataclass(frozen=True)
class Computed:
    result: Try[list[int]]
    reply_to: ActorRef[list[int]]


type AnalystCmd = TopSellers | CountSales | Computed


def analyst(orders: Orders) -> Behavior[AnalystCmd]:
    @Behaviors.receive
    def on_message(ctx: ActorContext[AnalystCmd], msg: AnalystCmd) -> Behavior[AnalystCmd]:
        match msg:
            case TopSellers(reply_to):
                work = asyncio.to_thread(top_sellers, orders)  # numpy runs in a thread
                ctx.pipe_to_self(work, lambda result: Computed(result, reply_to))
            case CountSales(reply_to):
                reply_to.tell(len(orders))
            case Computed(Success(top), reply_to):
                reply_to.tell(top)
            case Computed():
                pass  # it failed: the ask times out
        return Behaviors.same

    return on_message


async def main() -> None:
    orders = np.random.default_rng(0).integers(0, 1000, size=30_000_000)
    orders.flags.writeable = False  # a thread reads it: make sure nobody writes to it
    async with ActorSystem(analyst(orders), "reports") as system:
        report = asyncio.create_task(system.ask(TopSellers, timeout=10))
        await asyncio.sleep(0)  # let the request go out first
        print(await system.ask(CountSales, timeout=10), "sales")
        print("report still running:", not report.done())
        print("top sellers:", await report)


if __name__ == "__main__":
    asyncio.run(main())
```

It prints:

```
30000000 sales
report still running: True
top sellers: [947, 82, 762]
```

- **`asyncio.to_thread(top_sellers, orders)`** runs the numpy work in a thread, and **`ctx.pipe_to_self(work, ...)`** sends its result back to the analyst as a `Computed` message, holding a `Success` or a `Failure`.
- **The lambda keeps `reply_to`**, so the analyst knows whom to answer when the result arrives, without any state of its own.
- **The analyst stays free.** It answered `CountSales` while the report was still running. Call `top_sellers(orders)` in the handler instead and the second line says `False`: the analyst, and every other actor on the loop, would have waited for the report.
- **`orders.flags.writeable = False`**, because state and messages are shared by reference, never copied, and a thread is reading this array.

A thread pays off when numpy releases Python's global lock, as most large numpy operations do. [How it works](#numpy-and-parallelism) has the details.

## A third example: shared memory

Pure-Python work gains nothing from a thread, and belongs in another process. Sending the orders to a process would pickle and copy all 240 MB. A `SharedArray` puts them in shared memory once and sends a handle instead. The same report, in a worker process:

```python
import asyncio
import pickle
from concurrent.futures import ProcessPoolExecutor

import numpy as np

from ohra import ActorContext, ActorSystem, Behavior, Behaviors, Success
from ohra.ext.shm import SharedArray
from top_sellers import AnalystCmd, Computed, CountSales, TopSellers, top_sellers


def top_sellers_in(handle: SharedArray) -> list[int]:
    with handle.open() as orders:  # maps the same memory: nothing is copied
        return top_sellers(orders)


def analyst(orders: SharedArray, pool: ProcessPoolExecutor) -> Behavior[AnalystCmd]:
    @Behaviors.receive
    def on_message(ctx: ActorContext[AnalystCmd], msg: AnalystCmd) -> Behavior[AnalystCmd]:
        match msg:
            case TopSellers(reply_to):
                loop = asyncio.get_running_loop()
                work = loop.run_in_executor(pool, top_sellers_in, orders)  # another process
                ctx.pipe_to_self(work, lambda result: Computed(result, reply_to))
            case CountSales(reply_to):
                reply_to.tell(orders.shape[0])
            case Computed(Success(top), reply_to):
                reply_to.tell(top)
            case Computed():
                pass  # it failed: the ask times out
        return Behaviors.same

    return on_message


async def main() -> None:
    sales = np.random.default_rng(0).integers(0, 1000, size=30_000_000)
    orders = SharedArray.create(sales)  # one copy, into shared memory
    print(f"{sales.nbytes // 1_000_000} MB of orders, sent as {len(pickle.dumps(orders))} bytes")
    try:
        with ProcessPoolExecutor() as pool:
            async with ActorSystem(analyst(orders, pool), "reports") as system:
                print("top sellers:", await system.ask(TopSellers, timeout=30))
    finally:
        orders.unlink()  # the owner frees the memory, once


if __name__ == "__main__":
    asyncio.run(main())
```

It prints:

```
240 MB of orders, sent as 79 bytes
top sellers: [947, 82, 762]
```

- **`SharedArray.create(sales)`** copies the array into shared memory, once. The handle, `orders`, is three small fields: the memory's name, the shape and the dtype.
- **`loop.run_in_executor(pool, top_sellers_in, orders)`** sends the handle to a worker process, and `pipe_to_self` takes the result as before. The rest of the analyst is unchanged.
- **`handle.open()`** maps the same memory in the worker, read-only, so nothing is copied in either direction.
- **Whoever holds the handle owns the memory**, and the owner calls `unlink()` once, here when the program is done. [Appendix B](appendix-deviations.md) has the rule.

> **Coming from Apache Pekko**
>
> - FastAPI routes play the part of Pekko HTTP routes that ask actors. The integration itself, like `SharedArray`, has no Pekko counterpart.
> - `ctx.pipe_to_self(awaitable, adapt)` is `pipeToSelf`, with a thread in place of a Future on another dispatcher.
> - Under uvicorn the server keeps its signals. Its shutdown runs `system.terminate()` through the lifespan, instead of the system's `run_on_signals` hook, as [appendix B](appendix-deviations.md) explains.

## How it works

These integrations are not part of Apache Pekko. They are what a Python service over actors otherwise writes by hand.

### FastAPI

`ohra.ext.fastapi.Actors` owns an `ActorSystem` for the life of a FastAPI app and hands it to routes as a typed dependency, so `ask` replies are type-checked all the way to the response. An `AskTimeoutError` that escapes a route becomes a 504. Keyword arguments after the name go to `ActorSystem` (`address`, `cluster`, `shutdown`, `management`), so a cluster node can be owned by the app. Needs the `web` extra. To stop taking requests before a cluster node leaves, add a task to the `service-unbind` phase of the coordinated shutdown, and let uvicorn keep its signals: its shutdown should call `system.terminate()` rather than `run_on_signals` taking SIGTERM.

```python
from fastapi import FastAPI
from ohra.ext.fastapi import Actors

actors = Actors(counter(), "web")
app = FastAPI(lifespan=actors.lifespan)


@app.get("/total")
async def total(system: ActorSystem[Command] = actors.depends()) -> dict[str, int]:
    return {"total": await system.ask(GetTotal, timeout=1)}
```

Inside a WebSocket route the same dependency works, and the connection actor pattern in `examples/fastapi_chatroom.py` keeps one actor per socket.

### numpy and parallelism

A handler that blocks the event loop stalls every actor. Run numpy work with `ctx.pipe_to_self(asyncio.to_thread(fn, ...), adapt)` and stash until the result comes back. numpy releases the GIL, so eight such actors use eight cores while a timer keeps ticking on time; pure-Python work in threads gains nothing. For another process, `ohra.ext.shm.SharedArray` hands over a large array as a three-field handle instead of a copy.

Messages are shared by reference and never copied. Keep them immutable: frozen dataclasses, and for arrays either copy before sending or set `arr.flags.writeable = False`.

---

[← Management and Kubernetes](08-management-and-kubernetes.md) · [Contents](README.md) · [Putting it together: one cart request →](10-putting-it-together.md)
