# 9. FastAPI

## The problem

Most Python services are web apps, and the web framework already runs the event loop. The actor system has to start with the app and stop with it, and every route has to reach it.

## The idea

`ohra.ext.fastapi.Actors` ties an actor system to a FastAPI app. The system starts when the app starts and shuts down, through the coordinated shutdown of [chapter 2](02-actor-system.md), when the app stops, and each route receives it as a dependency.

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

> **Coming from Apache Pekko**
>
> - FastAPI routes play the part of Pekko HTTP routes that ask actors. The integration itself has no Pekko counterpart.
> - Under uvicorn the server keeps its signals. Its shutdown runs `system.terminate()` through the lifespan, instead of the system's `run_on_signals` hook, as [appendix B](appendix-deviations.md) explains.

## How it works

This integration is not part of Apache Pekko. It is what a Python service over actors otherwise writes by hand.

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

---

[← Management and Kubernetes](08-management-and-kubernetes.md) · [Contents](README.md) · [numpy and shared memory →](10-numpy-and-shared-memory.md)
