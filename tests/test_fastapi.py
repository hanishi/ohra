"""ohra.ext.fastapi: an ActorSystem owned by the app's lifespan, reached through a typed
dependency."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from fastapi import FastAPI  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402

from ohra import ActorRef, ActorSystem, Behavior, Behaviors  # noqa: E402
from ohra.ext.fastapi import Actors  # noqa: E402


@dataclass(frozen=True)
class Greet:
    name: str
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class Stall:
    reply_to: ActorRef[str]


type Cmd = Greet | Stall


@Behaviors.receive_message
async def greeter(msg: Cmd) -> Behavior[Cmd]:
    match msg:
        case Greet(name, reply_to):
            reply_to.tell(f"hello {name}")
        case Stall():
            await asyncio.sleep(0.2)
    return Behaviors.same


def make_app() -> tuple[FastAPI, Actors[Cmd]]:
    actors = Actors(greeter, "web")
    app = FastAPI(lifespan=actors.lifespan)

    @app.get("/greet/{name}")
    async def greet(name: str, system: ActorSystem[Cmd] = actors.depends()) -> dict[str, str]:
        return {"text": await system.ask(lambda r: Greet(name, r), timeout=1)}

    @app.get("/stall")
    async def stall(system: ActorSystem[Cmd] = actors.depends()) -> dict[str, str]:
        return {"text": await system.ask(Stall, timeout=0.01)}

    return app, actors


async def test_route_asks_the_system_through_the_dependency():
    app, actors = make_app()
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
            r = await client.get("/greet/pekko")
        assert r.status_code == 200
        assert r.json() == {"text": "hello pekko"}
        assert actors.system.is_terminated is False
    assert actors.system.is_terminated is True


async def test_ask_timeout_is_a_504():
    app, _ = make_app()
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
            r = await client.get("/stall")
    assert r.status_code == 504
    assert "timed out" in r.json()["detail"]


def test_system_before_startup_is_an_error():
    actors = Actors(greeter, "web")
    with pytest.raises(RuntimeError):
        _ = actors.system


async def test_system_settings_reach_the_actor_system():
    from ohra import CoordinatedShutdownSettings

    settings = CoordinatedShutdownSettings(default_phase_timeout=1.5)
    # address=, cluster=, management= pass the same way
    actors = Actors(greeter, "web", shutdown=settings)
    app = FastAPI(lifespan=actors.lifespan)
    async with app.router.lifespan_context(app):
        assert actors.system.coordinated_shutdown.settings is settings
