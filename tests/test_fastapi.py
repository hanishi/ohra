"""ohra.ext.fastapi: an ActorSystem owned by the app's lifespan, reached through a typed
dependency."""

from __future__ import annotations

import asyncio
import dataclasses
import gc
import importlib.util
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from fastapi import FastAPI  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402

from ohra import ActorRef, ActorSystem, Behavior, Behaviors  # noqa: E402
from ohra.ext.fastapi import Actors  # noqa: E402

from .util import eventually  # noqa: E402


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


def _chatroom() -> Any:
    """examples/fastapi_chatroom.py, loaded from its file as a reader runs it."""
    path = Path(__file__).resolve().parents[1] / "examples" / "fastapi_chatroom.py"
    spec = importlib.util.spec_from_file_location("fastapi_chatroom", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_chatroom_relays_messages_and_rejects_a_taken_name():
    from fastapi.testclient import TestClient

    with TestClient(_chatroom().app) as client:
        with client.websocket_connect("/ws/lobby/alice") as alice:
            with client.websocket_connect("/ws/lobby/bob") as bob:
                assert alice.receive_json() == {"type": "notice", "text": "bob joined"}
                alice.send_text("hello bob")
                said = {"type": "message", "from": "alice", "text": "hello bob"}
                assert bob.receive_json() == said
                assert alice.receive_json() == said
                with client.websocket_connect("/ws/lobby/alice") as impostor:
                    assert impostor.receive_json() == {
                        "type": "denied",
                        "text": "'alice' is taken",
                    }
            assert alice.receive_json() == {"type": "notice", "text": "bob left"}


def test_no_chatroom_actor_holds_the_socket():
    assert "ws" not in {f.name for f in dataclasses.fields(_chatroom().Join)}


class FakeSocket:
    """Just enough of a WebSocket for the chatroom route: frames in from a queue, and a send that
    fails with ``send_error``."""

    def __init__(self, send_error: BaseException) -> None:
        self.incoming: asyncio.Queue[str | BaseException] = asyncio.Queue()
        self.send_error = send_error
        self.sends = 0

    async def accept(self) -> None:
        pass

    async def send_json(self, data: Any) -> None:
        self.sends += 1
        raise self.send_error

    async def receive_text(self) -> str:
        item = await self.incoming.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        pass


async def test_a_chatroom_send_that_fails_leaves_no_unretrieved_exception():
    from fastapi import WebSocketDisconnect

    chatroom = _chatroom()
    loop = asyncio.get_running_loop()
    errors: list[str] = []
    loop.set_exception_handler(lambda _, context: errors.append(str(context["message"])))
    try:
        async with ActorSystem(chatroom.lobby(), "chat") as system:
            ws = FakeSocket(WebSocketDisconnect(1006))  # the client's TCP dropped mid-send
            route = asyncio.create_task(chatroom.chat(ws, "lobby", "alice", system))
            ws.incoming.put_nowait("hello")  # the room echoes it back to alice
            await eventually(lambda: ws.sends > 0)
            ws.incoming.put_nowait(WebSocketDisconnect(1006))
            await route
            del route
            gc.collect()
    finally:
        loop.set_exception_handler(None)
    assert errors == []
