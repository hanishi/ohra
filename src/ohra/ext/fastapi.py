"""FastAPI integration: an ActorSystem owned by the app's lifespan, reached through a typed
dependency.

    actors = Actors(guardian, "app")
    app = FastAPI(lifespan=actors.lifespan)

    @app.get("/x")
    async def x(system: ActorSystem[Cmd] = actors.depends()) -> ...:
        return await system.ask(..., timeout=1)

An ``AskTimeoutError`` escaping a route becomes a 504. ``actors.websocket`` gives a WebSocket
route an actor per connection, as Play's ``ActorFlow.actorRef`` does: the route owns the socket and
the actor only gets ``out``. Requires the ``web`` extra.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import Any, cast

from fastapi import Depends, FastAPI, Request, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState
from fastapi.responses import JSONResponse

from ..behavior import Behavior, Behaviors
from ..context import ActorContext
from ..patterns import AskTimeoutError
from ..ref import ActorRef
from ..signals import Signal, Terminated
from ..stream import ActorSource, OverflowStrategy
from ..system import ActorSystem

_log = logging.getLogger(__name__)


class Actors[T]:
    def __init__(self, guardian: Behavior[T], name: str = "app", **settings: Any) -> None:
        """``settings`` go to ``ActorSystem``: ``address``, ``cluster``, ``shutdown``,
        ``management``, ``mailbox``."""
        self._guardian = guardian
        self._name = name
        self._settings = settings
        self._system: ActorSystem[T] | None = None

    @property
    def system(self) -> ActorSystem[T]:
        """The running system. Only valid between startup and shutdown."""
        if self._system is None:
            raise RuntimeError(
                "ActorSystem is not running: pass actors.lifespan to FastAPI(lifespan=...)"
            )
        return self._system

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None]:
        app.add_exception_handler(AskTimeoutError, _ask_timeout_to_504)
        async with ActorSystem(self._guardian, self._name, **self._settings) as system:
            self._system = system
            yield

    def depends(self) -> ActorSystem[T]:
        """Default value for a route parameter: ``system: ActorSystem[Cmd] = actors.depends()``."""
        return cast(ActorSystem[T], Depends(lambda: self.system))

    async def websocket[In, Out](
        self,
        ws: WebSocket,
        props: Callable[[ActorRef[Out]], Behavior[In]],
        decode: Callable[[str], In],
        buffer_size: int = 16,
        overflow_strategy: OverflowStrategy = OverflowStrategy.fail,
    ) -> None:
        """Run one WebSocket connection through an actor, as Play's ``ActorFlow.actorRef``:
        accept the socket, spawn ``props(out)``, tell it ``decode(text)`` for every frame that
        comes in, and send every message it tells ``out`` as JSON. Returns when the connection is
        over. A closed socket stops the actor after the frames it already has; an actor that
        stops, or fails, closes the socket after the frames it told ``out``, and so does the
        system terminating. A client more than ``buffer_size`` frames behind is closed with 1008
        under ``OverflowStrategy.fail``, and a frame ``decode`` rejects closes it with 1003."""
        await ws.accept()
        out: ActorSource[Out | _Done] = ActorSource.actor_ref(
            completion_matcher=lambda m: m is _DONE,
            failure_matcher=lambda _: None,
            buffer_size=buffer_size,
            overflow_strategy=overflow_strategy,
        )
        root = self.system._root  # pyright: ignore[reportPrivateUsage]
        if root.stopping:  # the system is shutting down
            await ws.close(code=1001, reason="going away")
            return
        cell = root.spawn_child(_flow(props, out.ref), None, None)
        # Play's sink watches the wrapper: however it ends, the frames told so far go out first
        cell.done.add_done_callback(lambda _: out.ref.tell(_DONE))
        flow = cell.ref

        async def receive() -> None:
            with contextlib.suppress(WebSocketDisconnect):
                while ws.application_state == WebSocketState.CONNECTED:
                    text = await ws.receive_text()
                    try:
                        msg = decode(text)
                    except Exception:
                        if ws.application_state != WebSocketState.CONNECTED:
                            return  # the server closed meanwhile: the frame comes too late
                        _log.warning("closing a WebSocket: decode rejected a frame", exc_info=True)
                        await ws.close(code=1003, reason="unsupported data")
                        return
                    flow.tell(msg)

        async def send() -> None:
            with contextlib.suppress(WebSocketDisconnect):  # the client may be gone already
                async for frame in out:
                    await ws.send_json(frame)
                await ws.close()  # the actor stopped and its frames are sent

        receiving = asyncio.create_task(receive())
        sending = asyncio.create_task(send())
        overflowed = False

        def overflow() -> None:  # Pekko fails downstream at once, even in the middle of a send
            nonlocal overflowed
            overflowed = True
            sending.cancel()

        out._on_failure = overflow  # pyright: ignore[reportPrivateUsage]
        try:
            done, _ = await asyncio.wait({receiving, sending}, return_when=asyncio.FIRST_COMPLETED)
            if overflowed:
                if ws.application_state == WebSocketState.CONNECTED:  # else the client is gone
                    with contextlib.suppress(WebSocketDisconnect):
                        await ws.close(code=1008, reason="too slow")
                if receiving in done:  # it failed in the same tick: that is still an error
                    receiving.result()
            else:
                for task in done:
                    task.result()
        finally:  # everything before the await: in a cancelled scope (anyio) every await raises
            flow.tell(_CLOSE)
            out.cancel()
            receiving.cancel()
            sending.cancel()
            await asyncio.wait({receiving, sending})


def _ask_timeout_to_504(_request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse({"detail": f"actor ask timed out: {exc}"}, status_code=504)


class _Done:
    """Told to the outbox when the connection's wrapper has stopped."""


_DONE = _Done()


class _Close:
    """Told to the wrapper when the connection is over, as Play's wrapper gets Status.Success."""


_CLOSE = _Close()


def _flow[In, Out](
    props: Callable[[ActorRef[Out]], Behavior[In]], out: ActorRef[Out | _Done]
) -> Behavior[In | _Close]:
    """Play's wrapper actor: spawns the connection's actor, forwards frames to it, and stops it
    behind those frames (Play's PoisonPill) when the connection is over."""

    def setup(ctx: ActorContext[In | _Close]) -> Behavior[In | _Close]:
        actor = ctx.spawn(props(out), "flowActor")
        ctx.watch(actor)

        def on_message(msg: In | _Close) -> Behavior[In | _Close]:
            if isinstance(msg, _Close):
                ctx.stop(actor, after_pending=True)
            else:
                actor.tell(msg)
            return Behaviors.same

        def on_signal(_ctx: ActorContext[In | _Close], sig: Signal) -> Behavior[In | _Close]:
            if isinstance(sig, Terminated):
                return Behaviors.stopped()
            return Behaviors.unhandled

        return Behaviors.receive_message(on_message).receive_signal(on_signal)

    return Behaviors.setup(setup)
