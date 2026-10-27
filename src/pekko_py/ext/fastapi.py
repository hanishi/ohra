"""FastAPI integration: an ActorSystem owned by the app's lifespan, reached through a typed
dependency.

    actors = Actors(guardian, "app")
    app = FastAPI(lifespan=actors.lifespan)

    @app.get("/x")
    async def x(system: ActorSystem[Cmd] = actors.depends()) -> ...:
        return await system.ask(..., timeout=1)

An ``AskTimeoutError`` escaping a route becomes a 504. Requires the ``web`` extra.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from ..behavior import Behavior
from ..patterns import AskTimeoutError
from ..system import ActorSystem


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


def _ask_timeout_to_504(_request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse({"detail": f"actor ask timed out: {exc}"}, status_code=504)
