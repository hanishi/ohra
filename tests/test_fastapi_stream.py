"""ohra.ext.fastapi's actors.websocket on an ohra.stream graph: what the stream adds. The outbox
runs under the system's materializer, so terminating the system aborts it, as Apache Pekko's
materializer does with the streams it runs."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI  # noqa: E402

from ohra import Behaviors  # noqa: E402
from ohra.ext.fastapi import Actors  # noqa: E402

from .test_fastapi import FakeSocket, Said, echo  # noqa: E402


async def test_terminating_the_system_mid_send_closes_with_1001():
    actors = Actors(Behaviors.ignore, "ws")
    ws = FakeSocket()
    sending, gate = asyncio.Event(), asyncio.Event()
    send_json = ws.send_json

    async def slow_send(data: Any) -> None:
        sending.set()
        await gate.wait()
        await send_json(data)

    ws.send_json = slow_send
    async with actors.lifespan(FastAPI()):
        connection = asyncio.create_task(actors.websocket(ws.socket(), echo, Said))
        ws.incoming.put_nowait("hi")
        await sending.wait()
    gate.set()  # the system is gone: the stream was aborted under the send
    await asyncio.wait_for(connection, 1)
    assert ws.sent == [{"echo": "hi"}]
    assert ws.closed == 1001
