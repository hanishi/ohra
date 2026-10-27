"""The management module's HTTP/1.1 server and client, tested against each other."""

from __future__ import annotations

import asyncio

import pytest

from ohra.management.http import Response, get, json_response, serve


async def handler(method: str, path: str) -> Response:
    if path == "/boom":
        raise RuntimeError("boom")
    if path == "/slow":
        await asyncio.sleep(5)
        return Response(200)
    if path == "/hello":
        return json_response(200, {"hello": "world"})
    return Response(404, b"not found", "text/plain")


async def test_get_json_404_405_and_500():
    server = await serve("127.0.0.1", 0, handler)
    port = server.sockets[0].getsockname()[1]
    try:
        assert await get(f"http://127.0.0.1:{port}/hello?x=1", timeout=2) == (
            200,
            b'{"hello": "world"}',
        )
        assert (await get(f"http://127.0.0.1:{port}/nope", timeout=2))[0] == 404
        assert (await get(f"http://127.0.0.1:{port}/boom", timeout=2))[0] == 500
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"POST /hello HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
        assert (await reader.readline()).startswith(b"HTTP/1.1 405")
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


async def test_client_times_out():
    server = await serve("127.0.0.1", 0, handler)
    port = server.sockets[0].getsockname()[1]
    try:
        with pytest.raises(TimeoutError):
            await get(f"http://127.0.0.1:{port}/slow", timeout=0.2)
    finally:
        server.close()
        await server.wait_closed()


async def test_server_rejects_garbage_and_closes():
    server = await serve("127.0.0.1", 0, handler)
    port = server.sockets[0].getsockname()[1]
    try:
        for payload in (
            b"\r\n\r\n",
            b"GET\r\n\r\n",
            b"GET /hello HTTP/1.1\r\nX: " + b"a" * 100_000 + b"\r\n\r\n",
        ):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(payload)
            await writer.drain()
            first = await asyncio.wait_for(reader.readline(), 2)
            assert first == b"" or first.startswith(b"HTTP/1.1 400"), payload[:20]
            writer.close()
            await writer.wait_closed()
        # still serving
        assert await get(f"http://127.0.0.1:{port}/hello", timeout=2) == (
            200,
            b'{"hello": "world"}',
        )
    finally:
        server.close()
        await server.wait_closed()
