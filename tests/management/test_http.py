"""The management module's HTTP/1.1 server and client, tested against each other."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from ohra.management.http import HttpError, Response, get, json_response, serve


async def handler(method: str, path: str) -> Response:
    if path == "/boom":
        raise RuntimeError("boom")
    if path == "/slow":
        await asyncio.sleep(5)
        return Response(200)
    if path.startswith("/echo/"):
        return Response(200, path.encode(), "text/plain")
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


async def test_a_500_logs_the_error_and_sends_a_constant_body(caplog: pytest.LogCaptureFixture):
    server = await serve("127.0.0.1", 0, handler)
    try:
        with caplog.at_level(logging.ERROR, logger="ohra.management.http"):
            assert await get(f"http://127.0.0.1:{server.port}/boom", timeout=2) == (
                500,
                b"There was an internal server error.",
            )
        (record,) = caplog.records
        assert record.exc_info is not None and str(record.exc_info[1]) == "boom"
    finally:
        server.close()
        await server.wait_closed()


async def test_the_path_is_percent_decoded():
    server = await serve("127.0.0.1", 0, handler)
    try:
        assert await get(
            f"http://127.0.0.1:{server.port}/echo/ohra%3A%2F%2Fc%40h%3A1?x=%2F", timeout=2
        ) == (200, b"/echo/ohra://c@h:1")
    finally:
        server.close()
        await server.wait_closed()


async def test_an_absolute_form_target_is_served_by_its_path():
    server = await serve("127.0.0.1", 0, handler)
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        writer.write(b"GET http://example.com:80/echo/a%20b?x=1 HTTP/1.1\r\nHost: x\r\n\r\n")
        await writer.drain()
        reply = await reader.read()
        writer.close()
        await writer.wait_closed()
        assert reply.startswith(b"HTTP/1.1 200") and reply.endswith(b"\r\n\r\n/echo/a b")
    finally:
        server.close()
        await server.wait_closed()


async def test_close_cancels_a_connection_whose_task_has_not_started(
    monkeypatch: pytest.MonkeyPatch,
):
    """close() right after the accept, before the connection's task takes its first step: the
    task is cancelled too, so wait_closed() does not wait out the 10 s head timeout."""
    loop = asyncio.get_running_loop()
    server = await serve("127.0.0.1", 0, handler)
    create_task = loop.create_task

    def create_then_close(coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        task = create_task(coro, **kwargs)
        if getattr(coro, "__name__", "") == "on_connection":
            server.close()
        return task

    monkeypatch.setattr(loop, "create_task", create_then_close)
    _reader, writer = await asyncio.open_connection("127.0.0.1", server.port)  # says nothing
    try:
        await asyncio.wait_for(server.wait_closed(), 2)
    finally:
        writer.close()
        await writer.wait_closed()


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


_CHUNK = b"10000\r\n" + b"a" * 0x10000 + b"\r\n"  # 64 KiB


@pytest.mark.parametrize(
    "reply",
    [
        b"HTTP/1.1 \xb200 OK\r\n\r\n",  # latin-1 superscript two: isdigit, but no status
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n-5\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" + _CHUNK * 17,  # over 1 MiB
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort",  # the server hangs up early
        b"HTTP/1.1 200 OK\r\nX: " + b"a" * 100_000 + b"\r\n\r\n",  # a header past the limit
        b"HTTP/1.1 " + b"2" * 5000 + b" OK\r\n\r\n",  # past int()'s 4300-digit limit
        b"HTTP/1.1 200 OK\r\nContent-Length: " + b"9" * 5000 + b"\r\n\r\n",
    ],
    ids=[
        "unicode-status",
        "bad-chunk-size",
        "negative-chunk",
        "chunked-too-big",
        "truncated",
        "long-header",
        "long-status",
        "long-length",
    ],
)
async def test_a_malformed_reply_raises_http_error(reply: bytes):
    async def answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(reply)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(answer, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        with pytest.raises(HttpError):
            await get(f"http://127.0.0.1:{port}/", timeout=2)
    finally:
        server.close()
        await server.wait_closed()


async def test_a_body_without_a_length_is_read_to_the_end():
    async def answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nhello ")
        await writer.drain()
        await asyncio.sleep(0.05)  # the rest comes in a later segment
        writer.write(b"world")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(answer, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        assert await get(f"http://127.0.0.1:{port}/", timeout=2) == (200, b"hello world")
    finally:
        server.close()
        await server.wait_closed()
