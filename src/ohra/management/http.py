"""A minimal HTTP/1.1 server and client on asyncio streams: enough for the management endpoint
(GET, JSON, one request per connection) and for probing peers and the Kubernetes API, with nothing
beyond the standard library."""

from __future__ import annotations

import asyncio
import json
import ssl
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

REASONS: dict[int, str] = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    500: "Internal Server Error",
}
_LINE_LIMIT = 8 * 1024
_MAX_HEADERS = 100
# a client that connects and says nothing holds a connection task for at most this long
_HEAD_TIMEOUT = 10.0
_MAX_BODY = 1024 * 1024  # every caller expects small JSON


def _is_decimal(text: str) -> bool:
    # "²".isdigit(), but int("²") raises; so does int() past 4300 digits
    return text.isascii() and text.isdigit() and len(text) <= 18


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes = b""
    content_type: str = "application/json"


type Handler = Callable[[str, str], Awaitable[Response]]
"""``handler(method, path)``; the path has no query string."""


def json_response(status: int, payload: object) -> Response:
    return Response(status, json.dumps(payload).encode(), "application/json")


class HttpError(Exception):
    """A malformed response from the other side."""


# --- server --------------------------------------------------------------------------------------


def _encode(response: Response, extra: str = "") -> bytes:
    reason = REASONS.get(response.status, "Unknown")
    head = (
        f"HTTP/1.1 {response.status} {reason}\r\nContent-Type: {response.content_type}\r\n"
        f"Content-Length: {len(response.body)}\r\nConnection: close\r\n{extra}\r\n"
    )
    return head.encode() + response.body


async def _read_head(reader: asyncio.StreamReader) -> tuple[str, str, dict[str, str]] | None:
    """The request line and headers, or None when the request is malformed or too large."""
    try:
        line = await reader.readuntil(b"\n")
        parts = line.decode("latin-1").strip().split(" ")
        if len(parts) != 3 or not parts[2].startswith("HTTP/1."):
            return None
        method, target = parts[0], parts[1]
        headers: dict[str, str] = {}
        for _ in range(_MAX_HEADERS):
            header = (await reader.readuntil(b"\n")).decode("latin-1")
            if header in ("\r\n", "\n"):
                return method, target, headers
            name, sep, value = header.partition(":")
            if not sep:
                return None
            headers[name.strip().lower()] = value.strip()
        return None
    except (
        asyncio.LimitOverrunError,
        asyncio.IncompleteReadError,
        UnicodeDecodeError,
        ValueError,
    ):
        return None


class HttpServer:
    """A listening server whose connection tasks are cancelled when it closes, so a silent or
    half-open client never holds the owner's shutdown (asyncio's ``wait_closed`` waits for every
    handler to finish)."""

    def __init__(self) -> None:
        self._server: asyncio.Server | None = None
        self._connections: set[asyncio.Task[None]] = set()

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    @property
    def sockets(self) -> tuple[Any, ...]:
        assert self._server is not None
        return tuple(self._server.sockets)

    def track(self) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connections.add(task)
            task.add_done_callback(self._connections.discard)

    def close(self) -> None:
        if self._server is not None:
            self._server.close()
        for task in list(self._connections):
            task.cancel()

    async def wait_closed(self) -> None:
        if self._server is not None:
            await self._server.wait_closed()


async def serve(host: str, port: int, handler: Handler) -> HttpServer:
    """Listen and answer one GET per connection with the handler's response; the caller closes the
    server."""
    http_server = HttpServer()

    async def on_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        http_server.track()
        try:
            head = await asyncio.wait_for(_read_head(reader), _HEAD_TIMEOUT)
            if head is None:
                writer.write(_encode(Response(400, b"bad request", "text/plain")))
            else:
                method, target, _headers = head
                path = target.split("?", 1)[0]
                if method != "GET":
                    writer.write(
                        _encode(
                            Response(405, b"method not allowed", "text/plain"), "Allow: GET\r\n"
                        )
                    )
                else:
                    try:
                        response = await handler(method, path)
                    except Exception as e:  # one failing route must not take the server down
                        response = Response(500, str(e).encode(), "text/plain")
                    writer.write(_encode(response))
            await writer.drain()
        except (OSError, TimeoutError, asyncio.CancelledError):
            # a dead or silent client, or the server closing: nothing awaits this task, so
            # swallowing is safe
            pass
        finally:
            writer.close()

    http_server._server = await asyncio.start_server(on_connection, host, port, limit=_LINE_LIMIT)  # pyright: ignore[reportPrivateUsage]
    return http_server


# --- client --------------------------------------------------------------------------------------


async def get(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    timeout: float,
    ssl_context: ssl.SSLContext | None = None,
) -> tuple[int, bytes]:
    """One GET: the status and the body. Raises TimeoutError, OSError (connection) or HttpError
    (malformed reply)."""
    parts = urlsplit(url)
    if parts.hostname is None:
        raise ValueError(f"no host in {url!r}")
    secure = parts.scheme == "https"
    port = parts.port or (443 if secure else 80)
    target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
    context = (
        ssl_context
        if ssl_context is not None
        else (ssl.create_default_context() if secure else None)
    )

    async def exchange() -> tuple[int, bytes]:
        reader, writer = await asyncio.open_connection(
            parts.hostname, port, ssl=context, server_hostname=parts.hostname if context else None
        )
        try:
            return await read_reply(reader, writer)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as e:
            raise HttpError(f"truncated or oversized reply from {url}") from e
        finally:
            writer.close()

    async def read_reply(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> tuple[int, bytes]:
        lines = [f"GET {target} HTTP/1.1", f"Host: {parts.netloc}", "Connection: close"]
        lines += [f"{k}: {v}" for k, v in (headers or {}).items()]
        writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
        await writer.drain()
        status_line = (await reader.readuntil(b"\n")).decode("latin-1").split(" ", 2)
        if (
            len(status_line) < 2
            or not status_line[0].startswith("HTTP/1.")
            or not _is_decimal(status_line[1])
        ):
            raise HttpError(f"malformed status line from {url}")
        status = int(status_line[1])
        response_headers: dict[str, str] = {}
        while True:
            header = (await reader.readuntil(b"\n")).decode("latin-1")
            if header in ("\r\n", "\n"):
                break
            name, _, value = header.partition(":")
            response_headers[name.strip().lower()] = value.strip()
        if response_headers.get("transfer-encoding", "").lower() == "chunked":
            body = b""
            while True:
                size_field = (await reader.readuntil(b"\n")).split(b";")[0].strip() or b"0"
                if not all(c in b"0123456789abcdefABCDEF" for c in size_field):
                    raise HttpError(f"bad chunk size {size_field!r} from {url}")
                size = int(size_field, 16)
                if size == 0:
                    break
                if len(body) + size > _MAX_BODY:
                    raise HttpError(f"chunked body over {_MAX_BODY} bytes from {url}")
                body += await reader.readexactly(size)
                await reader.readuntil(b"\n")
        elif "content-length" in response_headers:
            length = response_headers["content-length"]
            if not _is_decimal(length) or int(length) > _MAX_BODY:
                raise HttpError(f"bad Content-Length {length!r} from {url}")
            body = await reader.readexactly(int(length))
        else:
            body = b""  # until the server closes
            while chunk := await reader.read(_MAX_BODY + 1 - len(body)):
                body += chunk
                if len(body) > _MAX_BODY:
                    raise HttpError(f"body over {_MAX_BODY} bytes from {url}")
        return status, body

    return await asyncio.wait_for(exchange(), timeout)
