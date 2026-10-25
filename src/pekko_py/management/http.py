"""A minimal HTTP/1.1 server and client on asyncio streams: enough for the management endpoint (GET, JSON, one
request per connection) and for probing peers and the Kubernetes API, with nothing beyond the standard library."""
from __future__ import annotations

import asyncio
import json
import ssl
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit

REASONS: dict[int, str] = {200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed", 500: "Internal Server Error"}
_LINE_LIMIT = 8 * 1024
_MAX_HEADERS = 100


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


# --- server --------------------------------------------------------------------------------------------------------


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
    except (asyncio.LimitOverrunError, asyncio.IncompleteReadError, UnicodeDecodeError, ValueError):
        return None


async def serve(host: str, port: int, handler: Handler) -> asyncio.Server:
    """Listen and answer one GET per connection with the handler's response; the caller closes the server."""

    async def on_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await _read_head(reader)
            if head is None:
                writer.write(_encode(Response(400, b"bad request", "text/plain")))
            else:
                method, target, _headers = head
                path = target.split("?", 1)[0]
                if method != "GET":
                    writer.write(_encode(Response(405, b"method not allowed", "text/plain"), "Allow: GET\r\n"))
                else:
                    try:
                        response = await handler(method, path)
                    except Exception as e:  # one failing route must not take the server down
                        response = Response(500, str(e).encode(), "text/plain")
                    writer.write(_encode(response))
            await writer.drain()
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    return await asyncio.start_server(on_connection, host, port, limit=_LINE_LIMIT)


# --- client --------------------------------------------------------------------------------------------------------


async def get(url: str, *, headers: Mapping[str, str] | None = None, timeout: float, ssl_context: ssl.SSLContext | None = None) -> tuple[int, bytes]:
    """One GET: the status and the body. Raises TimeoutError, OSError (connection) or HttpError (malformed reply)."""
    parts = urlsplit(url)
    if parts.hostname is None:
        raise ValueError(f"no host in {url!r}")
    secure = parts.scheme == "https"
    port = parts.port or (443 if secure else 80)
    target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
    context = ssl_context if ssl_context is not None else (ssl.create_default_context() if secure else None)

    async def exchange() -> tuple[int, bytes]:
        reader, writer = await asyncio.open_connection(parts.hostname, port, ssl=context, server_hostname=parts.hostname if context else None)
        try:
            lines = [f"GET {target} HTTP/1.1", f"Host: {parts.netloc}", "Connection: close"]
            lines += [f"{k}: {v}" for k, v in (headers or {}).items()]
            writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
            await writer.drain()
            status_line = (await reader.readuntil(b"\n")).decode("latin-1").split(" ", 2)
            if len(status_line) < 2 or not status_line[0].startswith("HTTP/1."):
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
                    size = int((await reader.readuntil(b"\n")).split(b";")[0].strip() or b"0", 16)
                    if size == 0:
                        break
                    body += await reader.readexactly(size)
                    await reader.readuntil(b"\n")
            elif "content-length" in response_headers:
                body = await reader.readexactly(int(response_headers["content-length"]))
            else:
                body = await reader.read()
            return status, body
        finally:
            writer.close()

    return await asyncio.wait_for(exchange(), timeout)
