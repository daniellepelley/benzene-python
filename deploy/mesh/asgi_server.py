"""A tiny asyncio ASGI/HTTP-1.1 server — just enough to run the mesh stack over *real* localhost sockets.

The Python port ships no ASGI server (no uvicorn/hypercorn in this environment), yet the whole point of
this demo is that the fleet talks to the Mesh Host over genuine HTTP between separate ASGI apps — feeds
POSTed, spec/health fetched, the UI fetched by a real browser. This module is the minimum that makes
that real: a stdlib-only ``asyncio.start_server`` loop that parses an HTTP/1.1 request, drives an ASGI
``app(scope, receive, send)``, and writes the response back.

It is deliberately small and demo-grade (one request per connection — ``Connection: close`` — computed
``Content-Length``, no chunked request bodies, no TLS). That is all four apps here need, and all a
headless Chromium needs to load a same-origin page and ``fetch`` its JSON artifacts. It is **not** a
production server; a real deployment runs these ASGI apps under uvicorn/hypercorn.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote


@dataclass
class RunningServer:
    """A started server: its bound ``port`` and an async :meth:`close`."""

    server: asyncio.base_events.Server
    host: str
    port: int

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    async def close(self) -> None:
        self.server.close()
        await self.server.wait_closed()


async def serve(app: Any, *, host: str = "127.0.0.1", port: int = 0) -> RunningServer:
    """Start ``app`` (an ASGI callable) on ``host:port`` (0 → an ephemeral port). Returns the handle."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await _handle_connection(app, reader, writer)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass  # a client that hung up mid-request is not our problem
        finally:
            with _suppress_close():
                writer.close()

    server = await asyncio.start_server(handle, host, port)
    bound_port = server.sockets[0].getsockname()[1]
    return RunningServer(server=server, host=host, port=bound_port)


class _suppress_close:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *_exc: object) -> bool:
        return True  # swallow any error from closing an already-broken writer


async def _handle_connection(
    app: Any, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    request_line = await reader.readline()
    if not request_line:
        return
    try:
        method, raw_target, _version = request_line.decode("latin-1").rstrip("\r\n").split(" ", 2)
    except ValueError:
        await _write_simple(writer, 400, b"Bad Request")
        return

    headers: list[tuple[bytes, bytes]] = []
    content_length = 0
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        name, _, value = line.decode("latin-1").rstrip("\r\n").partition(":")
        name = name.strip().lower()
        value = value.strip()
        headers.append((name.encode("latin-1"), value.encode("latin-1")))
        if name == "content-length":
            content_length = int(value or "0")

    body = await reader.readexactly(content_length) if content_length else b""

    path, _, query = raw_target.partition("?")
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method.upper(),
        "path": unquote(path),
        "raw_path": path.encode("latin-1"),
        "query_string": query.encode("latin-1"),
        "headers": headers,
        "scheme": "http",
    }

    body_sent = {"done": False}

    async def receive() -> dict:
        if body_sent["done"]:
            return {"type": "http.disconnect"}
        body_sent["done"] = True
        return {"type": "http.request", "body": body, "more_body": False}

    status_holder: dict[str, Any] = {"status": 500, "headers": []}
    chunks: list[bytes] = []

    async def send(message: dict) -> None:
        if message["type"] == "http.response.start":
            status_holder["status"] = message["status"]
            status_holder["headers"] = message.get("headers", [])
        elif message["type"] == "http.response.body":
            chunks.append(message.get("body", b"") or b"")

    try:
        await app(scope, receive, send)
    except Exception:  # noqa: BLE001 - a handler error becomes a 500, never a dropped connection
        await _write_simple(writer, 500, b"Internal Server Error")
        return

    payload = b"".join(chunks)
    # Own Content-Length + Connection ourselves (drop any the app set, to avoid a mismatch).
    out_headers = [
        (name, value)
        for name, value in status_holder["headers"]
        if name.lower() not in (b"content-length", b"connection")
    ]
    out_headers.append((b"content-length", str(len(payload)).encode("latin-1")))
    out_headers.append((b"connection", b"close"))
    await _write_response(writer, status_holder["status"], out_headers, payload, head=method.upper() == "HEAD")


_REASONS = {
    200: "OK", 201: "Created", 400: "Bad Request", 404: "Not Found",
    405: "Method Not Allowed", 500: "Internal Server Error", 503: "Service Unavailable",
}


async def _write_response(
    writer: asyncio.StreamWriter,
    status: int,
    headers: list[tuple[bytes, bytes]],
    body: bytes,
    *,
    head: bool = False,
) -> None:
    reason = _REASONS.get(status, "OK")
    lines = [f"HTTP/1.1 {status} {reason}".encode("latin-1")]
    lines.extend(name + b": " + value for name, value in headers)
    head_bytes = b"\r\n".join(lines) + b"\r\n\r\n"
    writer.write(head_bytes if head else head_bytes + body)
    await writer.drain()


async def _write_simple(writer: asyncio.StreamWriter, status: int, body: bytes) -> None:
    await _write_response(
        writer,
        status,
        [(b"content-type", b"text/plain; charset=utf-8"), (b"content-length", str(len(body)).encode())],
        body,
    )
