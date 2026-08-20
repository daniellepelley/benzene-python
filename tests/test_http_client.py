"""The outbound HTTP client — mapping, URL resolution, and an inbound↔outbound round-trip."""

from __future__ import annotations

import asyncio
import json

import pytest
from benzene.core import MessageSender, message
from benzene.http import BenzeneHttpApp, HttpMessageSender, HttpReply, HttpRouter, http_endpoint
from benzene.results import Result


def _sender(reply: HttpReply, captured: dict | None = None) -> HttpMessageSender:
    async def transport(url: str, headers: dict, body: str) -> HttpReply:
        if captured is not None:
            captured.update(url=url, headers=headers, body=body)
        return reply

    return HttpMessageSender("https://svc.example", transport=transport)


def test_it_is_a_message_sender() -> None:
    assert isinstance(_sender(HttpReply(200)), MessageSender)  # structurally satisfies the port


def test_success_response_maps_to_a_result() -> None:
    result = asyncio.run(_sender(HttpReply(201, '{"id": "o1"}')).send_message("orders:place", {"sku": "A"}))
    assert result.status == "created"
    assert result.payload == {"id": "o1"}


def test_failure_response_maps_status_and_detail() -> None:
    reply = HttpReply(404, '{"status": "not-found", "detail": "no such order"}')
    result = asyncio.run(_sender(reply).send_message("orders:get", {"id": "x"}))
    assert result.status == "not-found"
    assert result.errors == ("no such order",)


def test_it_forwards_headers_and_the_topic() -> None:
    captured: dict = {}
    asyncio.run(
        _sender(HttpReply(200), captured).send_message(
            "orders:place", {"sku": "A"}, headers={"x-correlation-id": "c1"}
        )
    )
    assert captured["url"] == "https://svc.example/orders:place"  # base + topic
    assert captured["headers"]["x-correlation-id"] == "c1"
    assert captured["headers"]["topic"] == "orders:place"          # topic travels as metadata
    assert json.loads(captured["body"]) == {"sku": "A"}


@pytest.mark.parametrize(
    "url_for,expected",
    [
        ("https://svc/", "https://svc/orders:place"),
        ({"orders:place": "https://svc/orders"}, "https://svc/orders"),
        (lambda t: f"https://svc/v1/{t}", "https://svc/v1/orders:place"),
    ],
)
def test_url_resolution(url_for, expected) -> None:
    captured: dict = {}

    async def transport(url, headers, body):
        captured["url"] = url
        return HttpReply(200)

    sender = HttpMessageSender(url_for, transport=transport)
    asyncio.run(sender.send_message("orders:place", {}))
    assert captured["url"] == expected


def test_url_map_missing_topic_raises_a_clear_error() -> None:
    sender = HttpMessageSender({"orders:place": "https://svc/orders"}, transport=_never)
    with pytest.raises(KeyError, match="No URL configured for topic 'orders:get'"):
        asyncio.run(sender.send_message("orders:get", {}))


def test_non_json_success_body_passes_through_as_text() -> None:
    # A 2xx whose body is not JSON becomes the payload verbatim (the _parse fallback).
    result = asyncio.run(_sender(HttpReply(200, "pong")).send_message("ping", {}))
    assert result.status == "ok"
    assert result.payload == "pong"


async def _never(url: str, headers: dict, body: str) -> HttpReply:  # transport that must not be called
    raise AssertionError("transport should not be reached")


def test_stdlib_transport_posts_and_maps_status_over_a_real_socket() -> None:
    # Exercise the zero-dependency default transport against a real local HTTP server (no mocking):
    # a 2xx round-trips the body, and a 4xx comes back as a mapped HttpReply, not an exception.
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    from benzene.http import stdlib_transport

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib callback name
            payload = self.rfile.read(int(self.headers.get("content-length", 0)))
            if self.path == "/ok":
                self.send_response(201)
                self.end_headers()
                self.wfile.write(b'{"echo": ' + payload + b"}")
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b'{"status": "not-found", "detail": "nope"}')

        def log_message(self, *_args) -> None:  # silence the default stderr logging
            pass

    server = HTTPServer(("localhost", 0), Handler)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        transport = stdlib_transport()
        ok = asyncio.run(transport(f"http://localhost:{port}/ok", {"content-type": "application/json"}, "5"))
        assert ok.status_code == 201
        assert json.loads(ok.body) == {"echo": 5}
        missing = asyncio.run(transport(f"http://localhost:{port}/missing", {}, "{}"))
        assert missing.status_code == 404  # HTTPError mapped to a reply, not raised
        assert json.loads(missing.body)["detail"] == "nope"
    finally:
        server.shutdown()


def test_round_trip_through_the_real_inbound_binding() -> None:
    # Dogfood: the outbound sender POSTs into a real BenzeneHttpApp; only the transport is faked.
    @http_endpoint("POST", "/orders")
    @message("orders:place")
    async def place(request: dict) -> Result:
        return Result.created({"sku": request["sku"], "accepted": True})

    inbound = BenzeneHttpApp(HttpRouter().add(place))

    async def transport(url: str, headers: dict, body: str) -> HttpReply:
        response = await inbound.handle("POST", url, headers=headers, body=body)  # url is the path
        return HttpReply(response.status_code, response.body)

    sender = HttpMessageSender({"orders:place": "/orders"}, transport=transport)
    result = asyncio.run(sender.send_message("orders:place", {"sku": "ABC"}))
    assert result.status == "created"                       # 201 mapped back through from_http
    assert result.payload == {"sku": "ABC", "accepted": True}


# --- the sender never raises (C5) -----------------------------------------------------------


def _raising_sender(exc: BaseException) -> HttpMessageSender:
    async def transport(url: str, headers: dict, body: str) -> HttpReply:
        raise exc

    return HttpMessageSender("https://svc.example", transport=transport)


def test_connection_failure_becomes_a_service_unavailable_result() -> None:
    # urllib raises URLError for a refused connection / DNS failure — the sender port promises a
    # Result, never an exception, so RetryingMessageSender (which retries on statuses) can see it.
    import urllib.error

    result = asyncio.run(
        _raising_sender(urllib.error.URLError("refused")).send_message("orders:place", {})
    )
    assert result.status == "service-unavailable"
    assert "refused" in " ".join(result.errors)


def test_any_transport_exception_becomes_a_service_unavailable_result() -> None:
    result = asyncio.run(_raising_sender(RuntimeError("socket exploded")).send_message("t", {}))
    assert result.status == "service-unavailable"
    assert "socket exploded" in " ".join(result.errors)


def test_transport_timeout_becomes_a_timeout_result() -> None:
    result = asyncio.run(_raising_sender(TimeoutError("timed out")).send_message("t", {}))
    assert result.status == "timeout"


def test_a_transports_missing_sdk_still_raises_importerror() -> None:
    # An httpx-backed transport whose package is missing is a deployment error to fix, not a
    # transport blip to retry — the same rule every sender follows for a missing SDK.
    with pytest.raises(ImportError):
        asyncio.run(_raising_sender(ImportError("No module named 'httpx'")).send_message("t", {}))
