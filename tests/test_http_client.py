"""The outbound HTTP client — mapping, URL resolution, and an inbound↔outbound round-trip."""

from __future__ import annotations

import asyncio
import json

import pytest
from benzene.core import MessageSender, message
from benzene.http import BenzeneHttpApp, HttpMessageSender, HttpReply, HttpRouter, http_endpoint
from benzene.results import Result

from ._async import run


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
    assert result.messages == ("no such order",)


def test_failure_response_carries_the_peer_structured_errors() -> None:
    """errors is authoritative when present (section 1.3), and each entry arrives whole.

    This client used to read `detail` alone, so a peer that had gone to the trouble of saying which
    field failed and which rule rejected it had both dropped one hop later - by the same framework
    that had just taught its handlers to send them.
    """
    reply = HttpReply(
        422,
        json.dumps(
            {
                "benzeneStatus": "validation-error",
                "detail": "sku is required",
                "errors": [{"message": "sku is required", "field": "sku", "code": "missing"}],
            }
        ),
    )

    result = asyncio.run(_sender(reply).send_message("orders:place", {}))

    assert result.status == "validation-error"
    assert len(result.errors) == 1
    assert (result.errors[0].field, result.errors[0].code) == ("sku", "missing")


def test_failure_response_falls_back_to_detail_as_one_opaque_message() -> None:
    """And specifically does NOT split it on ", " - a rule RFC 9457's revision withdrew, because
    error messages contain commas."""
    reply = HttpReply(400, json.dumps({"benzeneStatus": "bad-request", "detail": "one, two"}))

    result = asyncio.run(_sender(reply).send_message("orders:place", {}))

    assert result.messages == ("one, two",)


@pytest.mark.parametrize(
    "reply,status,messages",
    [
        # A gateway's HTML error page, an empty body, a JSON body that is not a problem document:
        # wire-contracts 4.1 says a failure body never fails the call - the reverse HTTP table
        # still gives the status, with no errors to rebuild.
        (HttpReply(502, "<html><body>Bad Gateway</body></html>"), "service-unavailable", ()),
        (HttpReply(404, ""), "not-found", ()),
        (HttpReply(500, '{"message": "Internal server error"}'), "unexpected-error", ()),
        (HttpReply(409, '["not", "an", "object"]'), "conflict", ()),
        # An application/json failure body carrying benzeneStatus is read as a problem document
        # (the MAY of 4.1) - this client reads any JSON object's errors/detail, whatever its label.
        (
            HttpReply(409, '{"benzeneStatus": "conflict", "detail": "already exists"}'),
            "conflict",
            ("already exists",),
        ),
    ],
)
def test_a_failure_body_that_is_not_a_problem_document_never_fails_the_call(
    reply, status, messages
) -> None:
    result = asyncio.run(_sender(reply).send_message("orders:place", {}))
    assert result.status == status
    assert not result.is_successful
    assert result.messages == messages


@pytest.mark.parametrize(
    "reply,status",
    [
        # An application-defined failure answers 500 (wire-contracts 4.1's forward table); the
        # reverse table would read it back as unexpected-error. The document says what it was.
        (HttpReply(500, '{"benzeneStatus": "card-declined", "detail": "insufficient funds"}'), "card-declined"),
        # A vocabulary status the table collapses: 400 is bad-request, but the peer said
        # validation-error (or 503/502 both read service-unavailable, 504/408 both timeout...).
        (HttpReply(400, '{"benzeneStatus": "validation-error", "errors": [{"message": "m"}]}'), "validation-error"),
        (HttpReply(502, '{"benzeneStatus": "timeout", "detail": "upstream timed out"}'), "timeout"),
    ],
)
def test_a_problem_documents_benzene_status_is_the_result_status(reply, status) -> None:
    # wire-contracts 4.1 (spec 1.1): a client that reads a problem document SHOULD take the result's
    # status from its benzeneStatus rather than from the reverse table.
    result = asyncio.run(_sender(reply).send_message("payments:charge", {}))
    assert result.status == status
    assert not result.is_successful
    assert len(result.errors) == 1


@pytest.mark.parametrize(
    "body",
    [
        '{"benzeneStatus": "ok", "detail": "nonsense"}',  # a success status in a failure's body
        '{"benzeneStatus": "", "detail": "empty"}',
        '{"benzeneStatus": 422, "detail": "not a string"}',
        '{"detail": "no status stated"}',
    ],
)
def test_the_reverse_table_applies_when_the_document_states_no_failure_status(body) -> None:
    result = asyncio.run(_sender(HttpReply(404, body)).send_message("orders:get", {}))
    assert result.status == "not-found"
    assert not result.is_successful


def test_an_application_defined_failure_status_survives_the_real_http_hop() -> None:
    # End to end through the real inbound binding: the server answers 500 with the problem document,
    # and the client reads the status the handler actually returned.
    @http_endpoint("POST", "/payments")
    @message("payments:charge")
    async def charge(request: dict) -> Result:
        return Result.failure("card-declined", {"message": "insufficient funds", "code": "nsf"})

    app = BenzeneHttpApp(HttpRouter().add(charge))

    async def transport(url: str, headers: dict, body: str) -> HttpReply:
        response = await app.handle("POST", "/payments", headers=headers, body=body)
        assert response.status_code == 500
        return HttpReply(response.status_code, response.body)

    sender = HttpMessageSender("https://svc.example", transport=transport)
    result = asyncio.run(sender.send_message("payments:charge", {}))
    assert result.status == "card-declined"
    assert [error.to_payload() for error in result.errors] == [
        {"message": "insufficient funds", "code": "nsf"}
    ]


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
        ok = run(transport(f"http://localhost:{port}/ok", {"content-type": "application/json"}, "5"))
        assert ok.status_code == 201
        assert json.loads(ok.body) == {"echo": 5}
        missing = run(transport(f"http://localhost:{port}/missing", {}, "{}"))
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
