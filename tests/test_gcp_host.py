"""The GCP host's deployment entry points, inbound decode, and duck-typed request extraction.

`http_function(app)` / `pubsub_function(app)` are the plain callables the Functions Framework invokes;
the request/event extraction is duck-typed so any Flask-/CloudEvent-shaped object works. These drive
the real deployment code paths (not the in-memory test host) with minimal stand-ins.
`decode_pubsub_message` — the Pub/Sub half of the wire contract those entry points sit on — is pinned
directly here, the same shape as the Kafka/SQS decode tests.
"""

from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass, field

import pytest

pytest.importorskip("benzene.gcp")

from benzene.core import Registry, message
from benzene.gcp import (
    GcpFunctionsApp,
    decode_pubsub_message,
    http_function,
    pubsub_function,
)
from benzene.gcp.testing import GcpFunctionsTestHost
from benzene.http import HttpRouter, http_endpoint
from benzene.results import Result


@http_endpoint("POST", "/echo")
@message("say:echo")
async def echo(request: dict) -> Result:
    return Result.ok({"saw": request.get("name"), "q": request.get("q")})


@dataclass
class _Flaskish:
    """A minimal Flask-style request whose body is on `.data` (no get_data) and query is bytes."""

    method: str = "POST"
    path: str = "/echo"
    query_string: bytes = b"q=1"
    data: bytes = b'{"name": "world"}'
    headers: dict = field(default_factory=dict)


def _app() -> GcpFunctionsApp:
    return GcpFunctionsApp(http_router=HttpRouter().add(echo))


def test_http_function_entry_point_extracts_a_flask_style_request() -> None:
    entry = http_function(_app())  # the callable the Functions Framework calls: entry(request)
    body, status, headers = entry(_Flaskish())
    assert status == 200
    # bytes query_string is decoded, the .data body is read, both reach the handler
    assert json.loads(body) == {"saw": "world", "q": "1"}
    assert headers["content-type"] == "application/json"


def test_app_without_a_router_reports_not_implemented() -> None:
    # A message-only app (no HTTP router) still answers HTTP with a clean 501, not a crash.
    body, status, _ = GcpFunctionsApp(registry=Registry().register("t", echo)).handle_http(_Flaskish())
    assert status == 501
    assert json.loads(body)["status"] == "not-implemented"


def test_pubsub_function_accepts_a_bare_message_dict() -> None:
    seen: list[str] = []

    async def on_created(request: dict) -> Result:
        seen.append(request["id"])
        return Result.ok()

    app = GcpFunctionsApp(registry=Registry().register("orders:created", on_created))
    entry = pubsub_function(app)

    # A CloudEvent whose .data is a bare {"message": {...}} (no "subscription" wrapper).
    @dataclass
    class _Event:
        data: dict

    message_payload = {
        "data": base64.b64encode(b'{"id": "ord-9"}').decode("ascii"),
        "attributes": {"topic": "orders:created"},
    }
    entry(_Event(data={"message": message_payload}))
    assert seen == ["ord-9"]


# --- inbound decode ------------------------------------------------------------------------------


def test_decode_lifts_topic_from_the_attributes_and_base64_decodes_the_data() -> None:
    envelope = decode_pubsub_message(
        {
            "data": base64.b64encode(b'{"sku": "A"}').decode("ascii"),
            "attributes": {"topic": "orders:place", "x-correlation-id": "c1"},
        }
    )
    assert envelope["topic"] == "orders:place"
    assert envelope["headers"] == {"x-correlation-id": "c1"}  # topic removed, rest preserved
    assert json.loads(envelope["body"]) == {"sku": "A"}


def test_decode_tolerates_absent_attributes_and_data() -> None:
    envelope = decode_pubsub_message({"messageId": "m1"})
    assert envelope == {"topic": "", "headers": {}, "body": ""}


def test_decode_passes_bytes_data_through_without_base64() -> None:
    # The Functions Framework hands `data` over already-decoded on some paths; bytes are the body.
    envelope = decode_pubsub_message({"data": b'{"sku": "A"}', "attributes": {"topic": "t"}})
    assert envelope["topic"] == "t"
    assert json.loads(envelope["body"]) == {"sku": "A"}


def test_decode_falls_back_to_the_raw_string_when_data_is_not_base64() -> None:
    envelope = decode_pubsub_message({"data": "plain text", "attributes": {}})
    assert envelope == {"topic": "", "headers": {}, "body": "plain text"}


# --- the sync test host, called from an async test (D10) -------------------------------------------


def test_a_sync_send_inside_a_running_loop_teaches_how_to_drive_the_app() -> None:
    host = GcpFunctionsTestHost(_app())

    async def an_async_test() -> None:
        host.send_http("POST", "/echo", {"name": "x"})

    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(an_async_test())
    detail = str(excinfo.value)
    assert "send_http() is synchronous" in detail
    assert "plain 'def' test" in detail
    assert "await host._app.handle(...)" in detail
