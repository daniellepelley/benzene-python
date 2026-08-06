"""The GCP host's deployment entry points and duck-typed request extraction.

`http_function(app)` / `pubsub_function(app)` are the plain callables the Functions Framework invokes;
the request/event extraction is duck-typed so any Flask-/CloudEvent-shaped object works. These drive
the real deployment code paths (not the in-memory test host) with minimal stand-ins.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

pytest.importorskip("benzene.gcp")

from benzene.core import Registry, message
from benzene.gcp import GcpFunctionsApp, http_function, pubsub_function
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

    import base64

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
