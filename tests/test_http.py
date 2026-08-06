"""Tests for the inbound HTTP (ASGI) binding."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import pytest
from benzene.core import message
from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint
from benzene.results import Result


@dataclass
class Greet:
    name: str = ""


@http_endpoint("GET", "/greet/{name}")
@message("say:hello", request_type=Greet)
async def greet(request: Greet) -> Result:
    return Result.ok({"greeting": f"Hello {request.name}"})


@http_endpoint("POST", "/orders")
@message("order:create")
async def create_order(request: dict) -> Result:
    return Result.created({"id": request["id"]})


@http_endpoint("GET", "/boom")
@message("boom")
async def boom(_request: dict) -> Result:
    raise RuntimeError("kaboom")


def build_app() -> BenzeneHttpApp:
    router = HttpRouter().add(greet).add(create_order).add(boom)
    return BenzeneHttpApp(router)


# --- routing --------------------------------------------------------------------------------


def test_router_matches_path_param() -> None:
    router = HttpRouter().add(greet)
    match = router.match("GET", "/greet/world")
    assert match is not None
    endpoint, params = match
    assert endpoint.topic == "say:hello"
    assert params == {"name": "world"}


def test_router_method_is_case_insensitive_but_distinct() -> None:
    router = HttpRouter().add(create_order)
    assert router.match("post", "/orders") is not None
    assert router.match("GET", "/orders") is None  # wrong method, no match


def test_router_no_match_returns_none() -> None:
    router = HttpRouter().add(greet)
    assert router.match("GET", "/nope") is None


def test_router_rejects_untagged_handler() -> None:
    async def plain(_req):  # noqa: ANN001
        return Result.ok()

    with pytest.raises(ValueError):
        HttpRouter().add(plain)


# --- handle() -------------------------------------------------------------------------------


def test_get_with_path_param_maps_to_request_field() -> None:
    app = build_app()
    resp = asyncio.run(app.handle("GET", "/greet/benzene"))
    assert resp.status_code == 200
    assert json.loads(resp.body) == {"greeting": "Hello benzene"}
    assert resp.headers["content-type"] == "application/json"


def test_post_body_reaches_handler_and_created_maps_to_201() -> None:
    app = build_app()
    resp = asyncio.run(app.handle("POST", "/orders", body='{"id": "abc"}'))
    assert resp.status_code == 201
    assert json.loads(resp.body) == {"id": "abc"}


def test_unmatched_route_is_404_not_found() -> None:
    app = build_app()
    resp = asyncio.run(app.handle("GET", "/missing"))
    assert resp.status_code == 404
    assert json.loads(resp.body)["status"] == "not-found"


def test_invalid_json_body_is_400_bad_request() -> None:
    app = build_app()
    resp = asyncio.run(app.handle("POST", "/orders", body="{not json"))
    assert resp.status_code == 400
    assert json.loads(resp.body)["status"] == "bad-request"


def test_handler_exception_maps_to_503() -> None:
    app = build_app()
    resp = asyncio.run(app.handle("GET", "/boom"))
    assert resp.status_code == 503
    assert json.loads(resp.body)["status"] == "service-unavailable"


def test_query_string_merges_into_request() -> None:
    router = HttpRouter().register("GET", "/echo", "echo", _echo)
    app = BenzeneHttpApp(router)
    resp = asyncio.run(app.handle("GET", "/echo", query_string="a=1&b=two"))
    assert json.loads(resp.body) == {"a": "1", "b": "two"}


def test_path_param_wins_over_body() -> None:
    router = HttpRouter().register("PUT", "/echo/{name}", "echo", _echo)
    app = BenzeneHttpApp(router)
    resp = asyncio.run(app.handle("PUT", "/echo/frompath", body='{"name": "frombody"}'))
    assert json.loads(resp.body)["name"] == "frompath"


async def _echo(request: dict) -> Result:
    return Result.ok(request)


# --- ASGI round-trip ------------------------------------------------------------------------


def test_asgi_call_round_trip() -> None:
    app = build_app()

    async def run() -> tuple[dict, bytes]:
        sent: list[dict] = []

        async def receive() -> dict:
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message_: dict) -> None:
            sent.append(message_)

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/greet/asgi",
            "query_string": b"",
            "headers": [(b"accept", b"application/json")],
        }
        await app(scope, receive, send)
        start = next(m for m in sent if m["type"] == "http.response.start")
        body = next(m for m in sent if m["type"] == "http.response.body")["body"]
        return start, body

    start, body = asyncio.run(run())
    assert start["status"] == 200
    assert json.loads(body) == {"greeting": "Hello asgi"}


def test_asgi_post_reads_streamed_body() -> None:
    app = build_app()

    async def run() -> dict:
        sent: list[dict] = []
        chunks = [
            {"type": "http.request", "body": b'{"id":', "more_body": True},
            {"type": "http.request", "body": b' "streamed"}', "more_body": False},
        ]

        async def receive() -> dict:
            return chunks.pop(0)

        async def send(message_: dict) -> None:
            sent.append(message_)

        scope = {
            "type": "http",
            "method": "POST",
            "path": "/orders",
            "query_string": b"",
            "headers": [],
        }
        await app(scope, receive, send)
        return next(m for m in sent if m["type"] == "http.response.start")

    start = asyncio.run(run())
    assert start["status"] == 201
