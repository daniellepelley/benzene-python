"""Tests for the inbound HTTP (ASGI) binding."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import pytest
from benzene.core import message
from benzene.http import BenzeneHttpApp, HttpEndpoint, HttpRouter, http_endpoint
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
    problem = json.loads(resp.body)
    # The Benzene status travels as benzeneStatus; `status` is RFC 9457's integer HTTP code
    # (wire-contracts.md 1.3, 4.1) and MUST equal the code actually sent.
    assert problem["benzeneStatus"] == "not-found"
    assert problem["status"] == 404
    assert problem["type"] == "https://benzene.app/problems/not-found"
    assert resp.headers["content-type"] == "application/problem+json"


def test_invalid_json_body_is_400_bad_request() -> None:
    app = build_app()
    resp = asyncio.run(app.handle("POST", "/orders", body="{not json"))
    assert resp.status_code == 400
    problem = json.loads(resp.body)
    assert problem["benzeneStatus"] == "bad-request"
    assert problem["status"] == 400
    assert resp.headers["content-type"] == "application/problem+json"


def test_handler_exception_maps_to_503() -> None:
    app = build_app()
    resp = asyncio.run(app.handle("GET", "/boom"))
    assert resp.status_code == 503
    problem = json.loads(resp.body)
    assert problem["benzeneStatus"] == "service-unavailable"
    assert problem["status"] == 503
    assert resp.headers["content-type"] == "application/problem+json"


def test_a_failure_carries_its_messages_in_errors_not_only_in_detail() -> None:
    # `errors` is authoritative and ordered (wire-contracts.md 1.3), which replaced the withdrawn
    # "split detail on ', '" rule - that was never safe, because messages contain commas.
    app = build_app()
    resp = asyncio.run(app.handle("GET", "/boom"))

    problem = json.loads(resp.body)
    assert problem["errors"], "a failed result's messages must be listed individually"
    assert all("message" in entry for entry in problem["errors"])
    # detail stays the compatibility member: the same messages, joined, for a reader that only
    # knows the old shape.
    assert problem["detail"] == ", ".join(entry["message"] for entry in problem["errors"])


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


def test_route_literals_are_matched_literally_not_as_regex() -> None:
    # A regex metacharacter in a route literal must match literally, not as a pattern.
    ep = HttpEndpoint("GET", "/users/me.json", "users:me")
    assert ep.match("GET", "/users/me.json") == {}
    assert ep.match("GET", "/users/meXjson") is None  # the '.' must not match an arbitrary char
    # Placeholders still capture a single segment.
    assert HttpEndpoint("GET", "/orders/{id}", "orders").match("GET", "/orders/7") == {"id": "7"}


def test_asgi_non_utf8_body_yields_400_not_a_crash() -> None:
    app = build_app()

    async def run() -> dict:
        sent: list[dict] = []

        async def receive() -> dict:
            return {"type": "http.request", "body": b"\xff\xfe not utf8", "more_body": False}

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

    # The host must never crash on request content: a non-UTF-8 body is a clean 400.
    start = asyncio.run(run())
    assert start["status"] == 400
