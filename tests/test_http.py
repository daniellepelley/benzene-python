"""Tests for the inbound HTTP (ASGI) binding."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import pytest
from benzene.core import DuplicateHandlerError, message
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


@http_endpoint("PUT", "/orders/{id}")
@message("order:update")
async def update_order(request: dict) -> Result:
    return Result.updated({"id": request["id"], "echo": "payload that must not cross the wire"})


@http_endpoint("GET", "/boom")
@message("boom")
async def boom(_request: dict) -> Result:
    raise RuntimeError("kaboom")


def build_app() -> BenzeneHttpApp:
    router = HttpRouter().add(greet).add(create_order).add(update_order).add(boom)
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


# --- 204/304 carry no body on the wire (C7) -------------------------------------------------


def _drive_asgi(app: BenzeneHttpApp, scope: dict, body: bytes = b"") -> tuple[dict, bytes]:
    """Run one request through the raw ASGI callable, returning (response.start, body bytes)."""

    async def run() -> tuple[dict, bytes]:
        sent: list[dict] = []

        async def receive() -> dict:
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message_: dict) -> None:
            sent.append(message_)

        await app(scope, receive, send)
        start = next(m for m in sent if m["type"] == "http.response.start")
        payload = next(m for m in sent if m["type"] == "http.response.body")["body"]
        return start, payload

    return asyncio.run(run())


def test_asgi_204_is_sent_with_an_empty_body() -> None:
    # RFC 9110: a 204 must not carry content — h11/uvicorn sever the connection if it does, so the
    # payload of Result.updated(...) is dropped at the HTTP hop (other transports still carry it).
    start, body = _drive_asgi(
        build_app(),
        {
            "type": "http",
            "method": "PUT",
            "path": "/orders/7",
            "query_string": b"",
            "headers": [],
        },
    )
    assert start["status"] == 204
    assert body == b""


def test_handle_drops_the_body_for_a_bodiless_code() -> None:
    # Normalising in handle() rather than only in the ASGI send path is what makes every other
    # host inherit the rule: the Lambda, Cloud Functions and Azure Functions adapters all return
    # this HttpResponse's body straight to their platform without inspecting the status code.
    response = asyncio.run(build_app().handle("PUT", "/orders/7"))

    assert response.status_code == 204
    assert response.body == ""


# --- ASGI lifespan (D10) ---------------------------------------------------------------------


def test_asgi_lifespan_scope_completes_startup_and_shutdown() -> None:
    # uvicorn --lifespan on opens a 'lifespan' scope before any request; the app must answer it.
    app = build_app()

    async def run() -> list[str]:
        incoming = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
        sent: list[dict] = []

        async def receive() -> dict:
            return incoming.pop(0)

        async def send(message_: dict) -> None:
            sent.append(message_)

        await app({"type": "lifespan"}, receive, send)
        return [m["type"] for m in sent]

    assert asyncio.run(run()) == ["lifespan.startup.complete", "lifespan.shutdown.complete"]


def test_asgi_still_rejects_other_scope_types() -> None:
    app = build_app()

    async def receive() -> dict:  # pragma: no cover - never reached
        return {}

    async def send(_message: dict) -> None:  # pragma: no cover - never reached
        return None

    with pytest.raises(ValueError, match="only handles 'http' scopes"):
        asyncio.run(app({"type": "websocket"}, receive, send))


# --- duplicate topic registration (D2) -------------------------------------------------------


async def _other_echo(request: dict) -> Result:
    return Result.ok({"other": True})


def test_a_second_route_binding_a_topic_to_a_different_handler_raises() -> None:
    router = HttpRouter().register("GET", "/a", "shared:topic", _echo)
    with pytest.raises(DuplicateHandlerError, match="different handler"):
        router.register("POST", "/b", "shared:topic", _other_echo)


def test_one_handler_may_own_several_routes() -> None:
    # The documented stacked-decorator case: same handler, same topic, two routes — still fine.
    @http_endpoint("GET", "/hello/{name}")
    @http_endpoint("GET", "/hi/{name}")
    @message("greet:twice", request_type=Greet)
    async def greet_twice(request: Greet) -> Result:
        return Result.ok({"greeting": f"Hi {request.name}"})

    app = BenzeneHttpApp(HttpRouter().add(greet_twice))
    for path in ("/hello/ada", "/hi/ada"):
        resp = asyncio.run(app.handle("GET", path))
        assert json.loads(resp.body) == {"greeting": "Hi ada"}


def test_registering_the_same_handler_twice_for_a_topic_is_allowed() -> None:
    router = HttpRouter().register("GET", "/a", "shared:topic", _echo)
    router.register("POST", "/b", "shared:topic", _echo)  # same function object, no rebinding
    assert len(router.endpoints()) == 2
    assert len(router.definitions()) == 1


# --- the sync test host teaches instead of leaking asyncio's error (D10) ---------------------


def test_send_http_from_an_async_test_raises_a_teaching_error() -> None:
    from benzene.http.testing import HttpTestHost

    host = HttpTestHost(build_app())

    async def inside_an_async_test() -> None:
        host.send_http("GET", "/greet/ada")

    with pytest.raises(RuntimeError, match=r"send_http\(\) is synchronous"):
        asyncio.run(inside_an_async_test())
