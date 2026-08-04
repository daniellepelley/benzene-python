"""Handler-version selection over the inbound version-header fallback list (versioning.md §2).

A message carries its version in one of an ordered list of headers — ``benzene-version`` (canonical),
then ``version``, then ``x-version`` — so a peer in any language reaches the right versioned handler.
Absent from all of them, the unversioned handler serves the request.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import pytest

from benzene.core import (
    VERSION_HEADER_NAMES,
    BenzeneMessageApplication,
    Handler,
    Registry,
    resolve_version,
)
from benzene.results import Result


def _app() -> BenzeneMessageApplication:
    async def v1(_request: dict) -> Result:
        return Result.ok({"handler": "v1"})

    async def v2(_request: dict) -> Result:
        return Result.ok({"handler": "v2"})

    registry = Registry()
    registry.register("orders:get", v1)  # unversioned (version "")
    registry.register("orders:get", v2, version="v2")
    return BenzeneMessageApplication(registry)


def _run(headers: dict[str, str]) -> str:
    response = asyncio.run(
        _app().handle({"topic": "orders:get", "headers": headers, "body": "{}"})
    )
    return json.loads(response["body"])["handler"]


def test_default_header_names_are_the_spec_list() -> None:
    assert VERSION_HEADER_NAMES == ("benzene-version", "version", "x-version")


@pytest.mark.parametrize("header", ["benzene-version", "version", "x-version"])
def test_any_fallback_header_selects_the_versioned_handler(header: str) -> None:
    assert _run({header: "v2"}) == "v2"


def test_absent_version_selects_the_unversioned_handler() -> None:
    assert _run({}) == "v1"


def test_canonical_header_wins_over_fallbacks() -> None:
    # benzene-version is first in the list, so it takes precedence over `version` / `x-version`.
    assert _run({"benzene-version": "v2", "version": "", "x-version": ""}) == "v2"


def test_resolve_version_is_order_sensitive() -> None:
    assert resolve_version({"x-version": "v2"}) == "v2"
    assert resolve_version({"version": "a", "x-version": "b"}) == "a"
    assert resolve_version({}) == ""


# --- the casting-handler pattern (versioning.md §3.1): serve multiple payload versions -----------
# One shared latest implementation; a thin forwarding handler per retired version upcasts the request
# and (here) returns the shared result. This needs NO framework code — just an extra registration.


@dataclass
class PlaceOrderV2:
    sku: str
    quantity: int = 1


@dataclass
class PlaceOrderV1:
    sku: str
    count: int = 1  # v1 called it `count`; v2 renamed it `quantity`


async def _place_v2(request: PlaceOrderV2) -> Result:
    return Result.created({"sku": request.sku, "quantity": request.quantity})


def _make_place_v1(latest: Handler) -> Handler:
    async def place_v1(request: PlaceOrderV1) -> Result:
        return await latest(PlaceOrderV2(sku=request.sku, quantity=request.count))  # upcast v1 -> v2

    return place_v1


def _versioned_orders_app() -> BenzeneMessageApplication:
    registry = Registry()
    registry.register("orders:place", _place_v2, version="v2", request_type=PlaceOrderV2)
    registry.register("orders:place", _make_place_v1(_place_v2), version="v1", request_type=PlaceOrderV1)
    return BenzeneMessageApplication(registry)


def _place(headers: dict[str, str], body: str) -> dict:
    response = asyncio.run(
        _versioned_orders_app().handle({"topic": "orders:place", "headers": headers, "body": body})
    )
    return json.loads(response["body"])


def test_casting_handler_upcasts_v1_to_the_latest_implementation() -> None:
    # A v1 client uses the old field name `count`; the forwarding handler upcasts to v2.
    v1 = _place({"version": "v1"}, '{"sku": "A", "count": 3}')
    v2 = _place({"benzene-version": "v2"}, '{"sku": "A", "quantity": 3}')
    assert v1 == v2 == {"sku": "A", "quantity": 3}  # both reach the one shared v2 implementation


def test_unknown_version_is_a_not_found() -> None:
    response = asyncio.run(
        _versioned_orders_app().handle(
            {"topic": "orders:place", "headers": {"version": "v9"}, "body": '{"sku": "A"}'}
        )
    )
    assert response["statusCode"] == "not-found"  # exact-match selection; no v9 handler registered


# --- HTTP `/{version}/` route segment drives selection (versioning.md §2) -------------------------

def _versioned_http_app():
    from benzene.http import BenzeneHttpApp, HttpRouter

    async def echo_v1(_request: dict) -> Result:
        return Result.ok({"served": "v1"})

    async def echo_v2(_request: dict) -> Result:
        return Result.ok({"served": "v2"})

    registry = Registry()
    registry.register("api:echo", echo_v1, version="v1")
    registry.register("api:echo", echo_v2, version="v2")

    router = HttpRouter()
    # One route with a {version} segment; the two handler versions live in the message registry.
    router.register("GET", "/{version}/echo", "api:echo", echo_v1)
    return BenzeneHttpApp(router, application=BenzeneMessageApplication(registry))


@pytest.mark.parametrize("segment,expected", [("v1", "v1"), ("v2", "v2")])
def test_http_version_route_segment_selects_the_handler(segment: str, expected: str) -> None:
    response = asyncio.run(_versioned_http_app().handle("GET", f"/{segment}/echo"))
    assert response.status_code == 200
    assert json.loads(response.body) == {"served": expected}


def test_http_version_segment_is_not_leaked_into_the_request() -> None:
    # `version` drives selection; it must not also arrive as a request field.
    captured: dict = {}

    async def echo(request: dict) -> Result:
        captured.update(request)
        return Result.ok({})

    from benzene.http import BenzeneHttpApp, HttpRouter

    registry = Registry()
    registry.register("api:echo", echo, version="v1")
    router = HttpRouter()
    router.register("GET", "/{version}/echo", "api:echo", echo)
    app = BenzeneHttpApp(router, application=BenzeneMessageApplication(registry))

    asyncio.run(app.handle("GET", "/v1/echo", query_string="q=1"))
    assert captured == {"q": "1"}  # the query param is present; `version` is not
