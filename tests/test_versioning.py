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
    NoCastPathError,
    Registry,
    SchemaCasters,
    casting_handler,
    highest_version,
    resolve_version,
)
from benzene.core.registry import _natural_key
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


def test_route_static_version_yields_to_any_caller_version_header() -> None:
    # A route pinned to a static version (no {version} segment) is only a default: the caller may
    # override it with ANY recognised version header, not just the canonical benzene-version.
    from benzene.http import BenzeneHttpApp, HttpRouter

    async def v1(_request: dict) -> Result:
        return Result.ok({"served": "v1"})

    async def v2(_request: dict) -> Result:
        return Result.ok({"served": "v2"})

    registry = Registry()
    registry.register("api:echo", v1, version="v1")
    registry.register("api:echo", v2, version="v2")
    router = HttpRouter()
    router.register("GET", "/echo", "api:echo", v1, version="v1")  # static default v1
    app = BenzeneHttpApp(router, application=BenzeneMessageApplication(registry))

    # No header -> the route's static default handler.
    assert json.loads(asyncio.run(app.handle("GET", "/echo")).body) == {"served": "v1"}
    # A fallback header (x-version, not the canonical one) still overrides the static default.
    overridden = asyncio.run(app.handle("GET", "/echo", headers={"x-version": "v2"}))
    assert json.loads(overridden.body) == {"served": "v2"}


# --- opt-in highest-version selector (versioning.md §3) ------------------------------------------

def _selector_app(selector):
    async def v1(_request: dict) -> Result:
        return Result.ok({"served": "v1"})

    async def v2(_request: dict) -> Result:
        return Result.ok({"served": "v2"})

    async def v10(_request: dict) -> Result:
        return Result.ok({"served": "v10"})

    registry = Registry()
    registry.register("api:thing", v1, version="v1")
    registry.register("api:thing", v2, version="v2")
    registry.register("api:thing", v10, version="v10")
    return BenzeneMessageApplication(registry, version_selector=selector)


def _served(app, version: str) -> dict:
    headers = {"version": version} if version else {}
    response = asyncio.run(app.handle({"topic": "api:thing", "headers": headers, "body": "{}"}))
    return json.loads(response["body"]) if response["body"] else {"statusCode": response["statusCode"]}


def test_highest_version_falls_back_to_the_latest_when_no_exact_match() -> None:
    app = _selector_app(highest_version)
    # v10 is the natural-highest (not v2 by string order), used when the requested version is absent…
    assert _served(app, "") == {"served": "v10"}
    # …or unknown.
    assert _served(app, "v9") == {"served": "v10"}


def test_highest_version_still_prefers_an_exact_match() -> None:
    app = _selector_app(highest_version)
    assert _served(app, "v1") == {"served": "v1"}
    assert _served(app, "v2") == {"served": "v2"}


def test_default_selector_is_exact_match_only() -> None:
    app = _selector_app(None)  # default: exact match, no fallback
    response = asyncio.run(
        app.handle({"topic": "api:thing", "headers": {"version": "v9"}, "body": "{}"})
    )
    assert response["statusCode"] == "not-found"


def test_natural_key_orders_versions_numerically() -> None:
    assert _natural_key("v2") < _natural_key("v10")     # not string order ("v10" < "v2")
    assert _natural_key("1.9") < _natural_key("1.10")
    assert sorted(["v10", "v1", "v2"], key=_natural_key) == ["v1", "v2", "v10"]


# --- transparent casting (versioning.md §4): register the casts, not the forwarders --------------
# The §3.1 pattern above hand-writes a forwarding handler per version. Transparent casting registers
# the one-step casts *between types* once; `casting_handler` builds the forwarder, upcasting the
# request in and downcasting the response out. Casts compose (BFS), so a new version only needs a
# cast from the one before it.


@dataclass
class OrderPlacedV2:
    id: str
    quantity: int


@dataclass
class OrderPlacedV1:
    id: str  # v1's response never carried the quantity


@dataclass
class PlaceOrderV0:
    sku: str  # v0 had no count at all; it defaults to 1 on the way up to v1


def _order_casters() -> SchemaCasters:

    return (
        SchemaCasters()
        # request upcasts (older -> canonical)
        .cast_between(PlaceOrderV0, PlaceOrderV1, lambda v0: PlaceOrderV1(sku=v0.sku, count=1))
        .cast_between(PlaceOrderV1, PlaceOrderV2, lambda v1: PlaceOrderV2(sku=v1.sku, quantity=v1.count))
        # response downcast (canonical -> older)
        .cast_between(OrderPlacedV2, OrderPlacedV1, lambda v2: OrderPlacedV1(id=v2.id))
    )


def test_schema_casters_apply_a_direct_cast() -> None:
    casters = _order_casters()
    up = casters.cast(PlaceOrderV1(sku="A", count=3), PlaceOrderV2)
    assert up == PlaceOrderV2(sku="A", quantity=3)


def test_schema_casters_chain_multiple_steps_by_bfs() -> None:
    # No direct V0 -> V2 cast is registered; it is found via V0 -> V1 -> V2.
    casters = _order_casters()
    assert casters.cast(PlaceOrderV0(sku="A"), PlaceOrderV2) == PlaceOrderV2(sku="A", quantity=1)


def test_schema_casters_prefer_a_direct_cast_over_a_chain() -> None:

    calls: list[str] = []
    casters = (
        SchemaCasters()
        .cast_between(PlaceOrderV0, PlaceOrderV1, lambda v0: (calls.append("chain"), PlaceOrderV1(v0.sku))[1])
        .cast_between(PlaceOrderV1, PlaceOrderV2, lambda v1: PlaceOrderV2(v1.sku, v1.count))
        .cast_between(PlaceOrderV0, PlaceOrderV2, lambda v0: (calls.append("direct"), PlaceOrderV2(v0.sku))[1])
    )
    casters.cast(PlaceOrderV0(sku="A"), PlaceOrderV2)
    assert calls == ["direct"]  # the one-step edge wins over the two-step path


def test_schema_casters_return_a_matching_value_untouched() -> None:
    casters = _order_casters()
    already = PlaceOrderV2(sku="A", quantity=9)
    assert casters.cast(already, PlaceOrderV2) is already  # no cast needed, no copy


def test_schema_casters_raise_a_clear_error_with_no_path() -> None:

    casters = SchemaCasters()  # nothing registered
    with pytest.raises(NoCastPathError, match="No cast path from PlaceOrderV1 to PlaceOrderV2"):
        casters.cast(PlaceOrderV1(sku="A"), PlaceOrderV2)


def _transparent_orders_app() -> BenzeneMessageApplication:

    async def place_v2(request: PlaceOrderV2) -> Result:
        return Result.created(OrderPlacedV2(id=f"ord-{request.sku}", quantity=request.quantity))

    casters = _order_casters()
    registry = Registry()
    registry.register("orders:place", place_v2, version="v2",
                      request_type=PlaceOrderV2, response_type=OrderPlacedV2)
    # v1 and v0 are served transparently — one line each, no hand-written forwarder.
    registry.register(
        "orders:place",
        casting_handler(place_v2, casters, to=PlaceOrderV2, response_to=OrderPlacedV1),
        version="v1", request_type=PlaceOrderV1, response_type=OrderPlacedV1,
    )
    registry.register(
        "orders:place",
        casting_handler(place_v2, casters, to=PlaceOrderV2, response_to=OrderPlacedV1),
        version="v0", request_type=PlaceOrderV0, response_type=OrderPlacedV1,
    )
    return BenzeneMessageApplication(registry)


def _transparent_place(headers: dict[str, str], body: str) -> dict:
    response = asyncio.run(
        _transparent_orders_app().handle({"topic": "orders:place", "headers": headers, "body": body})
    )
    return {"status": response["statusCode"], "body": json.loads(response["body"]) if response["body"] else None}


def test_casting_handler_upcasts_request_and_downcasts_response() -> None:
    # A v1 caller sends `count`; the handler sees v2 (`quantity`) and returns OrderPlacedV2, which is
    # downcast to OrderPlacedV1 (no `quantity`) on the way out.
    out = _transparent_place({"version": "v1"}, '{"sku": "A", "count": 3}')
    assert out == {"status": "created", "body": {"id": "ord-A"}}  # downcast dropped `quantity`


def test_casting_handler_composes_a_two_step_upcast() -> None:
    # v0 -> v1 -> v2 upcast is found by chaining; the handler still only knows v2.
    out = _transparent_place({"version": "v0"}, '{"sku": "Z"}')
    assert out == {"status": "created", "body": {"id": "ord-Z"}}


def test_canonical_version_is_untouched_by_casting() -> None:
    # The v2 handler keeps its native response type (quantity present).
    out = _transparent_place({"benzene-version": "v2"}, '{"sku": "A", "quantity": 5}')
    assert out == {"status": "created", "body": {"id": "ord-A", "quantity": 5}}


def test_casting_handler_passes_a_failure_through_unchanged() -> None:

    async def rejects(_request: PlaceOrderV2) -> Result:
        return Result.bad_request("nope")

    # No response cast is even consulted for a failure (there is no payload to downcast).
    handler = casting_handler(
        rejects,
        SchemaCasters().cast_between(PlaceOrderV1, PlaceOrderV2, lambda v1: PlaceOrderV2(v1.sku, v1.count)),
        to=PlaceOrderV2,
        response_to=OrderPlacedV1,
    )
    result = asyncio.run(handler(PlaceOrderV1(sku="A", count=1)))
    assert result.status == "bad-request" and result.errors == ("nope",)


def test_casting_handler_does_not_downcast_a_failure_payload() -> None:

    # A failure that happens to carry a payload of an uncastable type: since encode_response never
    # serialises a failure payload, casting_handler must leave it alone rather than raise NoCastPathError.
    async def rejects(_request: PlaceOrderV2) -> Result:
        return Result(status="bad-request", payload={"unmapped": True}, errors=("nope",))

    handler = casting_handler(
        rejects,
        SchemaCasters().cast_between(PlaceOrderV1, PlaceOrderV2, lambda v1: PlaceOrderV2(v1.sku, v1.count)),
        to=PlaceOrderV2,
        response_to=OrderPlacedV1,  # would have no path from {"unmapped": True} -> OrderPlacedV1
    )
    result = asyncio.run(handler(PlaceOrderV1(sku="A", count=1)))
    assert result.status == "bad-request" and result.payload == {"unmapped": True}  # untouched


# --- the response echoes the resolved version (wire-contracts §2.1, versioning.md §4.2) -----------
# A response carries the version it was served/downcast to in the outbound `benzene-version` header,
# so a consumer knows which schema the body is — but only when the request declared a version, so
# unversioned traffic (and every conformance case) is byte-for-byte unchanged.


def test_response_echoes_the_declared_version() -> None:
    response = asyncio.run(
        _transparent_orders_app().handle(
            {"topic": "orders:place", "headers": {"version": "v1"}, "body": '{"sku": "A", "count": 1}'}
        )
    )
    # Even though the caller used the `version` fallback header, the reply writes the canonical one.
    assert response["headers"]["benzene-version"] == "v1"


def test_unversioned_response_has_no_version_header() -> None:
    async def handler(_request: dict) -> Result:
        return Result.ok({"ok": True})

    app = BenzeneMessageApplication(Registry().register("api:thing", handler))
    response = asyncio.run(app.handle({"topic": "api:thing", "headers": {}, "body": "{}"}))
    assert "benzene-version" not in response["headers"]  # nothing declared -> nothing echoed


# --- a version miss says so (audit D5) -----------------------------------------------------------
# A wrongly-versioned message for a registered topic used to answer "No handler found for topic X",
# which points the developer at a topic that *is* registered and hides the version dimension.


def _not_found_detail(app: BenzeneMessageApplication, topic: str, headers: dict[str, str]) -> str:
    response = asyncio.run(app.handle({"topic": topic, "headers": headers, "body": "{}"}))
    assert response["statusCode"] == "not-found"
    return json.loads(response["body"])["detail"]


def test_unknown_version_detail_lists_the_registered_versions() -> None:
    detail = _not_found_detail(_selector_app(None), "api:thing", {"version": "v9"})
    assert "api:thing" in detail and "'v9'" in detail
    assert "v1" in detail and "v2" in detail and "v10" in detail  # every registered version named
    assert "benzene-version" in detail                             # …and how to send the version


def test_unversioned_message_to_a_versioned_topic_says_unversioned() -> None:
    detail = _not_found_detail(_selector_app(None), "api:thing", {})  # no version header at all
    assert "(unversioned)" in detail
    assert "v1" in detail and "v10" in detail


def test_unregistered_topic_keeps_the_plain_message() -> None:
    detail = _not_found_detail(_selector_app(None), "api:missing", {"version": "v9"})
    assert detail.startswith("No handler found for topic api:missing")
    assert "no handlers registered for this topic" in detail
    assert "benzene-version" not in detail  # nothing version-shaped to say about an unknown topic


def test_the_richer_message_is_built_only_after_the_selector_misses() -> None:
    # highest_version resolves v9 to the latest handler, so no not-found message is built at all.
    app = _selector_app(highest_version)
    response = asyncio.run(
        app.handle({"topic": "api:thing", "headers": {"version": "v9"}, "body": "{}"})
    )
    assert response["statusCode"] == "ok" and json.loads(response["body"]) == {"served": "v10"}
