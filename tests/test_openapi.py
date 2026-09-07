"""The OpenAPI projection — an OpenAPI 3.1 document derived from the handler registry.

Registers a couple of handlers with dataclass request/response types, generates the document, and
asserts it is structurally valid (top-level keys, one well-formed operation per topic, resolvable
``$ref``s), that the embedded schemas match ``benzene.core.json_schema`` exactly, that success and
failure responses are present, and that the output is deterministic. Pure — no third-party deps.
"""

from __future__ import annotations

from dataclasses import dataclass

from benzene.core import Registry, json_schema
from benzene.http import StandardPaths, to_http
from benzene.openapi import OPENAPI_VERSION, openapi_document, operation_id
from benzene.results import FAILURE_STATUSES, Result


@dataclass
class PlaceOrder:
    sku: str
    quantity: int
    note: str = ""


@dataclass
class OrderPlaced:
    order_id: str


@dataclass
class ShipOrder:
    order_id: str


@dataclass
class OrderShipped:
    tracking: str


async def place(request: PlaceOrder) -> Result:
    return Result.ok(OrderPlaced("o-1"))


async def ship(request: ShipOrder) -> Result:
    return Result.ok(OrderShipped("t-1"))


def _registry() -> Registry:
    return (
        Registry()
        .register("orders:place", place, request_type=PlaceOrder, response_type=OrderPlaced)
        .register("orders:ship", ship, request_type=ShipOrder, response_type=OrderShipped)
    )


def test_top_level_structure() -> None:
    document = openapi_document(_registry(), title="Orders", version="2.1.0")

    assert document["openapi"] == OPENAPI_VERSION
    assert document["info"] == {"title": "Orders", "version": "2.1.0"}
    assert isinstance(document["paths"], dict)
    assert isinstance(document["components"], dict)
    assert isinstance(document["components"]["schemas"], dict)


def test_one_operation_per_topic_with_request_ref() -> None:
    document = openapi_document(_registry())

    assert set(document["paths"]) == {
        "/benzene/invoke/orders:place",
        "/benzene/invoke/orders:ship",
    }

    place_op = document["paths"]["/benzene/invoke/orders:place"]["post"]
    assert place_op["operationId"] == "ordersPlace" == operation_id("orders:place")
    body_ref = place_op["requestBody"]["content"]["application/json"]["schema"]["$ref"]
    assert body_ref == "#/components/schemas/OrdersPlaceRequest"
    assert place_op["requestBody"]["required"] is True


def test_component_schemas_match_core_json_schema() -> None:
    document = openapi_document(_registry())
    schemas = document["components"]["schemas"]

    assert schemas["OrdersPlaceRequest"] == json_schema(PlaceOrder)
    assert schemas["OrdersPlaceResponse"] == json_schema(OrderPlaced)
    assert schemas["OrdersShipRequest"] == json_schema(ShipOrder)
    assert schemas["OrdersShipResponse"] == json_schema(OrderShipped)
    # PlaceOrder.note carries a default, so it is not required (declaration-time obligation).
    assert schemas["OrdersPlaceRequest"]["required"] == ["sku", "quantity"]
    # Wire-naming policy: order_id is emitted camelCase.
    assert "orderId" in schemas["OrdersPlaceResponse"]["properties"]


def test_success_and_failure_responses_present() -> None:
    document = openapi_document(_registry())
    responses = document["paths"]["/benzene/invoke/orders:place"]["post"]["responses"]

    # 200 success references the response schema.
    assert responses["200"]["content"]["application/json"]["schema"]["$ref"] == (
        "#/components/schemas/OrdersPlaceResponse"
    )
    # Every failure status is mapped to its HTTP code and shares the one problem-details body,
    # served as application/problem+json — the media type the HTTP binding actually sets (§4.1).
    expected_codes = {str(to_http(status)) for status in FAILURE_STATUSES}
    assert expected_codes <= set(responses)
    for code in expected_codes:
        assert responses[code]["content"]["application/problem+json"]["schema"]["$ref"] == (
            "#/components/schemas/BenzeneProblem"
        )
        assert "application/json" not in responses[code]["content"]


def test_the_problem_schema_is_the_document_the_port_actually_emits() -> None:
    # wire-contracts.md 1.3 WITHDREW the {status: string, detail: string} shape this component used
    # to advertise: `status` is RFC 9457's integer HTTP code, and the Benzene status travels as
    # `benzeneStatus`. What a caller receives is what benzene.core.error_payload builds plus 4.1's
    # HTTP additions, so that is what the document must promise.
    schemas = openapi_document(_registry())["components"]["schemas"]
    problem = schemas["BenzeneProblem"]

    assert problem["properties"]["status"]["type"] == "integer"
    assert problem["properties"]["benzeneStatus"]["type"] == "string"
    assert sorted(problem["required"]) == ["benzeneStatus", "status"]
    assert set(problem["properties"]) >= {"type", "title", "detail", "instance", "errors"}

    # errors is the authoritative, ordered array of structured errors — not prose to split.
    assert problem["properties"]["errors"]["items"]["$ref"] == (
        "#/components/schemas/BenzeneError"
    )
    assert schemas["BenzeneError"]["required"] == ["message"]
    assert set(schemas["BenzeneError"]["properties"]) == {"message", "field", "code"}


def test_the_advertised_problem_schema_matches_a_real_failure_response() -> None:
    # The check that keeps the two from drifting again: run a real failure through the HTTP binding
    # and hold its body against what the document promises.
    import json

    from benzene.http.app import http_problem_response
    from benzene.results import BenzeneError, Result

    response = http_problem_response(
        Result.failure("validation-error", BenzeneError("no sku", field="sku", code="required"))
    )
    body = json.loads(response.body)
    problem = openapi_document(_registry())["components"]["schemas"]["BenzeneProblem"]

    assert response.headers["content-type"] == "application/problem+json"
    assert set(body) <= set(problem["properties"])  # no member the document does not describe
    assert set(problem["required"]) <= set(body)  # every member it calls required is there
    assert set(body["errors"][0]) <= set(
        openapi_document(_registry())["components"]["schemas"]["BenzeneError"]["properties"]
    )


def _refs(node: object) -> list[str]:
    """Every ``$ref`` anywhere in the document — including the ones nested inside a component."""
    if isinstance(node, dict):
        found = [node["$ref"]] if isinstance(node.get("$ref"), str) else []
        return found + [ref for value in node.values() for ref in _refs(value)]
    if isinstance(node, list):
        return [ref for item in node for ref in _refs(item)]
    return []


def test_all_refs_resolve_to_components() -> None:
    document = openapi_document(_registry())
    schema_names = set(document["components"]["schemas"])

    refs = _refs(document)
    assert refs  # a walk that found nothing would pass vacuously
    for ref in refs:
        prefix = "#/components/schemas/"
        assert ref.startswith(prefix)
        assert ref[len(prefix) :] in schema_names


def test_versioned_topics_get_distinct_paths_and_ids() -> None:
    registry = Registry().register(
        "orders:place", place, request_type=PlaceOrder, response_type=OrderPlaced
    )
    registry.register(
        "orders:place", place, version="v2", request_type=ShipOrder, response_type=OrderShipped
    )
    document = openapi_document(registry)

    assert set(document["paths"]) == {
        "/benzene/invoke/orders:place",
        "/benzene/invoke/orders:place/v2",
    }
    versioned = document["paths"]["/benzene/invoke/orders:place/v2"]["post"]
    assert versioned["operationId"] == "ordersPlace_v2" == operation_id("orders:place", "v2")


def test_separator_only_topic_collision_is_disambiguated() -> None:
    # ``orders:place`` and ``orders-place`` both PascalCase to ``OrdersPlace`` / ``ordersPlace``.
    # Distinct paths must keep distinct, non-overwriting schemas and unique operationIds.
    registry = (
        Registry()
        .register("orders:place", place, request_type=PlaceOrder, response_type=OrderPlaced)
        .register("orders-place", ship, request_type=ShipOrder, response_type=OrderShipped)
    )
    document = openapi_document(registry)

    # Both topics keep their own path.
    assert set(document["paths"]) == {
        "/benzene/invoke/orders:place",
        "/benzene/invoke/orders-place",
    }
    # operationIds are unique across the whole document (OpenAPI requires it).
    op_ids = [item["post"]["operationId"] for item in document["paths"].values()]
    assert len(op_ids) == len(set(op_ids))

    # Neither topic's request schema was overwritten: each $ref resolves to a component whose value
    # matches that handler's own json_schema (not the other's).
    schemas = document["components"]["schemas"]
    for path, expected in (
        ("/benzene/invoke/orders:place", json_schema(PlaceOrder)),
        ("/benzene/invoke/orders-place", json_schema(ShipOrder)),
    ):
        ref = document["paths"][path]["post"]["requestBody"]["content"]["application/json"][
            "schema"
        ]["$ref"]
        assert schemas[ref.removeprefix("#/components/schemas/")] == expected


def test_custom_server_paths_relocate_the_invoke_base() -> None:
    document = openapi_document(_registry(), server_paths=StandardPaths(prefix="/api"))
    assert "/api/invoke/orders:place" in document["paths"]


def test_output_is_deterministic() -> None:
    assert openapi_document(_registry()) == openapi_document(_registry())
