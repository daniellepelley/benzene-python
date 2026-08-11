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
    # Every failure status is mapped to its HTTP code and shares the BenzeneError body.
    expected_codes = {str(to_http(status)) for status in FAILURE_STATUSES}
    assert expected_codes <= set(responses)
    for code in expected_codes:
        assert responses[code]["content"]["application/json"]["schema"]["$ref"] == (
            "#/components/schemas/BenzeneError"
        )


def test_all_refs_resolve_to_components() -> None:
    document = openapi_document(_registry())
    schema_names = set(document["components"]["schemas"])

    for path_item in document["paths"].values():
        operation = path_item["post"]
        refs = [operation["requestBody"]["content"]["application/json"]["schema"]["$ref"]]
        for response in operation["responses"].values():
            refs.append(response["content"]["application/json"]["schema"]["$ref"])
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
