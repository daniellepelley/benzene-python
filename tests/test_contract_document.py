"""The Contract Document a service derives and serves (contract-document.md).

The conformance runner pins the derivation against the canonical types (``derivationCases``,
``namingCases``, ``specTopicCases``); these tests cover what the fixtures leave to the port — its
API (``register_event``/``register_send``, ``NoPayload``, ``@schema_name``), the Python types the
fixtures do not name (enums, generics, ``datetime``, pydantic), and the presence rules of §1–§3 —
and feed a derived document through the reader this repo ships, end to end into a generated client.
"""

from __future__ import annotations

import asyncio
import datetime
import enum
import json
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

import pytest
from benzene.codegen_client import compute_contract_hash, generate_service_client, parse_document
from benzene.core import (
    AppDefinition,
    ComponentNameCollisionError,
    ContractDocument,
    ContractInfo,
    DuplicateOutboundRegistrationError,
    HttpMapping,
    NoPayload,
    OutboundRegistry,
    Registry,
    application_from,
    contract_hash,
    message,
    registration_hash,
    schema_name,
    to_jsonable,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths, http_endpoint
from benzene.results import Result


@dataclass
class PlaceOrder:
    sku: str
    quantity: int = 1


@dataclass
class OrderDto:
    id: str
    sku: str


@dataclass
class OrderPlaced:
    order_id: str


@dataclass
class TakePayment:
    order_id: str
    amount: float


@message("orders:place", response_type=OrderDto)
@http_endpoint("POST", "/orders")
async def place(request: PlaceOrder) -> Result:
    return Result.created(OrderDto(id="1", sku=request.sku))


@message("orders:list")
async def list_orders(request: dict) -> Result:
    return Result.ok([])


async def handle(_request: Any) -> Result:
    return Result.ok()


def _router() -> HttpRouter:
    return HttpRouter().add(place).register("GET", "/orders", "orders:list", list_orders)


def _outbound() -> OutboundRegistry:
    return (
        OutboundRegistry()
        .register_event("order:placed", OrderPlaced)
        .register_send("payment:take", TakePayment)
    )


def _derive(registry: Registry, **kwargs: Any) -> dict[str, Any]:
    return ContractDocument.derive(registry, service="orders", **kwargs).to_payload()


# --- §1: the top level -----------------------------------------------------------------------------


def test_the_registry_and_outbound_declarations_project_into_one_document() -> None:
    document = ContractDocument.derive(
        Registry.from_definitions(_router()),
        service="orders",
        service_version="1.4.2",
        version_scheme="semver",
        description="Order intake",
        message_endpoint="/benzene/invoke",
        transports=("http", "sqs"),
        outbound=_outbound(),
        http_mappings={("orders:place", ""): [HttpMapping("POST", "/orders")]},
    ).to_payload()

    assert list(document)[:9] == [
        "schemaDialect",
        "service",
        "serviceVersion",
        "versionScheme",
        "description",
        "messageEndpoint",
        "transports",
        "contractHash",
        "registrationHash",
    ]
    assert document["schemaDialect"] == "openapi-3.0"
    assert "openapi" not in document and "info" not in document and "tags" not in document
    assert [r["topic"] for r in document["requests"]] == ["orders:list", "orders:place"]
    assert [e["topic"] for e in document["events"]] == ["order:placed"]
    assert [s["topic"] for s in document["sends"]] == ["payment:take"]

    placed = next(r for r in document["requests"] if r["topic"] == "orders:place")
    assert placed["httpMappings"] == [{"method": "POST", "path": "/orders"}]
    assert placed["request"] == {"$ref": "#/components/schemas/PlaceOrder"}
    assert placed["response"] == {"$ref": "#/components/schemas/OrderDto"}
    schemas = document["components"]["schemas"]
    assert schemas["PlaceOrder"] == {
        "type": "object",
        "properties": {"sku": {"type": "string"}, "quantity": {"type": "integer"}},
        # quantity has a default, but the marshaler always writes it, so it is required (§4.2).
        "required": ["sku", "quantity"],
    }
    assert "additionalProperties" not in schemas["PlaceOrder"]  # records carry none (§4.2)
    # No synthesized examples: only an author-supplied one would be written (§2).
    assert all("example" not in entry for entry in document["requests"])


def test_both_hashes_are_always_present_and_computed_over_the_document() -> None:
    document = _derive(Registry.from_definitions(_router()), outbound=_outbound())
    assert document["contractHash"] == contract_hash(document)
    assert document["registrationHash"] == registration_hash(document)
    # A send is a dependency, not something a client of this service observes: it moves only the
    # registration hash.
    without_sends = _derive(
        Registry.from_definitions(_router()),
        outbound=OutboundRegistry().register_event("order:placed", OrderPlaced),
    )
    assert without_sends["contractHash"] == document["contractHash"]
    assert without_sends["registrationHash"] != document["registrationHash"]


def test_presence_rules_are_the_specs_and_not_the_convenient_ones() -> None:
    document = _derive(Registry())
    # serviceVersion, versionScheme, description, messageEndpoint, transports: absent, never empty.
    for absent in ("serviceVersion", "versionScheme", "description", "messageEndpoint", "transports"):
        assert absent not in document
    # requests/events/sends/components: REQUIRED, always present, possibly empty.
    assert document["requests"] == [] and document["events"] == [] and document["sends"] == []
    assert document["components"] == {"schemas": {}}


def test_a_service_name_is_required_and_a_version_scheme_must_be_known() -> None:
    with pytest.raises(ValueError, match="service name"):
        ContractDocument.derive(Registry(), service="")
    with pytest.raises(ValueError, match="version_scheme"):
        ContractDocument.derive(Registry(), service="orders", version_scheme="calver")


def test_an_unversioned_entry_omits_version_entirely() -> None:
    document = _derive(Registry().register("orders:place", place, version="v2"))
    assert document["requests"][0]["version"] == "v2"
    unversioned = _derive(Registry.from_definitions(_router()))
    # A producer MUST NOT write "version": "" (§2).
    assert all("version" not in entry for entry in unversioned["requests"])


def test_benzene_topics_are_never_listed() -> None:
    registry = Registry.from_definitions(_router()).register("benzene:ping", list_orders)
    outbound = OutboundRegistry().register_send("benzene:mesh:register", OrderPlaced)
    document = _derive(registry, outbound=outbound)
    assert "benzene:ping" not in {r["topic"] for r in document["requests"]}
    assert document["sends"] == []
    assert all("reserved" not in entry for entry in document["requests"])


# --- §2/§3: response presence, events and sends ------------------------------------------------------


def test_response_absent_means_no_payload_and_empty_means_any() -> None:
    registry = (
        Registry()
        .register("orders:cancel", handle, request_type=PlaceOrder, response_type=NoPayload)
        .register("orders:raw", handle, request_type=PlaceOrder)
    )
    entries = {r["topic"]: r for r in _derive(registry)["requests"]}
    assert "response" not in entries["orders:cancel"]  # declared: returns no payload
    assert entries["orders:raw"]["response"] == {}  # undeclared: a payload of any shape


def test_a_send_without_a_response_type_is_one_way() -> None:
    outbound = (
        OutboundRegistry()
        .register_send("payment:take", TakePayment)
        .register_send("inventory:reserve", PlaceOrder, OrderDto, version="v2")
    )
    sends = {s["topic"]: s for s in _derive(Registry(), outbound=outbound)["sends"]}
    assert "response" not in sends["payment:take"]
    assert sends["inventory:reserve"]["response"] == {"$ref": "#/components/schemas/OrderDto"}
    assert sends["inventory:reserve"]["version"] == "v2"


def test_a_topic_version_in_both_events_and_sends_is_a_start_up_error() -> None:
    outbound = OutboundRegistry().register_event("order:placed", OrderPlaced)
    with pytest.raises(DuplicateOutboundRegistrationError, match="both an event and a send"):
        outbound.register_send("order:placed", OrderPlaced)
    with pytest.raises(DuplicateOutboundRegistrationError, match="already declared as an event"):
        outbound.register_event("order:placed", OrderPlaced)
    # A distinct version is a distinct declaration.
    outbound.register_send("order:placed", OrderPlaced, version="v2")


def test_a_kind_less_register_says_which_kinds_exist() -> None:
    with pytest.raises(TypeError, match="register_event.*register_send"):
        OutboundRegistry().register("payment:take")


# --- §4: derivation and naming -----------------------------------------------------------------------


class Colour(str, enum.Enum):
    RED = "red"
    BLUE = "blue"


class Priority(enum.IntEnum):
    LOW = 1
    HIGH = 2


class Opaque(enum.Enum):  # no str/int mixin: json.dumps cannot marshal it
    A = object()


T = TypeVar("T")


@dataclass
class Page(Generic[T]):
    items: list[T]
    next_cursor: str | None


@dataclass
class Everything:
    when: datetime.datetime
    blob: bytes
    colour: Colour
    maybe_colour: Colour | None
    priority: Priority
    opaque: Opaque
    anything: Any
    either: int | str
    page: Page[OrderDto]
    labels: dict[str, list[int]] = field(default_factory=dict)


@dataclass
class _Defaulted:
    id: str
    count: int = 0
    tags: list[str] = field(default_factory=list)
    note: str | None = None


def test_required_is_every_property_the_marshaler_always_emits_defaults_included() -> None:
    """contract-document §4.2 (Benzene 9c7aa36d): required describes the payload as written.

    ``to_jsonable`` writes every dataclass field whatever its default, so every field is required -
    a field with a default, a default factory or a ``None`` default included.
    """
    async def handle(_request: _Defaulted) -> Result:
        return Result.ok()

    registry = Registry().register("t:x", handle, request_type=_Defaulted)
    document = ContractDocument.derive(registry, service="s").to_payload()
    schema = document["components"]["schemas"]["_Defaulted"]
    written = to_jsonable(_Defaulted(id="a"))
    assert set(written) == set(schema["properties"])  # the marshaler wrote every property...
    assert schema["required"] == ["id", "count", "tags", "note"]  # ...so each one is required


def test_the_derivation_table_through_pythons_types() -> None:
    document = _derive(Registry().register("things:do", handle, request_type=Everything))
    schemas = document["components"]["schemas"]
    properties = schemas["Everything"]["properties"]
    assert properties["when"] == {"type": "string", "format": "date-time"}
    assert properties["blob"] == {"type": "string", "format": "byte"}
    # A str/int enum is a named type: hoisted, with its values; nullable, it takes the wrapper.
    assert properties["colour"] == {"$ref": "#/components/schemas/Colour"}
    assert schemas["Colour"] == {"type": "string", "enum": ["red", "blue"]}
    assert properties["maybeColour"] == {
        "type": "string",
        "nullable": True,
        "allOf": [{"$ref": "#/components/schemas/Colour"}],
    }
    assert schemas["Priority"] == {"type": "integer", "enum": [1, 2]}
    # What the marshaler cannot serialize, or cannot describe in one schema, is {}.
    assert properties["opaque"] == {} and properties["anything"] == {} and properties["either"] == {}
    # A generic instance is named with its type arguments first, and its parameters bound.
    assert properties["page"] == {"$ref": "#/components/schemas/OrderDtoPage"}
    assert schemas["OrderDtoPage"]["properties"]["items"] == {
        "type": "array",
        "items": {"$ref": "#/components/schemas/OrderDto"},
    }
    assert schemas["OrderDtoPage"]["properties"]["nextCursor"] == {
        "type": "string",
        "nullable": True,
    }
    assert properties["labels"] == {
        "type": "object",
        "additionalProperties": {"type": "array", "items": {"type": "integer"}},
    }
    # A default does not make a field optional: the marshaler always emits it (§4.2).
    assert "labels" in schemas["Everything"]["required"]


def _item(module: str, field_name: str) -> type:
    from dataclasses import make_dataclass

    cls = make_dataclass("Item", [(field_name, str)])
    cls.__module__ = module
    return cls


def test_a_name_collision_fails_naming_both_types_the_topics_and_the_remedy() -> None:
    registry = (
        Registry()
        .register("orders:item", handle, request_type=_item("orders", "sku"))
        .register("payments:item", handle, request_type=_item("payments", "payment_id"))
    )
    with pytest.raises(ComponentNameCollisionError) as raised:
        _derive(registry)
    error = raised.value
    assert error.name == "Item"
    assert error.types == ("orders.Item", "payments.Item")
    assert error.topics == ("orders:item", "payments:item")
    assert "@schema_name" in str(error)


def test_the_collision_is_independent_of_registration_order() -> None:
    orders, payments = _item("orders", "sku"), _item("payments", "payment_id")
    first = Registry().register("a:1", handle, request_type=orders).register("b:1", handle, request_type=payments)
    second = Registry().register("b:1", handle, request_type=payments).register("a:1", handle, request_type=orders)
    errors = []
    for registry in (first, second):
        with pytest.raises(ComponentNameCollisionError) as raised:
            _derive(registry)
        errors.append(str(raised.value))
    assert errors[0] == errors[1]


def test_a_declared_schema_name_resolves_a_collision() -> None:
    registry = (
        Registry()
        .register("orders:item", handle, request_type=schema_name("OrderItem")(_item("orders", "sku")))
        .register("payments:item", handle, request_type=schema_name("PaymentItem")(_item("payments", "id")))
    )
    assert set(_derive(registry)["components"]["schemas"]) == {"OrderItem", "PaymentItem"}


def test_two_identical_schemas_may_share_one_name() -> None:
    registry = (
        Registry()
        .register("a:item", handle, request_type=_item("a", "sku"))
        .register("b:item", handle, request_type=_item("b", "sku"))
    )
    document = _derive(registry)
    assert set(document["components"]["schemas"]) == {"Item"}


def test_a_declared_name_is_not_inherited_and_invalid_characters_are_dropped() -> None:
    @schema_name("Base")
    @dataclass
    class Parent:
        a: str

    @dataclass
    class Child(Parent):
        b: str = ""

    Weird = type("Wéird<Name>", (), {})
    Weird = dataclass(Weird)
    registry = (
        Registry()
        .register("p:1", handle, request_type=Parent)
        .register("c:1", handle, request_type=Child)
        .register("w:1", handle, request_type=Weird)
    )
    assert set(_derive(registry)["components"]["schemas"]) == {"Base", "Child", "WirdName"}
    with pytest.raises(ValueError):
        schema_name("<>")


def test_the_collision_fails_at_the_composition_root_before_any_message() -> None:
    registry = (
        Registry()
        .register("orders:item", handle, request_type=_item("orders", "sku"))
        .register("payments:item", handle, request_type=_item("payments", "id"))
    )
    with pytest.raises(ComponentNameCollisionError):
        application_from(AppDefinition(registry=registry, contract=ContractInfo("shop")))


def test_a_pydantic_model_derives_to_the_open_schema() -> None:
    # Recorded gap (self-description-design Q11, deferred): this derivation reads dataclasses, so a
    # pydantic payload publishes {} until a pydantic schema source lands.
    base_model: Any = pytest.importorskip("pydantic").BaseModel

    class Model(base_model):
        name: str

    document = _derive(Registry().register("m:1", handle, request_type=Model))
    assert document["requests"][0]["request"] == {}


# --- the reader this repo ships is the writer's cheapest end-to-end check ----------------------------


def test_the_derived_document_parses_and_generates_a_client() -> None:
    raw = _derive(
        Registry.from_definitions(_router()),
        message_endpoint="/benzene/invoke",
        outbound=_outbound(),
        transports=("http", "sqs"),
        http_mappings={("orders:place", ""): [HttpMapping("POST", "/orders")]},
    )
    parsed = parse_document(raw)
    assert parsed.topics() == ("orders:list", "orders:place")
    assert parsed.message_endpoint == "/benzene/invoke"
    assert parsed.transports == ("http", "sqs")
    assert [s.topic for s in parsed.sends] == ["payment:take"]
    assert set(parsed.schemas) == {"PlaceOrder", "OrderDto", "OrderPlaced", "TakePayment"}

    generated = generate_service_client(parsed, service_name="Orders")
    assert generated.required_topics == ("orders:list", "orders:place")
    assert "class PlaceOrder" in generated.source
    # A default client embeds the served contractHash: the drift check compares exactly these.
    assert generated.contract_hash == raw["contractHash"] == compute_contract_hash(raw)


# --- the served surface ----------------------------------------------------------------------------


def _app(**contract: Any) -> BenzeneHttpApp:
    return BenzeneHttpApp.from_definition(
        AppDefinition(
            router=_router(),
            standard_paths=StandardPaths(),
            outbound=_outbound(),
            contract=ContractInfo("orders", **contract),
        )
    )


def _get(app: BenzeneHttpApp, query: str = "") -> dict:
    response = asyncio.run(app.handle("GET", "/benzene/spec", query))
    assert response.status_code == 200
    return json.loads(response.body)


def test_the_topic_and_the_route_serve_the_identical_document() -> None:
    app = _app(transports=("http", "sqs"))
    over_http = _get(app)
    envelope = {"topic": "benzene:spec", "headers": {}, "body": ""}
    via_invoke = asyncio.run(app.handle("POST", "/benzene/invoke", body=json.dumps(envelope)))
    assert json.loads(json.loads(via_invoke.body)["body"]) == over_http
    assert over_http["transports"] == ["http", "sqs"]
    assert over_http["messageEndpoint"] == "/benzene/invoke"


def test_the_message_endpoint_is_absent_when_invoke_is_off() -> None:
    app = BenzeneHttpApp.from_definition(
        AppDefinition(
            router=_router(), standard_paths=StandardPaths(invoke=False), contract=ContractInfo("orders")
        )
    )
    assert "messageEndpoint" not in _get(app)


def test_an_unsupported_type_or_format_is_not_implemented() -> None:
    for query in ("type=asyncapi", "type=native", "format=yaml", "type=benzene&format=yaml"):
        response = asyncio.run(_app().handle("GET", "/benzene/spec", query))
        assert response.status_code == 501, query
        assert json.loads(response.body)["benzeneStatus"] == "not-implemented"
