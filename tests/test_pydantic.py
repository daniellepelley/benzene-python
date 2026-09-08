"""Tests for the ``benzene.pydantic`` validation adapter.

Skipped when pydantic isn't installed (the adapter's one optional dependency); the rest of the port
never imports it.
"""

from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("pydantic")

from typing import Any

from benzene.core import (  # noqa: E402
    BenzeneMessageApplication,
    ContractDocument,
    Registry,
    ServiceSpec,
    message,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths  # noqa: E402
from benzene.pydantic import format_validation_errors, validated  # noqa: E402
from benzene.results import Result  # noqa: E402
from pydantic import BaseModel, ConfigDict, Field  # noqa: E402
from pydantic.alias_generators import to_camel  # noqa: E402


class PlaceOrder(BaseModel):
    sku: str
    quantity: int = 1


@message("orders:place")
@validated(PlaceOrder)
async def place(order: PlaceOrder) -> Result:
    return Result.created({"sku": order.sku, "quantity": order.quantity})


def _run(body: str) -> dict:
    app = BenzeneMessageApplication(Registry().add(place))
    return asyncio.run(app.handle({"topic": "orders:place", "headers": {}, "body": body}))


def test_valid_request_is_parsed_into_the_model() -> None:
    response = _run('{"sku": "ABC", "quantity": 2}')
    assert response["statusCode"] == "created"
    assert json.loads(response["body"]) == {"sku": "ABC", "quantity": 2}


def test_default_is_applied_by_the_model() -> None:
    response = _run('{"sku": "ABC"}')  # quantity defaults to 1
    assert json.loads(response["body"]) == {"sku": "ABC", "quantity": 1}


def test_invalid_request_becomes_validation_error_naming_the_fields() -> None:
    response = _run('{"quantity": "not-an-int"}')  # sku missing + quantity wrong type
    assert response["statusCode"] == "validation-error"

    # The bad fields are named in the structured errors, not glued into detail's prose. pydantic
    # already knows the location and the rule for each failure, so they travel as `field` and `code`
    # (the same rule .NET's FluentValidation adapter follows: the validator's message verbatim, its
    # property name and error code beside it, never reworded into one string).
    errors = json.loads(response["body"])["errors"]
    assert [error["field"] for error in errors] == ["sku", "quantity"]
    assert [error["code"] for error in errors] == ["missing", "int_parsing"]
    assert all(error["message"] for error in errors)

    # detail is still the messages joined, for a caller that only logs one line.
    detail = json.loads(response["body"])["detail"]
    assert detail == ", ".join(error["message"] for error in errors)


def test_structured_validation_errors_survive_a_round_trip() -> None:
    """A client decoding the response gets the field and code back, not just prose."""
    from benzene.core.envelope import decode_response

    result = decode_response(_run('{"quantity": "not-an-int"}'))

    assert result.status == "validation-error"
    assert [(error.field, error.code) for error in result.errors] == [
        ("sku", "missing"),
        ("quantity", "int_parsing"),
    ]


def test_the_handler_never_sees_an_invalid_request() -> None:
    seen: list = []

    @message("guarded")
    @validated(PlaceOrder)
    async def guarded(order: PlaceOrder) -> Result:
        seen.append(order)
        return Result.ok()

    app = BenzeneMessageApplication(Registry().add(guarded))
    asyncio.run(app.handle({"topic": "guarded", "headers": {}, "body": "{}"}))  # invalid
    assert seen == []  # short-circuited before the handler


def test_pydantic_model_response_serializes_via_model_dump() -> None:
    class Receipt(BaseModel):
        order_id: str

    @message("orders:receipt")
    @validated(PlaceOrder)
    async def receipt(order: PlaceOrder) -> Result:
        return Result.ok(Receipt(order_id=order.sku))

    app = BenzeneMessageApplication(Registry().add(receipt))
    response = asyncio.run(app.handle({"topic": "orders:receipt", "headers": {}, "body": '{"sku": "o1"}'}))
    assert json.loads(response["body"]) == {"order_id": "o1"}  # model_dump serialized the model


def test_camel_alias_generator_reaches_the_wire() -> None:
    class Receipt(BaseModel):
        model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
        order_id: str

    @message("orders:receipt2")
    @validated(PlaceOrder)
    async def receipt(order: PlaceOrder) -> Result:
        return Result.ok(Receipt(order_id=order.sku))

    app = BenzeneMessageApplication(Registry().add(receipt))
    response = asyncio.run(app.handle({"topic": "orders:receipt2", "headers": {}, "body": '{"sku": "o1"}'}))
    # by_alias=True in the wire mapper -> the camelCase alias, matching the Benzene naming policy
    assert json.loads(response["body"]) == {"orderId": "o1"}


def test_format_validation_errors_is_readable() -> None:
    class M(BaseModel):
        n: int

    from pydantic import ValidationError

    try:
        # The wrong type IS the test, so it is handed over as Any. Written inline as `M(n="x")` it
        # needs a `# type: ignore` that fires only where pydantic is installed - and CI's lint job
        # does not install it, so the ignore reads as unused there and warn_unused_ignores fails the
        # build. `dict[str, Any]` is an error in neither environment, which is what makes it stable.
        invalid: dict[str, Any] = {"n": "x"}
        M(**invalid)
    except ValidationError as exc:
        messages = format_validation_errors(exc)
        assert len(messages) == 1
        assert messages[0].startswith("n: ")


# --- @validated needs a model class (audit D3) ---------------------------------------------------
# The bare form `@validated` makes the decorated function itself the "model", which only fails later,
# per request, as `service-unavailable: object function can't be used in 'await' expression`. The
# decorator rejects that at decoration/import time instead.


def test_bare_validated_raises_at_decoration_time() -> None:
    with pytest.raises(TypeError, match="the bare form @validated is not supported"):

        @validated  # type: ignore[arg-type, type-var]  # the mistake under test: no model argument
        async def handler(order: PlaceOrder) -> Result:
            return Result.ok()


def test_validated_rejects_a_non_model_argument() -> None:
    with pytest.raises(TypeError, match="@validated needs a pydantic model class"):
        validated(dict)  # type: ignore[type-var]  # a type, but not a BaseModel subclass

    with pytest.raises(TypeError, match="got 'PlaceOrder'"):
        validated("PlaceOrder")  # type: ignore[arg-type]  # not even a class


# --- T0.4: a pydantic-modelled service must not publish an empty contract -------------------------
# `benzene.core.json_schema` fell through to `{}` for anything it did not recognise, and a pydantic
# BaseModel is exactly that — so every document derived from the registry (the Contract Document at
# /benzene/spec, this port's native ServiceSpec, the mesh ServiceDescriptor, the OpenAPI document)
# advertised the open schema for the one model type this package exists to support. The tests below
# drive the published surfaces, not the helper, because the helper being wrong is not the bug — the
# bug is four documents lying about the service's contract.

class Address(BaseModel):
    """A nested model — pydantic renders it as a `$defs` entry reached by `$ref`."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    line_one: str
    post_code: str = ""


class SubmitOrder(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    sku: str = Field(min_length=2, max_length=8, pattern="^A")
    quantity: int = Field(default=1, ge=1, le=99)
    ship_to: Address | None = None


class OrderReceipt(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    order_id: str


async def submit(order: SubmitOrder) -> Result:
    return Result.created(OrderReceipt(order_id=order.sku))


def _orders_registry() -> Registry:
    return Registry().register(
        "orders:submit", submit, request_type=SubmitOrder, response_type=OrderReceipt
    )


def _refs(node: Any) -> list[str]:
    """Every `$ref`/`$defs` key anywhere in a document — the Contract Document forbids both here."""
    if isinstance(node, dict):
        found = [key for key in node if key in ("$ref", "$defs")]
        return found + [ref for value in node.values() for ref in _refs(value)]
    if isinstance(node, list):
        return [ref for item in node for ref in _refs(item)]
    return []


def test_the_contract_document_publishes_the_models_schema() -> None:
    """`GET /benzene/spec` — the document every other port's client generator parses."""
    registry = _orders_registry()
    app = BenzeneHttpApp(
        HttpRouter(),
        application=BenzeneMessageApplication(registry),
        standard_paths=StandardPaths(
            contract=lambda: ContractDocument.derive(registry, service="orders")
        ),
    )

    response = asyncio.run(app.handle("GET", "/benzene/spec"))
    assert response.status_code == 200
    entry = json.loads(response.body)["requests"][0]

    assert entry["request"]["type"] == "object"
    # The aliased (wire) names, because `to_jsonable` dumps the model `by_alias=True`: the schema
    # has to describe what actually crosses the wire.
    assert set(entry["request"]["properties"]) == {"sku", "quantity", "shipTo"}
    assert entry["request"]["required"] == ["sku"]
    assert entry["response"]["properties"]["orderId"] == {"type": "string"}


def test_the_contract_document_carries_no_pydantic_refs() -> None:
    """contract-document.md §4: the only legal `$ref` is `#/components/schemas/<name>`, so a
    nested model's `$defs`/`#/$defs/...` must be inlined before it reaches the document."""
    registry = _orders_registry()
    document = ContractDocument.derive(registry, service="orders").to_payload()

    assert _refs(document) == []
    ship_to = document["requests"][0]["request"]["properties"]["shipTo"]
    assert ship_to["anyOf"][0]["properties"]["lineOne"] == {"type": "string"}


def test_the_native_spec_document_publishes_the_models_schema() -> None:
    """`GET /benzene/spec?type=native` — this port's own {service, topics} payload."""
    registry = _orders_registry()
    app = BenzeneHttpApp(
        HttpRouter(),
        application=BenzeneMessageApplication(registry),
        standard_paths=StandardPaths(spec=lambda: ServiceSpec.derive(registry, service="orders")),
    )

    doc = json.loads(asyncio.run(app.handle("GET", "/benzene/spec", "type=native")).body)
    assert doc["topics"][0]["requestSchema"]["properties"]["sku"]["type"] == "string"


def test_the_mesh_descriptor_publishes_the_models_schema() -> None:
    """The descriptor a service pushes to the mesh — and the hash the mesh diffs contracts on."""
    mesh = pytest.importorskip("benzene.mesh")
    registry = _orders_registry()
    descriptor = mesh.ServiceDescriptor.derive(registry, mesh.ServiceInfo(service="orders"))

    payload = descriptor.to_payload()
    assert payload["topics"][0]["requestSchema"]["properties"]["sku"]["type"] == "string"
    # The hash is content-derived: a descriptor that used to hash `{}` now hashes the real contract.
    assert payload["descriptorHash"] != _EMPTY_SCHEMA_DESCRIPTOR_HASH


#: The `descriptorHash` this service produced while its schemas were the open schema `{}` — pinned
#: here so the change is asserted, not stumbled into. See the note in `docs/reference/pydantic.md`.
_EMPTY_SCHEMA_DESCRIPTOR_HASH = (
    "sha256:c60828d7e1300f412548e4e1f2f1dd75be7f63ee19ee1db59282eafa1419b1af"
)


def test_field_constraints_reach_the_published_schema() -> None:
    """A model's `Field(...)` constraints are contract, and travel with it."""
    schema = ContractDocument.derive(_orders_registry(), service="orders").to_payload()["requests"][
        0
    ]["request"]

    assert schema["properties"]["sku"]["minLength"] == 2
    assert schema["properties"]["sku"]["maxLength"] == 8
    assert schema["properties"]["sku"]["pattern"] == "^A"
    assert schema["properties"]["quantity"]["minimum"] == 1
    assert schema["properties"]["quantity"]["maximum"] == 99


def test_the_schema_keys_are_the_keys_the_wire_actually_carries() -> None:
    """The property that matters: what the schema promises is what `to_jsonable` writes."""
    from benzene.core import json_schema, to_jsonable

    instance = SubmitOrder(sku="ABC", ship_to=Address(line_one="1 High St"))
    assert set(json_schema(SubmitOrder)["properties"]) == set(to_jsonable(instance))


def test_a_recursive_model_terminates_with_the_cycle_cut() -> None:
    """The rule `benzene.core` already applies to a recursive dataclass: cut with `{}`, never a $ref."""
    from benzene.core import json_schema

    class Node(BaseModel):
        name: str
        child: Node | None = None

    schema = json_schema(Node)

    assert schema["properties"]["name"] == {"type": "string"}
    assert schema["properties"]["child"]["anyOf"] == [{}, {"type": "null"}]
    assert _refs(schema) == []


def test_enums_and_literals_derive_an_enum_array() -> None:
    from enum import Enum
    from typing import Literal

    from benzene.core import json_schema

    class Colour(str, Enum):
        RED = "red"
        BLUE = "blue"

    class Paint(BaseModel):
        colour: Colour
        finish: Literal["matte", "gloss"]

    schema = json_schema(Paint)

    # The enum arrives through a `$defs` entry and the Literal inline; both must read the same way.
    assert schema["properties"]["colour"]["enum"] == ["red", "blue"]
    assert schema["properties"]["finish"]["enum"] == ["matte", "gloss"]
    assert _refs(schema) == []


def test_synthesised_titles_are_stripped_but_authored_prose_is_kept() -> None:
    from benzene.core import json_schema

    class Documented(BaseModel):
        line_one: str = Field(description="the first address line")

    schema = json_schema(Documented)

    assert "title" not in schema  # "Documented" — the class name, not contract
    assert schema["properties"]["line_one"] == {
        "type": "string",
        "description": "the first address line",
    }


def test_a_non_model_type_is_left_to_the_core_rules() -> None:
    """The provider defers rather than claiming: `None` means "not mine"."""
    from benzene.pydantic import pydantic_schema

    assert pydantic_schema(str) is None
    assert pydantic_schema(list[SubmitOrder]) is None  # core recurses and reaches the model itself
    assert pydantic_schema("not even a type") is None


def test_inline_defs_prefers_a_ref_sibling_over_the_target() -> None:
    """JSON Schema 2020-12 allows keywords beside a `$ref`; the more specific statement wins."""
    from benzene.pydantic import inline_defs

    document = {
        "$defs": {"A": {"type": "object", "description": "the definition"}},
        "$ref": "#/$defs/A",
        "description": "the use site",
    }
    assert inline_defs(document) == {"type": "object", "description": "the use site"}


def test_inline_defs_never_leaves_a_dangling_pointer() -> None:
    from benzene.pydantic import inline_defs

    assert inline_defs({"properties": {"a": {"$ref": "#/$defs/Missing"}}}) == {
        "properties": {"a": {}}
    }
    assert inline_defs({"properties": {"a": {"$ref": "https://example.test/x"}}}) == {
        "properties": {"a": {}}
    }


# --- @validated(Model) as the *only* declaration of the request shape ---------------------------
#
# The tests above register with an explicit ``request_type=``, which is why they passed while the
# documented idiomatic path did not: ``@validated(Model)`` leaves the wrapper annotated
# ``request: object``, so every published surface derived the empty schema of ``object``.


class DecoratedOrder(BaseModel):
    sku: str
    quantity: int = 1


@message("orders:decorated")
@validated(DecoratedOrder)
async def decorated(order: DecoratedOrder) -> Result:
    return Result.ok({"sku": order.sku})


def _decorated_registry() -> Registry:
    return Registry().add(decorated)


def test_a_validated_handler_publishes_its_models_schema_not_an_empty_one() -> None:
    """The four surfaces one derivation feeds — a contract nobody can read is worse than none."""
    registry = _decorated_registry()

    contract = ContractDocument.derive(registry, service="orders").to_payload()
    native = ServiceSpec.derive(registry, service="orders").to_payload()

    for schema in (
        contract["requests"][0]["request"],
        native["topics"][0]["requestSchema"],
    ):
        assert schema["type"] == "object"
        assert set(schema["properties"]) == {"sku", "quantity"}
        assert schema["required"] == ["sku"]


def test_describing_the_request_does_not_change_how_it_is_mapped() -> None:
    """The hint is schema-only, deliberately.

    Mapping to the model instead would make ``to_request`` construct it, so a bad body would fail
    at mapping time rather than reaching the decorator — turning the documented ``validation-error``
    into something else. ``request_type`` therefore stays exactly what the wrapper declares.
    """
    definition = _decorated_registry().definitions()[0]

    assert definition.request_type is object
    assert definition.request_schema_type is DecoratedOrder


def test_a_bad_body_is_still_a_validation_error_after_the_schema_fix() -> None:
    app = BenzeneMessageApplication(_decorated_registry())

    response = asyncio.run(
        app.handle(
            {"topic": "orders:decorated", "headers": {}, "body": json.dumps({"quantity": "nope"})}
        )
    )

    assert response["statusCode"] == "validation-error"
    assert response["isSuccessful"] is False
