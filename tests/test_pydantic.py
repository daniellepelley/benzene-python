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

from benzene.core import BenzeneMessageApplication, Registry, message  # noqa: E402
from benzene.pydantic import (  # noqa: E402
    field_pointer,
    format_validation_errors,
    validated,
    validation_errors,
)
from benzene.results import Result  # noqa: E402
from pydantic import (  # noqa: E402
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)
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
    assert [error["field"] for error in errors] == ["/sku", "/quantity"]  # JSON Pointers (§1.3)
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
        ("/sku", "missing"),
        ("/quantity", "int_parsing"),
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
        assert messages[0].startswith("/n: ")


# --- errors[].field is a JSON Pointer into the request as serialized (wire-contracts §1.3, D8) ----


class Line(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    sku_code: str = Field(min_length=1)


class Gift(BaseModel):
    note: str


class Basket(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    customer_name: str = Field(min_length=1)
    lines: list[Line]
    extra: Line | Gift | None = None
    renamed: str = Field("x", alias="Re-Named", serialization_alias="re/named")

    @model_validator(mode="after")
    def no_anonymous_gifts(self) -> Basket:
        if self.customer_name == "anonymous" and self.extra is not None:
            raise ValueError("an anonymous basket cannot carry an extra")
        return self


def _fields(body: dict[str, Any]) -> list[str | None]:
    try:
        Basket.model_validate(body)
    except ValidationError as exc:
        return [error.field for error in validation_errors(exc, Basket)]
    raise AssertionError("expected a ValidationError")


def test_pointers_use_the_serialized_names_and_array_indexes() -> None:
    # The alias generator's camelCase is the JSON the caller sent, so it is the pointer's spelling.
    assert _fields({"customerName": "", "lines": [{"skuCode": "a"}, {}]}) == [
        "/customerName",
        "/lines/1/skuCode",
    ]


def test_a_serialization_alias_wins_and_is_escaped() -> None:
    assert _fields({"customerName": "c", "lines": [], "Re-Named": 5}) == ["/re~1named"]


def test_union_member_tags_are_not_pointer_segments() -> None:
    # pydantic reports ('extra', 'Line', 'skuCode') - the member tag is not part of the JSON.
    fields = _fields({"customerName": "c", "lines": [], "extra": {"skuCode": ""}})
    assert fields == ["/extra/skuCode", "/extra/note"]


def test_a_rule_on_the_whole_request_omits_field() -> None:
    assert _fields({"customerName": "anonymous", "lines": [], "extra": {"note": "hi"}}) == [None]


def test_the_validated_decorator_emits_pointers_on_the_wire() -> None:
    @message("baskets:submit")
    @validated(Basket)
    async def submit(basket: Basket) -> Result:
        return Result.ok()

    app = BenzeneMessageApplication(Registry().add(submit))
    response = asyncio.run(
        app.handle({"topic": "baskets:submit", "headers": {}, "body": '{"customerName": "", "lines": [{}]}'})
    )
    assert [e["field"] for e in json.loads(response["body"])["errors"]] == [
        "/customerName",
        "/lines/0/skuCode",
    ]


def test_field_pointer_without_a_model_uses_the_location_as_reported() -> None:
    assert field_pointer(("items", 0, "sku")) == "/items/0/sku"
    assert field_pointer(()) is None
