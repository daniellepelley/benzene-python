"""Tests for the ``benzene.pydantic`` validation adapter.

Skipped when pydantic isn't installed (the adapter's one optional dependency); the rest of the port
never imports it.
"""

from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("pydantic")

from benzene.core import BenzeneMessageApplication, Registry, message  # noqa: E402
from benzene.pydantic import format_validation_errors, validated  # noqa: E402
from benzene.results import Result  # noqa: E402
from pydantic import BaseModel, ConfigDict  # noqa: E402
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
        M(n="x")
    except ValidationError as exc:
        messages = format_validation_errors(exc)
        assert len(messages) == 1
        assert messages[0].startswith("n: ")
