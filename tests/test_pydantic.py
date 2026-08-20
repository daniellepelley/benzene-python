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
    detail = json.loads(response["body"])["detail"]
    assert "sku" in detail and "quantity" in detail  # both bad fields named


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


# --- @validated needs a model class (audit D3) ---------------------------------------------------
# The bare form `@validated` makes the decorated function itself the "model", which only fails later,
# per request, as `service-unavailable: object function can't be used in 'await' expression`. The
# decorator rejects that at decoration/import time instead.


def test_bare_validated_raises_at_decoration_time() -> None:
    with pytest.raises(TypeError, match="the bare form @validated is not supported"):

        @validated  # type: ignore[arg-type]  # the mistake under test: no model argument
        async def handler(order: PlaceOrder) -> Result:
            return Result.ok()


def test_validated_rejects_a_non_model_argument() -> None:
    with pytest.raises(TypeError, match="@validated needs a pydantic model class"):
        validated(dict)  # type: ignore[type-var]  # a type, but not a BaseModel subclass

    with pytest.raises(TypeError, match="got 'PlaceOrder'"):
        validated("PlaceOrder")  # type: ignore[arg-type]  # not even a class
