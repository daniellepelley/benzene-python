"""Request-type inference from the handler's own annotation.

A handler already declares the shape it wants (``async def place(request: PlaceOrder)``), so the
registration APIs read ``request_type`` off the signature instead of making you repeat it. An explicit
``request_type`` still wins, and anything that isn't a concrete class (a bare ``dict[str, Any]``, no
annotation) infers to ``None`` — the request then passes through unmapped, exactly as before.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from benzene.core import (
    BenzeneMessageApplication,
    Context,
    MiddlewarePipeline,
    Registry,
    definition_of,
    infer_request_type,
    message,
    message_router,
)
from benzene.results import Result, Status


@dataclass
class PlaceOrder:
    sku: str = ""
    quantity: int = 1


def run(coro):
    return asyncio.run(coro)


# --- the inference helper ----------------------------------------------------------------------


def test_infers_a_dataclass_annotation() -> None:
    async def place(request: PlaceOrder) -> Result:
        return Result.ok()

    assert infer_request_type(place) is PlaceOrder


def test_does_not_infer_a_subscripted_generic() -> None:
    async def handle(request: dict[str, Any]) -> Result:
        return Result.ok()

    # dict[str, Any] is not a concrete class to_request can map against — leave it unmapped.
    assert infer_request_type(handle) is None


def test_does_not_infer_without_an_annotation() -> None:
    async def handle(request) -> Result:  # noqa: ANN001 - deliberately unannotated
        return Result.ok()

    assert infer_request_type(handle) is None


def test_does_not_infer_typing_any() -> None:
    # On Python 3.11+ ``typing.Any`` is a class, so it slips past ``isinstance(_, type)`` — but
    # ``request: Any`` means "give me whatever arrived", and to_request can't isinstance-check Any.
    async def handle(request: Any) -> Result:
        return Result.ok()

    assert infer_request_type(handle) is None


def test_any_annotated_handler_still_receives_the_raw_request() -> None:
    # The end-to-end guard: an Any-annotated handler registered without request_type gets the raw
    # decoded body (a dict), not a bad-request from a failed mapping.
    async def handle(request: Any) -> Result:
        return Result.ok(request)

    registry = Registry().register("t", handle)
    pipeline = MiddlewarePipeline().use(message_router(registry))
    ctx = Context("t", {"sku": "raw"})
    run(pipeline.handle(ctx))
    assert ctx.result is not None and ctx.result.payload == {"sku": "raw"}


def test_infers_a_bare_dict_annotation() -> None:
    async def handle(request: dict) -> Result:
        return Result.ok()

    assert infer_request_type(handle) is dict


# --- the registration entry points -------------------------------------------------------------


def test_message_decorator_infers_request_type() -> None:
    @message("orders:place")
    async def place(request: PlaceOrder) -> Result:
        return Result.created(request)

    definition = definition_of(place)
    assert definition is not None
    assert definition.request_type is PlaceOrder


def test_registry_register_infers_request_type() -> None:
    async def place(request: PlaceOrder) -> Result:
        return Result.created(request)

    registry = Registry().register("orders:place", place)
    assert (found := registry.find("orders:place")) is not None
    assert found.request_type is PlaceOrder


def test_explicit_request_type_wins_over_inference() -> None:
    @dataclass
    class Override:
        value: str = ""

    async def place(request: PlaceOrder) -> Result:
        return Result.ok()

    registry = Registry().register("orders:place", place, request_type=Override)
    assert (found := registry.find("orders:place")) is not None
    assert found.request_type is Override


def test_inferred_type_actually_maps_the_body_end_to_end() -> None:
    # The proof that inference is wired through: a JSON body is built into the inferred PlaceOrder.
    async def place(request: PlaceOrder) -> Result:
        assert isinstance(request, PlaceOrder)  # mapped, not a raw dict
        return Result.created({"sku": request.sku, "quantity": request.quantity})

    registry = Registry().register("orders:place", place)  # no request_type=
    app = BenzeneMessageApplication(registry, MiddlewarePipeline())
    response = run(
        app.handle({"topic": "orders:place", "headers": {}, "body": '{"sku": "A", "quantity": 3}'})
    )

    assert response["statusCode"] == Status.CREATED


def test_router_free_pipeline_still_maps_via_inference() -> None:
    async def place(request: PlaceOrder) -> Result:
        return Result.ok(request.sku)

    registry = Registry().register("orders:place", place)
    pipeline = MiddlewarePipeline().use(message_router(registry))
    ctx = Context("orders:place", {"sku": "Z"})
    run(pipeline.handle(ctx))
    assert ctx.result is not None and ctx.result.payload == "Z"
