"""Unit tests for the core behaviours the spec requires (independent of the wire fixtures)."""

from __future__ import annotations

import asyncio

import pytest

from benzene.core import (
    BenzeneMessageApplication,
    Context,
    DuplicateHandlerError,
    MiddlewarePipeline,
    Registry,
    message,
)
from benzene.results import Result, Status


def test_result_success_classification() -> None:
    assert Result.ok().is_successful
    assert Result.created().is_successful
    assert not Result.not_found().is_successful
    # Unknown/extension statuses are failures.
    assert not Result("app-specific-status").is_successful


def test_registry_rejects_duplicate_topic_version() -> None:
    async def h(_req):  # noqa: ANN001
        return Result.ok()

    registry = Registry().register("order:create", h)
    with pytest.raises(DuplicateHandlerError):
        registry.register("order:create", h)


def test_registry_version_selection() -> None:
    async def v1(_req):  # noqa: ANN001
        return Result.ok("v1")

    async def v2(_req):  # noqa: ANN001
        return Result.ok("v2")

    registry = Registry().register("t", v1).register("t", v2, version="2")
    assert registry.find("t").handler is v1  # no version -> unversioned
    assert registry.find("t", "2").handler is v2  # exact version match
    assert registry.find("t", "9") is None  # no fuzzy fallback


def test_pipeline_runs_in_order_and_short_circuits() -> None:
    order: list[str] = []

    async def first(ctx: Context, nxt) -> None:  # noqa: ANN001
        order.append("first-in")
        await nxt()
        order.append("first-out")

    async def blocker(ctx: Context, nxt) -> None:  # noqa: ANN001
        order.append("blocker")
        # Does NOT call next -> short-circuits.

    async def never(ctx: Context, nxt) -> None:  # noqa: ANN001
        order.append("never")
        await nxt()

    pipeline = MiddlewarePipeline([first, blocker, never])
    asyncio.run(pipeline.handle(Context("t", {})))
    assert order == ["first-in", "blocker", "first-out"]  # onion order, "never" never runs


def test_message_decorator_registers_and_dispatches() -> None:
    @message("say:hello")
    async def hello(request: dict) -> Result:
        return Result.ok({"greeting": f"Hello {request['name']}"})

    app = BenzeneMessageApplication(Registry().add(hello))
    response = asyncio.run(
        app.handle_async({"topic": "say:hello", "headers": {}, "body": '{"name":"benzene"}'})
    )
    assert response["statusCode"] == Status.OK
    assert response["body"] == '{"greeting": "Hello benzene"}'


def test_handler_exception_becomes_service_unavailable() -> None:
    @message("boom")
    async def boom(_request: dict) -> Result:
        raise RuntimeError("kaboom")

    app = BenzeneMessageApplication(Registry().add(boom))
    response = asyncio.run(app.handle_async({"topic": "boom", "headers": {}, "body": "{}"}))
    assert response["statusCode"] == Status.SERVICE_UNAVAILABLE
