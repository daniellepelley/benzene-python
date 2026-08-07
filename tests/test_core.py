"""Unit tests for the core behaviours the spec requires (independent of the wire fixtures)."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import pytest
from benzene.core import (
    BenzeneMessageApplication,
    Context,
    DuplicateHandlerError,
    MiddlewarePipeline,
    Registry,
    encode_body,
    message,
    to_camel,
    to_jsonable,
    to_request,
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
        app.handle({"topic": "say:hello", "headers": {}, "body": '{"name":"benzene"}'})
    )
    assert response["statusCode"] == Status.OK
    assert response["body"] == '{"greeting": "Hello benzene"}'


def test_handler_exception_becomes_service_unavailable() -> None:
    @message("boom")
    async def boom(_request: dict) -> Result:
        raise RuntimeError("kaboom")

    app = BenzeneMessageApplication(Registry().add(boom))
    response = asyncio.run(app.handle({"topic": "boom", "headers": {}, "body": "{}"}))
    assert response["statusCode"] == Status.SERVICE_UNAVAILABLE


# --- wire-contracts §6: payload naming policy (cross-language interop) ----------------------


@dataclass
class MultiWord:
    order_id: str = ""
    line_count: int = 0


def test_dataclass_payload_is_written_camelcase() -> None:
    assert to_camel("order_id") == "orderId"
    assert to_jsonable(MultiWord(order_id="o1", line_count=3)) == {"orderId": "o1", "lineCount": 3}
    assert encode_body(MultiWord("o1", 3)) == '{"orderId": "o1", "lineCount": 3}'


def test_dataclass_request_is_read_case_insensitively() -> None:
    # An inbound camelCase payload (from a .NET/Go/TS peer) populates snake_case Python fields.
    from_camel = to_request(MultiWord, {"orderId": "o1", "lineCount": 3})
    assert from_camel == MultiWord(order_id="o1", line_count=3)
    # snake_case and odd casing also work.
    assert to_request(MultiWord, {"order_id": "o2", "LINECOUNT": 5}) == MultiWord("o2", 5)


def test_to_request_passes_a_matching_instance_through_untouched() -> None:
    already = MultiWord("o1", 3)
    assert to_request(MultiWord, already) is already  # right type already: no copy, no re-map
    assert to_request(None, {"raw": 1}) == {"raw": 1}  # no declared type: the raw payload


def test_to_request_maps_a_dict_onto_a_non_dataclass_type() -> None:
    class Point:  # a plain class (not a dataclass) constructed from kwargs
        def __init__(self, x: int, y: int) -> None:
            self.x, self.y = x, y

    point = to_request(Point, {"x": 1, "y": 2})
    assert (point.x, point.y) == (1, 2)

    class Wrapper:  # **data fails (no such kwargs) -> falls back to passing the dict positionally
        def __init__(self, data: dict) -> None:
            self.data = data

    wrapped = to_request(Wrapper, {"a": 1})
    assert wrapped.data == {"a": 1}


def test_to_jsonable_passes_scalars_and_none_through() -> None:
    assert to_jsonable(None) is None
    assert to_jsonable(7) == 7
    assert to_jsonable("x") == "x"


def test_camelcase_payload_round_trips_through_the_envelope() -> None:
    @message("wire:echo", request_type=MultiWord)
    async def echo(request: MultiWord) -> Result:
        return Result.ok(request)

    app = BenzeneMessageApplication(Registry().add(echo))
    # Send camelCase in; expect camelCase back out.
    response = asyncio.run(
        app.handle(
            {"topic": "wire:echo", "headers": {}, "body": '{"orderId": "o9", "lineCount": 2}'}
        )
    )
    assert response["statusCode"] == Status.OK
    assert json.loads(response["body"]) == {"orderId": "o9", "lineCount": 2}


def test_registry_from_definitions_bridges_sources_and_chains() -> None:
    async def h(_request: dict) -> Result:
        return Result.ok()

    a = Registry().register("a:one", h)
    b = Registry().register("b:two", h)
    # merge two sources (anything with definitions()), then chain .register for a queue-only topic
    merged = Registry.from_definitions(a, b).register("c:three", h)
    assert {d.topic for d in merged.definitions()} == {"a:one", "b:two", "c:three"}


def test_registry_from_definitions_rejects_a_duplicate_pair() -> None:
    async def h(_request: dict) -> Result:
        return Result.ok()

    with pytest.raises(DuplicateHandlerError):
        Registry.from_definitions(Registry().register("dup", h), Registry().register("dup", h))


# --- malformed-input robustness (the entry point must return a Result, never crash) ----------


def test_malformed_json_body_is_bad_request_not_a_crash() -> None:
    async def h(_request: dict) -> Result:
        return Result.ok()

    app = BenzeneMessageApplication(Registry().register("t", h))
    response = asyncio.run(app.handle({"topic": "t", "headers": {}, "body": "{not json"}))
    assert response["statusCode"] == Status.BAD_REQUEST


def test_unmappable_request_is_bad_request_not_a_crash() -> None:
    @dataclass
    class Req:
        a: str
        b: int  # required, so a body omitting it can't be mapped

    async def h(_request: Req) -> Result:
        return Result.ok()

    app = BenzeneMessageApplication(Registry().register("t", h, request_type=Req))
    response = asyncio.run(app.handle({"topic": "t", "headers": {}, "body": '{"a": "x"}'}))
    assert response["statusCode"] == Status.BAD_REQUEST


def test_handler_exception_is_service_unavailable_not_bad_request() -> None:
    # The mapping-failure guard must not swallow a real handler error into bad-request.
    async def boom(_request: dict) -> Result:
        raise RuntimeError("kaboom")

    app = BenzeneMessageApplication(Registry().register("t", boom))
    response = asyncio.run(app.handle({"topic": "t", "headers": {}, "body": "{}"}))
    assert response["statusCode"] == Status.SERVICE_UNAVAILABLE


def test_to_jsonable_serializes_the_declared_wire_scalar_types() -> None:
    from datetime import datetime

    # The schema layer advertises these as valid wire types; the serializer must not crash on them.
    assert to_jsonable(datetime(2020, 1, 2, 3, 4, 5)) == "2020-01-02T03:04:05"
    assert to_jsonable(b"hi") == "aGk="  # base64
    assert sorted(to_jsonable({3, 1, 2})) == [1, 2, 3]  # set -> array


def test_datetime_payload_round_trips_through_a_response_envelope() -> None:
    from datetime import datetime

    @dataclass
    class Event:
        id: str
        at: datetime

    async def h(_request: dict) -> Result:
        return Result.ok(Event("e1", datetime(2020, 1, 1, 12, 0, 0)))

    app = BenzeneMessageApplication(Registry().register("t", h))
    response = asyncio.run(app.handle({"topic": "t", "headers": {}, "body": "{}"}))
    assert response["statusCode"] == Status.OK
    assert json.loads(response["body"]) == {"id": "e1", "at": "2020-01-01T12:00:00"}
