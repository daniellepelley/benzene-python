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
    clear_schema_providers,
    decode_response,
    encode_body,
    encode_response,
    json_schema,
    message,
    register_schema_provider,
    schema_providers,
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
    assert (unversioned := registry.find("t")) is not None
    assert unversioned.handler is v1  # no version -> unversioned
    assert (versioned := registry.find("t", "2")) is not None
    assert versioned.handler is v2  # exact version match
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


# --- decode_response (the inverse of encode_response) -------------------------------------------


def test_decode_response_round_trips_a_successful_payload() -> None:
    envelope = encode_response(Result.created({"id": "o-1"}))
    result = decode_response(envelope)
    assert result.status == Status.CREATED
    assert result.payload == {"id": "o-1"}


def test_decode_response_round_trips_a_payload_less_success() -> None:
    result = decode_response(encode_response(Result.ok()))
    assert result.is_successful
    assert result.payload is None


def test_decode_response_round_trips_failure_errors() -> None:
    envelope = encode_response(Result.bad_request("sku is required", "quantity must be positive"))
    result = decode_response(envelope)
    assert result.status == Status.BAD_REQUEST
    assert result.messages == ("sku is required", "quantity must be positive")


def test_decode_response_prefers_the_envelopes_is_successful_over_the_status_text() -> None:
    # The section 1.2 rule that matters: an application-defined status means nothing to a receiver
    # classifying by string alone, so isSuccessful is what decides. Without it a success sent
    # through Result.set round-trips as a failure - and the payload is read as a problem document.
    envelope = encode_response(Result.set("cache-warm", {"entries": 12}, successful=True))
    assert envelope["isSuccessful"] is True

    result = decode_response(envelope)
    assert result.status == "cache-warm"
    assert result.is_successful
    assert result.payload == {"entries": 12}


def test_decode_response_honours_a_stated_failure_on_a_success_status() -> None:
    # The other direction, and why the fallback tests `is None` rather than truthiness.
    result = decode_response(encode_response(Result.set(Status.OK, {"draining": True}, False)))
    assert result.status == Status.OK
    assert not result.is_successful


def test_decode_response_falls_back_to_the_status_class_for_a_peer_without_is_successful() -> None:
    # An older peer, or a port that has not adopted the member: deriving from the status is then
    # the best available answer, and the pre-existing behaviour is unchanged.
    assert decode_response({"statusCode": Status.CREATED, "body": '{"id": "o-1"}'}).is_successful
    assert not decode_response({"statusCode": Status.CONFLICT, "body": ""}).is_successful


def test_decode_response_treats_a_malformed_body_as_unexpected_error_not_a_crash() -> None:
    result = decode_response({"statusCode": Status.OK, "body": "not json"})
    assert result.status == Status.UNEXPECTED_ERROR


def test_decode_response_defaults_a_missing_status_to_unexpected_error() -> None:
    result = decode_response({})
    assert result.status == Status.UNEXPECTED_ERROR
    assert result.payload is None


# --- T0.6: a raising middleware must not escape the pipeline -------------------------------------
# Only the terminal router used to catch. Anything a *middleware* raised (auth, tracing, mesh
# interception, rate limiting, a user-written middleware) propagated out of `MiddlewarePipeline.handle`
# and into whichever transport adapter was hosting it — each of which handles it differently, or not
# at all. The framework's promise is that request content never crashes the host, so containment
# belongs at the pipeline boundary, mapped exactly as the router maps a handler exception.


async def _boom(_context: Context, _next) -> None:  # noqa: ANN001 - the Next callable
    raise RuntimeError("middleware exploded")


def test_middleware_exception_is_contained_at_the_pipeline_boundary() -> None:
    pipeline = MiddlewarePipeline().use(_boom)
    context = Context("t", {})

    asyncio.run(pipeline.handle(context))  # must not raise

    assert context.result is not None
    assert context.result.status == Status.SERVICE_UNAVAILABLE


def test_middleware_exception_becomes_a_service_unavailable_envelope() -> None:
    app = BenzeneMessageApplication(Registry(), MiddlewarePipeline().use(_boom))
    response = asyncio.run(app.handle({"topic": "t", "headers": {}, "body": "{}"}))

    assert response["statusCode"] == "service-unavailable"
    assert response["isSuccessful"] is False
    body = json.loads(response["body"])
    # The structured-error shape the router already produces, not a bare string: one BenzeneError
    # carrying the exception's message, and the problem document derived from the status.
    assert body["benzeneStatus"] == "service-unavailable"
    assert [error["message"] for error in body["errors"]] == ["middleware exploded"]


def test_pipeline_containment_does_not_swallow_cancellation() -> None:
    """Cooperative cancellation is not a request fault — it must stay cancelled, so the transport
    redelivers rather than settling a fabricated failure."""

    async def cancel(_context: Context, _next) -> None:  # noqa: ANN001
        raise asyncio.CancelledError

    pipeline = MiddlewarePipeline().use(cancel)
    context = Context("t", {})

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(pipeline.handle(context))
    assert context.result is None  # nothing fabricated on the way out


def test_a_result_already_set_survives_a_later_middleware_failure() -> None:
    """A middleware that produced a result and then failed while unwinding keeps its result."""

    async def answered(context: Context, next) -> None:  # noqa: A002, ANN001
        context.result = Result.ok({"answered": True})
        await next()

    pipeline = MiddlewarePipeline().use(answered).use(_boom)
    context = Context("t", {})

    asyncio.run(pipeline.handle(context))

    assert context.result is not None
    assert context.result.status == Status.OK


# --- T0.4: the schema-provider seam --------------------------------------------------------------
# `json_schema` used to answer `{}` for every type its table does not name, which silently included
# the pydantic BaseModel this port ships an adapter for. Core cannot import pydantic (an optional
# adoption choice the capability matrix is explicit about), so a provider may claim a type before
# the built-in rules run and `benzene.pydantic` registers one on import. These tests cover the seam
# itself; `tests/test_pydantic.py` covers the documents it fixes.


@pytest.fixture
def isolated_providers():  # noqa: ANN201 - a pytest fixture
    """Run with an empty provider list, then restore whatever was registered (importing
    ``benzene.pydantic`` anywhere in the session registers a provider process-wide)."""
    registered = schema_providers()
    clear_schema_providers()
    yield
    clear_schema_providers()
    for provider in registered:
        register_schema_provider(provider)


class _Opaque:
    """A type `json_schema`'s table cannot name — the case that used to be lost."""


def test_an_unclaimed_type_is_still_the_open_schema(isolated_providers) -> None:  # noqa: ANN001
    assert json_schema(_Opaque) == {}


def test_a_provider_claims_a_type_the_built_in_table_cannot_name(isolated_providers) -> None:  # noqa: ANN001
    register_schema_provider(
        lambda t: {"type": "object", "properties": {"id": {"type": "string"}}}
        if t is _Opaque
        else None
    )
    assert json_schema(_Opaque)["properties"] == {"id": {"type": "string"}}


def test_a_provider_reaches_a_type_nested_in_a_container(isolated_providers) -> None:  # noqa: ANN001
    """Core recurses into `list`/`dict`/optional itself, so a provider need only know the leaf."""
    register_schema_provider(lambda t: {"type": "string"} if t is _Opaque else None)

    assert json_schema(list[_Opaque]) == {"type": "array", "items": {"type": "string"}}
    assert json_schema(_Opaque | None) == {"type": ["string", "null"]}


def test_providers_are_consulted_in_order_and_the_first_answer_wins(isolated_providers) -> None:  # noqa: ANN001
    register_schema_provider(lambda t: {"type": "integer"} if t is _Opaque else None)
    register_schema_provider(lambda t: {"type": "boolean"} if t is _Opaque else None)

    assert json_schema(_Opaque) == {"type": "integer"}


def test_a_provider_may_override_a_built_in_rule(isolated_providers) -> None:  # noqa: ANN001
    """Providers run *before* the primitive/dataclass table — that is what makes a hand-authored
    schema catalogue expressible on the same seam."""

    @dataclass
    class Authored:
        name: str = ""

    register_schema_provider(lambda t: {"type": "object", "x-authored": True} if t is Authored else None)
    assert json_schema(Authored) == {"type": "object", "x-authored": True}
