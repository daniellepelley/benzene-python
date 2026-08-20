"""The gRPC transport: a real in-process server round-trip (skipped without grpcio)."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

grpc = pytest.importorskip("grpc")

from benzene.core import (  # noqa: E402
    BenzeneMessageApplication,
    Context,
    MiddlewarePipeline,
    Registry,
    message,
)
from benzene.grpc import (  # noqa: E402
    BENZENE_STATUS_TRAILER,
    GRPC_DETAILS_TRAILER,
    GrpcMessageSender,
    add_benzene_handler,
    method_for,
)
from benzene.grpc.details import DETAILS_SUPPORTED  # noqa: E402
from benzene.results import BenzeneError, Result  # noqa: E402


@message("orders:place")
async def place(request: dict) -> Result:
    if not request.get("sku"):
        return Result.bad_request("sku is required")
    return Result.created({"sku": request["sku"]})


@message("orders:get")
async def get_order(request: dict) -> Result:
    return Result.not_found(f"no order {request.get('id')!r}")


@contextmanager
def _serving(application: BenzeneMessageApplication):
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    add_benzene_handler(server, application)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield channel
    finally:
        channel.close()
        server.stop(None)


def _orders_app() -> BenzeneMessageApplication:
    return BenzeneMessageApplication(Registry().add(place).add(get_order))


def test_success_round_trip_preserves_the_exact_status() -> None:
    with _serving(_orders_app()) as channel:
        result = asyncio.run(GrpcMessageSender(channel).send_message("orders:place", {"sku": "ABC"}))
    # created and ok both map to gRPC OK — the benzene-status trailer preserves "created".
    assert result.status == "created"
    assert result.payload == {"sku": "ABC"}


def test_failure_maps_status_and_detail() -> None:
    with _serving(_orders_app()) as channel:
        result = asyncio.run(GrpcMessageSender(channel).send_message("orders:place", {}))
    assert result.status == "bad-request"          # -> gRPC InvalidArgument, trailer says bad-request
    assert "sku is required" in " ".join(result.messages)


def test_not_found_round_trip() -> None:
    with _serving(_orders_app()) as channel:
        result = asyncio.run(GrpcMessageSender(channel).send_message("orders:get", {"id": "x"}))
    assert result.status == "not-found"


def test_headers_propagate_as_request_metadata() -> None:
    seen: dict = {}

    async def capture(context: Context, next) -> None:
        seen["corr"] = context.headers.get("x-correlation-id")
        await next()

    app = BenzeneMessageApplication(Registry().add(place), MiddlewarePipeline().use(capture))
    with _serving(app) as channel:
        asyncio.run(
            GrpcMessageSender(channel).send_message(
                "orders:place", {"sku": "A"}, headers={"x-correlation-id": "c1"}
            )
        )
    assert seen["corr"] == "c1"  # forwarded as gRPC metadata, read off the context


@message("health:report")
async def health_report(request: dict) -> Result:
    # The section 1.3 escape hatch: an application-defined status the handler classifies itself.
    return Result.set("cache-warm", {"entries": 12}, successful=True)


def test_an_application_defined_status_marked_successful_round_trips_as_a_success() -> None:
    # section 4.2: an unknown status maps by isSuccessful, so this is OK and not Internal. Deriving
    # the classification from the status text instead would answer Internal and lose the payload.
    app = BenzeneMessageApplication(Registry().add(health_report))
    with _serving(app) as channel:
        result = asyncio.run(GrpcMessageSender(channel).send_message("health:report", {}))
    assert result.status == "cache-warm"
    assert result.is_successful
    assert result.payload == {"entries": 12}


# --- structured errors over the grpc-status-details-bin trailer (wire-contracts.md 4.2) ----------

needs_details = pytest.mark.skipif(
    not DETAILS_SUPPORTED,
    reason="structured gRPC details need googleapis-common-protos (the [transport] extra)",
)


@message("orders:submit")
async def submit(request: dict) -> Result:
    # Two errors that differ in the one thing a flattened detail string cannot carry: the field.
    return Result.validation_error(
        BenzeneError("sku is required", field="sku", code="missing"),
        BenzeneError("quantity must be positive", field="quantity", code="greater_than"),
    )


@message("orders:cancel")
async def cancel(request: dict) -> Result:
    # A failure with nothing structured at all: no errors, so no detail either.
    return Result.failure("conflict")


def _details_app() -> BenzeneMessageApplication:
    return BenzeneMessageApplication(Registry().add(submit).add(cancel).add(place))


def _raw_trailers(channel, topic: str, body: bytes = b"{}"):
    """The trailing metadata of a call, as a dict — the wire itself, not the client's reading of it."""
    invoke = channel.unary_unary(method_for(topic))
    try:
        _, call = invoke.with_call(body)
    except grpc.RpcError as exc:
        return dict(exc.trailing_metadata() or ())
    return dict(call.trailing_metadata() or ())


@needs_details
def test_structured_errors_survive_the_grpc_hop_with_their_fields() -> None:
    # The bug this covers: gRPC discards the body of a non-OK call, so a problem document's `errors`
    # reached the caller as one opaque prose string and the field a validator knew was gone.
    with _serving(_details_app()) as channel:
        result = asyncio.run(GrpcMessageSender(channel).send_message("orders:submit", {}))
    assert result.status == "validation-error"
    assert [(error.message, error.field) for error in result.errors] == [
        ("sku is required", "sku"),
        ("quantity must be positive", "quantity"),
    ]
    # `code` is deliberately not carried: section 4.2 does not say where it goes, and three ports
    # each inventing a home for it is the divergence the spec exists to prevent.
    assert all(error.code is None for error in result.errors)


@needs_details
def test_the_details_trailer_is_a_conformant_google_rpc_status() -> None:
    # A peer that isn't Benzene reads this trailer with the stock reader, which rejects a
    # google.rpc.Status whose code or message disagrees with the call's own - so this asserts the
    # shape is interoperable, not merely round-trippable by our own client.
    rpc_status = pytest.importorskip("grpc_status.rpc_status")
    error_details_pb2 = pytest.importorskip("google.rpc.error_details_pb2")

    with _serving(_details_app()) as channel:
        invoke = channel.unary_unary(method_for("orders:submit"))
        with pytest.raises(grpc.RpcError) as caught:
            invoke.with_call(b"{}")
        status = rpc_status.from_call(caught.value)

    assert status.code == grpc.StatusCode.INVALID_ARGUMENT.value[0]
    assert status.message == "sku is required, quantity must be positive"
    bad_request = error_details_pb2.BadRequest()
    assert status.details[0].Unpack(bad_request)
    assert [(v.field, v.description) for v in bad_request.field_violations] == [
        ("sku", "sku is required"),
        ("quantity", "quantity must be positive"),
    ]


@needs_details
def test_a_failure_without_structured_errors_falls_back_to_the_message_alone() -> None:
    with _serving(_details_app()) as channel:
        result = asyncio.run(GrpcMessageSender(channel).send_message("orders:cancel", {}))
    assert result.status == "conflict"
    # No BadRequest to read, so the client keeps exactly the behaviour it had: the call's details
    # string as one message-only error, with no invented field.
    assert result.messages == ("conflict",)
    assert result.errors[0].field is None


def test_a_success_attaches_no_details_trailer() -> None:
    # The trailer is a failure-only concern; a success carries benzene-status and nothing more.
    with _serving(_details_app()) as channel:
        trailers = _raw_trailers(channel, "orders:place", b'{"sku": "ABC"}')
    assert trailers.get(BENZENE_STATUS_TRAILER) == "created"
    assert GRPC_DETAILS_TRAILER not in trailers


@needs_details
def test_a_failure_attaches_both_trailers() -> None:
    with _serving(_details_app()) as channel:
        trailers = _raw_trailers(channel, "orders:submit")
    # The benzene-status trailer keeps working unchanged alongside the new one.
    assert trailers[BENZENE_STATUS_TRAILER] == "validation-error"
    assert isinstance(trailers[GRPC_DETAILS_TRAILER], bytes)
