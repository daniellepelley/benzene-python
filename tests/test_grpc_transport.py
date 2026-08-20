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
from benzene.grpc import GrpcMessageSender, add_benzene_handler  # noqa: E402
from benzene.results import Result  # noqa: E402


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
