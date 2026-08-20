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
    assert "sku is required" in " ".join(result.errors)


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


# --- the sender never raises (C5) -----------------------------------------------------------


class _RaisingChannel:
    """A duck-typed ``grpc.Channel`` whose call blows up the way a broken channel really does."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def unary_unary(self, method: str):  # noqa: ANN202 - duck-typed grpc surface
        exc = self._exc

        class _Callable:
            def with_call(self, request, metadata=()):  # noqa: ANN001, ANN202
                raise exc

        return _Callable()


def test_a_closed_channel_becomes_a_service_unavailable_result() -> None:
    # grpc raises ValueError (not RpcError) when the channel is already closed; the sender port
    # promises a Result either way, so a retrying sender can see it as a status.
    with _serving(_orders_app()) as channel:
        pass  # the context manager closes the channel on exit
    result = asyncio.run(GrpcMessageSender(channel).send_message("orders:place", {"sku": "A"}))
    assert result.status == "service-unavailable"


def test_any_channel_exception_becomes_a_service_unavailable_result() -> None:
    sender = GrpcMessageSender(_RaisingChannel(RuntimeError("channel exploded")))
    result = asyncio.run(sender.send_message("orders:place", {}))
    assert result.status == "service-unavailable"
    assert "channel exploded" in " ".join(result.errors)


def test_channel_timeout_becomes_a_timeout_result() -> None:
    sender = GrpcMessageSender(_RaisingChannel(TimeoutError("deadline")))
    result = asyncio.run(sender.send_message("orders:place", {}))
    assert result.status == "timeout"


def test_a_missing_sdk_still_raises_importerror() -> None:
    # A deployment error to fix, not a transport blip to retry (the sender-wide rule).
    sender = GrpcMessageSender(_RaisingChannel(ImportError("No module named 'grpc._cython'")))
    with pytest.raises(ImportError):
        asyncio.run(sender.send_message("orders:place", {}))
