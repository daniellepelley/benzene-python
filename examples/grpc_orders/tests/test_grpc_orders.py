"""Dogfooded tests for the gRPC orders example — a *real* in-process gRPC round trip.

Boot the real app from ``OrdersStartUp`` (same composition root as the cloud examples), serve it over
an in-process gRPC server on an ephemeral port, and drive it through the front door with the actual
``GrpcMessageSender`` client. Only the outbound edge is faked (``FakeMessageSender``), proving
ingress → handler → egress over gRPC — the method name *is* the topic, so ``orders:place`` /
``orders:get`` are reached with no route table.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("grpc")  # the transport needs grpcio (the benzene-grpc[transport] extra)

from benzene.core import MessageSender
from benzene.grpc import GrpcMessageSender
from benzene.testing import FakeMessageSender

from grpc_orders import build_grpc_orders_server
from orders_domain import ORDER_CREATED_TOPIC, OrderService, PlaceOrder
from orders_domain.model import OrderCreated


@pytest.fixture()
def grpc_orders():
    """A running gRPC server for the order domain + a client dialled at it, with a faked egress."""
    import grpc

    service = OrderService()
    sender = FakeMessageSender()
    server, port = build_grpc_orders_server(
        "localhost:0", service=service, sender=sender
    )
    server.start()
    channel = grpc.insecure_channel(f"localhost:{port}")
    client = GrpcMessageSender(channel)
    try:
        yield client, service, sender
    finally:
        channel.close()
        server.stop(grace=None)


def _send(client: GrpcMessageSender, topic: str, message, headers=None):
    return asyncio.run(client.send_message(topic, message, headers=headers))


def test_place_order_creates_and_publishes(grpc_orders) -> None:
    client, service, sender = grpc_orders

    result = _send(client, "orders:place", PlaceOrder(sku="ABC", quantity=2))

    assert result.status == "created"
    assert result.payload["sku"] == "ABC"
    assert result.payload["quantity"] == 2
    # Egress: the handler published OrderCreated over the injected (faked) sender — the same
    # ingress→handler→egress path the cloud examples prove, here across a real gRPC hop.
    assert sender.last_topic == ORDER_CREATED_TOPIC
    assert isinstance(sender.last_message, OrderCreated)
    assert sender.last_message.id == result.payload["id"]
    assert result.payload["id"] in service.orders


def test_get_order_round_trip(grpc_orders) -> None:
    client, _, _ = grpc_orders

    created = _send(client, "orders:place", PlaceOrder(sku="XYZ"))
    fetched = _send(client, "orders:get", {"id": created.payload["id"]})

    assert fetched.status == "ok"
    assert fetched.payload["id"] == created.payload["id"]


def test_get_unknown_order_maps_not_found_over_grpc(grpc_orders) -> None:
    client, _, _ = grpc_orders

    result = _send(client, "orders:get", {"id": "does-not-exist"})

    # not-found survives the gRPC hop verbatim via the benzene-status trailer (it maps to NOT_FOUND).
    assert result.status == "not-found"


def test_unknown_topic_is_not_found(grpc_orders) -> None:
    client, _, sender = grpc_orders

    result = _send(client, "orders:nope", {})

    assert result.status == "not-found"
    assert not sender.sent  # nothing published for an unroutable message
