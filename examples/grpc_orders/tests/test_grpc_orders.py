"""Dogfooded tests for the gRPC orders example.

Boot the real app from ``OrdersStartUp`` (the same composition root as the cloud examples) and drive
it through the gRPC front door with the shared harness — `create_test_host(...).build_grpc()` and
`send_grpc`, the *identical* setup to the AWS/GCP/Azure suites bar the one specialization call and the
`send_*` shape. Only the outbound edge is faked (`FakeMessageSender`), proving ingress → handler →
egress. One test at the bottom also exercises a *real* gRPC channel, to prove the `GrpcMessageSender`
client binding works over a live socket (the transport half, which the in-memory harness deliberately
does not touch).
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("grpc")  # the transport needs grpcio (the benzene-grpc[transport] extra)

from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host

from orders_domain import ORDER_CREATED_TOPIC, ORDER_EVENTS_KEY, OrderService, OrdersStartUp
from orders_domain.model import OrderCreated


def make_host():
    """Boot the real app from OrdersStartUp, fake only the edges, specialize to gRPC.

    Note the setup is identical to the GCP/AWS/Azure suites except the single ``.build_grpc()`` call
    and the ``send_grpc`` shape — the test-champion consistency law in action.
    """
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)      # override ANY registration...
        services.add_instance(MessageSender, sender)       # ...only the external edge is faked
        services.add_instance(ORDER_EVENTS_KEY, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_grpc()
    return host, service, sender, seen


def test_place_order_creates_and_publishes() -> None:
    host, service, sender, _ = make_host()

    reply = host.send_grpc("orders:place", body={"sku": "ABC", "quantity": 2})

    assert reply.status == "created"
    assert reply.payload["sku"] == "ABC"
    assert reply.payload["quantity"] == 2
    # Egress: the handler published OrderCreated carrying the new order's id (ingress->handler->egress).
    assert sender.last_topic == ORDER_CREATED_TOPIC
    assert isinstance(sender.last_message, OrderCreated)
    assert sender.last_message.id == reply.payload["id"]
    assert reply.payload["id"] in service.orders


def test_place_order_validates_sku() -> None:
    host, _, sender, _ = make_host()
    reply = host.send_grpc("orders:place", body={"quantity": 1})
    assert reply.status == "bad-request"
    assert not sender.sent  # nothing published on a rejected request


def test_get_order_round_trip() -> None:
    host, _, _, _ = make_host()
    created = host.send_grpc("orders:place", body={"sku": "XYZ"})

    fetched = host.send_grpc("orders:get", body={"id": created.payload["id"]})

    assert fetched.status == "ok"
    assert fetched.payload["id"] == created.payload["id"]


def test_get_unknown_order_maps_not_found() -> None:
    host, _, _, _ = make_host()
    reply = host.send_grpc("orders:get", body={"id": "does-not-exist"})
    # not-found survives verbatim via the benzene-status trailer, and the gRPC code is set on the wire.
    assert reply.status == "not-found"
    assert reply.code is not None  # a failure sets the mapped grpc.StatusCode too


def test_unknown_topic_is_not_found() -> None:
    host, _, sender, _ = make_host()
    reply = host.send_grpc("orders:nope", body={})
    assert reply.status == "not-found"
    assert not sender.sent  # nothing published for an unroutable message


def test_real_channel_round_trip() -> None:
    """The one socketed test: prove the GrpcMessageSender *client* binding works over a live channel.

    This is the transport half the in-memory harness above does not touch — a real server, a real
    channel, the client's encode → metadata → trailer-parse path end to end.
    """
    import grpc

    from benzene.grpc import GrpcMessageSender

    from grpc_orders import build_grpc_orders_server
    from orders_domain import PlaceOrder

    sender = FakeMessageSender()
    server, port = build_grpc_orders_server("localhost:0", sender=sender)
    server.start()
    channel = grpc.insecure_channel(f"localhost:{port}")
    try:
        client = GrpcMessageSender(channel)
        result = asyncio.run(client.send_message("orders:place", PlaceOrder(sku="ABC", quantity=2)))
        assert result.status == "created"
        assert result.payload["sku"] == "ABC"
        assert sender.last_topic == ORDER_CREATED_TOPIC  # egress crossed the real hop
    finally:
        channel.close()
        server.stop(grace=None)
