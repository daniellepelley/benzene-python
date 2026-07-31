"""Dogfood: the mesh module on the *real* order service (examples/orders_domain).

Proves mesh works on an actual Benzene app, not a toy: the descriptor is derived from the order
domain's real registry, the reserved ``benzene:mesh`` endpoint returns it, and a real
``orders:place`` invocation is both traced (one event, status ``created``) and produces the
``orders:created`` egress through a faked outbound client — ingress → handler → egress → trace.
"""

from __future__ import annotations

import asyncio
import json

from benzene.core import BenzeneMessageApplication, MiddlewarePipeline, Registry
from benzene.mesh import (
    MESH_TOPIC,
    InMemoryTraceExporter,
    ServiceDescriptor,
    ServiceInfo,
    mesh_interception,
    trace_middleware,
)
from benzene.testing import FakeMessageSender

from orders_domain.wiring import build_orders
from orders_domain.handlers import OrderService


def _orders_registry(fake: FakeMessageSender) -> Registry:
    return build_orders(OrderService(), fake, []).registry


def test_orders_descriptor_reflects_the_real_topics() -> None:
    descriptor = ServiceDescriptor.derive(
        _orders_registry(FakeMessageSender()),
        ServiceInfo(service="orders", service_version="1.4.2", placement={"cloud": "aws"}),
    )
    payload = descriptor.to_payload()
    assert {t["id"] for t in payload["topics"]} == {"orders:place", "orders:get", "orders:created"}

    place = next(t for t in payload["topics"] if t["id"] == "orders:place")
    assert set(place["requestSchema"]["properties"]) == {"sku", "quantity"}
    created = next(t for t in payload["topics"] if t["id"] == "orders:created")
    assert created["requestSchema"]["required"] == ["id", "sku"]


def test_orders_service_joins_the_mesh_and_traces_a_real_order() -> None:
    fake = FakeMessageSender()
    registry = _orders_registry(fake)
    descriptor = ServiceDescriptor.derive(
        registry, ServiceInfo(service="orders", service_version="1.4.2", placement={"cloud": "aws"})
    )
    exporter = InMemoryTraceExporter()
    pipeline = (
        MiddlewarePipeline()
        .use(trace_middleware(exporter, service="orders", instance_id="orders-7f9c"))
        .use(mesh_interception(descriptor))
    )
    app = BenzeneMessageApplication(registry, pipeline)

    # 1. the reserved topic returns this service's descriptor
    mesh = asyncio.run(app.handle({"topic": MESH_TOPIC, "headers": {}, "body": ""}))
    assert mesh["statusCode"] == "ok"
    assert json.loads(mesh["body"])["service"] == "orders"

    # 2. a real order is placed: handler runs, egress fires, and the invocation is traced
    placed = asyncio.run(
        app.handle(
            {
                "topic": "orders:place",
                "headers": {"x-correlation-id": "corr-42"},
                "body": '{"sku": "ABC", "quantity": 2}',
            }
        )
    )
    assert placed["statusCode"] == "created"
    assert fake.last_topic == "orders:created"          # egress proven

    order_traces = [e for e in exporter if e.topic == "orders:place"]
    assert len(order_traces) == 1
    assert order_traces[0].status == "created"
    assert order_traces[0].correlation_id == "corr-42"  # trace carries the correlation id
