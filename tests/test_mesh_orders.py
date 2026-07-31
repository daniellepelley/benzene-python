"""Dogfood: a mesh-enabled service booted through the *public* harness (the gold-standard shape).

Proves the composition-root middleware seam end to end: a `MeshOrdersStartUp` installs
`mesh_interception` + `trace_middleware` in its `configure`, and every host builds the app from that
startup. So `create_test_host(MeshOrdersStartUp).with_services(...).build_aws()` yields a service that

- answers `GET /benzene/spec` with its `ServiceDescriptor` (the reserved `benzene:mesh` topic,
  surfaced over HTTP), and
- emits one `TraceEvent` per invocation to an exporter reachable via `host.scope`,

with the *same* setup an adopter would write — only `build_aws()` names the cloud. The descriptor is
still derived from the real order registry (not a toy).
"""

from __future__ import annotations

import json

from benzene.core import AppDefinition, MessageSender
from benzene.mesh import (
    MESH_TOPIC,
    InMemoryTraceExporter,
    ServiceDescriptor,
    ServiceInfo,
    TraceExporter,
    mesh_interception,
    trace_middleware,
)
from benzene.results import Result
from benzene.testing import FakeMessageSender, create_test_host

from orders_domain.startup import OrdersStartUp

_SERVICE_INFO = ServiceInfo(service="orders", service_version="1.4.2", placement={"cloud": "aws"})


async def _unreachable(_request: dict) -> Result:  # /benzene/spec is answered by mesh_interception
    return Result.not_found("mesh interception should have handled benzene:mesh")


class MeshOrdersStartUp(OrdersStartUp):
    """`OrdersStartUp` + mesh — self-description on `GET /benzene/spec` and a trace per invocation."""

    def configure_services(self, services, config):  # type: ignore[override]
        super().configure_services(services, config)
        services.try_add_singleton(TraceExporter, lambda _scope: InMemoryTraceExporter())

    def configure(self, services, config):  # type: ignore[override]
        base = super().configure(services, config)
        descriptor = ServiceDescriptor.derive(base.registry, _SERVICE_INFO)
        # A convenience HTTP surface for the reserved topic; mesh_interception answers it.
        base.router.register("GET", "/benzene/spec", MESH_TOPIC, _unreachable)
        exporter = services.get_service(TraceExporter)
        return AppDefinition(
            registry=base.registry,
            router=base.router,
            middleware=[
                trace_middleware(exporter, service="orders", instance_id="orders-7f9c"),
                mesh_interception(descriptor),
            ],
        )


def _host():
    fake = FakeMessageSender()
    host = (
        create_test_host(MeshOrdersStartUp)
        .with_services(lambda services: services.add_instance(MessageSender, fake))
        .build_aws()
    )
    return host, fake


def test_orders_descriptor_reflects_the_real_topics() -> None:
    descriptor = ServiceDescriptor.derive(
        OrdersStartUp().configure(_scope_for(OrdersStartUp()), {}).registry, _SERVICE_INFO
    )
    payload = descriptor.to_payload()
    assert {t["id"] for t in payload["topics"]} == {"orders:place", "orders:get", "orders:created"}
    created = next(t for t in payload["topics"] if t["id"] == "orders:created")
    assert created["requestSchema"]["required"] == ["id", "sku"]


def test_mesh_enabled_service_answers_benzene_spec_through_the_harness() -> None:
    host, _ = _host()
    response = host.send_http("GET", "/benzene/spec")
    assert response.status_code == 200
    body = json.loads(response.body)
    assert body["service"] == "orders"
    assert body["descriptorHash"].startswith("sha256:")
    # the reserved topic is NOT itself a declared topic in the descriptor
    assert {t["id"] for t in body["topics"]} == {"orders:place", "orders:get", "orders:created"}


def test_mesh_enabled_service_traces_a_real_order_through_the_harness() -> None:
    host, fake = _host()
    result = host.send_sqs(
        "orders:place", {"sku": "ABC", "quantity": 2}, headers={"x-correlation-id": "corr-42"}
    )
    assert result.batch_item_failures == []       # transport response
    assert fake.last_topic == "orders:created"     # egress proven

    exporter = host.scope.get_service(TraceExporter)  # reachable via the exposed root scope
    order_traces = [e for e in exporter if e.topic == "orders:place"]
    assert len(order_traces) == 1
    assert order_traces[0].status == "created"
    assert order_traces[0].correlation_id == "corr-42"


def _scope_for(startup: OrdersStartUp):
    from benzene.core import Container

    container = Container()
    startup.configure_services(container, {})
    return container.create_scope()
