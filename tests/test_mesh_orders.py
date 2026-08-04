"""Dogfood: an *operational* service booted through the *public* harness (the gold-standard shape).

Proves the composition-root middleware seam end to end: a `MeshOrdersStartUp` installs the whole
cross-cutting stack — `trace_middleware`, `health_interception`, `mesh_interception` — in its
`configure`, and every host builds the app from that startup. So
`create_test_host(MeshOrdersStartUp).with_services(...).build_aws()` yields a service that

- answers `GET /benzene/spec` with its `ServiceDescriptor` (the reserved `benzene:mesh` topic),
- answers `GET /benzene/health` with its health aggregate (the reserved `benzene:healthcheck` topic), and
- emits one `TraceEvent` per invocation to an exporter reachable via `host.scope`,

with the *same* setup an adopter would write — only `build_aws()` names the cloud. The descriptor is
still derived from the real order registry (not a toy).
"""

from __future__ import annotations

import json
from dataclasses import replace

from benzene.core import (
    HEALTH_TOPIC,
    HealthChecks,
    MessageSender,
    health_interception,
)
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


async def _intercepted(_request: dict) -> Result:  # the reserved-topic routes are answered by middleware
    return Result.not_found("a reserved-topic interceptor should have handled this")


class MeshOrdersStartUp(OrdersStartUp):
    """`OrdersStartUp` made operational: mesh self-description, a trace per invocation, and a health
    endpoint — the whole cross-cutting stack installed once, in the composition root."""

    def configure_services(self, services, config):  # type: ignore[override]
        super().configure_services(services, config)
        services.try_add_singleton(TraceExporter, lambda _scope: InMemoryTraceExporter())

    def configure(self, services, config):  # type: ignore[override]
        base = super().configure(services, config)
        descriptor = ServiceDescriptor.derive(base.registry, _SERVICE_INFO)
        health = HealthChecks().add("order-store", lambda: True)
        # Convenience HTTP surfaces for the reserved topics; the interceptors answer them.
        base.router.register("GET", "/benzene/spec", MESH_TOPIC, _intercepted)
        base.router.register("GET", "/benzene/health", HEALTH_TOPIC, _intercepted)
        exporter = services.get_service(TraceExporter)
        # Add cross-cutting middleware to whatever the base startup produced — `replace` keeps the
        # base's registry/router (and any future field) instead of re-threading them by hand.
        return replace(
            base,
            middleware=[
                trace_middleware(exporter, service="orders", instance_id="orders-7f9c"),
                health_interception(health),
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


def test_operational_service_answers_benzene_health_through_the_harness() -> None:
    host, _ = _host()
    response = host.send_http("GET", "/benzene/health")
    assert response.status_code == 200                         # healthy -> ok -> 200
    body = json.loads(response.body)
    assert body["isHealthy"] is True
    assert body["healthChecks"]["order-store"]["isHealthy"] is True


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
