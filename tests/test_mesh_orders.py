"""Dogfood: an *operational* service booted through the *public* harness (the gold-standard shape).

Proves the composition-root seam end to end: a `MeshOrdersStartUp` declares its self-description (a
`ContractInfo`, so `benzene:spec` answers with the Contract Document) and installs the cross-cutting
stack — `trace_middleware`, `health_interception` — in its `configure`, and every host builds the app
from that startup. So `create_test_host(MeshOrdersStartUp).with_services(...).build_aws()` yields a
service that

- answers `GET /benzene/spec` with its Contract Document — the very body it registers with a mesh
  collector (mesh.md §2),
- answers `GET /benzene/health` with its health aggregate (the reserved `benzene:healthcheck` topic), and
- emits one `TraceEvent` per invocation to an exporter reachable via `host.scope`,

with the *same* setup an adopter would write — only `build_aws()` names the cloud. The document is
derived from the real order registry (not a toy).
"""

from __future__ import annotations

import json
from dataclasses import replace

from benzene.core import (
    HEALTH_TOPIC,
    ContractInfo,
    HealthChecks,
    MessageSender,
    health_interception,
)
from benzene.http import StandardPaths
from benzene.mesh import (
    InMemoryTraceExporter,
    TraceExporter,
    registration_document,
    trace_middleware,
)
from benzene.results import Result
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain.startup import OrdersStartUp

_CONTRACT = ContractInfo(
    "orders", service_version="1.4.2", version_scheme="semver", placement={"cloud": "aws"}
)


async def _intercepted(
    _request: dict,
) -> Result:  # the reserved-topic routes are answered by middleware
    return Result.not_found("a reserved-topic interceptor should have handled this")


class MeshOrdersStartUp(OrdersStartUp):
    """`OrdersStartUp` made operational: its self-description, a trace per invocation, and a health
    endpoint — the whole cross-cutting stack installed once, in the composition root."""

    def configure_services(self, services, config):
        super().configure_services(services, config)
        services.try_add_singleton(TraceExporter, lambda _scope: InMemoryTraceExporter())

    def configure(self, services, config):
        base = super().configure(services, config)
        health = HealthChecks().add("order-store", lambda: True)
        base.router.register("GET", "/benzene/health", HEALTH_TOPIC, _intercepted)
        exporter = services.get_service(TraceExporter)
        # Add the self-description and cross-cutting middleware to whatever the base startup
        # produced — `replace` keeps the base's registry/router (and any future field).
        return replace(
            base,
            contract=_CONTRACT,
            standard_paths=StandardPaths(),
            middleware=[
                trace_middleware(exporter, service="orders", instance_id="orders-7f9c"),
                health_interception(health),
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


def _definition():
    startup = MeshOrdersStartUp()
    return startup.configure(_scope_for(startup), {})


def test_orders_registration_reflects_the_real_topics() -> None:
    payload = registration_document(_definition()).to_payload()
    assert {r["topic"] for r in payload["requests"]} == {
        "orders:place",
        "orders:get",
        "orders:created",
    }
    created = next(r for r in payload["requests"] if r["topic"] == "orders:created")
    assert created["request"]["$ref"].startswith("#/components/schemas/")


def test_mesh_enabled_service_answers_benzene_spec_through_the_harness() -> None:
    host, _ = _host()
    response = host.send_http("GET", "/benzene/spec")
    assert response.status_code == 200
    body = json.loads(response.body)
    assert body["service"] == "orders"
    assert body["registrationHash"].startswith("sha256:")
    # the reserved topic is NOT itself a declared topic in the document
    assert {r["topic"] for r in body["requests"]} == {"orders:place", "orders:get", "orders:created"}
    # ...and it is exactly what the service registers with a collector
    assert body == registration_document(_definition()).to_payload()


def test_operational_service_answers_benzene_health_through_the_harness() -> None:
    host, _ = _host()
    response = host.send_http("GET", "/benzene/health")
    assert response.status_code == 200  # healthy -> ok -> 200
    body = json.loads(response.body)
    assert body["isHealthy"] is True
    assert body["healthChecks"]["order-store"]["status"] == "ok"


def test_mesh_enabled_service_traces_a_real_order_through_the_harness() -> None:
    host, fake = _host()
    result = host.send_sqs(
        "orders:place", {"sku": "ABC", "quantity": 2}, headers={"x-correlation-id": "corr-42"}
    )
    assert result.batch_item_failures == []  # transport response
    assert fake.last_topic == "orders:created"  # egress proven

    exporter = host.scope.get_service(TraceExporter)  # reachable via the exposed root scope
    order_traces = [e for e in exporter if e.topic == "orders:place"]
    assert len(order_traces) == 1
    assert order_traces[0].status == "created"
    assert order_traces[0].correlation_id == "corr-42"


def _scope_for(startup: OrdersStartUp):
    from benzene.core import Container

    container = Container()
    startup.configure_services(container, {})
    container.add_instance(MessageSender, FakeMessageSender())  # the one dependency a host supplies
    return container.create_scope()
