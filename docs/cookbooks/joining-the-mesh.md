# Joining the mesh (self-description + tracing + collector feeds)

Take an existing Benzene service — here the order domain, unchanged — and make it show up in a
**mesh**: it describes itself on the reserved `benzene:mesh` topic, traces every invocation, and
reports its descriptor, heartbeats, traces, and issues into a collector. None of this touches the handlers;
mesh is additive middleware plus an outbound feed.

## Prerequisites

- Python 3.10+
- `pip install benzene-mesh` (installs `benzene-core`; add `benzene-testing` for the fakes below)
- An existing service with a `benzene.core` `Registry` — this walkthrough reuses the order domain from
  the [examples](https://github.com/daniellepelley/benzene-python/tree/main/examples) (`build_orders`).

## 1. Derive the descriptor from the real registry

The descriptor is **derived**, never written by hand — `derive()` reads the registry, so it is always
the truth of what the service serves. Give it the identity and placement the registry can't know:

```python
# mesh_wiring.py
from benzene.mesh import ServiceDescriptor, ServiceInfo
from orders_domain.wiring import build_orders
from orders_domain.handlers import OrderService

def build_descriptor(registry) -> ServiceDescriptor:
    return ServiceDescriptor.derive(
        registry,
        ServiceInfo(
            service="orders",
            service_version="1.4.2",
            instance_id="orders-7f9c",
            placement={"cloud": "aws", "region": "eu-west-1"},
        ),
    )
```

The derived descriptor carries one topic entry per registered topic — for the order domain that is
`orders:place`, `orders:get`, and `orders:created` — each with the request/response JSON Schema taken
from the handler's declared types, and a `descriptorHash` over the contract:

```python
from benzene.testing import FakeMessageSender

registry = build_orders(OrderService(), FakeMessageSender()).registry
descriptor = build_descriptor(registry)

payload = descriptor.to_payload()
assert {t["id"] for t in payload["topics"]} == {"orders:place", "orders:get", "orders:created"}
assert descriptor.descriptor_hash().startswith("sha256:")
```

## 2. Answer the reserved topic and trace every invocation

Two pieces of middleware. `trace_middleware` goes **outermost** so it times the whole invocation;
`mesh_interception` goes **before the router** so it short-circuits `benzene:mesh` and lets everything
else route normally.

```python
# app.py
from benzene.core import BenzeneMessageApplication, MiddlewarePipeline
from benzene.mesh import InMemoryTraceExporter, mesh_interception, trace_middleware
from benzene.testing import FakeMessageSender

from mesh_wiring import build_descriptor
from orders_domain.wiring import build_orders
from orders_domain.handlers import OrderService

sender = FakeMessageSender()                          # a real outbound client in production
registry = build_orders(OrderService(), sender).registry
descriptor = build_descriptor(registry)

exporter = InMemoryTraceExporter()                    # your TraceExporter in production
pipeline = (
    MiddlewarePipeline()
    .use(trace_middleware(exporter, service="orders", instance_id="orders-7f9c"))
    .use(mesh_interception(descriptor))
)
app = BenzeneMessageApplication(registry, pipeline)
```

The reserved topic now returns the descriptor, and a real order both routes and gets traced:

```python
import asyncio, json

# The reserved topic returns this service's descriptor (status ok).
mesh = asyncio.run(app.handle({"topic": "benzene:mesh", "headers": {}, "body": ""}))
assert mesh["statusCode"] == "ok"
assert json.loads(mesh["body"])["service"] == "orders"

# A real order: the handler runs, egress fires, and the invocation is traced once.
placed = asyncio.run(app.handle({
    "topic": "orders:place",
    "headers": {"x-correlation-id": "corr-42"},
    "body": '{"sku": "ABC", "quantity": 2}',
}))
assert placed["statusCode"] == "created"
assert sender.last_topic == "orders:created"          # egress proven

traces = [e for e in exporter if e.topic == "orders:place"]
assert len(traces) == 1
assert traces[0].status == "created"
assert traces[0].correlation_id == "corr-42"          # trace carries the correlation id
```

## 2b. Install it in your composition root (so every host — and every test — boots it)

The snippet above hand-builds the pipeline to show the pieces. In a real service you install the mesh
middleware in your `BenzeneStartUp` instead, by returning it on the `AppDefinition`. Then *every* host
(and the test harness) boots the same pipeline — you never wire mesh per-cloud, and you can test a
mesh-enabled service through the front door. Registering the reserved topic on an HTTP route gives you
a `GET /benzene/spec` URL, answered by the same interceptor:

```python
# startup.py
from benzene.core import AppDefinition
from benzene.mesh import (InMemoryTraceExporter, MESH_TOPIC, ServiceDescriptor, ServiceInfo,
                          TraceExporter, mesh_interception, trace_middleware)
from benzene.results import Result

async def _spec(_request):                # /benzene/spec is answered by mesh_interception
    return Result.not_found("benzene:mesh is handled by mesh_interception")

class MeshOrdersStartUp(OrdersStartUp):
    def configure_services(self, services, config):
        super().configure_services(services, config)
        services.try_add_singleton(TraceExporter, lambda _scope: InMemoryTraceExporter())

    def configure(self, services, config):
        base = super().configure(services, config)          # the real registry + HTTP router
        descriptor = ServiceDescriptor.derive(base.registry, ServiceInfo(service="orders",
                                              service_version="1.4.2", placement={"cloud": "aws"}))
        base.router.register("GET", "/benzene/spec", MESH_TOPIC, _spec)   # a URL for the descriptor
        exporter = services.get_service(TraceExporter)
        return AppDefinition(
            registry=base.registry,
            router=base.router,
            middleware=[trace_middleware(exporter, service="orders"), mesh_interception(descriptor)],
        )
```

Now the harness boots the mesh-enabled service like any other — only `build_aws()` names the cloud:

```python
from benzene.core import MessageSender
from benzene.mesh import TraceExporter
from benzene.testing import FakeMessageSender, create_test_host

fake = FakeMessageSender()
host = (create_test_host(MeshOrdersStartUp)
        .with_services(lambda s: s.add_instance(MessageSender, fake))
        .build_aws())                                        # or .build_gcp() / .build_azure()

spec = host.send_http("GET", "/benzene/spec")                # the descriptor, over HTTP
assert spec.status_code == 200 and json.loads(spec.body)["service"] == "orders"

host.send_sqs("orders:place", {"sku": "ABC"}, headers={"x-correlation-id": "c1"})
assert host.scope.get_service(TraceExporter)[0].correlation_id == "c1"   # trace, via the root scope
```

`trace_middleware` joins an inbound `traceparent` trace when present (else starts a fresh one) and
reads `x-correlation-id` for the business correlation id. Exporter failures are swallowed — tracing
never breaks the request.

A **health endpoint** installs exactly the same way — it is another reserved-topic interceptor. Add a
`HealthChecks`, put `health_interception(checks)` in the same `middleware` list, and register a
`GET /benzene/health` route to the `benzene:healthcheck` topic. The health aggregate it returns is the
same `{isHealthy, healthChecks}` shape the mesh `Heartbeat` reports, so one set of checks feeds both:

```python
from benzene.core import HEALTH_TOPIC, HealthChecks, health_interception

health = HealthChecks().add("order-store", check_store)          # a bool or HealthCheckResult
base.router.register("GET", "/benzene/health", HEALTH_TOPIC, _spec)   # answered by the interceptor
middleware = [trace_middleware(exporter, service="orders"),
              health_interception(health), mesh_interception(descriptor)]
# host.send_http("GET", "/benzene/health") -> 200 {"isHealthy": true, "healthChecks": {...}}
```

## 3. Report into a collector

`MeshFeedSender` pushes the feeds to a collector over any outbound `MessageSender`. Each feed is
independent and fire-and-report: it returns the outbound `Result` so you can log a failure, but it
never raises and never blocks traffic. Use the same `benzene.core` `MessageSender` you already publish
events with (Pub/Sub, SNS/SQS, Service Bus, or an HTTP POST of the wire envelope).

```python
from datetime import datetime, timezone
from benzene.mesh import Heartbeat, MeshFeedSender

feeds = MeshFeedSender(collector_sender)              # any benzene.core MessageSender

# At startup: register the descriptor (benzene:mesh:register).
await feeds.register(descriptor)

# Periodically: a heartbeat with the descriptor hash so the collector spots contract drift.
await feeds.publish_heartbeat(Heartbeat(
    service="orders",
    sent_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    instance_id="orders-7f9c",
    descriptor_hash=descriptor.descriptor_hash(),
))

# Periodically: flush the batched trace events (benzene:mesh:traces -> {"events": [...]}).
await feeds.publish_traces(list(exporter))
exporter.clear()

# Periodically: flush deduplicated failure signatures (benzene:mesh:issues). `count` is a delta.
from benzene.mesh import IssueAggregator

issues = IssueAggregator(service="orders")
issues.record(topic="orders:place", status="service-unavailable", exception_type="HttpError")
await feeds.publish_issues(issues.flush())           # flush() resets the window
```

## Adopt only what you need

Every feed is optional on both sides. Install `mesh_interception` and nothing else to be discoverable
without tracing; add `trace_middleware` for a trace feed; add `MeshFeedSender` to push into a collector.
Leave any of them out and the rest of the service is unchanged — an unprovisioned endpoint, an
unreachable collector, or a failing exporter must never affect service traffic.

## Troubleshooting

- **The descriptor has no topics.** `derive()` reads the `Registry` you pass — build it after wiring
  your handlers/routes (`build_orders(...).registry`), not from an empty one.
- **`benzene:mesh` routes to a handler / 404s instead of returning the descriptor.** Register
  `mesh_interception` in the pipeline *before* the router runs (pass it to the `MiddlewarePipeline`, as
  above) — the router is the terminal middleware.
- **Traces missing or double-counted.** Install `trace_middleware` once, outermost. It emits exactly
  one `TraceEvent` per routed invocation.
- **The hash changed but the code didn't.** `descriptor_hash()` excludes `instanceId`, `degraded`, and
  `profile`, but includes `serviceVersion`, `placement`, the topic set, and the schemas — bumping the
  version or changing a request/response type is meant to change it.

## See also

- [`benzene.mesh` reference](../reference/mesh.md) — every type, signature, and wire shape.
- [`benzene.core` reference](../reference/core.md), [`benzene.testing` reference](../reference/testing.md).
- [mesh specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/mesh.md).
