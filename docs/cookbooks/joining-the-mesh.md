# Joining the mesh (self-description + tracing + collector feeds)

Take an existing Benzene service — here the order domain, unchanged — and make it show up in a
**mesh**: it describes itself once (its Contract Document, on the reserved `benzene:spec` topic),
registers that same document with a collector, traces every invocation, and reports heartbeats,
traces, and issues. None of this touches the handlers; mesh is a declaration on the composition root,
additive middleware, and an outbound feed.

## Prerequisites

- Python 3.10+
- `pip install benzene-mesh` (installs `benzene-core`; add `benzene-http` and `benzene-testing` for the
  surfaces and fakes below)
- An existing service with a `BenzeneStartUp` — this walkthrough reuses the order domain from the
  [examples](https://github.com/daniellepelley/benzene-python/tree/main/examples) (`OrdersStartUp`).

## 1. Declare the self-description on the composition root

A service describes itself with its **Contract Document**: derived from the registry (`requests`) and
its outbound declarations (`events`, `sends`) — never written by hand — and served on `benzene:spec`.
Declare what the registry can't know (identity and placement) as a `ContractInfo`, and declare what
the service publishes and sends on an `OutboundRegistry`:

```python
# startup.py
from dataclasses import replace

from benzene.core import ContractInfo, HealthChecks, OutboundRegistry
from benzene.http import StandardPaths
from benzene.mesh import InMemoryTraceExporter, TraceExporter, trace_middleware
from orders_domain import OrdersStartUp


class MeshOrdersStartUp(OrdersStartUp):
    def configure_services(self, services, config):
        super().configure_services(services, config)
        services.try_add_singleton(TraceExporter, lambda _scope: InMemoryTraceExporter())

    def configure(self, services, config):
        base = super().configure(services, config)          # the real registry + HTTP router
        exporter = services.get_service(TraceExporter)
        return replace(
            base,
            contract=ContractInfo(
                "orders",
                service_version="1.4.2",
                version_scheme="semver",
                placement={"cloud": "aws"},                 # add "region" only where documented
            ),
            # orders:created is an event this service publishes and defines; a call to another
            # service's topic would be register_send(...).
            outbound=OutboundRegistry().register_event("orders:created"),
            # /benzene/invoke, /benzene/health and /benzene/spec (the Contract Document)
            standard_paths=StandardPaths(health=HealthChecks().add("order-store", lambda: True)),
            # outermost, so it times the whole invocation
            middleware=[trace_middleware(exporter, service="orders", instance_id="orders-7f9c")],
        )
```

The document lists one `requests[]` entry per handler, with OpenAPI 3.0 request/response schemas from
the handlers' declared types, the `events[]`/`sends[]` you declared, and two hashes
(`contractHash`, `registrationHash`). Every host — and the test harness — boots the same composition
root, so you can check it through the front door:

```python
import json
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host

fake = FakeMessageSender()
host = (create_test_host(MeshOrdersStartUp)
        .with_services(lambda s: s.add_instance(MessageSender, fake))
        .build_aws())                                        # or .build_gcp() / .build_azure()

spec = json.loads(host.send_http("GET", "/benzene/spec").body)
assert spec["service"] == "orders"
assert {r["topic"] for r in spec["requests"]} == {"orders:place", "orders:get", "orders:created"}
assert spec["registrationHash"].startswith("sha256:")
```

## 2. Trace every invocation

`trace_middleware` (installed above) joins an inbound `traceparent` trace when present (else starts a
fresh one), reads `x-correlation-id` for the business correlation id, and emits exactly one
`TraceEvent` per routed invocation. Exporter failures are swallowed — tracing never breaks the request.

```python
from benzene.mesh import TraceExporter

host.send_sqs("orders:place", {"sku": "ABC"}, headers={"x-correlation-id": "c1"})
assert fake.last_topic == "orders:created"                                 # egress proven
placed = [e for e in host.scope.get_service(TraceExporter) if e.topic == "orders:place"]
assert len(placed) == 1 and placed[0].correlation_id == "c1"              # one trace per invocation
```

## 3. Report into a collector

`MeshFeedSender` pushes the feeds to a collector over any outbound `MessageSender`. Each feed is
independent and fire-and-report: it returns the outbound `Result` so you can log a failure, but it
never raises and never blocks traffic. Use the same `benzene.core` `MessageSender` you already publish
events with (Pub/Sub, SNS/SQS, Service Bus, or an HTTP POST of the wire envelope).

The registration **is** the Contract Document — `registration_document(definition)` returns exactly
what `benzene:spec` serves, so the mesh and the service can never disagree about the contract:

```python
from datetime import datetime, timezone
from benzene.core import build_application
from benzene.mesh import Heartbeat, IssueAggregator, MeshFeedSender, registration_document

definition, scope = build_application(
    MeshOrdersStartUp, overrides=[lambda s: s.add_instance(MessageSender, FakeMessageSender())]
)
document = registration_document(definition)
feeds = MeshFeedSender(collector_sender)              # any benzene.core MessageSender

# At startup: register the document (benzene:mesh:register).
await feeds.register(document)

# Periodically: a heartbeat carrying the registered document's registrationHash and serviceVersion,
# so the collector spots an instance running a contract it hasn't learned.
await feeds.publish_heartbeat(Heartbeat.for_document(
    document,
    sent_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    instance_id="orders-7f9c",
))

# Periodically: flush the batched trace events (benzene:mesh:traces -> {"events": [...]}).
exporter = scope.get_service(TraceExporter)
await feeds.publish_traces(list(exporter))
exporter.clear()

# Periodically: flush deduplicated failure signatures (benzene:mesh:issues). `count` is a delta.
issues = IssueAggregator(service="orders")
issues.record(topic="orders:place", status="service-unavailable", exception_type="HttpError")
await feeds.publish_issues(issues.flush())           # flush() resets the window
```

To advertise the [Cloud Service Profile](../cloud-service-profile.md) verdict (R1–R5, R7) on the
document (served and registered alike), wrap the definition: `declare_cloud_service_profile(definition)`.
Whether the service is also **meshed** (mesh.md §7.1: M1 — the feeds above — and M2 — trace context
joined and forwarded) is a separate claim that never enters `profile.missing`; grade it with
`evaluate_cloud_service_profile(definition, mesh_feeds=True, trace_propagation=True).mesh_missing`.

## Adopt only what you need

Every feed is optional on both sides. Declare `contract=ContractInfo(...)` and nothing else to be
discoverable (a collector's poller reads `/benzene/spec` — the same document — without any push);
add `trace_middleware` for a trace feed; add `MeshFeedSender` to push into a collector. Leave any of
them out and the rest of the service is unchanged — an unprovisioned endpoint, an unreachable
collector, or a failing exporter must never affect service traffic.

## Troubleshooting

- **`benzene:spec` answers `not-found`.** The composition root declares no `contract=ContractInfo(...)`;
  `registration_document` raises `ValueError` for the same reason.
- **The document has no topics.** It reads the registry the startup returns — make sure `configure`
  returns the `AppDefinition` with the real `registry`/`router`, not an empty one.
- **No provider edge in the collector's graph.** Edges come from the registration's `events` and
  `sends`, never from traces: declare them on `AppDefinition(outbound=...)`.
- **Traces missing or double-counted.** Install `trace_middleware` once, outermost. It emits exactly
  one `TraceEvent` per routed invocation.
- **The hash changed but the code didn't.** `registrationHash` covers `requests`, `events` and
  `sends` (their topics, versions, HTTP mappings and dereferenced schemas). Identity, `placement`,
  `runtime`, `profile` and `degraded` are outside it; changing a request/response type changes it.

## See also

- [`benzene.mesh` reference](../reference/mesh.md) — every type, signature, and wire shape.
- [`benzene.core` reference](../reference/core.md), [`benzene.testing` reference](../reference/testing.md).
- [mesh specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/mesh.md).
