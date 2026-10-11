# benzene-mesh

The **mesh** module for the [Benzene Python port](https://github.com/daniellepelley/benzene-python).
Make a Python Benzene service a first-class citizen of a Benzene mesh: it registers its
self-description with a collector, traces every invocation, and reports heartbeats, traces and
issues — all optional and additive. The receiving side, `MeshCollector`, ships here too.

Depends only on [`benzene-core`](https://pypi.org/project/benzene-core/).

```bash
pip install benzene-mesh
```

## Describe the service once — and register that

A service describes itself with its **Contract Document**, derived at the composition root from the
handler registry and the outbound declarations, and served on the reserved `benzene:spec` topic (and
`GET /benzene/spec`). The mesh has no self-description of its own: the `benzene:mesh:register` body
*is* that document, so what the mesh knows can never drift from what the service serves.

```python
from benzene.core import AppDefinition, ContractInfo, OutboundRegistry, Registry, application_from
from benzene.mesh import registration_document

definition = AppDefinition(
    registry=Registry(),  # ... your handlers, e.g. .add(place_order)
    outbound=OutboundRegistry().register_send("payments:capture", CapturePayment),
    contract=ContractInfo("orders", service_version="1.4.2", version_scheme="semver",
                          placement={"cloud": "aws"}),
)
app = application_from(definition)               # benzene:spec answers with the document
document = registration_document(definition)     # ...and this is the very same document
```

## Trace every invocation

`trace_middleware` emits exactly one `TraceEvent` per invocation — joining an inbound W3C
`traceparent` trace or starting a fresh one, and recording the topic, the semantic status, and the
duration. Export is asynchronous, non-blocking, and lossy: tracing never breaks the request.

```python
from benzene.mesh import QueueTraceExporter, trace_middleware

exporter = QueueTraceExporter()
definition.middleware.insert(0, trace_middleware(exporter, service="orders"))  # outermost, before application_from
```

## Report into a collector

`MeshFeedSender` pushes a service's mesh feeds to a collector over any outbound `MessageSender`
(Pub/Sub, SNS, Service Bus, or an HTTP POST) — `register` announces the Contract Document once, and
`publish_heartbeat` / `publish_traces` / `publish_issues` stream the ongoing telemetry:

```python
from benzene.mesh import Heartbeat, IssueAggregator, MeshFeedSender

feeds = MeshFeedSender(outbound_client)
await feeds.register(document)                                   # benzene:mesh:register
await feeds.publish_heartbeat(Heartbeat.for_document(            # registrationHash + serviceVersion
    document, sent_at="2026-07-31T12:00:00Z", instance_id="orders-7f9c"))
await feeds.publish_traces(exporter.drain())                     # any iterable of TraceEvent

issues = IssueAggregator(service="orders")
issues.record(topic="order:create", status="service-unavailable", exception_type="HttpError")
await feeds.publish_issues(issues.flush())   # deduplicated failure signatures; count is a delta
```

Mirrors .NET's `Benzene.Mesh`, and contributes the `benzene.mesh` subpackage to the shared `benzene`
namespace. The wire shapes (the registration, heartbeat, TraceEvent and collector topics) are the
cross-language mesh contract — a Python service and a .NET/Go/TypeScript one show up in the same mesh.
