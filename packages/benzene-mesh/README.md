# benzene-mesh

The **mesh** module for the [Benzene Python port](https://github.com/daniellepelley/benzene-python).
Make a Python Benzene service a first-class citizen of a Benzene mesh: it describes itself, answers
the reserved `benzene:mesh` topic, traces every invocation, and reports into a collector — all
optional and additive.

Depends only on [`benzene-core`](https://pypi.org/project/benzene-core/).

```bash
pip install benzene-mesh
```

## Describe the service + answer `benzene:mesh`

The `ServiceDescriptor` is *derived* from your handler registry — it is always the truth of what the
service serves, with a request/response schema per topic and a content hash over the contract:

```python
from benzene.core import BenzeneMessageApplication, MiddlewarePipeline, Registry
from benzene.mesh import ServiceDescriptor, ServiceInfo, mesh_interception

registry = Registry()  # ... your handlers, e.g. registry.add(place_order)

descriptor = ServiceDescriptor.derive(
    registry,
    ServiceInfo(service="orders", service_version="1.4.2", placement={"cloud": "aws"}),
)

pipeline = MiddlewarePipeline().use(mesh_interception(descriptor))   # install before the router
app = BenzeneMessageApplication(registry, pipeline)

# A message on the reserved topic returns status `ok` with the descriptor as payload:
await app.handle({"topic": "benzene:mesh", "headers": {}, "body": ""})
```

Interception is by topic id alone (version ignored), exactly like health-check interception. Don't
install it and the endpoint simply doesn't exist — every other feed keeps working.

## Trace every invocation

`trace_middleware` emits exactly one `TraceEvent` per invocation — joining an inbound W3C
`traceparent` trace or starting a fresh one, and recording the topic, the semantic status, and the
duration. Export is asynchronous, non-blocking, and lossy: tracing never breaks the request.

```python
from benzene.mesh import InMemoryTraceExporter, trace_middleware

exporter = InMemoryTraceExporter()
pipeline = (
    MiddlewarePipeline()
    .use(trace_middleware(exporter, service="orders"))   # outermost: times the whole invocation
    .use(mesh_interception(descriptor))
)
```

## Report into a collector

`MeshFeedSender` pushes a service's four mesh feeds to a collector over any outbound `MessageSender`
(Pub/Sub, SNS, Service Bus, or an HTTP POST) — `register` announces the descriptor once, and
`publish_heartbeat` / `publish_traces` / `publish_issues` stream the ongoing telemetry:

```python
from benzene.mesh import IssueAggregator, MeshFeedSender

feeds = MeshFeedSender(outbound_client)
await feeds.register(descriptor)
await feeds.publish_traces(exporter)   # any iterable of TraceEvent

issues = IssueAggregator(service="orders")
issues.record(topic="order:create", status="service-unavailable", exception_type="HttpError")
await feeds.publish_issues(issues.flush())   # deduplicated failure signatures; count is a delta
```

## Run the Mesh Host (`benzene-mesh[host]`)

`MeshCollector` is an ordinary Benzene service, so the **Mesh Host** turns it into a real, networked,
config-driven mesh runtime — collector + aggregator + UI in one process. It needs the HTTP binding, so
it lives behind the `host` extra (`pip install benzene-mesh[host]`); importing `benzene.mesh` for
descriptors/tracing never pulls `benzene-http` in.

```python
from benzene.mesh.aggregator import MeshServiceEntry, MeshServiceRegistry
from benzene.mesh.host import MeshHost, MeshHostConfig

host = MeshHost(MeshHostConfig(
    registry=MeshServiceRegistry([
        MeshServiceEntry(name="orders",   base_url="http://orders:8080"),
        MeshServiceEntry(name="payments", base_url="http://payments:8080"),
    ]),
    out_dir="mesh-artifacts",
    ui_html="mesh-ui.html",
))
host.start_polling()          # timer-driven aggregation (the local/compose seam)
# `host` is an ASGI app: /benzene/invoke is the networked collector (services POST their feeds here),
# every other GET serves the emitted UI + artifacts. Run it under uvicorn/hypercorn.
```

Each pass HTTP-fetches every service's `/benzene/spec` + `/benzene/health`, queries the co-hosted
collector, and emits the six mesh-UI artifacts. Services report in over HTTP with a `MeshFeedSender`
over `benzene.http.InvokeMessageSender` (the outbound counterpart of `/benzene/invoke`). See
[`deploy/mesh`](../../deploy/mesh) for a runnable multi-process stack + a browser-driven proof.

---

Mirrors .NET's `Benzene.Mesh` (+ its `Benzene.Mesh.Aggregator` / `deploy/Mesh/Benzene.Mesh.Host`), and
contributes the `benzene.mesh` subpackage to the shared `benzene` namespace. The wire shapes
(descriptor, TraceEvent, collector topics) are the cross-language mesh contract — a Python service and a
.NET/Go/TypeScript one show up in the same mesh.
