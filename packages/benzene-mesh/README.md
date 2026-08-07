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

### Shared-secret feed auth (the simple option)

The ingest feeds are open by default. Pass a `key` to close them with a **shared secret**: the sender
attaches it as the `MESH_KEY_HEADER` (`x-benzene-mesh-key`) on every feed, and a collector configured with
the same key rejects any ingest lacking it as `unauthorized` (the `benzene:mesh:query:*` read models stay
open). Unset on both sides → today's open behaviour, unchanged.

```python
from benzene.mesh import MeshFeedSender

feeds = MeshFeedSender(outbound_client, key="s3cret")   # attaches x-benzene-mesh-key
# host side: collector_service_app(collector, key="s3cret"), or MeshHostConfig(mesh_key="s3cret")
```

This is the **simple** option only; deeper auth (IAM SigV4, mTLS, an API Gateway authorizer) is a
follow-up layered in front of the feed endpoint. See [`deploy/aws`](../../deploy/aws/) for the wired-up
AWS deployment (the key stored in SSM + injected on the services and host).

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
[`deploy/mesh`](../../deploy/mesh) for a runnable multi-process stack + a browser-driven proof, and
[`deploy/aws`](../../deploy/aws) for the same fleet lifted onto AWS (Lambda + App Runner).

Two host hooks make it AWS-ready: `MeshHostConfig(topology_source=…, usage_source=…)` threads the AWS
enrichment sources (below) into every pass, and `MeshHostConfig(registry_provider=…)` re-discovers the
fleet each pass (wrap `AwsLambdaDiscoveryProvider.discover`) so the registry tracks services that come and
go — instead of a registry fixed once at boot.

## Enrich with AWS observability data (`benzene-mesh[aws]`)

The collector plane derives `topology.json` edges from trace parentage (no timing) and `usage.json`
counts from observed invocations (no transport/duration). Two optional **enrichment sources** layer real
data from AWS on top, via the emitter's `topology_source` / `usage_source` hooks:

- **`XRayTopologySource`** — queries AWS X-Ray's `GetServiceGraph` and maps each `client → server` edge to
  real `requestsPerMinute` (`TotalCount` ÷ window), `errorRate` (error + fault counts ÷ total), and
  `p50/p95/p99LatencyMs` (from the edge's response-time histogram), tagged `source: "xray"`.
- **`CloudWatchUsageSource`** — lists the `benzene.messages.processed` counter's live dimension
  combinations and sums each per (topic, transport, status), plus the `benzene.message.duration` timer
  (`Sum` ÷ `SampleCount`) for `avgDurationMs`, tagged `source: "cloudwatch"`.

```python
from benzene.mesh import MeshArtifactEmitter
from benzene.mesh.aws import (
    Boto3CloudWatchClient, Boto3XRayServiceGraphClient,
    CloudWatchUsageSource, XRayTopologySource,
)

emitter = MeshArtifactEmitter(
    services, collector, generated_at=now,
    topology_source=XRayTopologySource(Boto3XRayServiceGraphClient()),
    usage_source=CloudWatchUsageSource(Boto3CloudWatchClient()),
)
```

**Merge rule (deterministic, mirrors .NET's `Benzene.Mesh.Aggregator` layering).** The external source is
richer, so it wins over the collector baseline: a topology edge's `(client, server)` pair is replaced by
the X-Ray edge when present (collector edges survive only for pairs X-Ray didn't observe); a usage topic's
entries are replaced wholesale by the CloudWatch rows for every topic CloudWatch reports (topics it doesn't
cover keep their collector entries). With no source wired, output is unchanged (pure collector plane).

Each AWS client is a minimal `typing.Protocol` (`XRayServiceGraphClient`, `CloudWatchClient`), so unit
tests pass hand-written fakes with **no `boto3`**; only the thin `Boto3*Client` adapters import `boto3`,
behind the `aws` extra (`pip install benzene-mesh[aws]`) — importing `benzene.mesh` stays SDK-free.
[`examples/mesh_fleet/prove_enriched.py`](../../examples/mesh_fleet) renders the enriched artifacts in the
mesh UI (X-Ray latency percentiles + a CloudWatch usage feed) and asserts them from the live DOM.

Ports .NET's `Benzene.Mesh.Fleet.Aws.XRay` + `Benzene.Mesh.Usage.CloudWatch`. **Bend from the .NET port:**
the CloudWatch source additionally fills `avgDurationMs` from the duration timer (the .NET adapter leaves
it null — its documented follow-up), because the pinned `usage.json` fixture carries mean durations.

### Self-discover the fleet from tagged Lambda functions (`AwsLambdaDiscoveryProvider`)

Instead of hand-feeding the aggregator a `mesh.json`, discover the mesh's services from the AWS account:
`AwsLambdaDiscoveryProvider` enumerates the Lambda functions (paginated `list_functions`), reads each
function's tags (`list_tags`, over a bounded thread pool), keeps the ones matching a `MeshDiscoveryFilter`
(by default: they carry the `benzene` tag), and emits them as `MeshServiceEntry` records in a
`MeshServiceRegistry` the aggregator then polls.

```python
from benzene.mesh.aws import AwsLambdaDiscoveryProvider, Boto3LambdaClient, MeshDiscoveryFilter

registry = AwsLambdaDiscoveryProvider(Boto3LambdaClient()).discover()   # → MeshServiceRegistry
# registry.services is the aggregator's input (MeshAggregator / MeshHost read it)
```

- **Interrogation transport = HTTP.** .NET binds discovered functions to a *Lambda-Invoke* interrogation
  source; the Python aggregator instead fetches every service's `/benzene/spec` + `/benzene/health` **over
  HTTP** (`SpecHealthSource`). So this port emits **HTTP registry entries**: a discovered function's
  `base_url` is the HTTP API it fronts (API Gateway stage / Function URL), read from the `benzene:mesh-url`
  tag — keeping discovery on the aggregator's one existing transport rather than adding an Invoke path.
- **`benzene:mesh-path`** (optional) overrides the entry's `/benzene` prefix (`{base_url}{prefix}/spec`),
  the Python analog of the .NET `SourceOptions["meshPath"]` descriptor-path override.
- A mesh-tagged function **without** a reachable URL is still emitted (visible as a fleet member); the
  aggregator records it `unreachable` until a URL is supplied — an honest state, not a silent drop.
- The `LambdaClient` seam is a two-method `typing.Protocol` (`list_functions` / `list_tags`), so unit
  tests drive it with a hand-written fake and **no `boto3`**; only `Boto3LambdaClient` imports `boto3`
  (lazily), behind the same `aws` extra. Ports .NET's `Benzene.Mesh.Discovery.Aws`.

### HTTP route mappings over the wire (`topics[].http`)

The derived spec is transport-neutral (topics + schemas, no route table), so a distributed aggregator
reading a peer's `/benzene/spec` over HTTP could not recover that peer's `(method, path)` mappings — the
mesh "producer gap". `benzene-http` now **optionally** rides those mappings along on `/benzene/spec` as an
additive `topics[].http: [{method, path}]` field (default on; `StandardPaths(spec_http_mappings=False)` to
opt out), and `MeshAggregator` reads them back into `ServiceCatalog.http_mappings` → `topics.json`
`consumers[].httpMappings`. Purely additive and Python-only for now; the cross-language spec follow-up is
drafted in [`docs/proposals/topic-http-mappings.md`](../../docs/proposals/topic-http-mappings.md).

---

Mirrors .NET's `Benzene.Mesh` (+ its `Benzene.Mesh.Aggregator` / `deploy/Mesh/Benzene.Mesh.Host`), and
contributes the `benzene.mesh` subpackage to the shared `benzene` namespace. The wire shapes
(descriptor, TraceEvent, collector topics) are the cross-language mesh contract — a Python service and a
.NET/Go/TypeScript one show up in the same mesh.
