# `benzene.mesh_fleet`

Cloud **service discovery** and **fleet trace-mappers** for a Benzene mesh — the two adapters a mesh
grows once it spans a real fleet. **Distribution: `benzene-mesh-fleet` (depends on `benzene-core`,
`benzene-mesh`).**

```bash
pip install benzene-mesh-fleet            # StaticDiscovery + all three trace-mappers, no SDK
pip install "benzene-mesh-fleet[aws]"     # + boto3 for AWS Cloud Map discovery
pip install "benzene-mesh-fleet[azure]"
pip install "benzene-mesh-fleet[kubernetes]"
```

## Overview

Two adjacent capabilities the mesh needs once it spans a real fleet, both of which the port already
had the model for and only needed the adapters:

- **Discovery** answers *which services are in the mesh, and where* by reading a cloud registry into a
  flat list of `ServiceEndpoint`s. A `benzene.mesh.MeshPoller` already knows how to *read* a service
  once it has the address; discovery is the seam that supplies the addresses instead of a hand-written
  list.
- **Fleet trace-mappers** project the mesh's own `benzene.mesh.TraceEvent` span model into the JSON a
  tracing backend ingests. Because a mesh trace already *is* a cross-language span, this ships tracing
  ahead of the field: the same telemetry lands in Jaeger, Tempo, or X-Ray by choosing a mapper, with
  no re-instrumentation.

Every cloud SDK is an optional, lazily-imported extra (`[aws]` / `[azure]` / `[kubernetes]`) and every
client is injectable, so the whole package — and its tests — import and run with no SDK present.
Mirrors .NET's `Benzene.Mesh.Discovery.*` and `Benzene.Mesh.Fleet.*`.

## Discover the mesh

`Discovery` is a `runtime_checkable` protocol: a single `async def discover(self) ->
list[ServiceEndpoint]`. It **must never raise for an empty mesh** — an unpopulated namespace or a
registry with no matching services is an empty list, not an error.

`ServiceEndpoint` is a frozen, hashable dataclass — the lowest common denominator every cloud registry
can produce: `name` (the mesh service identity, the same value a `benzene.mesh.ServiceDescriptor`
carries in `service`), `address` (a URL or host the poller/router reaches), and free-form `metadata`
(availability zone, instance id, ARN, labels — whatever the registry knew and the port did not model).

```python
from benzene.mesh_fleet import KubernetesDiscovery, ServiceEndpoint, StaticDiscovery

# The SDK-free default / test double.
discovery = StaticDiscovery([ServiceEndpoint("orders", "https://orders.svc")])
endpoints = await discovery.discover()

# Or read a live registry (boto3 / azure / kubernetes imported lazily, client injectable):
discovery = KubernetesDiscovery(namespace="mesh", label_selector="benzene.io/mesh=true")
endpoints = await discovery.discover()  # [] for an empty namespace, never an error
```

- `StaticDiscovery(endpoints=None)` — a `Discovery` over a fixed, in-memory endpoint list (the default
  and the test double). `discover` returns a fresh copy each call, so a caller mutating the result
  never disturbs the source.
- `AwsCloudMapDiscovery(namespace_id, *, client=None)` — lists a Cloud Map namespace's services and,
  per service, its registered instances, turning each into a `ServiceEndpoint` addressed by
  `AWS_INSTANCE_CNAME` / `AWS_INSTANCE_IPV4` (plus `AWS_INSTANCE_PORT` when present). `namespace_id` is
  the namespace **id** (`ns-xxxxxxxx`), not its human name — the `list_services` `NAMESPACE_ID` filter
  matches on the id. `boto3` is the `[aws]` extra.
- `AzureDiscovery(subscription_id, *, client=None, service_tag="benzene:service")` — reads a resource
  client's `resources.list` feed, turning each `benzene:service`-tagged resource into a
  `ServiceEndpoint` addressed by its `defaultHostName` / FQDN. `azure-identity` + a resource client are
  the `[azure]` extra.
- `KubernetesDiscovery(*, namespace="default", label_selector=None, client=None)` — lists the
  `Service` objects in `namespace` via a `CoreV1Api`-shaped client, addressing each by its in-cluster
  DNS name (`<svc>.<namespace>.svc.cluster.local`, plus the first port when declared). The
  `kubernetes` client is the `[kubernetes]` extra.

Two invariants hold across all three cloud adapters (per the `Discovery` contract): an empty registry
is an empty list rather than an error, and a discovered service with no resolvable address is skipped
rather than emitted with a blank one.

## Map traces to any backend

A mesh `TraceEvent` already *is* a cross-language span, so a `TraceMapper` is a pure field/units
transform — no backend SDK, no network, no clock. `TraceMapper` is a `runtime_checkable` protocol with
one method, `map(trace) -> dict[str, Any]`, where a *trace* is either a single `TraceEvent` or the
iterable of spans sharing a `trace_id`.

```python
from benzene.mesh_fleet import JaegerTraceMapper, TempoTraceMapper, XRayTraceMapper

jaeger_doc = JaegerTraceMapper().map(spans)  # {"traceID", "spans", "processes"}, microseconds
tempo_doc = TempoTraceMapper().map(spans)    # OTLP-JSON {"resourceSpans"}, Unix nanoseconds
xray_doc = XRayTraceMapper().map(spans)      # X-Ray segment + subsegments, epoch seconds
```

The backends differ mostly in **time units**, which is where fidelity matters most:

- `JaegerTraceMapper` — a Jaeger query-service trace `{"traceID", "spans": [...], "processes": {...}}`.
  Each span carries `startTime` and `duration` **in microseconds** (Jaeger's unit), a `CHILD_OF`
  reference to its parent when present, the span fields as `tags`, and a `processID` into the shared
  `processes` map (one process per mesh service). An `exception_type` also raises the conventional
  `error=true` tag.
- `TempoTraceMapper` — the OTLP-JSON `{"resourceSpans": [...]}` document Grafana Tempo ingests, spans
  grouped by mesh service (the `service.name` resource attribute). Each span carries
  `startTimeUnixNano` / `endTimeUnixNano` **in Unix nanoseconds** (OTLP's unit), the fields as string
  `attributes`, and an OTLP `status` (`STATUS_CODE_ERROR` when an `exception_type` is present, else
  `STATUS_CODE_OK`).
- `XRayTraceMapper` — an AWS X-Ray segment document: a root **segment** with nested **subsegments**,
  the tree reconstructed from the spans' `parent_span_id` links. Each node carries `start_time` /
  `end_time` **in floating-point epoch seconds** (X-Ray's unit); the root also carries `trace_id` in
  X-Ray's `1-<8hex>-<24hex>` form, derived from the 32-hex W3C `trace_id`. An `exception_type` sets
  `"fault": true`.

## Exports

`Discovery`, `ServiceEndpoint`, `StaticDiscovery`, `AwsCloudMapDiscovery`, `AzureDiscovery`,
`KubernetesDiscovery`, `TraceMapper`, `JaegerTraceMapper`, `TempoTraceMapper`, `XRayTraceMapper`.

## See also

- [`benzene.mesh`](mesh.md) — the `TraceEvent` / `ServiceDescriptor` / `MeshPoller` model this extends.
- [`benzene.otel`](otel.md) — exports the same `TraceEvent` model live to an OpenTelemetry tracer
  (where the mappers project a whole trace into a backend document).
</content>
