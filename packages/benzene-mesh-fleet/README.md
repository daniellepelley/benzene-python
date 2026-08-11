# benzene-mesh-fleet

Cloud **service discovery** and **fleet trace-mappers** for the
[Benzene Python port](https://github.com/daniellepelley/benzene-python) — the two adapters a
[`benzene-mesh`](https://pypi.org/project/benzene-mesh/) mesh grows once it spans a real fleet.
Depends on `benzene-core` and `benzene-mesh`.

```bash
pip install benzene-mesh-fleet            # StaticDiscovery + all three trace-mappers, no SDK
pip install "benzene-mesh-fleet[aws]"     # + boto3 for AWS Cloud Map discovery
pip install "benzene-mesh-fleet[kubernetes]"
```

Every cloud SDK is an optional extra imported lazily and every client is injectable, so the package
imports and runs — and its tests pass — with no cloud SDK installed.

## Discover the mesh

A `Discovery` answers *which services are in the mesh, and where*, returning a flat list of
`ServiceEndpoint`s (name, address, metadata). The `benzene.mesh` `MeshPoller` already reads a service
once it has the address; discovery supplies the addresses instead of a hand-written list.

```python
from benzene.mesh_fleet import KubernetesDiscovery, ServiceEndpoint, StaticDiscovery

# The SDK-free default / test double.
discovery = StaticDiscovery([ServiceEndpoint("orders", "https://orders.svc")])
endpoints = await discovery.discover()

# Or read a live registry (boto3 / azure / kubernetes imported lazily, client injectable):
discovery = KubernetesDiscovery(namespace="mesh", label_selector="benzene.io/mesh=true")
endpoints = await discovery.discover()  # [] for an empty namespace, never an error
```

- **`StaticDiscovery`** — a fixed, in-memory endpoint list (the default and test impl).
- **`AwsCloudMapDiscovery`** — lists a Cloud Map namespace's services + instances (`[aws]`, `boto3`).
- **`AzureDiscovery`** — the `benzene:service`-tagged resources in a subscription (`[azure]`).
- **`KubernetesDiscovery`** — the `Service`s in a namespace, addressed by in-cluster DNS (`[kubernetes]`).

An empty registry is an empty list, never a raise; a discovered service with no resolvable address is
skipped rather than emitted blank.

## Map traces to any backend

A mesh `TraceEvent` already *is* a cross-language span, so this port ships tracing ahead of the field:
a `TraceMapper` projects a trace (the spans sharing a `trace_id`) into the JSON a backend ingests, no
re-instrumentation — you pick the mapper, not the instrumentation.

```python
from benzene.mesh_fleet import JaegerTraceMapper, TempoTraceMapper, XRayTraceMapper

jaeger_doc = JaegerTraceMapper().map(spans)  # {"traceID", "spans", "processes"}, µs
tempo_doc = TempoTraceMapper().map(spans)  # OTLP-JSON {"resourceSpans"}, Unix ns
xray_doc = XRayTraceMapper().map(spans)  # X-Ray segment + subsegments, epoch seconds
```

The backends differ mostly in time units, which is where fidelity matters: **Jaeger** microseconds,
**Tempo** (OTLP-JSON) Unix nanoseconds, **X-Ray** floating-point epoch seconds — and X-Ray's tree of
subsegments is reconstructed from each span's `parent_span_id`.

Mirrors .NET's `Benzene.Mesh.Discovery.*` and `Benzene.Mesh.Fleet.*`, and contributes the
`benzene.mesh_fleet` subpackage to the shared `benzene` namespace. The discovery endpoints and the
trace documents are ordinary data, so the whole package is exercised in memory with no SDK, no
network, and no backend.
