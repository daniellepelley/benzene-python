# `benzene.mesh`

Make a Python Benzene service a first-class citizen of a **mesh**: it describes itself, answers the
reserved `benzene:mesh` topic, traces every invocation, and reports into a collector. Everything here
is optional and additive. **Distribution: `benzene-mesh` (depends only on `benzene-core`).**

```bash
pip install benzene-mesh
```

## Overview

The mesh module implements the language-neutral [mesh specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/mesh.md).
Its wire shapes — the `ServiceDescriptor`, the `TraceEvent`, and the collector topics — are the
**cross-language mesh contract**: a Python service and a .NET/Go/TypeScript one emit the same shapes
and appear in the same mesh. It gives you four independent capabilities, each of which you can adopt
on its own:

- **Self-description** — project the handler registry into a `ServiceDescriptor` (identity, placement,
  per-topic request/response schemas, a content hash).
- **The reserved endpoint** — `mesh_interception()` answers `benzene:mesh` with that descriptor.
- **Tracing** — `trace_middleware()` emits exactly one `TraceEvent` per invocation.
- **Collector feeds** — `MeshFeedSender` pushes the descriptor, heartbeats, traces, and issues to a
  collector over any outbound `MessageSender`.

Every feed is independent and optional on both sides. An unreachable collector, a failing exporter, or
a missing endpoint must never affect service traffic — the module is built so a mesh feed can never
break, slow, or block an invocation.

## The service descriptor

`ServiceDescriptor` is a service's self-description, **derived from its registries** — never
hand-written, so it is always the truth of what the service serves *and* calls. `ServiceInfo` carries
what the registries can't know (identity and placement); an `OutboundRegistry` (below) carries what a
service declares it sends. As of the 2026-08 mesh.md revision, this alone is what puts an edge in the
mesh's producer/consumer graph — the graph exists before a single message has flowed (mesh.md §4).

```python
from benzene.core import Registry
from benzene.mesh import OutboundRegistry, ServiceDescriptor, ServiceInfo

outbound = OutboundRegistry().register("payments:capture", request_type=CapturePayment)

descriptor = ServiceDescriptor.derive(
    registry,                                    # a benzene.core Registry — what this service provides
    ServiceInfo(
        service="orders",                        # the only required field
        service_version="1.4.2",
        instance_id="orders-7f9c",
        placement={"cloud": "aws", "region": "eu-west-1"},
    ),
    outbound,                                     # what this service consumes (omit for none)
)

payload = descriptor.to_payload()                # the ServiceDescriptor wire payload (a dict)
digest = descriptor.descriptor_hash()            # "sha256:..."
```

### `ServiceInfo`

What the host/app knows about itself that the registry can't derive. Only `service` is required; a port
**emits what it knows and omits (never nulls) the rest**. `runtime` defaults to `"python"` — the
per-port identity of this implementation.

```python
ServiceInfo(
    service: str,
    service_version: str | None = None,
    instance_id: str | None = None,
    runtime: str | None = "python",
    binding: str | None = None,
    placement: dict[str, str] | None = None,
    degraded: list[str] | None = None,
    profile: dict[str, Any] | None = None,
)
```

### `ServiceDescriptor`

- `ServiceDescriptor.derive(registry, info, consumes=None)` — project a `Registry` + `ServiceInfo` +
  optional `OutboundRegistry` (or any iterable of `OutboundDefinition`) into a descriptor. One
  `TopicDescriptor` per registered/consumed topic, **sorted by `(id, version)`** so the output is
  deterministic (the hash depends on it). Each topic's schemas come from the handler's or outbound
  registration's declared `request_type` / `response_type`. Omitting `consumes` yields an empty
  `consumes` list — a service that genuinely calls nothing, not a degraded/unknown state.
- `descriptor.to_payload()` — the full wire payload, including `descriptorHash`. Fields are emitted in a
  fixed order and camelCase: `service`, `serviceVersion`, `runtime`, `binding`, `instanceId`,
  `placement`, `topics`, `consumes`, `degraded`, `profile`, `descriptorHash`. Absent optional fields are
  omitted; `topics` and `consumes` are always present (possibly `[]`).
- `descriptor.descriptor_hash()` — `"sha256:" + hex(sha256(canonical-json))` over the **contract**. The
  canonical form sorts keys lexicographically with no insignificant whitespace, and **excludes**
  `instanceId`, `degraded`, `profile`, and `descriptorHash` itself. So two instances of the same build
  hash identically, but the hash changes when the service version, placement, topic set, consumed-topic
  set, or a schema changes. It detects this service's own redeploys and is **never compared across
  ports**.

### `OutboundRegistry`

Declares which topics a service **may send** (mesh.md §2.3) — the outbound counterpart to
`benzene.core.Registry`'s inbound handler discovery: an explicit `(topic, version, request_type,
response_type)` record, with no handler, since nothing here receives. This is what makes
`ServiceDescriptor.consumes` a **hard-coded contract** rather than an inference — a port must not
attempt to derive it by scanning call sites or any other form of static analysis.

```python
from benzene.mesh import OutboundRegistry

outbound = OutboundRegistry()
outbound.register("payments:capture", request_type=CapturePaymentRequest)
```

Registering the same `(topic, version)` pair twice raises `DuplicateOutboundRegistrationError` — the
same strictness inbound registration already has. No destination address, queue name, or topic ARN is
needed: that's transport/deployment configuration, orthogonal to the contract this registers.

### `TopicDescriptor`

One registered topic's projection.

```python
from benzene.mesh import TopicDescriptor

TopicDescriptor(
    id: str,
    version: str = "",
    request_schema: Schema = {},
    response_schema: Schema = {},
)
```

`to_payload()` emits `id`, `requestSchema`, `responseSchema`, and `version` only when it is non-empty
(an empty version is omitted, not nulled).

## The Cloud Service Profile self-check

The [Cloud Service Profile](../cloud-service-profile.md) names eight requirements (R1–R8) a service
must satisfy to be a first-class fleet citizen. `evaluate_cloud_service_profile` grades a composition
root's [`AppDefinition`](core.md) against them at **wiring time** — a self-assessment of what the setup
provisioned, not a runtime probe — and its verdict rides on the descriptor's optional `profile` field
(§2), so any tool that reaches the reserved `benzene:mesh` topic can ask a running service whether it
claims the profile and, if not, which requirements it is missing.

```python
from benzene.mesh import evaluate_cloud_service_profile

report = evaluate_cloud_service_profile(definition, mesh_feeds=True, trace_propagation=True)
report.is_conformant          # True when every requirement is satisfied
report.missing                # e.g. ["R6", "R8"] — the ids the wiring does not satisfy, in order
report.reason("R6")           # the explanation recorded for one requirement
report.to_profile()           # {"name": "cloud-service"} (or {..., "missing": [...]})

descriptor = ServiceDescriptor.derive(
    definition.registry, ServiceInfo(service="orders", profile=report.to_profile())
)
```

- Most requirements are read straight off the definition: **R1** (a registry/router to host), **R2**
  (≥1 non-reserved application topic), and — from the [`StandardPaths`](http.md) declaration — **R3**
  (health aggregate), **R4** (`/benzene/invoke` enabled), **R5** (derived spec), and **R7** (the
  surfaces under the default `/benzene/` prefix).
- Two requirements are structurally invisible to definition inspection and are passed explicitly —
  exactly the pair the profile spec singles out as unobservable from a single service:
  **`mesh_feeds`** (R6 — the `benzene:mesh` descriptor plus register/heartbeat/trace *sending*, wired
  into the host loop) and **`trace_propagation`** (R8 — outbound clients forwarding `traceparent`). The
  default for each is the honest "not provisioned".
- Like `degraded`, the `profile` field is self-description, **excluded from the `descriptorHash`**, and
  never changes because of runtime degradation — an unreachable collector does not make a conformant
  service stop claiming the profile.

`CloudServiceProfileReport` carries one `RequirementCheck(id, satisfied, reason)` per requirement
(`REQUIREMENT_IDS` lists them, R1–R8); `PROFILE_NAME` is `"cloud-service"`. See the runnable
[`mesh_dashboard` profile example](https://github.com/daniellepelley/benzene-python/tree/main/examples/mesh_dashboard/profile.py).

### The live-probe checker — `probe_cloud_service`

The outside-in counterpart: point it at a *deployed* service's base URL and it audits the profile over
plain HTTP, speaking only the language-neutral surfaces (`/benzene/spec`, `/benzene/health`,
`/benzene/invoke`) so it grades a Go, Node, or .NET service exactly as a Python one. Each requirement
gets a **tri-state** `Verdict` — `satisfied` / `not-satisfied` / `inconclusive` — always with a reason.

```python
from benzene.mesh import probe_cloud_service

report = await probe_cloud_service("https://orders.example.com")   # inject http= in tests
report.not_satisfied      # ids positively found unmet — the actionable failures
report.inconclusive       # ids a black-box probe can't verify (never a failure by itself)
report.is_clean           # True when nothing was positively found unmet
report.to_payload()       # {"baseUrl", "requirements": [{"id", "verdict", "reason"}, ...]}
```

A black-box probe cannot verify everything a self-check can, so three verdicts are `inconclusive`
**by design**, never silently upgraded (cloud-service-profile.md §5): **R8** (propagation needs a second
service or a collector to observe forwarded `traceparent`), **R6**'s register/heartbeat half (only the
`benzene:mesh` descriptor response is observable; delivery to a collector is not), and **R7** whenever
the caller probes a non-default prefix (the service's own defaults become unknowable). The CLI form is
`python -m benzene.mesh.probe <url> [--prefix /benzene] [--json]`, exiting non-zero when a requirement
is positively unmet.

## Schema derivation

`json_schema(py_type)` derives the **JSON Schema 2020-12 subset** the mesh uses to describe what a topic
accepts and returns. `Schema` is just an alias for `dict[str, Any]`. This is what `derive()` calls for
each handler's request/response type.

```python
from dataclasses import dataclass
from benzene.mesh import json_schema

@dataclass
class PlaceOrder:
    sku: str
    quantity: int = 1

json_schema(PlaceOrder)
# {"type": "object",
#  "properties": {"sku": {"type": "string"}, "quantity": {"type": "integer"}},
#  "required": ["sku"]}
```

The Python-type → JSON-Schema mapping:

| Python type | JSON Schema |
|---|---|
| `str` | `{"type": "string"}` |
| `bool` | `{"type": "boolean"}` (checked before `int`) |
| `int` | `{"type": "integer"}` |
| `float` | `{"type": "number"}` |
| `datetime` | `{"type": "string", "format": "date-time"}` (RFC 3339) |
| `bytes` | `{"type": "string"}` (base64 on the wire) |
| `T \| None` | `T`'s schema with `"null"` added to its `type` |
| `list[T]` / `tuple[T, ...]` / `set` | `{"type": "array", "items": <T>}` |
| `dict[str, T]` | `{"type": "object", "additionalProperties": <T>}` |
| a `@dataclass` | `{"type": "object", "properties": {...}, "required": [...]}` |
| anything else / `None` / `Any` | `{}` (open schema — matches anything) |

Two rules make the schema describe **what actually crosses the wire**:

- **Property names follow the wire naming policy** (`benzene.core.to_camel`) — dataclass fields are
  emitted camelCase, in declaration order.
- **A field is `required` iff it has no default** (no `default` and no `default_factory`) — those are
  the properties the marshaler always emits.

A genuine multi-type union (e.g. `int | str`) and a recursive dataclass both fall back to the open
schema `{}` rather than emitting a `$ref`.

## The reserved endpoint

`mesh_interception()` is ordinary Benzene middleware that answers the reserved topic `benzene:mesh`
(the `MESH_TOPIC` constant) with the descriptor, as status `ok`. Interception is **by topic id, version
ignored** — exactly like health-check interception. Install it **before the message router** (which is
the terminal middleware) so it short-circuits the reserved topic and leaves every other message to
route normally.

```python
from benzene.core import BenzeneMessageApplication, MiddlewarePipeline
from benzene.mesh import mesh_interception

pipeline = MiddlewarePipeline().use(mesh_interception(descriptor))
app = BenzeneMessageApplication(registry, pipeline)

# GET the descriptor by sending the reserved topic:
response = await app.handle({"topic": "benzene:mesh", "headers": {}, "body": ""})
# response["statusCode"] == "ok"; json.loads(response["body"])["service"] == "orders"
```

- `mesh_interception(descriptor, *, aliases=())` — `descriptor` is a `ServiceDescriptor` **or a
  zero-arg callable returning one** (`DescriptorSource`), so a service can recompute it per request —
  e.g. to reflect a degraded subsystem. `aliases` add extra topic ids that also return the descriptor.
- Provisioning it is a deployment choice. Don't install it and the endpoint simply doesn't exist, while
  every other mesh feed keeps working.

In a real service, install this (and `trace_middleware`) in your `BenzeneStartUp` by returning it on
the `AppDefinition`'s `middleware` — then every host and the test harness boot it identically, and you
can answer it over HTTP by mapping a `GET /benzene/spec` route to `benzene:mesh`. See
[Joining the mesh](../cookbooks/joining-the-mesh.md) §2b for the composition-root pattern and testing
it through `create_test_host(...).build_aws()`.

## Tracing

`trace_middleware()` emits exactly one `TraceEvent` per routed invocation — the topic, the semantic
status, how long it took, and its place in a W3C trace. Install it **outermost** (first in the pipeline)
so it times the whole invocation, including routing.

```python
from benzene.mesh import InMemoryTraceExporter, trace_middleware

exporter = InMemoryTraceExporter()
pipeline = (
    MiddlewarePipeline()
    .use(trace_middleware(exporter, service="orders", instance_id="orders-7f9c"))
    .use(mesh_interception(descriptor))
)
```

The middleware reads the inbound `traceparent` header to **join an existing trace** (or starts a fresh
one), reads `x-correlation-id` for the business correlation id, times the pipeline, and hands the
finished event to the exporter. **Export must never affect the traffic it observes** — exporter errors
are swallowed, so a mesh feed can never break the request.

### `TraceEvent`

One invocation's trace record. `to_payload()` is its camelCase wire form, omitting (never nulling) what
isn't known.

```python
TraceEvent(
    trace_id: str,
    span_id: str,
    service: str,
    topic: str,
    status: str,                         # the Benzene status, verbatim
    parent_span_id: str | None = None,
    instance_id: str | None = None,
    topic_version: str | None = None,
    exception_type: str | None = None,
    duration_ms: float | None = None,
    started_at: str | None = None,       # RFC 3339
    correlation_id: str | None = None,
)
```

Wire keys: `traceId`, `spanId`, `service`, `topic`, `status` (always present) plus `parentSpanId`,
`instanceId`, `topicVersion`, `exceptionType`, `durationMs`, `startedAt`, `correlationId` when known.

### `TraceExporter` and W3C helpers

- `TraceExporter` — a runtime-checkable `Protocol` with `export(event: TraceEvent) -> None`. Your
  implementation **must be non-blocking and must not raise**; export is asynchronous and lossy under
  backpressure by design.
- `InMemoryTraceExporter` — a `list` subclass whose `export()` appends; the fake for tests and
  dogfooding. Iterate it to read the events. Unbounded, so prefer `QueueTraceExporter` in a service.
- `QueueTraceExporter(maxlen=10_000)` — the non-blocking, bounded default for a real deployment.
  `export()` appends to a fixed-size `deque` in O(1) and never blocks or does I/O; when full, the
  oldest event is dropped (lossy under backpressure). A background task periodically `drain()`s it and
  ships the events via `publish_traces`, so the collector never touches the request path.
- `parse_traceparent(header)` — parse a W3C `traceparent` (`00-<32hex>-<16hex>-<2hex>`) →
  `(trace_id, parent_span_id)`, or `None` for a malformed or absent header (the caller then starts a
  fresh trace). The all-zero trace-id and all-zero parent-id are invalid per W3C, as is any
  wrong-length or non-hex segment.
- `new_trace_id()` — a fresh 16-byte lowercase-hex trace-id. `new_span_id()` — a fresh 8-byte
  lowercase-hex span-id.

### Outbound propagation

`trace_middleware` records the current invocation's trace in a `contextvar`, so an outbound call made
*during* the invocation can forward it (mesh.md §3):

- `current_traceparent()` — the W3C `traceparent` (`00-<traceId>-<spanId>-01`) for the invocation
  currently running, or `None` outside a trace.
- `with_trace_propagation(sender)` (class `TracePropagatingMessageSender`) — wraps a `MessageSender` so
  each published message carries the current `traceparent`; the downstream service then joins the same
  trace, so the resulting `TraceEvent`s feed the collector's invocation/error stats for the edge the
  descriptor already declared (mesh.md §4) — trace parentage is never what admits the edge itself. A
  `traceparent` the caller already set is left untouched. Compose it with the core client decorators:
  `with_retry(with_trace_propagation(sender))`.

## Collector feeds

`MeshFeedSender` pushes a service's mesh feeds to a collector over an outbound
`benzene.core.MessageSender` (Pub/Sub, SNS/SQS, Service Bus, or an HTTP POST of the wire envelope).
Sending is **fire-and-report**: it returns the outbound `Result` so a caller can log a failed feed, but
it does not raise, and a failing feed must never affect service traffic.

```python
from benzene.mesh import Heartbeat, MeshFeedSender

feeds = MeshFeedSender(sender)                       # any benzene.core MessageSender

await feeds.register(descriptor)                     # -> benzene:mesh:register
await feeds.publish_heartbeat(Heartbeat(
    service="orders",
    sent_at="2026-07-31T12:00:00Z",
    instance_id="orders-7f9c",
    descriptor_hash=descriptor.descriptor_hash(),
))                                                   # -> benzene:mesh:heartbeat
await feeds.publish_traces(exporter)                 # -> benzene:mesh:traces  {"events": [...]}
await feeds.publish_issues(aggregator.flush())       # -> benzene:mesh:issues  (see below)
```

The collector topic constants and their bodies (the cross-language contract):

| Constant | Topic | Body | Success response |
|---|---|---|---|
| `REGISTER_TOPIC` | `benzene:mesh:register` | `ServiceDescriptor` | `{"accepted": 1}` |
| `HEARTBEAT_TOPIC` | `benzene:mesh:heartbeat` | `Heartbeat` | `{"accepted": 1}` |
| `TRACES_TOPIC` | `benzene:mesh:traces` | `{"events": [TraceEvent, ...]}` | `{"accepted": <count>}` |
| `ISSUES_TOPIC` | `benzene:mesh:issues` | IssueBatch | `{"accepted": <count>}` |

- `MeshFeedSender.register(descriptor)` — announce the `ServiceDescriptor` payload.
- `MeshFeedSender.publish_heartbeat(heartbeat)` — send a `Heartbeat`.
- `MeshFeedSender.publish_traces(events)` — send an iterable of `TraceEvent`s as `{"events": [...]}`.
- `MeshFeedSender.publish_issues(batch)` — send an `IssueBatch`.

`MeshFeedSender` is the **sender** half (a service reporting in); the **receiver** is `MeshCollector`
(below).

### `Heartbeat`

A liveness beat: identity + descriptor hash + the health aggregate. `descriptor_hash` lets the collector
notice a descriptor change it hasn't learned yet (a hash mismatch means "re-register").

```python
Heartbeat(
    service: str,
    sent_at: str,                                    # RFC 3339
    instance_id: str | None = None,
    descriptor_hash: str | None = None,
    is_healthy: bool = True,
    health_checks: Mapping[str, Any] | None = None,
)
```

`to_payload()` emits `service`, `sentAt`, optional `instanceId` / `descriptorHash`, and a `health`
object `{"isHealthy": ..., "healthChecks": {...}}`.

### Issues — `IssueAggregator`, `Issue`, `IssueBatch`

The issues feed reports **deduplicated failure signatures**. Two pieces are normative so a Python
service produces the same signatures a .NET one would (the collector merges by `fingerprint` across
instances):

- `classify(status, exception_type=None)` — maps a failure to the closed vocabulary `validation`,
  `exception`, `config-wiring`, `dependency`, `unclassified`, in the spec's precedence order.
  (`contract-drift` is reserved for collector-derived issues and is never produced here.)
- `issue_fingerprint(service, topic, version, classification, discriminator)` — the exact signature:
  lowercase hex of the first 16 bytes of `sha256("service|topic|version|classification|discriminator")`,
  where `discriminator` is the exception type when present, else the status.

`IssueAggregator` is the pit of success — `record(...)` each failure, `flush()` to an `IssueBatch`:

```python
from benzene.mesh import IssueAggregator

issues = IssueAggregator(service="orders")
issues.record(topic="order:create", status="service-unavailable", version="v2",
              transport="sqs", exception_type="HttpError", trace_id=event.trace_id)
await feeds.publish_issues(issues.flush())           # count is a DELTA; flush() resets the window
```

`flush()` drains everything seen since the previous flush and resets, so every `count` is a delta,
never a cumulative total. Flushing an empty aggregator is valid — that batch is the feed's liveness
beat.

## The collector — `MeshCollector`

A collector is the receiving side: **an ordinary Benzene service** that ingests the feeds and renders
the fleet. `collector_registry(collector)` wires a `MeshCollector` onto a registry, so you run it
through a `BenzeneMessageApplication` like any other service:

```python
from benzene.core import BenzeneMessageApplication
from benzene.mesh import MeshCollector, collector_registry

app = BenzeneMessageApplication(collector_registry(MeshCollector()))
await app.handle({"topic": "benzene:mesh:register", "headers": {}, "body": descriptor_json})
fleet = await app.handle({"topic": "benzene:mesh:query:fleet", "headers": {}, "body": "{}"})
```

It ingests `benzene:mesh:register` / `:heartbeat` / `:traces` / `:issues` and answers four read
models — `benzene:mesh:query:fleet` / `:service` / `:topic` / `:trace`. The catalog it derives (per
mesh.md §§4–6, pinned by `mesh-collector-cases.json`):

- **The producer/consumer graph is built from the latest registered `ServiceDescriptor` alone**:
  `topics` gives provider edges, `consumes` gives consumer edges; re-registration **replaces** both
  wholesale. Trace parentage is **never** used to admit an edge into this graph — it feeds a queried
  topic's `invocations` / `errors` / `statusCounts` for the edges the descriptor already declared
  (mesh.md §4).
- **Declared vs. observed** (mesh.md §4.2) — `query_topic`'s `providerActivity` / `consumerActivity`
  maps every declared name to `{"lastObservedAt": "..."}` when a matching trace has been seen, else
  `{}` — an entry present with no timestamp is a **decommission candidate**, never removed from
  `providers`/`consumers` on that basis alone. Symmetrically, the first trace naming a topic a
  service *hasn't* declared (as either provider or consumer) — checked only for a service that has
  registered a descriptor; an anonymous one has no contract to diverge from — is filed into the
  issues feed as a `contract-drift` issue, merged by fingerprint like any other issue (§4.1).
- **Health** aggregates a service's heartbeat instances (`healthy` / `degraded` / `unhealthy` /
  `unknown`), and a per-instance `hashMatches` surfaces a descriptor-hash drift.
- **`missingFeeds`** names which of `descriptor` / `health` / `traces` a service hasn't reported, so a
  partial fleet renders as reduced rather than absent.

`service` is required on `register`, `heartbeat`, and `issues` (→ `bad-request`); an unknown service /
topic / trace query is `not-found`; the query read models are one collector's shapes (the spec pins
them only as the observable surface for the ingest rules). Sender feeds live in `benzene.mesh` (`MeshFeedSender`).

The optional **issues** feed is supported too: `benzene:mesh:issues` batches merge by `fingerprint`
(`count` is a delta — occurrences accrue, exemplars accumulate), a malformed entry is skipped rather
than rejecting the batch, and `issues` appears in a service's `missingFeeds` only when a failing trace
is unexplained. Conformance-green against both `mesh-collector-cases` and `mesh-issue-cases`.

### Persistence — `CollectorStore`

A `MeshCollector` is in-memory by default, which is exactly right for tests and single runs. A
long-lived collector (the Fargate Mesh Host) should not forget the whole fleet every time its task is
replaced, so pass a `CollectorStore`:

```python
from benzene.mesh import JsonFileCollectorStore, MeshCollector

collector = MeshCollector(store=JsonFileCollectorStore("/data/mesh-state.json"))
```

The collector **restores** the last snapshot on construction and **saves** a fresh one after every
mutating ingest, so a restarted host rehydrates the fleet it already knew. `CollectorStore` is a small
two-method `Protocol` (`load() -> dict | None`, `save(dict)`), so any backend fits; two ship:

- **`NullCollectorStore`** — the default, keeps nothing (pure in-memory; tests pay nothing).
- **`JsonFileCollectorStore(path)`** — the snapshot as JSON on a mounted volume. Writes atomically
  (temp file + `os.replace`) so a task killed mid-write leaves no half-written file, and a missing or
  corrupt file loads as a first boot rather than crashing the host (the catalog refills from the fleet
  within one poll interval).

The snapshot is a plain JSON-able dict — `collector.snapshot()` / `collector.restore(snap)` are public,
so you can persist it anywhere (S3, a database) by implementing the two-method protocol over them.

### The mesh-ui artifacts — `build_artifacts` / `write_artifacts`

The canonical, cross-language **Benzene Mesh UI** (`mesh-ui.html`, one page every port vendors) is
data-driven from a fixed set of static JSON artifacts an aggregator publishes. `benzene.mesh.artifacts`
projects a collector's catalog into that read-model contract (the main repo's `docs/guides/mesh-ui.md`,
pinned by `website/demos/mesh/`):

```python
from benzene.mesh import write_artifacts

write_artifacts("/data/mesh-ui", collector, sources=poller_sources, generated_at=now_iso)
# manifest.json, topology.json, topics.json, usage.json, asyncapi.json, annotations.json,
# and services/{name}.json — the UI fetches all of these by relative path.
```

- **`build_artifacts(collector, *, sources=(), generated_at)`** returns the artifacts as dicts
  (`{"manifest", "topology", "topics", "usage", "asyncapi", "annotations", "services": {name: doc}}`) —
  pure and deterministic (inject `generated_at`); `sources` (any objects with `name` / `spec_url` /
  `health_url`, e.g. `HttpServiceSource`) supply the manifest's `specUrl` / `healthUrl` links.
- **`write_artifacts(dir, ...)`** lays them out on disk (atomically) for the UI to fetch by relative
  path.

The projection honours the contract's **"must not invent fields, degrade when absent"** rule. From the
descriptor/spec feed it derives the estate (health mapped to healthy/unhealthy/unreachable,
contract-drift + `previousSpecHash` history), the functional map (topics with **declared**
consumers/producers, `benzene:*` flagged `reserved`, **request/response schemas, version,
`schemaMismatch`** when two providers disagree, **`changes[]`** when a provider re-registers a topic
with a new schema, and **`removedTopics`** for a topic no longer provided), per-service **spec +
per-check health**, and an **AsyncAPI 3.0** export of the domain topics; the topology's client→server
edges come from that same declared graph, and the trace feed layers each edge's **error rate** plus
**usage** (exercise counts per topic/service/status) on top. `annotations.json` is an honest empty
read-model (writing is a backend-gated live-plane feature). Only what genuinely needs feeds the
collector doesn't have — latency/rate metrics, a usage time window, and transports — is emitted as
`null`. The field set is pinned by `tests/test_mesh_artifact_contract.py`. See `deploy/mesh` for the
host that serves them.

## The poller — `MeshPoller` (pull aggregator)

`MeshFeedSender` is the **push** side (a service reports in). `MeshPoller` is the **pull** side,
mirroring the .NET Mesh Host: it reaches out to a configured fleet on a timer, reads each service's
`/benzene/spec` + `/benzene/health` (the [`StandardPaths`](http.md) surfaces), and folds the result
into the **same** `MeshCollector` — so a service appears in the mesh with no egress wiring, just by
being pollable.

```python
from benzene.mesh import MeshCollector, MeshPoller, HttpServiceSource

collector = MeshCollector()
poller = MeshPoller(collector, [
    HttpServiceSource("orders", "https://orders.svc"),
    HttpServiceSource("inventory", "https://inventory.svc"),
])
await poller.poll_once()               # one sweep (call on a timer); collector now reflects the fleet
collector.query_fleet({})
```

- `HttpServiceSource(name, base_url, *, prefix="/benzene", fetch=None)` — GET-polls `{prefix}/spec` and
  `{prefix}/health` (a `503` health reply is read as the unhealthy aggregate, not an error). `fetch` is
  an injectable `async (url) -> (status, body)`, so a test drives it with no network; the default uses
  `urllib` on a worker thread. `CallableServiceSource(name, spec=, health=)` backs a source with two
  async callables — for tests or a bespoke transport (e.g. a Lambda invoke).
- `poll_once()` sweeps all sources concurrently and returns a `PollResult` per source; a down service is
  a failed result, never a broken sweep. `MeshPoller._poll` forwards a polled spec's `consumes` into the
  collector exactly like `topics`, so a source whose spec document *carries* `consumes` gives the
  collector consumer edges too (mesh.md §4). `HttpServiceSource`'s `{prefix}/spec` is answered by
  `benzene.core.ServiceSpec` (the Cloud Service Profile surface, `{"service", "topics"}` — no
  `consumes` field today), so a fleet polled purely over HTTP gets provider edges from the pull and no
  consumer edges from it; a `CallableServiceSource` whose `spec` callable instead returns a full
  `ServiceDescriptor.to_payload()` (as the AWS Lambda mesh example's `benzene:mesh`-topic source does)
  gets both. Traces still feed invocation/error stats, never graph membership, and every source composes
  with a push feed in one collector.

## Exports

`ServiceInfo`, `ServiceDescriptor`, `TopicDescriptor`, `OutboundRegistry`, `OutboundDefinition`,
`DuplicateOutboundRegistrationError`, `MESH_TOPIC`, `Schema`, `json_schema`,
`MeshPoller`, `HttpServiceSource`, `CallableServiceSource`, `ServiceSource`, `PollResult`, `PollError`,
`mesh_interception`, `DescriptorSource`, `trace_middleware`, `TraceEvent`, `TraceExporter`,
`InMemoryTraceExporter`, `QueueTraceExporter`, `parse_traceparent`, `new_trace_id`, `new_span_id`,
`current_traceparent`, `with_trace_propagation`, `TracePropagatingMessageSender`,
`MeshFeedSender`, `Heartbeat`, `Issue`, `IssueBatch`, `IssueAggregator`, `classify`,
`issue_fingerprint`, `CLASSIFICATIONS`, `MeshCollector`, `collector_registry`, `CollectorError`,
`CollectorBadRequest`, `CollectorNotFound`, `REGISTER_TOPIC`, `HEARTBEAT_TOPIC`, `TRACES_TOPIC`,
`ISSUES_TOPIC`, `QUERY_FLEET_TOPIC`, `QUERY_SERVICE_TOPIC`, `QUERY_TOPIC_TOPIC`, `QUERY_TRACE_TOPIC`,
`evaluate_cloud_service_profile`, `CloudServiceProfileReport`, `RequirementCheck`, `REQUIREMENT_IDS`,
`PROFILE_NAME`, `probe_cloud_service`, `CloudServiceProbeReport`, `RequirementProbe`, `Verdict`,
`build_artifacts`, `write_artifacts`.

## See also

- [Joining the mesh](../cookbooks/joining-the-mesh.md) — a runnable walkthrough on the order service.
- [`benzene.core`](core.md) — the registry, pipeline, and `MessageSender` this module builds on.
- [mesh specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/mesh.md)
  — the language-neutral contract these wire shapes implement.
