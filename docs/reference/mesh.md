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

`ServiceDescriptor` is a service's self-description, **derived from its registry** — never hand-written,
so it is always the truth of what the service serves. `ServiceInfo` carries what the registry can't
know (identity and placement).

```python
from benzene.core import Registry
from benzene.mesh import ServiceDescriptor, ServiceInfo

descriptor = ServiceDescriptor.derive(
    registry,                                    # a benzene.core Registry
    ServiceInfo(
        service="orders",                        # the only required field
        service_version="1.4.2",
        instance_id="orders-7f9c",
        placement={"cloud": "aws", "region": "eu-west-1"},
    ),
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

- `ServiceDescriptor.derive(registry, info)` — project a `Registry` + `ServiceInfo` into a descriptor.
  One `TopicDescriptor` per registered topic, **sorted by `(id, version)`** so the output is
  deterministic (the hash depends on it). Each topic's schemas come from the handler's declared
  `request_type` / `response_type`.
- `descriptor.to_payload()` — the full wire payload, including `descriptorHash`. Fields are emitted in a
  fixed order and camelCase: `service`, `serviceVersion`, `runtime`, `binding`, `instanceId`,
  `placement`, `topics`, `degraded`, `profile`, `descriptorHash`. Absent optional fields are omitted.
- `descriptor.descriptor_hash()` — `"sha256:" + hex(sha256(canonical-json))` over the **contract**. The
  canonical form sorts keys lexicographically with no insignificant whitespace, and **excludes**
  `instanceId`, `degraded`, `profile`, and `descriptorHash` itself. So two instances of the same build
  hash identically, but the hash changes when the service version, placement, topic set, or a schema
  changes. It detects this service's own redeploys and is **never compared across ports**.

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

- **Providers** come from `register`; re-registration **replaces** a service's topics wholesale.
- **Consumer edges** are derived from trace parentage — an event whose parent span belongs to a
  *different* service makes that service a consumer of the event's topic.
- **Health** aggregates a service's heartbeat instances (`healthy` / `degraded` / `unhealthy` /
  `unknown`), and a per-instance `hashMatches` surfaces a descriptor-hash drift.
- **`missingFeeds`** names which of `descriptor` / `health` / `traces` a service hasn't reported, so a
  partial fleet renders as reduced rather than absent.

`service` is required on `register` and `heartbeat` (→ `bad-request`); an unknown service / topic /
trace query is `not-found`; the query read models are one collector's shapes (the spec pins them only
as the observable surface for the ingest rules). Sender feeds live in `benzene.mesh` (`MeshFeedSender`).

## Exports

`ServiceInfo`, `ServiceDescriptor`, `TopicDescriptor`, `MESH_TOPIC`, `Schema`, `json_schema`,
`mesh_interception`, `DescriptorSource`, `trace_middleware`, `TraceEvent`, `TraceExporter`,
`InMemoryTraceExporter`, `QueueTraceExporter`, `parse_traceparent`, `new_trace_id`, `new_span_id`,
`MeshFeedSender`, `Heartbeat`, `Issue`, `IssueBatch`, `IssueAggregator`, `classify`,
`issue_fingerprint`, `CLASSIFICATIONS`, `MeshCollector`, `collector_registry`, `CollectorBadRequest`,
`CollectorNotFound`, `REGISTER_TOPIC`, `HEARTBEAT_TOPIC`, `TRACES_TOPIC`, `ISSUES_TOPIC`,
`QUERY_FLEET_TOPIC`, `QUERY_SERVICE_TOPIC`, `QUERY_TOPIC_TOPIC`, `QUERY_TRACE_TOPIC`.

## See also

- [Joining the mesh](../cookbooks/joining-the-mesh.md) — a runnable walkthrough on the order service.
- [`benzene.core`](core.md) — the registry, pipeline, and `MessageSender` this module builds on.
- [mesh specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/mesh.md)
  — the language-neutral contract these wire shapes implement.
