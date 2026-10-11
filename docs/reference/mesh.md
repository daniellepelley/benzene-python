# `benzene.mesh`

Make a Python Benzene service a first-class citizen of a **mesh**: it registers its self-description
with a collector, traces every invocation, and reports heartbeats, traces and issues. Everything here
is optional and additive. **Distribution: `benzene-mesh` (depends only on `benzene-core`).**

```bash
pip install benzene-mesh
```

## Overview

The mesh module implements the language-neutral [mesh specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/mesh.md).
Its wire shapes — the registration (a Contract Document), the `Heartbeat`, the `TraceEvent`, and the
collector topics — are the **cross-language mesh contract**: a Python service and a
.NET/Go/TypeScript one emit the same shapes and appear in the same mesh. It gives you independent
capabilities, each of which you can adopt on its own:

- **Self-description** — the service's [Contract Document](core.md), derived once at the composition
  root from `AppDefinition(contract=ContractInfo(...))` and served on the reserved `benzene:spec`
  topic (and `GET /benzene/spec`). The mesh has no self-description of its own (mesh.md §1).
- **Registration** — `registration_document(definition)` is that same document, the body of
  `benzene:mesh:register` (mesh.md §2).
- **Tracing** — `trace_middleware()` emits exactly one `TraceEvent` per invocation.
- **Collector feeds** — `MeshFeedSender` pushes the registration, heartbeats, traces, and issues to a
  collector over any outbound `MessageSender`.

Every feed is independent and optional on both sides. An unreachable collector, a failing exporter, or
a missing endpoint must never affect service traffic — the module is built so a mesh feed can never
break, slow, or block an invocation.

## Registration — the Contract Document

A service describes itself once: the Contract Document its composition root derives from its handler
registry (`requests`) and its outbound declarations (`events`, `sends`). `benzene:spec` serves it,
and the `benzene:mesh:register` body **is** it (mesh.md §2) — so what the mesh registers can never
drift from what the service serves. As of the mesh.md declared-graph rule, the registration alone is
what puts an edge in the mesh's producer/consumer graph — the graph exists before a single message
has flowed (mesh.md §4).

A service that registers a handler for a topic is that topic's **consumer** (`requests`); a service
that publishes an event (`events`) or sends to a topic (`sends`) is its **provider** — standard
pub/sub terminology (Kafka/SQS/SNS/EventBridge: the receiver is the "consumer").

```python
from benzene.core import AppDefinition, ContractInfo, OutboundRegistry
from benzene.mesh import registration_document

definition = AppDefinition(
    registry=registry,                                   # what this service consumes
    outbound=OutboundRegistry().register_send("payments:capture", CapturePayment),
    contract=ContractInfo(
        "orders",                                        # the only required field
        service_version="1.4.2",
        version_scheme="semver",
        placement={"cloud": "aws"},                      # + "region" only where documented
    ),
)

document = registration_document(definition)            # == what benzene:spec serves
payload = document.to_payload()                         # the register body (a dict)
document.registration_hash                              # "sha256:..." (contract-document §6)
```

- `registration_document(definition)` — `definition.contract_document()`, or `ValueError` when the
  definition declares no `contract` (nothing to register).
- The deployment-side fields — `runtime` (`"python"` by default), `placement`, `profile` and
  `degraded` — are declared on `ContractInfo` and ride on the document **outside both hashes**
  (contract-document §1), so the served document and the registered one are identical. `degraded`
  names the members (`"requests"`, `"events"`, `"sends"`) the producer could not determine; it is
  omitted when empty. The document carries no `instanceId` — instances are a heartbeat's business.

### `OutboundRegistry`

Declares what a service puts on the wire — in `benzene.core` (contract-document §3.3), re-exported
here — as explicit records with no handler, since nothing here receives. Two kinds, split by who owns
the contract: `register_event(topic, message_type)` for a topic this service publishes and defines,
`register_send(topic, request_type, response_type=None)` for a message to a topic another party
defines (leave `response_type` off for one-way). The Contract Document lists them as `events[]` and
`sends[]`; a collector reads both as provider edges. A port must not infer them from call sites or
any other static analysis.

```python
from benzene.core import OutboundRegistry

outbound = (
    OutboundRegistry()
    .register_event("order:placed", OrderPlaced)
    .register_send("payments:capture", CapturePaymentRequest)
)
```

Declaring one `(topic, version)` twice — as the same kind or as both — raises
`DuplicateOutboundRegistrationError` at start-up; there is no kind-less `register`. No destination
address, queue name, or topic ARN is needed: that's deployment configuration, orthogonal to the
contract.

## The Cloud Service Profile self-check

The [Cloud Service Profile](../cloud-service-profile.md) names six requirements (R1–R5, R7) a service
must satisfy so any tool can pull its operational surfaces from it with no collector; being **meshed**
(mesh.md §7.1) adds M1 (registration, heartbeats, traces) and M2 (trace context propagation) — the
profile's former R6 and R8, ids that are retired. `evaluate_cloud_service_profile` grades a composition
root's [`AppDefinition`](core.md) against them at **wiring time** — a self-assessment of what the setup
provisioned, not a runtime probe — and `declare_cloud_service_profile` puts its verdict on the
Contract Document's optional `profile` field, so `benzene:spec` serves it and the registration carries
it: the fleet can see whether a running service claims the profile and, if not, which requirements it
is missing.

```python
from benzene.mesh import declare_cloud_service_profile, evaluate_cloud_service_profile

report = evaluate_cloud_service_profile(definition, mesh_feeds=True, trace_propagation=True)
report.is_conformant          # True when every profile requirement (R1–R5, R7) is satisfied
report.missing                # e.g. ["R3", "R7"] — the profile ids the wiring does not satisfy, in order
report.reason("R7")           # the explanation recorded for one requirement (R or M)
report.to_profile()           # {"name": "cloud-service"} (or {..., "missing": [...]})
report.mesh_missing           # e.g. ["M2"] — the meshed-service claim, reported separately
report.is_meshed              # the profile in full, plus M1 and M2

definition = declare_cloud_service_profile(definition)
# definition.contract.profile == report.to_profile(); served on benzene:spec and registered alike
```

- The profile is read straight off the definition: **R1** (a registry/router to host), **R2** (≥1
  non-reserved application topic), **R5** (a `contract`, so the pipeline answers `benzene:spec` with
  the derived document wherever the transport carries a reply — a one-way binding never costs R5 — plus — where there is an HTTP surface — `/benzene/spec`), and — from the
  [`StandardPaths`](http.md) declaration — **R4** (`/benzene/invoke` enabled) and **R7** (the
  surfaces under the default `/benzene/` prefix; a service with no HTTP surface — no standard paths
  and no router — exposes no well-known HTTP surfaces, so meets R7 trivially). **R3** is `benzene:healthcheck` intercepted in the
  pipeline (a `health_interception` in `middleware`, or `StandardPaths(health=...)`, which
  `application_from` installs as one) plus — only where there is an HTTP surface — `/benzene/health`
  (`StandardPaths(health=...)`); a message-only service meets it with the topic alone.
- The meshed-service claim's two requirements are invisible to definition inspection and are passed
  explicitly: **`mesh_feeds`** (M1's three feeds — registration, heartbeats, traces — whose *sending* is
  wired into the host loop) and **`trace_propagation`** (M2 — inbound trace context joined and outbound
  clients forwarding `traceparent`). The default for each is the honest "not provisioned". M1 also
  needs a document to register (the `contract`), so it is unmet without one. They land on
  `report.mesh` (`RequirementCheck`s for M1, M2) and **never** in `missing` / `profile.missing`, which
  is why `declare_cloud_service_profile` takes neither.
- Like `degraded`, the `profile` field is **outside both document hashes**, and never changes because
  of runtime degradation — an unreachable collector does not make a conformant service stop claiming
  the profile.

`CloudServiceProfileReport` carries one `RequirementCheck(id, satisfied, reason)` per requirement on
`checks` (`REQUIREMENT_IDS` lists them: R1–R5, R7) and on `mesh` (`MESH_REQUIREMENT_IDS`: M1, M2);
`PROFILE_NAME` is `"cloud-service"`. See the runnable
[`mesh_dashboard` profile example](https://github.com/daniellepelley/benzene-python/tree/main/examples/mesh_dashboard/profile.py).

### The live-probe checker — `probe_cloud_service`

The outside-in counterpart: point it at a *deployed* service's base URL and it audits the profile over
plain HTTP, speaking only the language-neutral surfaces (`/benzene/spec`, `/benzene/health`,
`/benzene/invoke`) so it grades a Go, Node, or .NET service exactly as a Python one — it reads the
Contract Document every port's `/benzene/spec` answers with. Each requirement gets a **tri-state**
`Verdict` — `satisfied` / `not-satisfied` / `inconclusive` — always with a reason.

```python
from benzene.mesh import probe_cloud_service

report = await probe_cloud_service("https://orders.example.com")   # inject http= in tests
report.not_satisfied      # profile ids positively found unmet — the actionable failures
report.inconclusive       # profile ids a black-box probe can't verify (never a failure by itself)
report.is_clean           # True when no profile requirement was positively found unmet
report.mesh               # the meshed-service claim (M1, M2), reported separately
report.to_payload()       # {"baseUrl", "requirements": [{"id", "verdict", "reason"}, ...], "mesh": [...]}
```

The probe audits **R1–R5 and R7**. **R5** has two halves and both must hold: `GET /benzene/spec`
returns a Contract Document, and a `benzene:spec` envelope over `/benzene/invoke` answers with one
(either half positively unmet → `not-satisfied`). **R3** is graded the same way: a
`benzene:healthcheck` envelope over `/benzene/invoke` answers with the health report, and — the probed
service having an HTTP surface — `GET /benzene/health` returns the aggregate. **R7** is `inconclusive` whenever the caller probes a
non-default prefix (the service's own defaults become unknowable). The meshed-service claim is
reported apart on `report.mesh` and is **not observable from outside**, so both are `inconclusive` by
design, never silently upgraded and never counted toward `is_clean`: **M1** (registration, heartbeat
and trace delivery to a collector — the register body is the document R5 already audits) and **M2**
(propagation needs a second service or a collector to observe forwarded `traceparent`). The CLI form is
`python -m benzene.mesh.probe <url> [--prefix /benzene] [--json]`, exiting non-zero when a requirement
is positively unmet.

## Tracing

`trace_middleware()` emits exactly one `TraceEvent` per routed invocation — the topic, the semantic
status, how long it took, and its place in a W3C trace. Install it **outermost** (first in the pipeline)
so it times the whole invocation, including routing.

```python
from benzene.mesh import InMemoryTraceExporter, trace_middleware

exporter = InMemoryTraceExporter()
pipeline = MiddlewarePipeline().use(
    trace_middleware(exporter, service="orders", instance_id="orders-7f9c")
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
  currently running, or `None` outside a trace. `current_tracestate()` — the inbound `tracestate` of
  the trace it joined, carried forward unchanged (`None` when there was none, or when the inbound
  `traceparent` was invalid and a fresh trace started).
- `with_trace_propagation(sender)` (class `TracePropagatingMessageSender`) — wraps a `MessageSender` so
  each outbound message carries the current `traceparent` and `tracestate` (M2, mesh.md §7.1). Sends
  (one-way or with a reply) and published events go through the same `MessageSender` port in this
  port, so wrap the event publisher as well as the send client and both are covered; the downstream
  service then joins the same
  trace, so the resulting `TraceEvent`s feed the collector's invocation/error stats for the edge the
  registration already declared (mesh.md §4) — trace parentage is never what admits the edge itself. A
  `traceparent` the caller already set is left untouched. Compose it with the core client decorators:
  `with_retry(with_trace_propagation(sender))`.

## Collector feeds

`MeshFeedSender` pushes a service's mesh feeds to a collector over an outbound
`benzene.core.MessageSender` (Pub/Sub, SNS/SQS, Service Bus, or an HTTP POST of the wire envelope).
Sending is **fire-and-report**: it returns the outbound `Result` so a caller can log a failed feed, but
it does not raise, and a failing feed must never affect service traffic.

```python
from benzene.mesh import Heartbeat, MeshFeedSender, registration_document

feeds = MeshFeedSender(sender)                       # any benzene.core MessageSender
document = registration_document(definition)         # the document benzene:spec serves

await feeds.register(document)                       # -> benzene:mesh:register
await feeds.publish_heartbeat(Heartbeat.for_document(
    document,                                        # service, serviceVersion, registrationHash
    sent_at="2026-07-31T12:00:00Z",
    instance_id="orders-7f9c",
))                                                   # -> benzene:mesh:heartbeat
await feeds.publish_traces(exporter)                 # -> benzene:mesh:traces  {"events": [...]}
await feeds.publish_issues(aggregator.flush())       # -> benzene:mesh:issues  (see below)
```

The collector topic constants and their bodies (the cross-language contract):

| Constant | Topic | Body | Success response |
|---|---|---|---|
| `REGISTER_TOPIC` | `benzene:mesh:register` | the Contract Document | `{"accepted": 1}` |
| `HEARTBEAT_TOPIC` | `benzene:mesh:heartbeat` | `Heartbeat` | `{"accepted": 1}` |
| `TRACES_TOPIC` | `benzene:mesh:traces` | `{"events": [TraceEvent, ...]}` | `{"accepted": <count>}` |
| `ISSUES_TOPIC` | `benzene:mesh:issues` | IssueBatch | `{"accepted": <count>}` |

- `MeshFeedSender.register(document)` — announce a `ContractDocument` (its `to_payload()`).
- `MeshFeedSender.publish_heartbeat(heartbeat)` — send a `Heartbeat`.
- `MeshFeedSender.publish_traces(events)` — send an iterable of `TraceEvent`s as `{"events": [...]}`.
- `MeshFeedSender.publish_issues(batch)` — send an `IssueBatch`.

`MeshFeedSender` is the **sender** half (a service reporting in); the **receiver** is `MeshCollector`
(below).

### `Heartbeat`

A liveness beat: identity, the registered document's hash, and the health aggregate (mesh.md §5).
`registration_hash` is the `registrationHash` of the document this instance registered — a collector
whose registration carries a different one knows the instance runs a contract it hasn't learned (the
instance should re-register). `service_version` scopes that comparison to one release in a collector
implementing Service Versions.

```python
Heartbeat(
    service: str,
    sent_at: str,                                    # RFC 3339
    instance_id: str | None = None,
    registration_hash: str | None = None,
    service_version: str | None = None,
    is_healthy: bool = True,
    health_checks: Mapping[str, Any] | None = None,
)
Heartbeat.for_document(document, *, sent_at, instance_id=None, is_healthy=True, health_checks=None)
```

`to_payload()` emits `service`, optional `serviceVersion` / `instanceId` / `registrationHash`,
`sentAt`, and a `health` object `{"isHealthy": ..., "healthChecks": {...}}`. Prefer
`Heartbeat.for_document`, which takes `service`, `serviceVersion` and `registrationHash` from the
registered document so they cannot disagree with it.

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
await app.handle({"topic": "benzene:mesh:register", "headers": {}, "body": document_json})
fleet = await app.handle({"topic": "benzene:mesh:query:fleet", "headers": {}, "body": "{}"})
```

It ingests `benzene:mesh:register` / `:heartbeat` / `:traces` / `:issues` and answers four read
models — `benzene:mesh:query:fleet` / `:service` / `:topic` / `:trace`. The catalog it derives (per
mesh.md §§4–6, pinned by `mesh-collector-cases.json`):

- **The register body is a Contract Document, and only `service` is required** (mesh.md §2): no
  `schemaDialect` or hash check — a collector does not interpret schemas, and it trusts the carried
  `registrationHash`. A member that is absent, or named in `degraded`, is **unknown**: it contributes
  no edges, and a trace on one of its topics is not "undeclared".
- **The producer/consumer graph is built from the latest registration alone**: `requests` gives
  consumer edges (a handler registration is that topic's consumer), `events` ∪ `sends` gives provider
  edges (a publisher is a provider exactly as a sender is); `benzene:` entries are ignored;
  re-registration **replaces** both wholesale. Trace parentage is **never** used to admit an edge into
  this graph — it feeds a queried topic's `invocations` / `errors` / `statusCounts` for the edges the
  registration already declared (mesh.md §4).
- **Declared vs. observed** (mesh.md §4.2) — `query_topic`'s `providerActivity` / `consumerActivity`
  maps every declared name to `{"lastObservedAt": "..."}` when a matching trace has been seen, else
  `{}` — an entry present with no timestamp is a **decommission candidate**, never removed from
  `providers`/`consumers` on that basis alone. Symmetrically, the first trace naming a topic a
  service *hasn't* declared (as either provider or consumer) — checked only for a service that has
  registered, and only against members it asserted; an anonymous one has no contract to diverge from
  — is filed into the
  issues feed as a `contract-drift` issue, merged by fingerprint like any other issue (§4.1).
- **Health** aggregates a service's heartbeat instances (`healthy` / `degraded` / `unhealthy` /
  `unknown`), and a per-instance `hashMatches` compares the heartbeat's `registrationHash` with the
  service's registration. This collector keys its catalog by `service` alone — it does not implement
  [Service Versions](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/service-versions.md)
  — so that is the service's one registration (mesh.md §5).
- **`missingFeeds`** names which of `descriptor` (the read models' name for the registration) /
  `health` / `traces` a service hasn't reported, so a partial fleet renders as reduced rather than
  absent.

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
registration feed it derives the estate (health mapped to healthy/unhealthy/unreachable,
contract-drift + `previousSpecHash` history), the functional map (topics with **declared**
consumers/producers, `benzene:*` flagged `reserved`, **request/response schemas, version,
`schemaMismatch`** when two consumers (handlers) disagree — schema authority stays with the handler
registration — **`changes[]`** when a consumer re-registers a topic with a new schema, and
**`removedTopics`** for a topic no longer consumed), per-service **spec (the registered Contract
Document, as `specJson`) + per-check health**, and an **AsyncAPI 3.0** export of the domain topics; the topology's client→server
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
  a failed result, never a broken sweep.
- **The served document is the registration.** `{prefix}/spec` answers the Contract Document in every
  port, and that document is exactly the register body (mesh.md §2), so the poller ingests it as a
  registration unchanged (`requests[]` consumed, `events[]` ∪ `sends[]` provided) — a polled .NET, Go,
  or TypeScript service folds into the collector exactly like a Python one, provider edges included.
  `benzene:` entries are ignored, and `$ref`s are resolved against `components.schemas` on the way in
  (`benzene.mesh.specdoc.resolve_schema`), a cycle cut with `{}` (a reference into a catalogue the
  collector never sees would compare as "the schema changed" on a class rename). The poller's
  heartbeat carries the **served `registrationHash`** and `serviceVersion` — it computes no hash of its
  own. A `CallableServiceSource` whose `spec` callable invokes the `benzene:spec` topic (as
  the AWS Lambda mesh example's direct-invoke source does) is read the same way. Traces still feed
  invocation/error stats, never graph membership, and every source composes with a push feed in one
  collector.

## Exports

`registration_document`, `OutboundRegistry`, `EventDeclaration`,
`SendDeclaration`, `DuplicateOutboundRegistrationError`,
`MeshPoller`, `HttpServiceSource`, `CallableServiceSource`, `ServiceSource`, `PollResult`, `PollError`,
`trace_middleware`, `TraceEvent`, `TraceExporter`,
`InMemoryTraceExporter`, `QueueTraceExporter`, `parse_traceparent`, `new_trace_id`, `new_span_id`,
`current_traceparent`, `current_tracestate`, `with_trace_propagation`, `TracePropagatingMessageSender`,
`MeshFeedSender`, `Heartbeat`, `Issue`, `IssueBatch`, `IssueAggregator`, `classify`,
`issue_fingerprint`, `CLASSIFICATIONS`, `is_contract_document`, `spec_service`,
`spec_service_version`, `spec_registration_hash`, `spec_entries`, `spec_topic_ids`, `known_members`,
`resolve_schema`,
`MeshCollector`, `collector_registry`, `CollectorError`,
`CollectorBadRequest`, `CollectorNotFound`, `REGISTER_TOPIC`, `HEARTBEAT_TOPIC`, `TRACES_TOPIC`,
`ISSUES_TOPIC`, `QUERY_FLEET_TOPIC`, `QUERY_SERVICE_TOPIC`, `QUERY_TOPIC_TOPIC`, `QUERY_TRACE_TOPIC`,
`evaluate_cloud_service_profile`, `declare_cloud_service_profile`, `CloudServiceProfileReport`,
`RequirementCheck`, `REQUIREMENT_IDS`,
`PROFILE_NAME`, `probe_cloud_service`, `CloudServiceProbeReport`, `RequirementProbe`, `Verdict`,
`build_artifacts`, `write_artifacts`, `CollectorStore`, `NullCollectorStore`,
`JsonFileCollectorStore`; and, for the cloud mesh deployments, `S3CollectorStore`, `S3ArtifactStore`
/ `write_artifacts_to_s3`, `BlobArtifactStore` / `write_artifacts_to_blob`, and `S3TraceInbox`.

Gone in 3b (pre-1.0): `ServiceDescriptor`, `ServiceInfo`, `TopicDescriptor`, `MESH_TOPIC`,
`mesh_interception`, `DescriptorSource`, `OutboundDefinition` (and `OutboundRegistry.definitions()`),
`spec_topics`, `spec_produces`, and the `json_schema` /
`Schema` re-export (use `benzene.core.json_schema`).

## See also

- [Joining the mesh](../cookbooks/joining-the-mesh.md) — a runnable walkthrough on the order service.
- [`benzene.core`](core.md) — the registry, pipeline, and `MessageSender` this module builds on.
- [mesh specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/mesh.md)
  — the language-neutral contract these wire shapes implement.
