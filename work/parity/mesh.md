# Mesh parity gap analysis — Python vs .NET

**Domain:** the mesh (service catalog + observability): `packages/benzene-mesh`,
`packages/benzene-mesh-fleet`, `deploy/mesh` vs `Benzene.Mesh.*` in `/workspace/benzene-dotnet/src`.
**Date:** 2026-09-07. **Analysis only — no code was changed.**

## How the two ports line up

.NET carries **two generations** of the mesh side by side:

| Generation | .NET packages | Python equivalent |
|---|---|---|
| **mesh.md wire/collector** (push: descriptor + heartbeat + traces + issues, `mesh:query:*`) | `Benzene.Mesh.Wire`, `.Collector` | `benzene.mesh` — descriptor, feeds, trace, issues, collector |
| **1.0 pull aggregator** (poll `spec`+`health`, publish `manifest.json`/`topics.json`/`topology.json`/…) | `Benzene.Mesh.Contracts`, `.Aggregator`, `.Reporting`, `.Artifacts`, `.Dispatch`, `.Ui` | folded into `benzene.mesh` — `poller.py` + `artifacts.py` project the **collector catalog** into the same artifact set |
| **backend-composed plane** (read the fleet back out of X-Ray/Tempo/Jaeger/CloudWatch) | `.Fleet.*`, `.Tracing.Tempo`, `.Usage.*`, `CompositeMeshFleetReadModel` | **absent** — `benzene.mesh_fleet` maps the *other* direction (mesh → backend document) |

Python's single-collector architecture is the better shape and should not be undone: one catalog
feeds both the live query surface and the static artifacts, so `topology.json` comes from the
declared graph for free where .NET needed a separate structural derivation. The real gaps are
**depth inside the collector** (retention, counters, latency, the live-plane read model) and the
**backend-composed plane** — not the .NET package layout.

## Frozen-contract flags (read before implementing anything below)

The following are cross-language and **cannot** move without the spec repo moving first. Nothing in
this document proposes changing them, and each item below states which side of the line it sits on:

- descriptor shape and `descriptorHash` (`descriptor.py`) — untouched by every item here.
- collector ingest semantics + the `benzene:mesh:*` reserved topics — untouched.
- the issue classification vocabulary and `issue_fingerprint` recipe — untouched.
- **the mesh-ui artifact field set**, pinned key-for-key ("no more, no less") by
  `tests/test_mesh_artifact_contract.py`. Items below only ever **fill fields that already exist
  and are currently hard-coded to `null`/`False`** (`status`, `avgDurationMs`, `requestsPerMinute`,
  `p50/p95/p99LatencyMs`, `changes`). **Do not add `versionCompatibility`, `transports`,
  `owningTeam`, or `snapshotAtUtc`** — see "Not worth porting".
- **Query-response shapes are subset-matched**, not exact (`tests/conformance_runner.py::_mesh_subset`,
  and the fixture header says "body parsed-JSON subset"). *Adding* keys to a `benzene:mesh:query:*`
  response is therefore conformance-safe; renaming or removing one is not. Items 4, 6, 7 and 12
  rely on this and on nothing else.

---

## G1. The collector leaks memory: `max_events` bounds the ring but nothing else

**Severity: CRITICAL** (a long-lived collector — the Fargate Mesh Host — grows without bound).

**Python:** `packages/benzene-mesh/benzene/mesh/collector.py`

- `self._events: deque[_Event] = deque(maxlen=max_events)` is bounded, **but**
- `self._span_owner[event.span_id] = event.service` (in `ingest_traces`) is a plain `dict` that is
  **never pruned**. `deque(maxlen=…)` evicts silently, so the owner index keeps one entry per span
  id ever seen — for the lifetime of the process. At ~100 msg/s that is ~8.6M entries/day
  (hundreds of MB), and it is *also* serialized indirectly: `restore()` rebuilds `_span_owner` from
  the bounded events, so a restart silently shrinks it — the leak is invisible in tests and only
  shows up in a long-running container.
- `self._issues: dict[str, dict]` is likewise unbounded. Issues are keyed by fingerprint
  (`service|topic|version|classification|discriminator`), so the key space is bounded in a healthy
  fleet — but `contract-drift` issues are synthesized per observed-undeclared `(service, topic)`
  pair, and a fleet with generated/parameterised topic ids (or one noisy misconfigured emitter) can
  grow it indefinitely. It is also copied whole by `snapshot()` on **every mutating ingest**, so
  growth is quadratic in store-write cost, not linear.

**.NET:** `src/Benzene.Mesh.Collector/MeshCollectorStore.cs:140-200` — the ring is a fixed-capacity
circular `List` and the eviction path explicitly removes the evicted span from the owner index
(`_spanService.Remove(evicted.SpanId)`); issues are bounded by a `maxIssues` ctor arg (default 1024)
evicting the oldest `lastSeen` (`MeshCollectorStore.cs:219, 549`).

### Implementation spec

Package `benzene-mesh`, file `collector.py`. No new deps, no wire change.

1. Replace the `maxlen` deque with explicit eviction so the index stays in lockstep:

```python
def __init__(self, *, store=None, max_events=10_000, max_issues=1_024) -> None:
    self._events: deque[_Event] = deque()          # unbounded deque, bounded by _append_event
    self._max_events = max_events
    self._max_issues = max_issues

def _append_event(self, event: _Event) -> None:
    self._events.append(event)
    if event.span_id:
        self._span_owner[event.span_id] = event.service
    while len(self._events) > self._max_events:
        evicted = self._events.popleft()
        # span ids are unique per event, so the owner entry belongs to exactly this event
        if evicted.span_id and self._span_owner.get(evicted.span_id) == evicted.service:
            self._span_owner.pop(evicted.span_id, None)
```

   `restore()` must funnel through the same helper (build an empty deque, then `_append_event` each
   restored raw event) so the trim-on-restore behaviour and index rebuild stay in one place.

2. Bound `_issues` in `_merge_issue`: when inserting a *new* fingerprint and
   `len(self._issues) >= self._max_issues`, evict the entry with the smallest `lastSeen`
   (`min(self._issues.items(), key=lambda kv: kv[1].get("lastSeen") or "")`), then insert. Never
   evict to make room for a *merge* into an existing fingerprint. Document the rule in the
   docstring the way `max_events` already is.

3. `max_issues` is a keyword-only ctor arg, defaulting to 1024 (match .NET so a cross-language
   fleet behaves the same under pressure).

**Tests** (`tests/test_mesh_collector.py`):
- `test_span_owner_is_evicted_with_the_event` — `MeshCollector(max_events=2)`, ingest 5 events,
  assert `len(collector._span_owner) <= 2` and that a parent span aged out yields no observed
  consumer (the existing §4.2 derivation still degrades correctly, never raises).
- `test_issues_are_bounded_and_evict_oldest_last_seen` — `max_issues=2`, ingest 3 distinct
  fingerprints with increasing `lastSeen`, assert the oldest is gone and the newest two remain;
  then re-merge into a surviving fingerprint and assert no eviction happened.
- `test_snapshot_round_trips_a_trimmed_catalog` — snapshot → `restore()` into a collector with a
  smaller `max_events`, assert the newest events survive and `_span_owner` matches them exactly.

---

## G2. Invocation counts silently decay — stats are derived from the bounded ring

**Severity: HIGH** (numbers on the UI go *down* over time with no explanation; the mesh's own
"if the numbers don't add up, the user won't believe the system" rule).

**Python:** every stat is a full scan of the retained window —
`_invocations_on` / `_invocations_by` / `query_topic`'s `errors`+`statusCounts` /
`artifacts._usage()` all iterate `self._events` (`collector.py`). Since `max_events` landed, a
topic's `invocations` **falls** as its events age out, `usage.json` counts shrink between
publishes, and the `deprecation-candidate` signal (zero traffic) becomes reachable purely by
eviction. Two secondary costs: `query_fleet` is O(topics × events) (it calls `_invocations_on` per
topic) and `build_artifacts` calls `query_topic` **twice per topic** (topology + topics), each a
fresh full scan — at 10k events × 200 topics that is millions of iterations per 15s UI poll.

**.NET:** counters are incremented at ingest and are **cumulative since process start**
(`MeshCollectorStore.cs:174-192`: `topic.Invocations++`, `topic.StatusCounts[status]++`,
`topic.Errors++`, `service.Invocations++`), deliberately outliving the ring window. The honesty
mechanism is `MeshWindow.CountsWindowed=false` + `CountsSince=StartedAtUtc` — "these counts answer a
different window", badged rather than blanked.

### Implementation spec

Package `benzene-mesh`, file `collector.py`. Additive to the wire (subset-matched responses).

1. Add ingest-time counter state, all plain dicts on `MeshCollector`:

```python
self._started_at: str                       # ISO-Z, set in __init__ (injectable clock for tests)
self._topic_stats: dict[str, _TopicStats]   # keyed by topic id (see note on version below)
self._service_stats: dict[str, _ServiceStats]
```

   with `@dataclass class _TopicStats: invocations: int = 0; errors: int = 0;
   status_counts: Counter[str]; total_duration_ms: float = 0.0; durations: deque[float]` and
   `_ServiceStats: invocations, errors`. Increment them in `ingest_traces` (one pass, no scans).
   Keep keying by **topic id only** (Python's `_topics`/`_providers_of` are id-keyed and the wire
   fixtures assert id-keyed topics); .NET keys by `(topic, version)` — do **not** change the key
   space here, it would move the query shape.

2. Rewrite `_invocations_on`/`_invocations_by`/`query_topic`'s `errors`+`statusCounts` and
   `artifacts._usage()` to read the counters. The ring stays the source only for things that are
   genuinely per-event: `query_trace`, the §4.2 `providerActivity`/`consumerActivity` derivation,
   and recent flows (G4).

3. Honesty: add `"countsSince": self._started_at` to the fleet and topic query responses (additive
   key, subset-safe) and document in the module docstring that **counts are cumulative since
   collector start / last restore, while flows and liveness are windowed by `max_events`** — the
   same split .NET states. `usage.json`'s `windowStartUtc` should become `self._started_at`
   (currently `None`) and `windowEndUtc` the `generated_at` stamp; both fields already exist in the
   pinned artifact set, so this is a fill-in, not a shape change.

4. Bump `_SNAPSHOT_VERSION` to 3 and persist the counters + `startedAt` in `snapshot()`/`restore()`
   (an older snapshot is already ignored wholesale, which is the correct degradation).

**Tests:**
- `test_invocation_counts_survive_ring_eviction` — `max_events=2`, ingest 5 events on one topic,
  assert `query_topic(...)["invocations"] == 5` while `query_trace` for the first trace raises
  `CollectorNotFound`.
- `test_counts_since_is_reported` — assert `countsSince` is present and equals the injected start.
- `test_counters_restore_from_snapshot` — snapshot after 5 events, restore into a fresh collector,
  assert counts continue from 5 rather than restarting.
- Existing conformance fixtures must stay green unchanged (they never evict).

---

## G3. Trace duration is dropped at ingest, so every latency field is permanently null

**Severity: HIGH** (the wire already carries the data; the artifact contract already has the
fields; the UI renders them as "reduced" forever).

**Python:** `TraceEvent` (`trace.py`) carries `duration_ms` and `exception_type` and
`to_payload()` sends them, but the collector's `_Event` (`collector.py`) keeps only
`trace_id/span_id/parent_span_id/service/topic/status/started_at` — duration and exception type are
**discarded on ingest**. Consequently `artifacts.py` hard-codes `avgDurationMs: None` in every usage
entry and `requestsPerMinute`/`p50`/`p95`/`p99LatencyMs: None` on every topology edge, with the
comment "rate/latency need a metrics feed this collector doesn't have" — which is no longer true.

**.NET:** `MeshCollectorStore.cs:176` accumulates `topic.TotalDurationMs += traceEvent.DurationMs`
and reports `AvgDurationMs = TotalDurationMs / Invocations` (`:651`); recent-flow rows compute a
real span from `StartedAt + DurationMs` (`:664-672`). `TopicSummary.missingFeeds` carries
`"duration"` only on planes that genuinely can't measure it — the push collector "observes every
dimension".

### Implementation spec

Package `benzene-mesh`, files `collector.py` + `artifacts.py`. **Fills existing artifact fields
only — no new keys.**

1. `_Event` gains `duration_ms: float | None = None` and `exception_type: str | None = None`,
   parsed in `ingest_traces` (`raw.get("durationMs")`, `raw.get("exceptionType")`) and carried
   through `snapshot()`/`restore()` (already covered by the G2 version bump).

2. `_TopicStats` accumulates `total_duration_ms` and a bounded `durations: deque[float]`
   (`maxlen=1024`, newest-wins) for percentiles. Percentiles from a bounded reservoir are honest
   enough for a UI and cost nothing; document the bound. Expose:
   - `avg_duration_ms(topic) -> float | None` — `None` when no event carried a duration (absent ≠ 0).
   - `percentiles(topic) -> tuple[float|None, float|None, float|None]` — p50/p95/p99 via
     `statistics.quantiles` or a plain sorted index; `None` below a floor of 2 samples.

3. `artifacts._usage()`: set `avgDurationMs` from the per-(topic, service, status) average when any
   duration was observed, else keep `None`.

4. `artifacts._topology()`: per client→server edge, fill
   - `errorRate` (already computed),
   - `requestsPerMinute` = `invocations / minutes_observed` where `minutes_observed` is derived from
     the **retained events'** `startedAt` span for that edge, and **`None`** when fewer than two
     timestamps or a zero span (no divisor → no number; never a lower bound — this is the same
     honesty rule .NET's `AttributeEdge` states),
   - `p50/p95/p99LatencyMs` from the provider side's retained durations, `None` when unknown.
   Because these read the **windowed ring** while counts are cumulative (G2), the rate must be
   computed from windowed invocations, not the cumulative counter — otherwise the rate is inflated.
   State that in the code comment.

**Tests** (`tests/test_mesh_artifacts.py` + `tests/test_mesh_collector.py`):
- `test_usage_reports_average_duration_when_observed` and `..._stays_null_when_absent`.
- `test_topology_edge_carries_rate_and_percentiles` — two events 60s apart → a defined
  `requestsPerMinute`; single event → `None`.
- `tests/test_mesh_artifact_contract.py` must pass **unchanged** (this is the acceptance criterion:
  values change, key sets do not).

---

## G4. The fleet query answers none of the live-plane fields the vendored UI reads

**Severity: HIGH** (the live plane is unreachable in Python today; the shipped
`deploy/mesh/collector/ui/mesh-ui.html` is a *newer* build than .NET's and already expects them).

**Python:** `MeshCollector.query_fleet` returns `{"services", "topics", "issues"}`. The vendored UI
bundle initialises its fleet slice as
`{generatedAt, services, topics, traces, issues, inboxIssues, window, …}` and its reducer does
`r.traces = s.traces` then `s.traces.length > 0` and
`s.topics.some(d => !d.missingFeeds.includes("stats") && d.invocations > 0)` — i.e. a Python fleet
body would `TypeError` on `undefined.length` / `undefined.includes`. `deploy/mesh/collector/static.py`
only wires the **static artifact** mode (no `data-fleet-url`), which is why nothing is visibly
broken — but it also means the Live traffic view, the issue inbox's flow classes, and the trace
waterfall cannot be turned on for a Python mesh at all.

**.NET:** `FleetView` carries `Traces` (recent flows, newest first, capped at 20 —
`MeshCollectorStore.cs:390, 659-672`), `Window` (`MeshWindow`, `WhenWritingNull`), and
`TopicSummary.MissingFeeds` naming absent **stat dimensions** (`descriptor`/`duration`/`stats`) so
the UI renders "—" not "0". `FleetQuery.IncludeFlows` (absent/null ⇒ true) is a **cost hint**: a
plane that pays per flow lookup may return an empty `Traces`.

### Implementation spec

Package `benzene-mesh`, file `collector.py`; wiring in `deploy/mesh/collector/`. All keys are
**additive** to a subset-matched response.

1. `query_fleet(body)` gains:
   - `"generatedAt"`: ISO-Z now (injectable clock).
   - `"traces"`: recent flows, built by grouping the retained ring by `trace_id`, newest first by
     the earliest event's `startedAt`, capped at **20**. Row shape (confirm each key against the
     bundle before shipping — grep `mesh-ui.html` for `startedAt`, `failed`, `durationMs`,
     `services`, `events`, `topic`):
     `{"traceId", "startedAt", "durationMs", "services": [...], "events": <span count>,
       "failed": <any non-success status>, "topic": <earliest event's topic or null>}`.
     `topic: null` is meaningful (the UI's "uninstrumented failure" class) — emit `null`, never omit
     the key, and never fabricate a topic.
   - `"window"`: `{"countsWindowed": False, "countsSince": self._started_at}` (pairs with G2).
   - per-topic `"missingFeeds"`: `["descriptor"]` when no service declares the topic;
     `["duration"]` when no retained event carried a duration; `["stats"]` when the topic has no
     observed traffic at all. Empty list when everything is observed (the push plane's normal case).
2. `"includeFlows"` cost hint: when `body.get("includeFlows") is False`, skip flow construction and
   return `"traces": []`. An empty list was always a legal answer, so this is safe by construction
   and lets the 24h inbox poll stop scanning the ring.
3. `deploy/mesh/collector/static.py`: stamp `data-fleet-url="/mesh/fleet"` on the served page when
   the host is running the collector (it always is), so the live plane lights up. Keep the static
   artifact path working with no fleet endpoint — that is the documented static floor.

**Tests:**
- `tests/test_mesh_collector.py::test_fleet_reports_recent_flows_newest_first` (cap of 20, a
  single-event trace, a trace whose events have no `startedAt`).
- `..._include_flows_false_returns_no_flows`.
- `..._topic_missing_feeds_names_absent_dimensions`.
- `deploy/mesh/collector/tests/test_host.py::test_fleet_route_body_satisfies_the_ui_contract` — a
  guard asserting the exact key set the bundle reads (`services/topics/traces/issues/window/
  generatedAt` and per-topic `missingFeeds`/`invocations`), so a future change can't silently dark
  the live plane again.

---

## G5. No fleet read seam: an existing OTel backend cannot back the Python mesh

**Severity: HIGH** (architectural; it is the difference between "adopt the mesh" and "re-instrument
your fleet to adopt the mesh").

**Python:** `collector_registry()` binds the five query topics **directly to `MeshCollector`
methods** (`_query_handler(collector.query_fleet)` etc.). There is no interface between the query
handlers and the in-memory catalog, so the only way to answer `benzene:mesh:query:*` is for every
service to push into this collector. `benzene-mesh-fleet`'s `mappers.py` is explicitly the *export*
direction — "the mesh trace model **into** each backend's shape" — the inverse of what .NET's
`Benzene.Mesh.Fleet.*` do.

**.NET:** `IMeshFleetReadModel` (`src/Benzene.Mesh.Collector/IMeshFleetReadModel.cs`) is the read
seam; `MeshCollectorStore` is one implementation and `CompositeMeshFleetReadModel`
(`CompositeMeshFleetReadModel.cs`) is the other — it composes an `IMeshTraceSource`
(`IMeshTraceSource.cs`; implementations: `Benzene.Mesh.Fleet.Aws.XRay`, `.Fleet.Jaeger`,
`.Fleet.Tempo`) with `IMeshUsageSource`s for counts. `MeshCollectorHandlers.Queries` is the
five query handlers **with no ingest**, for a host that has no ring at all. Each source is fetched
in its own try/except so one failing backend degrades its own slice to empty rather than blanking
the view; per-service and single-topic pages return `null` on that plane (no descriptor feed) and
rows carry `missingFeeds: ["descriptor","health","stats"]`.

### Implementation spec

Two steps; the seam alone is cheap and unblocks the rest.

**Step A — the seam (`benzene-mesh`, `collector.py`, ~40 lines).**

```python
@runtime_checkable
class FleetReadModel(Protocol):
    """Answers the mesh:query:* read models. MeshCollector is the push-plane implementation."""
    async def fleet(self, query: dict[str, Any]) -> dict[str, Any]: ...
    async def service(self, query: dict[str, Any]) -> dict[str, Any]: ...
    async def topic(self, query: dict[str, Any]) -> dict[str, Any]: ...
    async def trace(self, query: dict[str, Any]) -> dict[str, Any]: ...
```

Give `MeshCollector` async `fleet/service/topic/trace` wrappers over its existing sync
`query_*` methods (keep the sync ones — the artifact builder calls them directly and must stay
synchronous). Then:

- `collector_registry(collector=None, *, read_model=None)` — unchanged default behaviour.
- `query_registry(read_model)` — the five query topics only, no ingest (the `Queries` analogue).
  Handlers raise/translate `CollectorBadRequest`/`CollectorNotFound` exactly as today, so the
  fixture-pinned status mapping is untouched.

**Step B — one read adapter as proof (`benzene-mesh-fleet`, new `trace_sources.py`).**

Pick **Tempo or Jaeger, not X-Ray**: both are plain HTTP+JSON, so the adapter needs **no new
dependency** (stdlib `urllib` on a worker thread, the same `HttpGet` seam `poller.py` already
defines and injects for tests). X-Ray needs `boto3` and is the least portable; add it later behind
the existing `[aws]` extra if asked.

```python
@runtime_checkable
class TraceSource(Protocol):
    async def get_trace(self, trace_id: str) -> dict | None: ...
    async def get_correlation(self, correlation_id: str) -> dict | None: ...
    async def recent_flows(self, *, limit: int = 20) -> list[dict]: ...

class JaegerTraceSource:   # GET /api/traces/{id}, /api/traces?service=…&tags=…, /api/services
    def __init__(self, base_url: str, *, services: list[str] | None = None,
                 correlation_lookback_s: float = 86_400, recent_lookback_s: float = 3_600,
                 search_limit_per_service: int = 20, fetch: HttpGet | None = None) -> None: ...

class CompositeFleetReadModel:            # implements FleetReadModel
    def __init__(self, *, traces: TraceSource, usage: UsageSource | None = None) -> None: ...
```

Behaviour rules to copy verbatim from .NET because they are product decisions, not implementation
detail:
- **Fetch isolation** — each source in its own `try/except`; a failing source contributes an empty
  slice, never an exception out of `fleet()`.
- **Absent ≠ zero** — anonymous (trace-only) service rows carry
  `missingFeeds: ["descriptor","health","stats"]`; `service()`/`topic()` return `not-found` on this
  plane (there is no descriptor feed), which is honest, not a bug.
- **Reachable-but-unsuccessful → empty; connection failure → raise** (so the composite can degrade
  the slice and the host can log it).
- Jaeger's search **requires a service**, so a fleet-wide search fans out over the configured
  service list or `GET /api/services`; dedupe by trace id; cap per service.
- Prefer a `benzene.service` span tag over the backend's own process/service name (on Lambda the
  backend name is the handler, not the service).

New optional extra: none for Jaeger/Tempo. Mark the adapter's docstring "verified against the
documented API shapes with an injected `fetch`, not against a live instance" — the same caveat .NET
carries, and honest.

**Tests:** `tests/test_mesh_fleet.py` — drive `JaegerTraceSource` with a fake `fetch` returning
recorded Jaeger JSON: trace mapping, non-Benzene span filtering, correlation fan-out + dedupe +
earliest-first ordering, recent-flow rows, service discovery, the null/empty cases. Plus
`test_composite_fleet_read_model_degrades_a_failing_source` and a test that `query_registry`
answers all five topics against a duck-typed fake read model (no backend at all).

---

## G6. No correlation-id query — cross-service triage from a ticket id is impossible

**Severity: MEDIUM-HIGH** (the data is already on the wire and thrown away).

**Python:** `TraceEvent.correlation_id` is populated by `trace_interception` from `x-correlation-id`
and serialized by `to_payload()` — and then **dropped** by the collector's `_Event`. There is no
`benzene:mesh:query:correlation` topic (`collector.py` registers four query topics).

**.NET:** `CorrelationQueryMessageHandler` / `MeshCollectorStore.Correlation(id)` — filter the ring
by `CorrelationId`, group by trace id, return `CorrelationView { CorrelationId, Traces: [TraceView] }`
(events in start order, traces earliest-first) so the UI renders each through the same waterfall.
Empty id → `bad-request`; nothing matched → `not-found`; a null correlation id never matches (the
mesh never fabricates one). Explicitly **read-model only** — no wire, ingest, or spec change — and
.NET flags it as *not yet conformance-pinned across languages*.

### Implementation spec

`benzene-mesh`, `collector.py`. Additive.

1. `_Event` gains `correlation_id: str | None` (parsed from `raw.get("correlationId")`), persisted
   in the snapshot (covered by the G2/G3 version bump).
2. `QUERY_CORRELATION_TOPIC = "benzene:mesh:query:correlation"`; export it from
   `benzene/mesh/__init__.py` alongside the other four.
3. `query_correlation(body)`:
   - `_require(body, "correlationId")` → `bad-request` when empty.
   - Filter retained events on an exact, non-null match; group by `trace_id`; order events within a
     trace by `started_at` (stable fallback: insertion order); order traces by earliest `started_at`.
   - No matches → `CollectorNotFound` → `not-found`.
   - Return `{"correlationId": …, "traces": [{"traceId": …, "events": [{"spanId", "service"}, …]}]}`
     — exactly the `query_trace` per-trace shape so the UI reuses one renderer. (If G4's flow rows
     land first, mirror whatever `query_trace` emits then; the two must not diverge.)
4. Register it in `collector_registry` (and `query_registry` from G5).

**Docs:** `docs/reference/mesh.md` must state this is a **port-local read model, not yet
cross-language pinned** — matching .NET's own caveat — so nobody treats it as spec.

**Tests:** `tests/test_mesh_collector.py` — multi-trace correlation grouping and ordering, null
correlation ids never matching, empty id → `bad-request`, unknown id → `not-found`.

---

## G7. Topic `status` is always null and the catalog diff only knows `schema-changed`

**Severity: MEDIUM** (a shipped, pinned artifact field that never carries a value — the whole
"is this topic still used / who is missing" product surface is dark).

**Python:** `artifacts._topics()` emits `"status": None` unconditionally, and `_topic_changes()`
produces only `schema-changed` (from `previous_topic_specs`). `removedTopics` works.

**.NET:** `Benzene.Mesh.Aggregator` computes `MeshTopicStatus`:
- `deprecation-candidate` — produced somewhere, consumed by nobody. A candidate for retiring, *not*
  proof it is safe to delete.
- `gap` — consumed somewhere (through non-HTTP bindings only), produced nowhere in the fleet.
  Deliberately carved out for HTTP-invoked topics, whose "producer" is inherently external —
  without the carve-out nearly every REST endpoint false-positives.
- `null` — both sides present, or no reliable signal.
Never set for a reserved topic. It also diffs the fresh catalog against the previous published one
for `topic-added` / `producers-changed` / `consumers-changed` (`+name`/`-name` deltas), and a first
run or unreadable previous catalog claims **no** changes (never a wall of "added" noise).

### Implementation spec

`benzene-mesh`, `artifacts.py`. **Fills existing fields only.** The vocabulary
(`deprecation-candidate` / `gap` / `topic-added` / `producers-changed` / `consumers-changed` /
`schema-changed`) is the shared read-model contract — use these exact strings, do not invent new ones.

1. `_topic_status(topic, providers, consumers) -> str | None`:
   - `None` for a reserved (`benzene:`-prefixed) topic — no status ever.
   - `"deprecation-candidate"` when `providers` and not `consumers`.
   - `"gap"` when `consumers` and not `providers`. Python's collector has no HTTP-binding
     information, so the .NET carve-out cannot be reproduced exactly — the honest equivalent is to
     apply it only to topics **declared in a descriptor's `consumes`** (a fleet-internal
     declaration), which is already the only way `consumers` gets populated here. Document that
     difference in the function docstring; do **not** guess an HTTP binding.
   - `None` otherwise.
2. Catalog diff: `build_artifacts`/`write_artifacts*` gain an optional
   `previous: dict | None = None` (the previously published `topics.json`, read back by the caller —
   `write_artifacts` from disk, `write_artifacts_to_s3`/`_to_blob` via a `read` on the store). When
   `previous` is `None` or unparseable, claim **no** changes at all. Otherwise append to `changes`:
   `{"kind": "topic-added", "description": …}` for a (topic) absent before, and
   `{"kind": "producers-changed"/"consumers-changed", "description": "+orders, -legacy"}` from the
   set delta. Never flag a reserved topic.
   Adding a `read(key)` method to `S3ArtifactStore`/`BlobArtifactStore` is a small, additive change
   to those classes (missing object → `None`, matching the collector stores' forgiving `load`).

**Tests:** `tests/test_mesh_artifacts.py` — a produced-but-unconsumed topic is
`deprecation-candidate`; a consumed-but-unproduced one is `gap`; a reserved topic is always `None`;
a first run claims no `changes`; a second run reports added/producers-changed/consumers-changed.
`tests/test_mesh_artifact_contract.py` unchanged.

---

## G8. No usage-source seam and no metrics-backend adapters

**Severity: MEDIUM** (matters for a fleet that already exports OTel metrics but cannot push traces
to a collector — and for filling `requestsPerMinute` where G3's ring-derived rate is too coarse).

**Python:** `usage.json` is derived purely from the collector's own retained traces
(`artifacts._usage`). There is no seam for a metrics backend; `benzene-otel` emits the metrics but
nothing reads them back.

**.NET:** `IMeshUsageSource.FetchUsageAsync(MeshUsageWindow?)` with three implementations:
`Benzene.Mesh.Usage.CloudWatch` (`ListMetrics` + `GetMetricData` `Sum` per dimension combo, over
`benzene.messages.processed` tagged `topic`/`transport`/`result`),
`Benzene.Mesh.Usage.ApplicationInsights` (KQL `summarize sum(valueSum) by topic, transport, result`
over Log Analytics `customMetrics`), and `Benzene.Mesh.Collector.CollectorUsageSource` (the
collector's own cumulative counters bridged into the same shape). The aggregator polls every source
concurrently with a 10s per-fetch bound, merges, and publishes **only when at least one source
reported** — the artifact's absence means "no feed wired", a present-but-empty `entries` means
"wired, no traffic". Both cloud adapters leave `service`/`avgDurationMs` null (the counter is not
tagged that way) and both depend on **delta temporality** in the export pipeline.

### Implementation spec

1. **Seam** (`benzene-mesh`, `artifacts.py` or a new `usage.py`):

```python
@runtime_checkable
class UsageSource(Protocol):
    """A metrics backend reporting counts per (topic, transport, status) over a window."""
    async def fetch_usage(self, window: tuple[str, str] | None = None) -> dict[str, Any]: ...
```
   returning the `usage.json` body shape already pinned by the artifact contract
   (`generatedAtUtc`/`windowStartUtc`/`windowEndUtc`/`entries[]` with
   `topic/version/service/transport/status/count/avgDurationMs/source`). `build_artifacts` gains
   `usage_sources: Iterable[UsageSource] = ()`; each is awaited with `asyncio.wait_for(..., 10.0)`
   in its own `try/except` (a throwing/hung source contributes nothing and never fails the pass);
   entries are concatenated, windows widened. **Keep the collector-derived usage as the default
   source** so today's behaviour is unchanged when no source is wired.
   Note: `build_artifacts` is currently sync — either add an `async def build_artifacts_async` used
   by the S3/Blob/host writers, or take pre-fetched usage documents as a parameter. Prefer the
   latter (keeps the projection pure and synchronous, which the artifact tests depend on).

2. **Adapters** (`benzene-mesh-fleet`, new `usage_sources.py`):
   - `PrometheusUsageSource(base_url, *, fetch=None)` — **first**, because it is HTTP+JSON with
     **no new dependency** and covers Tempo/Grafana/K8s meshes; instant PromQL over
     `benzene_messages_processed_total` grouped by topic/transport/result.
   - `CloudWatchUsageSource(namespace="Benzene/Mesh", metric_name="benzene.messages.processed", …,
     client=None)` under the existing `[aws]` extra, lazily importing `boto3`, injectable client —
     the established pattern in `store.py`/`s3_artifacts.py`.
   - Application Insights only if an Azure mesh actually asks; it needs `azure-monitor-query`, a new
     extra, for a strictly coarser signal.
   Carry .NET's two documented caveats verbatim in the docstrings: **delta temporality is required**
   (a cumulative export makes `Sum` over-count badly), and **absent dimensions stay `None`, never
   guessed** (`service`, `avgDurationMs`).

**Tests:** injected fake clients/fetches only (no network): dimension→entry mapping, empty result →
empty `entries` (never `None`), a throwing source is skipped and the pass still publishes, window
echo-back.

---

## G9. No annotations write path — the discussion feature is read-only-empty

**Severity: MEDIUM** (the vendored UI has an `annotationsEndpoint`; Python publishes a permanently
empty `annotations.json`).

**Python:** `artifacts._annotations()` returns `{"generatedAtUtc": …, "annotations": []}` with the
comment "writing is a backend-gated live-plane feature". Nothing implements the write.

**.NET:** `MeshAnnotationPublisher` (`src/Benzene.Mesh.Aggregator/MeshAnnotationPublisher.cs`) +
`MeshAnnotationsMessageHandler` (`mesh:annotations:add`, `POST /mesh/annotations`, returns
`Created`). Shape-only validation: entity/author/text required and trimmed, bounds
`MaxEntityLength 200` / `MaxAuthorLength 80` / `MaxTextLength 4000` (one post must not bloat the
artifact every reader downloads). Identity is deliberately **not** enforced — `author` is a
self-declared display name and who may post is the fronting gateway's decision. Read-modify-write is
serialized per process; a corrupt existing log is **parked** to a timestamped
`annotations.unreadable-*.json` sibling before starting fresh, because notes are the one artifact
that cannot be regenerated from the fleet.

### Implementation spec

`benzene-mesh`, new `annotations.py`; wired in `deploy/mesh/collector/host.py`.

```python
ANNOTATIONS_TOPIC = "benzene:mesh:annotations:add"
MAX_ENTITY, MAX_AUTHOR, MAX_TEXT = 200, 80, 4000

class AnnotationStore(Protocol):          # duck-typed; the three artifact stores already fit
    def read(self, key: str) -> dict[str, Any] | None: ...
    def write(self, key: str, document: dict[str, Any]) -> None: ...

class AnnotationPublisher:
    def __init__(self, store: AnnotationStore, *, key: str = "annotations.json",
                 clock: Callable[[], str] = _now_iso) -> None: ...
    async def add(self, entity: str, author: str, text: str) -> dict[str, Any]:
        """Append one annotation and return that entity's whole thread."""

def annotations_registry(publisher) -> Registry   # or a handler added to collector_registry
```

- Validation is **shape only**, in the handler: missing/blank entity, author or text (after
  `strip()`) → `bad-request`; over-length → `bad-request` naming the field and the bound. Success →
  `Result.created(thread)` to match .NET's status.
- Concurrency: serialize read-modify-write behind an `asyncio.Lock`, rebound per running loop
  exactly as `MeshCollector._save_gate()` already does (a warm Lambda container reuses the instance
  across `asyncio.run` calls). Do the blocking store I/O via `asyncio.to_thread`, like
  `persist_off_loop`.
- Corrupt log: on unparseable existing content, write it to
  `annotations.unreadable-{timestamp}.json` **before** starting a fresh log. Never discard silently,
  never fail the write.
- `artifacts._annotations()` gains an optional `existing` parameter so a publish pass carries the
  recorded thread forward instead of overwriting it with `[]` — **this is load-bearing**: without
  it, the next artifact publish erases every note.

**Tests:** `tests/test_mesh_annotations.py` — append/thread-return, per-entity threading, each
validation bound, corrupt-log parking, artifacts carrying an existing log through a publish, and
concurrent `add()`s under `asyncio.gather` producing no lost update.

---

## G10. Issues must be recorded by hand; the aggregator is unbounded and has no flush loop

**Severity: MEDIUM** (a pit-of-success gap — the feed exists but nothing fills it automatically).

**Python:** `IssueAggregator` (`issues.py`) is correct and normative (classification + fingerprint),
but a service must call `record(...)` itself for every failure and drive `flush()` on a timer — the
cookbook shows exactly that. `self._issues` in the aggregator is **unbounded** (a fingerprint
explosion from a noisy topic grows it without limit), and there is no middleware analogue of
`trace_interception` for issues.

**.NET:** `UseMeshIssues(info, exporter, statusReader)` — wired immediately **inside**
`UseMeshTrace` (trace outermost) so `MeshSpan.Current` supplies the exemplar trace id.
`HttpMeshIssueExporter` is a **bounded accumulator**: 256 fingerprints, **drop-new** when full,
newest-3 exemplars, 30s interval flush **including empty liveness batches**, delta counts per
flush, same lossy/dispose rules as the trace exporter.

### Implementation spec

`benzene-mesh`, `issues.py` (+ a small addition to `trace.py`'s contextvar usage).

1. Bound the aggregator: `IssueAggregator(service, *, max_fingerprints=256)`. When full, a **new**
   fingerprint is **dropped** (never evict an existing one — its accumulated delta would be lost);
   count the drops and expose `dropped` so a host can log it. Existing fingerprints keep merging.
2. `issues_interception(aggregator, *, service=None) -> Middleware` — mirrors
   `trace_interception`'s signature and rules:
   - install **inside** `trace_interception` so `current_traceparent()` yields the exemplar trace id
     (parse the trace id out of it; do not invent a second contextvar if the existing one suffices).
   - after `await next()`, read `context.result.status`; when it is not successful (reuse
     `benzene.results.is_successful`, the same predicate the collector uses), call
     `aggregator.record(topic=context.topic, status=…, version=context.version or "",
     exception_type=…, trace_id=…)`.
   - **never raise** — wrap in `contextlib.suppress(Exception)` exactly as the trace middleware does.
   - a propagating exception must still be recorded (`try/except/raise`) before re-raising.
3. `IssueFlusher` (or a documented `asyncio` recipe in the cookbook): flush every 30s via
   `MeshFeedSender.publish_issues`, **including empty batches** — the empty batch is the feed's
   liveness assertion and the collector's `missingFeeds` logic depends on it
   (`_missing_feeds` marks `issues` missing only when a failure needs explaining). A helper is worth
   it because "always flush, even when empty" is exactly the part hand-rolled code gets wrong.
4. Exemplars: Python keeps the **first** 5, .NET the **newest** 3. The collector trims to 3 on merge
   either way, so this is cosmetic — but align to newest-3 while touching the file, so a Python and
   a .NET emitter for the same failure ship the same exemplars.

**Tests:** `tests/test_mesh.py` — the middleware records exactly one issue per failing invocation
and none per success; the exemplar trace id matches the enclosing trace; a throwing aggregator never
affects the response; `max_fingerprints` drops new signatures while still merging known ones; the
flusher emits an empty batch when idle.

---

## G11. Storage adapters: no GCS artifact store, no Azure/GCS collector snapshot stores

**Severity: MEDIUM** (Azure snapshot) / **LOW-MEDIUM** (GCS) — a real gap for Azure/GCP-hosted meshes.

**Current Python state** (note: `blob_artifacts.py` landed during this analysis, so the Azure
artifact half is **already closed**):

| | filesystem | S3 | Azure Blob | GCS |
|---|---|---|---|---|
| **artifact store** (mesh-ui documents) | `artifacts.write_artifacts` | `s3_artifacts.S3ArtifactStore` | `blob_artifacts.BlobArtifactStore` ✅ new | **missing** |
| **collector snapshot store** (`CollectorStore`) | `JsonFileCollectorStore` | `S3CollectorStore` | **missing** | **missing** |

**.NET:** `Benzene.Mesh.Aws.S3`, `.Azure.Blob`, `.GoogleCloud.Storage` all implement the one
`IMeshArtifactStore` port (`PublishAsync`/`TryReadAsync`, 404 → `null`), so any hosting model can
persist the catalog centrally.

### Implementation spec

1. **`BlobCollectorStore`** (`benzene-mesh`, `store.py`) — highest value of the three: an Azure
   Functions / Container Apps mesh has no disk, so today it must run in-memory and forget the fleet
   on every restart. Same shape as `S3CollectorStore`: `__init__(container_url_or_client,
   blob_name="collector.json", *, client=None)`, lazy `azure.storage.blob` import raising an
   `ImportError` that names `pip install benzene-mesh[azure]` (the extra now exists), `load()`
   returning `None` for a missing blob (`ResourceNotFoundError`) or unparseable JSON, `save()` an
   `upload_blob(..., overwrite=True)` (atomic replace, like S3 `PutObject`).
2. **`GcsArtifactStore`** (`benzene-mesh`, new `gcs_artifacts.py`) + `write_artifacts_to_gcs` —
   copy `s3_artifacts.py` structurally: `__init__(bucket, prefix="", *, client=None)`, lazy
   `google.cloud.storage` import, new optional extra `gcp = ["google-cloud-storage>=2.0"]`.
3. **`GcsCollectorStore`** — same shape; only if a GCP mesh deployment actually exists (there is a
   `examples/gcp_orders` but no GCS mesh host today). Otherwise defer and say so.
4. Add a `read(key)` to all three artifact stores (needed by G7's catalog diff and G9's annotation
   log); missing object → `None`, matching the collector stores' forgiving `load`.

**Tests:** mirror `tests/test_mesh_s3_artifacts.py` and `tests/test_mesh_store.py` with an injected
fake client (a dict-backed double — no SDK, no network, matching the port's duck-typed fake
convention): key/prefix joining, round-trip, missing object → `None`, corrupt JSON → `None`,
overwrite semantics, and an `ImportError` naming the right extra when no client is injected and the
SDK is absent.

---

## G12. No time-range window on the query surface

**Severity: MEDIUM-LOW** (matters once the live plane is on — G4 — and the UI's range picker exists).

**Python:** every query is unfiltered; `query_topic`/`query_fleet` take no window.

**.NET:** `MeshTimeRangeResolver` (`src/Benzene.Mesh.Collector/MeshTimeRangeResolver.cs`) parses the
Grafana relative grammar (`now`, `now-5m`, `now-1h`, `now-7d`; units s/m/h/d/w/M/y) or ISO-8601
absolute, resolved against server `now`. Threaded onto `FleetQuery`/`ServiceQuery`/`TopicQuery`/
`CorrelationQuery` as `window`; a null range means unfiltered (today's behaviour) and the response's
`window` field is **omitted entirely** so old clients and fixtures are untouched.
`mesh:query:trace` deliberately has **no** window — it is a by-id lookup and a window would only let
a valid id outside the range answer `NotFound`.

### Implementation spec

`benzene-mesh`, new `timerange.py` + `collector.py`:

- `resolve_range(expr: str, *, now: datetime) -> tuple[datetime, datetime]` for the relative
  grammar and ISO-8601 absolutes; unparseable → `CollectorBadRequest`.
- `query_fleet`/`query_service`/`query_topic`/`query_correlation` read an optional
  `body["window"]`; **flows and liveness honour it, counts do not** (they are cumulative — G2) and
  the response's `window` says so via `countsWindowed: False` + `countsSince`. This split is the
  honesty core: a windowed count that cannot honour the window is a real number answering a
  *different* window, badged — never blanked and never silently wrong.
- Absent window ⇒ omit the `window` key entirely ⇒ existing fixtures unaffected.

**Tests:** grammar coverage (each unit, `now`, absolute, malformed → `bad-request`), flows filtered
by window while counts stay cumulative, absent window == today's response byte-for-byte.

---

## G13. Poller: no per-service fetch timeout; spec and health fetched sequentially

**Severity: LOW-MEDIUM** (one hung service can stall a sweep when a custom `fetch` is injected).

**Python:** `MeshPoller.poll_once` gathers sources concurrently and isolates failures per source
(good), but `_poll` awaits `fetch_spec()` **then** `fetch_health()` with no bound of its own — the
only timeout is inside the default `urllib` fetch (10s). An injected `fetch` (the documented seam,
used by `deploy/mesh` and by a Lambda-invoke source) has no such guarantee, and a hung source holds
the sweep's `gather` open indefinitely.

**.NET:** `MeshAggregator.RunOnceAsync` bounds **each** service's fetch independently at a 10s
`PerServiceFetchTimeout`, uniform across every `IMeshServiceSource` — the timeout lives in the
aggregator, not in each adapter, precisely so a new adapter cannot forget it.

### Implementation spec

`benzene-mesh`, `poller.py`: `MeshPoller(collector, sources, *, fetch_timeout_s: float = 10.0)`;
wrap the spec+health pair in `asyncio.wait_for`, and fetch them with `asyncio.gather` (they are
independent). A timeout is recorded as `PollResult(name, ok=False, error="TimeoutError")` — the
existing failure path, with the **error type name only**, never a message (the aggregate artifact
has broader visibility than one service's own health page; Python already follows this rule
elsewhere, keep it).

**Tests:** `tests/test_mesh_poller.py` — a source whose injected fetch never returns yields a failed
`PollResult` within the timeout while the rest of the sweep still ingests.

---

## G14. Discovery has no reusable runner / registry seam

**Severity: LOW.**

**Python:** the discover → interrogate → publish `registry.json` pass exists but lives in an
**example** (`examples/aws_lambda_mesh/mesh/discovery_service.py:93-140`), so every deployment
copies it. There is no static-seed union.

**.NET:** `MeshDiscoveryRunner` + `MeshRegistryJson` (in `Benzene.Mesh.Contracts`) — run all
providers, union with an optional hand-written static seed where **the seed wins on a name clash**
(a human pin is an intentional override), dedupe by name, serialize to the `mesh.json`
`{"services":[…]}` shape. "Discovery writes the config; runtime monitoring reads it" is a
deliberate hard seam.

### Implementation spec

`benzene-mesh-fleet`, `discovery.py`:

```python
async def run_discovery(sources: Iterable[Discovery], *,
                        seed: Iterable[ServiceEndpoint] = ()) -> list[ServiceEndpoint]:
    """Union every source's endpoints with a hand-written seed; seed wins on a name clash."""

def registry_document(endpoints, *, generated_at: str) -> dict[str, Any]:
    """The registry.json shape the poller/host reads back (lifted from the AWS example)."""
```

Each source in its own `try/except` (one unreachable registry must not blank the fleet); dedupe by
`name`, seed entries inserted last-wins-for-seed. Then have the example import these instead of
defining them, so the two cannot drift.

**Tests:** `tests/test_mesh_fleet.py` — union, seed-wins-on-clash, empty discovery, one failing
source, `registry_document` round-trip through `MeshConfig`-style consumption.

---

# Not worth porting (and why)

- **`Benzene.Mesh.Reporting`** (`HttpMeshReportPublisher`, `MeshSelfReportMiddleware`) — it exists
  because the .NET 1.0 aggregator is **pull-only** and an SQS-only Lambda has no endpoint to poll.
  Python's mesh is **push-native**: `MeshFeedSender.register`/`publish_heartbeat` already is the
  self-report path, and `S3TraceInbox` already solves the stateless-concurrent-writer case the .NET
  package's follow-ups are still chasing. Porting it would add a second, weaker way to do what
  `MeshFeedSender` does. Also inherits .NET's own documented "staleness has no representation" gap.

- **`Benzene.Mesh.Dispatch`** (`mesh:dispatch` — invoke a real handler with a caller-supplied
  payload) — a live-fire write path into production handlers, gated on `ASPNETCORE_ENVIRONMENT`
  being explicitly non-Production. Python has no equivalent ambient environment convention, the
  vendored UI here is wired static-only (no Send button reachable), and the blast radius (real DB
  writes, real downstream publishes) is severe. **If it is ever asked for**, the two gates are
  non-negotiable: explicit opt-in registration (never discovered by a registry scan) **and** a
  runtime environment gate whose *unset* default is "production, refuse", returning `forbidden` with
  the reason — never a silent no-op.

- **`Benzene.Mesh.Artifacts`** (HTTP middleware serving artifacts out of a store) — Python's
  `deploy/mesh/collector/static.py` already serves the artifact set plus the vendored UI from a
  directory, and the S3/Blob paths are read by a bucket website endpoint/CDN. The .NET package
  exists to collapse five copy-pasted example middlewares; Python has one copy. Revisit only if a
  Python host needs to serve artifacts **out of a bucket** through its own pipeline.

- **`AsyncApiCompositor`'s namespacing + `$ref` rewriting + orphan pruning** — .NET merges N
  *per-service* AsyncAPI documents, so channel/operation/schema keys collide across services and
  every `$ref` must be rewritten. Python's `artifacts._asyncapi` builds **one** document from the
  single catalog, where keys are topic ids (globally unique by construction) and there are no refs
  to rewrite. Porting the machinery would add complexity to solve a problem this port does not have.

- **`topics.json` `versionCompatibility`, `manifest.json` `transports` / `owningTeam` /
  `snapshotAtUtc`, extra `MeshTopicStatus` values** — all **new artifact fields**. The field set is
  frozen and pinned key-for-key by `tests/test_mesh_artifact_contract.py`; the shared `mesh-ui.html`
  is one page across every port. **These require the spec repo to move first** (`docs/guides/mesh-ui.md`
  + the `website/demos/mesh/` fixtures + the other ports). Flagged loudly here precisely so nobody
  adds them as "just one more field". The same applies to any change to the descriptor shape,
  `descriptorHash`, the `mesh:*` topic ids, or the issue classification vocabulary.

- **.NET keying topic state by `(topic, version)`** — Python's collector, its fixtures, and its
  artifact projection are all topic-id-keyed. Changing the key space would move the query shape and
  the artifact shape at once. Not a gap; a deliberate divergence to preserve.

- **DI/registration surface (`AddMeshAggregator`, `UseMeshDispatch`, `AddCloudWatchUsage`, …)** —
  pure .NET-container ergonomics. Python's equivalent is a constructor plus an explicit
  `Registry`/middleware-list, which the port already does consistently. Nothing to port.

- **`mesh-spec-ui.html`** (the .NET spec browser page) — Python serves `/benzene/spec` and has
  `specdoc.py` for reading either document shape; a second vendored 230KB HTML page is not a mesh
  capability gap.

# Suggested sequencing

1. **G1** (leak) — a one-file fix that makes a long-lived Python collector safe to deploy.
2. **G2 + G3** together (counters + duration) — one pass over `collector.py`'s ingest and one
   snapshot version bump; between them they make every number stable and fill five artifact fields
   that are shipped-but-null today.
3. **G4** — turns on the live plane the vendored UI is already built for.
4. **G7** (topic status) — cheap, contract-safe, high product visibility.
5. **G6, G9, G10, G11** — independent, small.
6. **G5** — the read seam (Step A) is cheap and worth doing early even if the adapter (Step B)
   waits; it is what makes the composite plane possible at all.
7. **G8, G12, G13, G14** — as demand appears.
