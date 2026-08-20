# `benzene.otel`

Export the port's existing traces to **OpenTelemetry**, plus a response-as-event pattern.
**Distribution: `benzene-otel` (depends on `benzene-core`, `benzene-mesh`).**

```bash
pip install "benzene-otel[otel]"   # the [otel] extra pulls opentelemetry-api / opentelemetry-sdk
```

## Overview

The Benzene Python port **already traces itself**: `benzene.mesh.trace_interception` times every
invocation and emits one `benzene.mesh.TraceEvent` (topic, status, W3C trace ids, start time,
duration) through a `benzene.mesh.TraceExporter` seam. Until now that tracing was *Benzene-internal
only* — it fed a mesh collector, never a general OpenTelemetry pipeline. This distribution closes that
gap **without re-instrumenting anything**:

- **Trace export** — `OtelTraceExporter` *is* a `TraceExporter`, so it drops straight into the
  existing `trace_interception` and forwards each already-finished span to a real OpenTelemetry tracer.
- **Response-as-event** — `response_event_interception` reshapes each invocation's *result* into a
  `ResponseEvent` and publishes it to a `ResponseEventSink` after the handler runs, mirroring .NET's
  `Benzene.ResponseEvents`.

Both seams — the tracer and the sink — are duck-typed, so everything here runs (and is fully tested)
with **no** `opentelemetry` package and no network. The SDK is imported lazily and only when no tracer
is injected. Mirrors the role of .NET's `Benzene.Diagnostics` OTel wiring and `Benzene.ResponseEvents`.

## `OtelTraceExporter` — trace export

`OtelTraceExporter` consumes the mesh trace model and pushes each span to an OpenTelemetry tracer,
rather than instrumenting the pipeline a second time. Wire it as the sink of the port's *existing*
tracing:

```python
from benzene.mesh import trace_interception
from benzene.otel import OtelTraceExporter

definition.middleware.insert(0, trace_interception(OtelTraceExporter(), service="orders"))
```

- `OtelTraceExporter(tracer=None, *, instrumentation_name="benzene.otel")` — with no `tracer` the
  OpenTelemetry SDK is imported lazily and a real tracer is acquired via
  `opentelemetry.trace.get_tracer(instrumentation_name)`; inject a `tracer` (a fake, or a
  pre-configured real one) and **nothing** is imported.
- `export(event)` — the `TraceExporter` seam `trace_interception` calls. Like every mesh feed it is
  non-blocking and lossy: it **never raises**, so a bad tracer cannot break the invocation it observes
  (mesh §3).
- One `TraceEvent` is exactly one span, so mapping is one-to-one: `topic` → span name, `started_at` →
  OTel start nanos, `started_at + duration_ms` → end nanos, every identifying field → a `benzene.*`
  attribute (`benzene.service`, `benzene.topic`, `benzene.status`, `benzene.trace_id`, `benzene.span_id`,
  and the present-only `benzene.parent_span_id` / `benzene.correlation_id` / …), and the semantic status
  → the OTel span status (`StatusCode.OK` / `ERROR`, falling back to plain `"OK"` / `"ERROR"` strings
  when the SDK is absent).
- `export_span(event)` maps a single event; `export_traces(events)` maps a batch (e.g. a drained
  `benzene.mesh.QueueTraceExporter`) one by one.

`OtelTracer` and `OtelSpan` are `runtime_checkable` protocols describing the slice of an OpenTelemetry
`Tracer` / `Span` the exporter drives (`start_span(name, *, start_time=...)`; `set_attribute` /
`set_status` / `end`) — a real `opentelemetry` object satisfies them, and so does a test fake.

## Response-as-event

Where the exporter records *traces* (how long, where in the trace), this pattern records *outcomes*:
after a handler runs, the invocation's result — its topic, semantic status, and optional payload,
tagged with the business correlation id — is emitted as a discrete event to a pluggable
`ResponseEventSink`. That feeds an event stream (an OTel event/log pipeline, an outbox, an audit
topic) with "topic *x* produced status *y*" without the handler knowing it is observed.

```python
from benzene.otel import RecordingSink, response_event_interception

sink = RecordingSink()  # swap for an OTel-log / outbox / audit-topic sink in production
definition.middleware.append(response_event_interception(sink))
```

- `response_event_interception(sink, *, when=None, include_payload=True)` — an ordinary middleware,
  installed anywhere ahead of the message router. It emits *after* `await next()`, once the result
  exists (a missing result is treated as `unexpected-error`, so a swallowed crash still surfaces an
  event). `when` filters which results are emitted; the default is **every** result (successes *and*
  failures — a complete audit of outcomes), so pass `when=lambda r: r.is_successful` for successes
  only. `include_payload=False` drops the payload for a lighter or less sensitive stream. A sink that
  raises is logged and swallowed — emitting an event never breaks the invocation.
- `ResponseEvent` — a frozen dataclass (`topic`, `status`, `version`, `correlation_id`, `payload`,
  `errors`) with `is_successful` and `to_payload()` (its camelCase wire form, omitting rather than
  nulling absent fields). The correlation id is read from the `x-correlation-id` header
  (`CORRELATION_ID_HEADER`), the same header `trace_interception` reads.
- `ResponseEventSink` — the `runtime_checkable` protocol a concrete stream implements: a single
  `async def emit(self, event: ResponseEvent) -> None`. `RecordingSink` is the in-memory fake — a
  `list` subclass whose `emit` appends — for tests and dogfooding.

## Troubleshooting

- **No spans reach my collector.** The exporter never raises by design, so a misconfigured tracer
  fails silently rather than crashing the pipeline. Inject an explicit tracer and assert on it in a
  test, or check that `trace_interception(OtelTraceExporter(...), service=...)` is actually installed on
  `definition.middleware`.
- **`ModuleNotFoundError: opentelemetry`.** Install the extra (`pip install "benzene-otel[otel]"`) or
  inject a pre-built `tracer` so the lazy import is never reached.

## Exports

`OtelTraceExporter`, `OtelTracer`, `OtelSpan`, `response_event_interception`, `ResponseEvent`,
`ResponseEventSink`, `RecordingSink`, `CORRELATION_ID_HEADER`.

## See also

- [`benzene.mesh`](mesh.md) — the `TraceEvent` / `TraceExporter` / `trace_interception` model this
  consumes.
- [`benzene.mesh_fleet`](mesh-fleet.md) — the sibling fleet trace-mappers (Jaeger / Tempo / X-Ray)
  that project the same `TraceEvent` model into a backend's JSON.
- [`benzene.core`](core.md) — the middleware pipeline `response_event_interception` installs into.
</content>
