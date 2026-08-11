# benzene-otel

OpenTelemetry integration for [Benzene Python](https://github.com/daniellepelley/benzene-python). The
port already traces itself — `benzene.mesh.trace_middleware` emits one `TraceEvent` per invocation —
but that tracing was Benzene-internal only, feeding a mesh collector rather than a general OTel
pipeline. This package exports it to a real OpenTelemetry SDK, and adds a **response-as-event** pattern
mirroring .NET's `Benzene.ResponseEvents`. Depends on `benzene-core` and `benzene-mesh`.

```bash
pip install "benzene-otel[otel]"   # the [otel] extra pulls opentelemetry-api / opentelemetry-sdk
```

`OtelTraceExporter` *is* a `benzene.mesh.TraceExporter`, so it wires into the existing tracing
middleware — no second instrumentation. Each finished span maps one-to-one onto an OTel span: topic →
span name, `started_at`/`duration_ms` → OTel start/end nanoseconds, every field → a `benzene.*`
attribute, and the semantic status → the OTel span status.

```python
from benzene.mesh import trace_middleware
from benzene.otel import OtelTraceExporter, RecordingSink, response_event_interception

# Trace export: forward the port's own spans to OpenTelemetry (real tracer acquired lazily).
definition.middleware.insert(0, trace_middleware(OtelTraceExporter(), service="orders"))

# Response-as-event: publish each result to a sink after the handler runs.
sink = RecordingSink()  # swap for an OTel-log / outbox / audit-topic sink in production
definition.middleware.append(response_event_interception(sink))
```

- **`OtelTraceExporter`** — consumes the mesh trace model and pushes each `TraceEvent` to an injected
  (or lazily created) OTel tracer. The tracer is a duck-typed seam (`OtelTracer` / `OtelSpan`), so a
  fake tracer exercises it with no `opentelemetry` installed; export is non-blocking and never raises,
  so a bad tracer can't break the invocation it observes.
- **`response_event_interception(sink, *, when=..., include_payload=...)`** — after `await next()`,
  emits a `ResponseEvent` (topic, status, correlation id, optional payload) to a `ResponseEventSink`.
  Emits **every** result by default; pass `when=lambda r: r.is_successful` for successes only. A sink
  that raises is logged and swallowed — emitting an event never breaks the pipeline.
- **`RecordingSink`** — an in-memory `list` of emitted events, the fake for tests and dogfooding.

Nothing here needs a collector, a real clock, or the OpenTelemetry package to run in tests: the tracer
and sink are injectable seams, so the whole surface is exercised in memory. Mirrors the role of .NET's
`Benzene.Diagnostics` OTel wiring and `Benzene.ResponseEvents`, and contributes the `benzene.otel`
subpackage to the shared `benzene` namespace.
