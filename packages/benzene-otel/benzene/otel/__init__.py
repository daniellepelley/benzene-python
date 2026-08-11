"""``benzene.otel`` — export the port's existing traces to OpenTelemetry, plus response-as-event.

The Benzene Python port **already traces itself**: :func:`benzene.mesh.trace_middleware` times every
invocation and emits one :class:`~benzene.mesh.TraceEvent` (topic, status, W3C trace ids, start time,
duration) through a :class:`~benzene.mesh.TraceExporter` seam. Until now that tracing was
*Benzene-internal only* — it fed a mesh collector, never a general OpenTelemetry pipeline. This
distribution (``benzene-otel``) closes that gap without re-instrumenting anything:

* **Trace export** — :class:`OtelTraceExporter` *is* a :class:`~benzene.mesh.TraceExporter`, so it
  drops straight into the existing ``trace_middleware`` and forwards each finished span to a real
  OpenTelemetry tracer (topic → span name, ``started_at``/``duration_ms`` → OTel start/end nanos,
  every field → a ``benzene.*`` attribute, semantic status → OTel span status).
* **Response-as-event** — :func:`response_event_interception` reshapes each invocation's *result*
  into a :class:`ResponseEvent` and publishes it to a :class:`ResponseEventSink` after the handler
  runs, mirroring .NET's ``Benzene.ResponseEvents``.

The OpenTelemetry SDK is an **optional** dependency (the ``[otel]`` extra: ``opentelemetry-api`` /
``opentelemetry-sdk``), imported lazily and only when no tracer is injected. Both seams — the tracer
and the sink — are duck-typed, so everything here runs, and is fully tested, with **no**
``opentelemetry`` package and no network. Depends on ``benzene-core`` and ``benzene-mesh`` (it
consumes the mesh trace model). Contributes the ``benzene.otel`` subpackage to the shared ``benzene``
namespace.

    pip install "benzene-otel[otel]"
"""

from __future__ import annotations

from .exporter import OtelSpan, OtelTraceExporter, OtelTracer
from .response_events import (
    CORRELATION_ID_HEADER,
    RecordingSink,
    ResponseEvent,
    ResponseEventSink,
    response_event_interception,
)

__all__ = [
    # trace export
    "OtelTraceExporter",
    "OtelTracer",
    "OtelSpan",
    # response-as-event
    "response_event_interception",
    "ResponseEvent",
    "ResponseEventSink",
    "RecordingSink",
    "CORRELATION_ID_HEADER",
]
