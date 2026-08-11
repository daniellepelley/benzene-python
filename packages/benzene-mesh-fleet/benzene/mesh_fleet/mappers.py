"""Fleet trace-mappers — the mesh trace model into each backend's shape (Benzene.Mesh.Fleet.*).

The mesh emits a :class:`~benzene.mesh.TraceEvent` per invocation — a W3C-shaped span carrying its
``trace_id`` / ``span_id`` / ``parent_span_id``, the ``topic`` it handled, the semantic ``status``,
the ``duration_ms``, and the ISO-8601 ``started_at``. A **trace** is the set of those spans that
share a ``trace_id``. A :class:`TraceMapper` projects one such trace into the JSON document a
tracing backend ingests, so the same mesh telemetry lands in Jaeger, Grafana Tempo, or AWS X-Ray
without the service knowing which backend the fleet runs.

The mappers are pure and dependency-free — no backend SDK, no network — because a mesh trace already
*is* the cross-language span model; mapping is a field/units transform. The three backends differ
mostly in time units, which is where fidelity matters most: **Jaeger** wants microseconds, **Tempo**
(OTLP-JSON) wants Unix nanoseconds, and **X-Ray** wants floating-point epoch seconds.

Mirrors .NET's ``Benzene.Mesh.Fleet.Jaeger`` / ``.Tempo`` / ``.XRay`` mappers.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

from benzene.mesh import TraceEvent

# A trace is one span or an iterable of the spans sharing a trace id.
Trace = TraceEvent | Iterable[TraceEvent]


@runtime_checkable
class TraceMapper(Protocol):
    """Maps a mesh trace (:class:`~benzene.mesh.TraceEvent` spans) into a backend document.

    ``map`` takes either a single span or the iterable of spans that share a ``trace_id`` and returns
    the JSON-serialisable ``dict`` its backend ingests. It is pure: no I/O, no SDK, no clock.
    """

    def map(self, trace: Trace) -> dict[str, Any]: ...


def _spans(trace: Trace) -> list[TraceEvent]:
    """Normalise the ``map`` input to a list of spans (a lone :class:`TraceEvent` is a one-span trace)."""
    if isinstance(trace, TraceEvent):
        return [trace]
    return list(trace)


def _epoch_seconds(started_at: str | None) -> float:
    """Parse a :class:`~benzene.mesh.TraceEvent`'s ISO-8601 ``started_at`` into epoch seconds (UTC).

    ``started_at`` is the ``...Z`` form the trace middleware writes; an absent or unparseable value
    is treated as the Unix epoch (``0.0``) rather than raising — a malformed timestamp must not sink
    an otherwise-good trace on its way to the backend.
    """
    if not started_at:
        return 0.0
    try:
        parsed = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _duration_seconds(span: TraceEvent) -> float:
    """A span's duration in seconds (``duration_ms`` is optional; absent means zero)."""
    return (span.duration_ms or 0.0) / 1000.0


def _extra_tags(span: TraceEvent) -> dict[str, str]:
    """The optional, present-only span fields as a flat string map (shared across the three shapes)."""
    candidates = {
        "benzene.service": span.service,
        "benzene.status": span.status,
        "benzene.topic.version": span.topic_version,
        "benzene.instance_id": span.instance_id,
        "benzene.correlation_id": span.correlation_id,
        "error.type": span.exception_type,
    }
    return {key: value for key, value in candidates.items() if value}


class JaegerTraceMapper:
    """Maps a mesh trace into a Jaeger query-service JSON trace (Benzene.Mesh.Fleet.Jaeger).

    Produces ``{"traceID", "spans": [...], "processes": {...}}``: each span carries ``traceID``,
    ``spanID``, ``operationName`` (the mesh ``topic``), a ``CHILD_OF`` reference to its parent when
    present, ``startTime`` and ``duration`` **in microseconds** (Jaeger's unit), the span fields as
    ``tags``, and a ``processID`` into the shared ``processes`` map (one process per mesh service,
    keyed by ``serviceName``). An ``exception_type`` also raises the conventional ``error=true`` tag.
    """

    def map(self, trace: Trace) -> dict[str, Any]:
        spans = _spans(trace)
        processes: dict[str, dict[str, Any]] = {}
        process_ids: dict[str, str] = {}
        out_spans: list[dict[str, Any]] = []
        for span in spans:
            if span.service not in process_ids:
                process_id = f"p{len(process_ids) + 1}"
                process_ids[span.service] = process_id
                processes[process_id] = {"serviceName": span.service, "tags": []}
            out_spans.append(
                {
                    "traceID": span.trace_id,
                    "spanID": span.span_id,
                    "operationName": span.topic,
                    "references": (
                        [
                            {
                                "refType": "CHILD_OF",
                                "traceID": span.trace_id,
                                "spanID": span.parent_span_id,
                            }
                        ]
                        if span.parent_span_id
                        else []
                    ),
                    "startTime": int(_epoch_seconds(span.started_at) * 1_000_000),
                    "duration": int((span.duration_ms or 0.0) * 1000),
                    "tags": self._tags(span),
                    "processID": process_ids[span.service],
                }
            )
        trace_id = spans[0].trace_id if spans else ""
        return {"traceID": trace_id, "spans": out_spans, "processes": processes}

    @staticmethod
    def _tags(span: TraceEvent) -> list[dict[str, Any]]:
        tags: list[dict[str, Any]] = [
            {"key": key, "type": "string", "value": value}
            for key, value in _extra_tags(span).items()
        ]
        if span.exception_type:
            tags.append({"key": "error", "type": "bool", "value": True})
        return tags


class TempoTraceMapper:
    """Maps a mesh trace into OTLP-JSON trace data — the shape Grafana Tempo ingests (Benzene.Mesh.Fleet.Tempo).

    Produces the OTLP ``{"resourceSpans": [...]}`` document, grouping spans by mesh service into one
    ``resourceSpans`` entry each (the ``service.name`` resource attribute) with a single
    ``scopeSpans`` scope. Each span carries ``traceId``, ``spanId``, ``parentSpanId`` (omitted at the
    root), ``name`` (the mesh ``topic``), ``startTimeUnixNano`` / ``endTimeUnixNano`` **in Unix
    nanoseconds** (OTLP's unit), the span fields as string ``attributes``, and an OTLP ``status``
    (``STATUS_CODE_ERROR`` when an ``exception_type`` is present, else ``STATUS_CODE_OK``).
    """

    def map(self, trace: Trace) -> dict[str, Any]:
        spans = _spans(trace)
        by_service: dict[str, list[dict[str, Any]]] = {}
        for span in spans:
            by_service.setdefault(span.service, []).append(self._span(span))
        resource_spans = [
            {
                "resource": {
                    "attributes": [_otlp_attribute("service.name", service)],
                },
                "scopeSpans": [{"scope": {"name": "benzene.mesh"}, "spans": otlp_spans}],
            }
            for service, otlp_spans in by_service.items()
        ]
        return {"resourceSpans": resource_spans}

    @staticmethod
    def _span(span: TraceEvent) -> dict[str, Any]:
        start_nanos = int(_epoch_seconds(span.started_at) * 1_000_000_000)
        end_nanos = start_nanos + int((span.duration_ms or 0.0) * 1_000_000)
        otlp: dict[str, Any] = {
            "traceId": span.trace_id,
            "spanId": span.span_id,
            "name": span.topic,
            "startTimeUnixNano": start_nanos,
            "endTimeUnixNano": end_nanos,
            "attributes": [_otlp_attribute(key, value) for key, value in _extra_tags(span).items()],
            "status": {"code": "STATUS_CODE_ERROR" if span.exception_type else "STATUS_CODE_OK"},
        }
        if span.parent_span_id:
            otlp["parentSpanId"] = span.parent_span_id
        return otlp


class XRayTraceMapper:
    """Maps a mesh trace into an AWS X-Ray segment document (Benzene.Mesh.Fleet.XRay).

    Produces a single root **segment** with nested **subsegments** — X-Ray's tree shape, reconstructed
    from the spans' ``parent_span_id`` links. Each node carries ``name`` (the mesh service), the mesh
    ``span_id`` as ``id`` (8 bytes / 16 hex, exactly an X-Ray id), and ``start_time`` / ``end_time``
    **in floating-point epoch seconds** (X-Ray's unit). The root also carries ``trace_id`` in X-Ray's
    ``1-<8hex>-<24hex>`` form, derived from the 32-hex W3C ``trace_id``. A span's ``topic`` / ``status``
    ride in ``annotations``, and an ``exception_type`` sets ``"fault": true``.
    """

    def map(self, trace: Trace) -> dict[str, Any]:
        spans = _spans(trace)
        if not spans:
            return {}
        by_id = {span.span_id: span for span in spans}
        children: dict[str | None, list[TraceEvent]] = {}
        for span in spans:
            parent = span.parent_span_id if span.parent_span_id in by_id else None
            children.setdefault(parent, []).append(span)
        root = children.get(None, [spans[0]])[0]
        segment = self._segment(root, children, root=True)
        segment["trace_id"] = _xray_trace_id(root.trace_id)
        return segment

    def _segment(
        self, span: TraceEvent, children: dict[str | None, list[TraceEvent]], *, root: bool
    ) -> dict[str, Any]:
        start = _epoch_seconds(span.started_at)
        node: dict[str, Any] = {
            "name": span.service,
            "id": span.span_id,
            "start_time": start,
            "end_time": start + _duration_seconds(span),
            "annotations": {"topic": span.topic, "status": span.status},
        }
        if span.exception_type:
            node["fault"] = True
        subsegments = [
            self._segment(child, children, root=False) for child in children.get(span.span_id, [])
        ]
        if subsegments:
            node["subsegments"] = subsegments
        return node


def _otlp_attribute(key: str, value: str) -> dict[str, Any]:
    """One OTLP key/value attribute (string-valued — the only value type the mapper emits)."""
    return {"key": key, "value": {"stringValue": value}}


def _xray_trace_id(trace_id: str) -> str:
    """A 32-hex W3C trace id → X-Ray's ``1-<8hex>-<24hex>`` form (unchanged if already X-Ray-shaped)."""
    if trace_id.startswith("1-"):
        return trace_id
    if len(trace_id) == 32:
        return f"1-{trace_id[:8]}-{trace_id[8:]}"
    return trace_id
