"""Export the port's existing mesh trace to a real OpenTelemetry SDK (roadmap item 25).

The Benzene port already produces distributed traces on its own: :func:`benzene.mesh.trace_middleware`
times every invocation and hands a finished :class:`~benzene.mesh.TraceEvent` — the topic it handled,
its status, its place in a W3C trace, when it started and how long it took — to a
:class:`~benzene.mesh.TraceExporter`. That tracing is *Benzene-internal only*: the events feed a mesh
collector over ``benzene:mesh:traces``, never a general OTel pipeline.

:class:`OtelTraceExporter` bridges that gap. It **is** a :class:`~benzene.mesh.TraceExporter` (it
implements ``export(event)``), so it drops straight into the existing tracing middleware —
``trace_middleware(OtelTraceExporter(tracer), service=...)`` — and forwards each already-finished span
to an OpenTelemetry tracer instead of re-instrumenting the pipeline. One :class:`TraceEvent` in the
port is exactly one span, so mapping is one-to-one: topic → span name, ``started_at`` → OTel start
nanos, ``duration_ms`` → end nanos, every remaining field → a span attribute, and the semantic status
→ the OTel span status.

The OpenTelemetry SDK is an **optional** dependency (the ``[otel]`` extra: ``opentelemetry-api`` /
``opentelemetry-sdk``). It is imported *lazily* and only when no tracer is injected — the tracer is a
duck-typed seam (:class:`OtelTracer` / :class:`OtelSpan`), so tests drive a fake tracer that records
spans with **no** ``opentelemetry`` package installed, and status codes fall back to plain strings
when the real ``StatusCode`` enum is absent.

Mirrors the role of .NET's ``Benzene.Diagnostics`` OpenTelemetry wiring, which likewise adapts the
framework's own activity/trace surface onto the OTel exporter contract.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterable
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from benzene.mesh import TraceEvent
from benzene.results import is_successful

_NANOS_PER_MILLI = 1_000_000
_NANOS_PER_SECOND = 1_000_000_000


@runtime_checkable
class OtelSpan(Protocol):
    """The slice of an OpenTelemetry ``Span`` this exporter drives (duck-typed).

    A real ``opentelemetry.trace.Span`` satisfies it; so does the test fake. ``set_status`` takes an
    OTel ``StatusCode`` when the SDK is present and a plain ``"OK"``/``"ERROR"`` string otherwise
    (see :data:`_StatusCode`), so nothing here forces the ``opentelemetry`` import.
    """

    def set_attribute(self, key: str, value: Any) -> None: ...
    def set_status(self, status: Any, description: str | None = None) -> None: ...
    def end(self, end_time: int | None = None) -> None: ...


@runtime_checkable
class OtelTracer(Protocol):
    """The slice of an OpenTelemetry ``Tracer`` this exporter drives (duck-typed).

    Only :meth:`start_span` is used, called with a name and an explicit ``start_time`` in nanoseconds
    (the span is already finished, so the exporter supplies both endpoints itself). A real
    ``opentelemetry.trace.Tracer`` matches — its extra parameters are all defaulted.
    """

    def start_span(self, name: str, *, start_time: int | None = None) -> OtelSpan: ...


class _StatusCode:
    """Fallback for ``opentelemetry.trace.StatusCode`` when the SDK isn't installed.

    Plain strings the fake tracer can record; when ``opentelemetry`` *is* present the real enum is
    used instead (see :meth:`OtelTraceExporter._status_code`).
    """

    OK = "OK"
    ERROR = "ERROR"


def _start_nanos(started_at: str | None) -> int | None:
    """Parse a :class:`TraceEvent`'s ISO-8601 ``started_at`` (``...Z``) to Unix epoch nanoseconds."""
    if not started_at:
        return None
    # trace_middleware writes ``...+00:00`` as ``Z``; fromisoformat wants the offset spelled out.
    parsed = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    return int(parsed.timestamp() * _NANOS_PER_SECOND)


class OtelTraceExporter:
    """A :class:`~benzene.mesh.TraceExporter` that forwards each span to an OpenTelemetry tracer.

    Wire it as the sink of the port's existing tracing rather than instrumenting the pipeline twice::

        from benzene.mesh import trace_middleware
        from benzene.otel import OtelTraceExporter

        definition.middleware.insert(0, trace_middleware(OtelTraceExporter(), service="orders"))

    With no ``tracer`` argument the OpenTelemetry SDK is imported lazily and a real tracer is acquired
    via ``opentelemetry.trace.get_tracer``; inject a ``tracer`` (a fake, or a pre-configured real one)
    and **nothing** is imported. Like every mesh feed, export is non-blocking and lossy: it never
    raises — a bad tracer must not break the invocation it observes (mesh.md §3).
    """

    def __init__(
        self,
        tracer: OtelTracer | None = None,
        *,
        instrumentation_name: str = "benzene.otel",
    ) -> None:
        if tracer is None:
            # Lazy, optional dependency — only needed when the caller hasn't supplied a tracer.
            from opentelemetry import trace  # noqa: PLC0415 - deferred optional import

            tracer = trace.get_tracer(instrumentation_name)
        self._tracer = tracer

    def export(self, event: TraceEvent) -> None:
        """:class:`~benzene.mesh.TraceExporter` seam — map one finished span and never raise."""
        # Non-blocking + lossy: a tracer error must never affect the invocation (mesh.md §3).
        with contextlib.suppress(Exception):  # pragma: no cover - defensive
            self.export_span(event)

    def export_span(self, event: TraceEvent) -> None:
        """Map a single :class:`~benzene.mesh.TraceEvent` onto one OpenTelemetry span.

        The invocation is already over, so the span is opened at the event's ``started_at`` and closed
        at ``started_at + duration_ms`` in the same call; every identifying field becomes a
        ``benzene.*`` attribute and the semantic status is mapped onto the OTel span status.
        """
        start = _start_nanos(event.started_at)
        span = self._tracer.start_span(event.topic, start_time=start)

        # One set_attribute per known field — omit what the event didn't carry (never set null).
        attributes: dict[str, Any] = {
            "benzene.service": event.service,
            "benzene.topic": event.topic,
            "benzene.status": event.status,
            "benzene.trace_id": event.trace_id,
            "benzene.span_id": event.span_id,
        }
        optional = {
            "benzene.parent_span_id": event.parent_span_id,
            "benzene.instance_id": event.instance_id,
            "benzene.topic_version": event.topic_version,
            "benzene.exception_type": event.exception_type,
            "benzene.correlation_id": event.correlation_id,
        }
        attributes.update({k: v for k, v in optional.items() if v is not None})
        for key, value in attributes.items():
            span.set_attribute(key, value)

        span.set_status(self._status_code(is_successful(event.status)))

        end = (
            None
            if start is None or event.duration_ms is None
            else (start + int(event.duration_ms * _NANOS_PER_MILLI))
        )
        span.end(end_time=end)

    def export_traces(self, events: Iterable[TraceEvent]) -> None:
        """Map a batch of events (e.g. a drained :class:`~benzene.mesh.QueueTraceExporter`) one by one."""
        for event in events:
            self.export(event)

    @staticmethod
    def _status_code(ok: bool) -> Any:
        """The OTel ``StatusCode`` for a Benzene status — the real enum if the SDK is here, else a string.

        Resolving it lazily keeps the whole exporter runnable (and testable) with no ``opentelemetry``
        package: a fake tracer just records whichever object it's handed.
        """
        try:
            from opentelemetry.trace import StatusCode  # noqa: PLC0415 - deferred optional import
        except ImportError:
            return _StatusCode.OK if ok else _StatusCode.ERROR
        return StatusCode.OK if ok else StatusCode.ERROR
