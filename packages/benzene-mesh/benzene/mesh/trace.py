"""Distributed tracing — one TraceEvent per invocation (mesh.md §3).

A mesh-enabled service emits exactly one **TraceEvent** for every routed invocation: the topic it
handled, the semantic status it produced, how long it took, and its place in a W3C trace. The
:func:`trace_middleware` reads an inbound ``traceparent`` header to join an existing trace (or
starts a fresh one), times the pipeline, and hands the finished event to a :class:`TraceExporter`.

**Export must never affect the traffic it observes** — it is asynchronous, non-blocking, and lossy
under backpressure (mesh.md §3). The middleware therefore swallows any exporter error: no mesh feed
may fail, slow, or block the invocation.
"""

from __future__ import annotations

import collections
import contextlib
import contextvars
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

from benzene.core import Context, Middleware, Next
from benzene.results import Result, Status

# The trace the current invocation is inside — (trace_id, span_id) — set by the trace middleware so an
# outbound call can forward it (contextvars carry it across awaits within the same task; §3 propagation).
_CURRENT_TRACE: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar(
    "benzene_current_trace", default=None
)


def current_traceparent() -> str | None:
    """The W3C ``traceparent`` for the invocation currently running, or ``None`` outside a trace."""
    trace = _CURRENT_TRACE.get()
    if trace is None:
        return None
    trace_id, span_id = trace
    return f"00-{trace_id}-{span_id}-01"

_HEX = re.compile(r"[0-9a-f]+")
_ALL_ZERO = re.compile(r"0+")


def _is_hex(value: str, length: int) -> bool:
    return len(value) == length and _HEX.fullmatch(value) is not None


def parse_traceparent(header: str | None) -> tuple[str, str] | None:
    """Parse a W3C ``traceparent`` (``00-<32hex>-<16hex>-<2hex>``) → ``(trace_id, parent_span_id)``.

    Returns ``None`` for a malformed or absent header (the caller then starts a fresh trace). The
    all-zero trace-id and all-zero parent-id are invalid per W3C, as is any wrong-length or non-hex
    segment or a wrong number of segments.
    """
    if not header:
        return None
    parts = header.split("-")
    if len(parts) != 4:
        return None
    version, trace_id, parent_id, flags = parts
    if not (_is_hex(version, 2) and _is_hex(trace_id, 32) and _is_hex(parent_id, 16) and _is_hex(flags, 2)):
        return None
    if _ALL_ZERO.fullmatch(trace_id) or _ALL_ZERO.fullmatch(parent_id):
        return None
    return trace_id, parent_id


def new_trace_id() -> str:
    """A fresh W3C trace-id: 16 random bytes, lowercase hex."""
    return os.urandom(16).hex()


def new_span_id() -> str:
    """A fresh W3C span-id: 8 random bytes, lowercase hex."""
    return os.urandom(8).hex()


@dataclass(frozen=True)
class TraceEvent:
    """One invocation's trace record (mesh.md §3). ``to_payload`` is its camelCase wire form."""

    trace_id: str
    span_id: str
    service: str
    topic: str
    status: str
    parent_span_id: str | None = None
    instance_id: str | None = None
    topic_version: str | None = None
    exception_type: str | None = None
    duration_ms: float | None = None
    started_at: str | None = None
    correlation_id: str | None = None

    def to_payload(self) -> dict[str, Any]:
        """The wire payload — camelCase keys, omitting (never nulling) what isn't known."""
        payload: dict[str, Any] = {
            "traceId": self.trace_id,
            "spanId": self.span_id,
            "service": self.service,
            "topic": self.topic,
            "status": self.status,
        }
        optional = {
            "parentSpanId": self.parent_span_id,
            "instanceId": self.instance_id,
            "topicVersion": self.topic_version,
            "exceptionType": self.exception_type,
            "durationMs": self.duration_ms,
            "startedAt": self.started_at,
            "correlationId": self.correlation_id,
        }
        payload.update({k: v for k, v in optional.items() if v is not None})
        return payload


@runtime_checkable
class TraceExporter(Protocol):
    """Receives finished :class:`TraceEvent`s from the trace middleware.

    ``export`` is called inline on the request path, so it **must be non-blocking**: buffer or
    enqueue the event and return immediately — never do network/disk I/O inline, and never raise
    (mesh.md: trace export is asynchronous, non-blocking, and lossy under backpressure; no mesh feed
    may slow or break the invocation it observes). :class:`QueueTraceExporter` is the non-blocking
    default; a real deployment drains it to a collector on a background task.
    """

    def export(self, event: TraceEvent) -> None: ...


class InMemoryTraceExporter(list):
    """A ``list`` of exported events (``export`` appends) — the fake for tests and dogfooding.

    Unbounded and synchronous: perfect for tests, but prefer :class:`QueueTraceExporter` in a
    long-running service so a slow/absent collector can't grow the buffer without bound.
    """

    def export(self, event: TraceEvent) -> None:
        self.append(event)


class QueueTraceExporter:
    """A bounded, lossy :class:`TraceExporter` — non-blocking by construction (the deployment default).

    ``export`` appends to a fixed-size ``deque`` in O(1) and never blocks or does I/O; when the
    buffer is full the **oldest** event is dropped (lossy under backpressure, per mesh.md). A
    background task periodically :meth:`drain`s the buffer and ships it via
    :meth:`~benzene.mesh.MeshFeedSender.publish_traces` — so the collector never touches the request
    path.
    """

    def __init__(self, maxlen: int = 10_000) -> None:
        self._buffer: collections.deque[TraceEvent] = collections.deque(maxlen=maxlen)

    def export(self, event: TraceEvent) -> None:
        self._buffer.append(event)  # deque(maxlen) evicts the oldest when full — lossy, never blocks

    def drain(self) -> list[TraceEvent]:
        """Remove and return all buffered events (hand them to ``publish_traces``)."""
        events = list(self._buffer)
        self._buffer.clear()
        return events

    def __len__(self) -> int:
        return len(self._buffer)


def trace_middleware(
    exporter: TraceExporter, *, service: str, instance_id: str | None = None
) -> Middleware:
    """Middleware that emits one :class:`TraceEvent` per invocation to ``exporter``.

    Install it *outermost* (first in the pipeline) so it times the whole invocation, including
    routing. It joins the inbound ``traceparent`` trace when present, else starts a fresh one, and
    reads ``x-correlation-id`` for the business correlation id. Exporter failures are swallowed —
    tracing never breaks the request.
    """

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        parsed = parse_traceparent(context.headers.get("traceparent"))
        trace_id, parent_span_id = parsed if parsed else (new_trace_id(), None)
        span_id = new_span_id()
        started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        start = time.perf_counter()

        token = _CURRENT_TRACE.set((trace_id, span_id))  # so outbound calls can forward this span
        try:
            await next()
        finally:
            _CURRENT_TRACE.reset(token)
            status = context.result.status if context.result is not None else Status.UNEXPECTED_ERROR
            event = TraceEvent(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=parent_span_id,
                service=service,
                instance_id=instance_id,
                topic=context.topic,
                topic_version=context.version or None,
                status=status,
                duration_ms=(time.perf_counter() - start) * 1000,
                started_at=started_at,
                correlation_id=context.headers.get("x-correlation-id"),
            )
            # non-blocking + lossy: an exporter error must never affect the invocation (mesh.md §3)
            with contextlib.suppress(Exception):  # pragma: no cover - defensive
                exporter.export(event)

    return middleware


class TracePropagatingMessageSender:
    """Wraps a :class:`~benzene.core.MessageSender`, forwarding the current ``traceparent`` (mesh.md §3).

    When a handler running under :func:`trace_middleware` publishes a message, this injects
    ``traceparent: 00-<traceId>-<spanId>-01`` for the invocation's span, so the downstream service joins
    the same trace and the collector can derive the consumer edge. A ``traceparent`` the caller already
    set is left untouched; outside a trace, nothing is added.
    """

    def __init__(self, inner: Any, *, header: str = "traceparent") -> None:
        self._inner = inner
        self._header = header

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        out = dict(headers or {})
        traceparent = current_traceparent()
        if traceparent and not any(key.lower() == self._header.lower() for key in out):
            out[self._header] = traceparent
        return await self._inner.send_message(topic, message, out)


def with_trace_propagation(inner: Any, **options: Any) -> TracePropagatingMessageSender:
    """Sugar for :class:`TracePropagatingMessageSender`."""
    return TracePropagatingMessageSender(inner, **options)
