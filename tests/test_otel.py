"""The OpenTelemetry bridge — trace export onto a fake OTel tracer, and response-as-event.

Everything runs in memory with **no** ``opentelemetry`` package and no network: the OTel tracer is a
fake that records the spans it is handed, and the response-event sink is the in-memory
:class:`~benzene.otel.RecordingSink`. The trace side builds a real :class:`~benzene.mesh.TraceEvent`
(the port's own span model) and asserts the fake span's name, times, attributes, and status; the
response side drives a real ``MiddlewarePipeline`` + ``message_router``, as ``test_resilience.py`` does.
"""

from __future__ import annotations

import asyncio
from typing import Any

from benzene.core import Context, MiddlewarePipeline, Registry, message_router
from benzene.mesh import TraceEvent, trace_middleware
from benzene.otel import (
    OtelTraceExporter,
    RecordingSink,
    ResponseEvent,
    response_event_interception,
)
from benzene.results import Result, Status


def run(coro):
    return asyncio.run(coro)


# --- a fake OpenTelemetry tracer (records spans; no opentelemetry package) ----------------------


class FakeSpan:
    """Records everything the exporter does to it — attributes, status, and the end timestamp."""

    def __init__(self, name: str, start_time: int | None) -> None:
        self.name = name
        self.start_time = start_time
        self.end_time: int | None = None
        self.attributes: dict[str, Any] = {}
        self.status: Any = None
        self.ended = False

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def set_status(self, status: Any, description: str | None = None) -> None:
        self.status = status

    def end(self, end_time: int | None = None) -> None:
        self.end_time = end_time
        self.ended = True


class FakeTracer:
    """A stand-in for an OTel ``Tracer`` — ``start_span`` mints and remembers a :class:`FakeSpan`."""

    def __init__(self) -> None:
        self.spans: list[FakeSpan] = []

    def start_span(self, name: str, *, start_time: int | None = None) -> FakeSpan:
        span = FakeSpan(name, start_time)
        self.spans.append(span)
        return span


def _trace_event(**overrides: Any) -> TraceEvent:
    """A representative finished span (mesh.md §3): a child span with a business correlation id."""
    fields: dict[str, Any] = {
        "trace_id": "0af7651916cd43dd8448eb211c80319c",
        "span_id": "b7ad6b7169203331",
        "parent_span_id": "00f067aa0ba902b7",
        "service": "orders",
        "instance_id": "orders-7",
        "topic": "orders:create",
        "topic_version": "2",
        "status": Status.OK,
        "duration_ms": 12.5,
        "started_at": "2026-08-11T10:00:00Z",
        "correlation_id": "corr-abc",
    }
    fields.update(overrides)
    return TraceEvent(**fields)


# --- trace export ------------------------------------------------------------------------------


def test_exporter_maps_span_name_times_and_attributes_onto_the_tracer() -> None:
    tracer = FakeTracer()
    exporter = OtelTraceExporter(tracer)

    exporter.export(_trace_event())

    assert len(tracer.spans) == 1
    span = tracer.spans[0]

    # Name is the topic; start is started_at in epoch nanos; end is start + duration_ms in nanos.
    assert span.name == "orders:create"
    expected_start = 1_786_442_400 * 1_000_000_000  # 2026-08-11T10:00:00Z in epoch nanoseconds
    assert span.start_time == expected_start
    assert span.end_time == expected_start + int(12.5 * 1_000_000)
    assert span.ended is True

    # Every TraceEvent field becomes a benzene.* attribute.
    assert span.attributes == {
        "benzene.service": "orders",
        "benzene.topic": "orders:create",
        "benzene.status": "ok",
        "benzene.trace_id": "0af7651916cd43dd8448eb211c80319c",
        "benzene.span_id": "b7ad6b7169203331",
        "benzene.parent_span_id": "00f067aa0ba902b7",
        "benzene.instance_id": "orders-7",
        "benzene.topic_version": "2",
        "benzene.correlation_id": "corr-abc",
    }
    # A successful status maps to the OK code (a string fallback, since opentelemetry isn't installed).
    assert span.status == "OK"


def test_exporter_omits_absent_fields_and_maps_failure_to_error_status() -> None:
    tracer = FakeTracer()
    OtelTraceExporter(tracer).export(
        TraceEvent(
            trace_id="a" * 32,
            span_id="b" * 16,
            service="orders",
            topic="orders:create",
            status=Status.SERVICE_UNAVAILABLE,
        )
    )

    span = tracer.spans[0]
    # No started_at → no start/end time supplied (the SDK would default them).
    assert span.start_time is None
    assert span.end_time is None
    # Optional fields that were None are never set (no null attributes).
    assert "benzene.parent_span_id" not in span.attributes
    assert "benzene.correlation_id" not in span.attributes
    assert span.attributes["benzene.status"] == "service-unavailable"
    # A failure status maps to the ERROR code.
    assert span.status == "ERROR"


def test_exporter_export_traces_maps_a_batch() -> None:
    tracer = FakeTracer()
    exporter = OtelTraceExporter(tracer)
    exporter.export_traces([_trace_event(topic="a"), _trace_event(topic="b")])
    assert [span.name for span in tracer.spans] == ["a", "b"]


def test_exporter_wires_into_the_existing_trace_middleware() -> None:
    """The exporter is a mesh TraceExporter, so trace_middleware drives it end-to-end."""
    tracer = FakeTracer()

    async def handler(_request) -> Result:
        return Result.ok({"id": 1})

    registry = Registry().register("orders:create", handler)
    pipeline = MiddlewarePipeline(
        [trace_middleware(OtelTraceExporter(tracer), service="orders")]
    ).use(message_router(registry))

    run(pipeline.handle(Context("orders:create", {})))

    assert len(tracer.spans) == 1
    span = tracer.spans[0]
    assert span.name == "orders:create"
    assert span.attributes["benzene.status"] == "ok"
    assert span.attributes["benzene.service"] == "orders"
    assert span.start_time is not None and span.end_time is not None


# --- response-as-event -------------------------------------------------------------------------


def _response_pipeline(sink, handler, **options):
    registry = Registry().register("t", handler)
    return MiddlewarePipeline([response_event_interception(sink, **options)]).use(
        message_router(registry)
    )


def test_response_event_emitted_to_sink_after_the_handler_runs() -> None:
    sink = RecordingSink()

    async def handler(_request) -> Result:
        return Result.created({"id": 7})

    registry = Registry().register("t", handler, version="3")
    pipeline = MiddlewarePipeline([response_event_interception(sink)]).use(message_router(registry))
    run(pipeline.handle(Context("t", {}, headers={"x-correlation-id": "corr-9"}, version="3")))

    assert len(sink) == 1
    event = sink[0]
    assert isinstance(event, ResponseEvent)
    assert event.topic == "t"
    assert event.status == Status.CREATED
    assert event.version == "3"
    assert event.correlation_id == "corr-9"
    assert event.payload == {"id": 7}
    assert event.is_successful


def test_response_event_defaults_to_emitting_failures_too() -> None:
    sink = RecordingSink()

    async def handler(_request) -> Result:
        return Result.not_found("missing")

    pipeline = _response_pipeline(sink, handler)
    run(pipeline.handle(Context("t", {})))

    assert len(sink) == 1
    assert sink[0].status == Status.NOT_FOUND
    assert sink[0].errors == ("missing",)
    assert not sink[0].is_successful


def test_response_event_when_predicate_filters_out_failures() -> None:
    sink = RecordingSink()
    outcomes = iter([Result.ok(), Result.service_unavailable("down")])

    async def handler(_request) -> Result:
        return next(outcomes)

    pipeline = _response_pipeline(sink, handler, when=lambda r: r.is_successful)
    run(pipeline.handle(Context("t", {})))  # success → emitted
    run(pipeline.handle(Context("t", {})))  # failure → filtered out

    assert [event.status for event in sink] == [Status.OK]


def test_response_event_include_payload_false_drops_the_payload() -> None:
    sink = RecordingSink()

    async def handler(_request) -> Result:
        return Result.ok({"secret": True})

    pipeline = _response_pipeline(sink, handler, include_payload=False)
    run(pipeline.handle(Context("t", {})))

    assert sink[0].payload is None
    assert sink[0].to_payload() == {"topic": "t", "status": "ok"}


def test_a_raising_sink_does_not_break_the_pipeline() -> None:
    class BoomSink:
        async def emit(self, event: ResponseEvent) -> None:
            raise RuntimeError("sink is down")

    async def handler(_request) -> Result:
        return Result.ok({"id": 1})

    pipeline = _response_pipeline(BoomSink(), handler)
    ctx = Context("t", {})
    run(pipeline.handle(ctx))  # must not raise

    # The invocation's own result is untouched by the failing observer.
    assert ctx.result is not None and ctx.result.status == Status.OK
    assert ctx.result.payload == {"id": 1}
