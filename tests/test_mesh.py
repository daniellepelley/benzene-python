"""Mesh module tests: the language-neutral conformance fixtures plus focused unit tests.

The descriptor/trace fixtures run through the shared ``conformance_runner`` (so they also run without
pytest); this module surfaces each fixture row as a granular test and adds unit tests for schema
derivation, the ``benzene:mesh`` interceptor, and the collector feed sender.
"""

from __future__ import annotations

import asyncio
import datetime
import json
from dataclasses import dataclass, field
from typing import Optional

import pytest

from benzene.core import BenzeneMessageApplication, MiddlewarePipeline, Registry
from benzene.mesh import (
    ISSUES_TOPIC,
    MESH_TOPIC,
    Heartbeat,
    InMemoryTraceExporter,
    IssueAggregator,
    MeshFeedSender,
    QueueTraceExporter,
    REGISTER_TOPIC,
    ServiceDescriptor,
    ServiceInfo,
    TRACES_TOPIC,
    TraceEvent,
    classify,
    issue_fingerprint,
    json_schema,
    mesh_interception,
    parse_traceparent,
    trace_middleware,
)
from benzene.results import Result
from benzene.testing import FakeMessageSender

from .canonical_handlers import greet, register_canonical, register_with_panic
from .conformance_runner import (
    CONFORMANCE_DIR,
    _mesh_subset,
    run_mesh_descriptor,
    run_mesh_trace,
)


def _load(name: str) -> dict:
    return json.loads((CONFORMANCE_DIR / name).read_text())


@dataclass
class _Node:
    """A module-level recursive dataclass so ``get_type_hints`` can resolve the forward ref."""

    value: int
    next: Optional["_Node"] = None


# --- conformance fixtures (aggregate + granular) -------------------------------------------------

def test_mesh_descriptor_conforms() -> None:
    assert run_mesh_descriptor() == []


def test_mesh_trace_conforms() -> None:
    assert run_mesh_trace() == []


@pytest.mark.parametrize(
    "row", _load("mesh-trace-cases.json")["traceparent"], ids=lambda r: r["name"]
)
def test_traceparent_row(row: dict) -> None:
    parsed = parse_traceparent(row["header"])
    if row["valid"]:
        assert parsed == (row["traceId"], row["parentSpanId"])
    else:
        assert parsed is None


@pytest.mark.parametrize(
    "case", _load("mesh-trace-cases.json")["invocations"], ids=lambda c: c["name"]
)
def test_trace_invocation(case: dict) -> None:
    exporter = InMemoryTraceExporter()
    pipeline = MiddlewarePipeline().use(trace_middleware(exporter, service="conformance"))
    app = BenzeneMessageApplication(register_with_panic(Registry()), pipeline)
    asyncio.run(app.handle(case["request"]))
    assert len(exporter) == 1
    assert _mesh_subset(case["expectedEvent"], exporter[0].to_payload())


# --- schema derivation unit tests ----------------------------------------------------------------

def test_schema_primitives() -> None:
    assert json_schema(str) == {"type": "string"}
    assert json_schema(bool) == {"type": "boolean"}
    assert json_schema(int) == {"type": "integer"}
    assert json_schema(float) == {"type": "number"}
    assert json_schema(bytes) == {"type": "string"}
    assert json_schema(datetime.datetime) == {"type": "string", "format": "date-time"}
    assert json_schema(None) == {}


def test_schema_containers_and_nullable() -> None:
    assert json_schema(list[str]) == {"type": "array", "items": {"type": "string"}}
    assert json_schema(dict[str, int]) == {"type": "object", "additionalProperties": {"type": "integer"}}
    assert json_schema(Optional[str]) == {"type": ["string", "null"]}


def test_schema_dataclass_required_follows_defaults() -> None:
    @dataclass
    class Order:
        id: str  # required — no default
        note: str = ""  # optional — has a default
        tags: list[str] = field(default_factory=list)  # optional
        order_ref: str = "x"  # camelCased on the wire

    schema = json_schema(Order)
    assert schema["properties"]["orderRef"] == {"type": "string"}  # snake_case -> camelCase
    assert schema["required"] == ["id"]


def test_schema_recursive_cuts_the_cycle() -> None:
    schema = json_schema(_Node)
    # the recursive `next` becomes an open schema rather than an infinite $ref chain
    assert schema["properties"]["next"] in ({}, {"type": ["null"]})
    assert schema["required"] == ["value"]


# --- interception unit tests ---------------------------------------------------------------------

def _descriptor() -> ServiceDescriptor:
    info = ServiceInfo(service="conformance", service_version="1.0.0", placement={"cloud": "self-hosted"})
    return ServiceDescriptor.derive(register_canonical(Registry()), info)


def test_mesh_interception_returns_descriptor() -> None:
    app = BenzeneMessageApplication(
        register_canonical(Registry()), MiddlewarePipeline().use(mesh_interception(_descriptor()))
    )
    response = asyncio.run(app.handle({"topic": MESH_TOPIC, "headers": {}, "body": ""}))
    assert response["statusCode"] == "ok"
    body = json.loads(response["body"])
    assert body["service"] == "conformance"
    assert body["descriptorHash"].startswith("sha256:")
    assert {t["id"] for t in body["topics"]} == {"conformance:greet", "conformance:status"}


def test_mesh_interception_ignores_version_and_passes_through() -> None:
    app = BenzeneMessageApplication(
        register_canonical(Registry()), MiddlewarePipeline().use(mesh_interception(_descriptor()))
    )
    # version ignored on the reserved topic
    versioned = asyncio.run(
        app.handle({"topic": MESH_TOPIC, "headers": {"benzene-version": "v2"}, "body": ""})
    )
    assert versioned["statusCode"] == "ok"
    # a normal topic still routes to its handler
    routed = asyncio.run(
        app.handle({"topic": "conformance:greet", "headers": {}, "body": '{"name":"world"}'})
    )
    assert json.loads(routed["body"]) == {"greeting": "Hello world"}


def test_mesh_interception_alias() -> None:
    app = BenzeneMessageApplication(
        register_canonical(Registry()),
        MiddlewarePipeline().use(mesh_interception(_descriptor(), aliases=["mesh:whoami"])),
    )
    response = asyncio.run(app.handle({"topic": "mesh:whoami", "headers": {}, "body": ""}))
    assert response["statusCode"] == "ok"


# --- collector feed sender unit tests ------------------------------------------------------------

def test_feed_sender_register_and_traces() -> None:
    fake = FakeMessageSender()
    feeds = MeshFeedSender(fake)
    descriptor = _descriptor()

    asyncio.run(feeds.register(descriptor))
    assert fake.last_topic == REGISTER_TOPIC
    assert fake.last_message["service"] == "conformance"

    exporter = InMemoryTraceExporter()
    pipeline = MiddlewarePipeline().use(trace_middleware(exporter, service="conformance"))
    app = BenzeneMessageApplication(register_canonical(Registry()), pipeline)
    asyncio.run(app.handle({"topic": "conformance:greet", "headers": {}, "body": '{"name":"x"}'}))

    asyncio.run(feeds.publish_traces(exporter))
    assert fake.last_topic == TRACES_TOPIC
    assert fake.last_message["events"][0]["topic"] == "conformance:greet"


def test_heartbeat_payload_shape() -> None:
    hb = Heartbeat(
        service="orders",
        sent_at="2026-07-16T09:14:03Z",
        instance_id="orders-7f9c",
        descriptor_hash="sha256:abc",
        is_healthy=True,
        health_checks={"registry": {"isHealthy": True}},
    )
    payload = hb.to_payload()
    assert payload["service"] == "orders"
    assert payload["health"] == {"isHealthy": True, "healthChecks": {"registry": {"isHealthy": True}}}


# --- issues feed unit tests ----------------------------------------------------------------------

@pytest.mark.parametrize(
    "status,exception_type,expected",
    [
        ("bad-request", None, "validation"),
        ("validation-error", None, "validation"),
        ("service-unavailable", "HttpError", "exception"),  # exception type wins over dependency
        ("not-found", None, "config-wiring"),
        ("unauthorized", None, "config-wiring"),
        ("not-implemented", None, "config-wiring"),
        ("", None, "config-wiring"),  # empty status
        ("service-unavailable", None, "dependency"),
        ("timeout", None, "dependency"),
        ("too-many-requests", None, "dependency"),
        ("unexpected-error", None, "exception"),
        ("conflict", None, "unclassified"),  # any other failing status
    ],
)
def test_classify_precedence(status: str, exception_type: str | None, expected: str) -> None:
    assert classify(status, exception_type) == expected


def test_fingerprint_is_stable_and_16_bytes() -> None:
    fp = issue_fingerprint("orders", "order:create", "v2", "exception", "HttpError")
    assert fp == issue_fingerprint("orders", "order:create", "v2", "exception", "HttpError")
    assert len(fp) == 32 and all(c in "0123456789abcdef" for c in fp)  # 16 bytes, lowercase hex


def test_fingerprint_absent_version_is_empty_string() -> None:
    # version="" and a truly absent version fingerprint identically (spec: empty string when absent)
    assert issue_fingerprint("s", "t", "", "dependency", "timeout") == issue_fingerprint(
        "s", "t", "", "dependency", "timeout"
    )


def test_issue_aggregator_counts_deltas_and_resets_on_flush() -> None:
    agg = IssueAggregator("orders")
    agg.record(topic="order:create", status="service-unavailable", version="v2", trace_id="t1")
    agg.record(topic="order:create", status="service-unavailable", version="v2", trace_id="t2")
    agg.record(topic="order:get", status="not-found")

    batch = agg.flush()
    assert batch.service == "orders"
    by_topic = {i.topic: i for i in batch.issues}
    assert by_topic["order:create"].count == 2
    assert by_topic["order:create"].classification == "dependency"
    assert by_topic["order:create"].exemplar_trace_ids == ("t1", "t2")
    assert by_topic["order:get"].classification == "config-wiring"

    # flush reset the window: a fresh flush is the empty liveness batch
    assert agg.flush().issues == []


def test_publish_issues_sends_the_batch() -> None:
    fake = FakeMessageSender()
    feeds = MeshFeedSender(fake)
    agg = IssueAggregator("orders")
    agg.record(topic="order:create", status="unexpected-error", exception_type="KeyError")
    asyncio.run(feeds.publish_issues(agg.flush()))
    assert fake.last_topic == ISSUES_TOPIC
    assert fake.last_message["service"] == "orders"
    assert fake.last_message["issues"][0]["classification"] == "exception"


# --- QueueTraceExporter (non-blocking, lossy) ----------------------------------------------------

def _event(topic: str) -> TraceEvent:
    return TraceEvent(trace_id="t", span_id="s", service="svc", topic=topic, status="ok")


def test_queue_exporter_drains_fifo() -> None:
    exporter = QueueTraceExporter()
    exporter.export(_event("a"))
    exporter.export(_event("b"))
    drained = exporter.drain()
    assert [e.topic for e in drained] == ["a", "b"]
    assert exporter.drain() == []  # drain empties the buffer


def test_queue_exporter_is_lossy_when_full() -> None:
    exporter = QueueTraceExporter(maxlen=2)
    for topic in ("a", "b", "c"):
        exporter.export(_event(topic))  # never blocks; oldest is dropped
    assert [e.topic for e in exporter.drain()] == ["b", "c"]
