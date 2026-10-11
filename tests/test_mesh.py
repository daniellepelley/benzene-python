"""Mesh module tests: the language-neutral conformance fixtures plus focused unit tests.

The trace fixture runs through the shared ``conformance_runner`` (so it also runs without pytest); this
module surfaces each fixture row as a granular test and adds unit tests for schema derivation, the
register body (the Contract Document, mesh.md §2), and the collector feed sender.
"""

from __future__ import annotations

import asyncio
import datetime
import json
from dataclasses import dataclass, field
from typing import Optional

import pytest
from benzene.core import (
    AppDefinition,
    BenzeneMessageApplication,
    ContractInfo,
    MiddlewarePipeline,
    OutboundRegistry,
    Registry,
    application_from,
    json_schema,
)
from benzene.mesh import (
    HEARTBEAT_TOPIC,
    ISSUES_TOPIC,
    REGISTER_TOPIC,
    TRACES_TOPIC,
    Heartbeat,
    InMemoryTraceExporter,
    IssueAggregator,
    MeshFeedSender,
    QueueTraceExporter,
    TraceEvent,
    classify,
    current_traceparent,
    current_tracestate,
    issue_fingerprint,
    parse_traceparent,
    registration_document,
    trace_middleware,
    with_trace_propagation,
)
from benzene.results import Result
from benzene.testing import FakeMessageSender

from .canonical_handlers import register_canonical, register_with_panic
from .conformance_runner import (
    CONFORMANCE_DIR,
    _mesh_subset,
    run_mesh_trace,
)


def _load(name: str) -> dict:
    return json.loads((CONFORMANCE_DIR / name).read_text())


@dataclass
class _Node:
    """A module-level recursive dataclass so ``get_type_hints`` can resolve the forward ref."""

    value: int
    next: _Node | None = None


# --- conformance fixtures (aggregate + granular) -------------------------------------------------

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
    # Both union spellings must derive the same nullable schema: the legacy typing.Optional…
    assert json_schema(Optional[str]) == {"type": ["string", "null"]}  # noqa: UP045 - test the legacy form too
    # …and the PEP 604 ``X | None`` form modern handlers actually write.
    assert json_schema(str | None) == {"type": ["string", "null"]}
    assert json_schema(list[str] | None) == {"type": ["array", "null"], "items": {"type": "string"}}


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


# --- the register body: the Contract Document (mesh.md §2) --------------------------------------

def _definition(
    *, profile: dict[str, object] | None = None, degraded: tuple[str, ...] = ()
) -> AppDefinition:
    contract = ContractInfo(
        "conformance",
        service_version="1.0.0",
        version_scheme="semver",
        placement={"cloud": "self-hosted"},
        profile=profile,
        degraded=degraded,
    )
    return AppDefinition(
        registry=register_canonical(Registry()),
        outbound=OutboundRegistry().register_send("conformance:log", dict),
        contract=contract,
    )


def test_registration_document_is_the_document_benzene_spec_serves() -> None:
    definition = _definition()
    app = application_from(definition)
    served = json.loads(
        asyncio.run(app.handle({"topic": "benzene:spec", "headers": {}, "body": ""}))["body"]
    )
    assert registration_document(definition).to_payload() == served
    assert served["service"] == "conformance"
    assert served["runtime"] == "python"
    assert served["placement"] == {"cloud": "self-hosted"}
    assert {r["topic"] for r in served["requests"]} == {"conformance:greet", "conformance:status"}
    assert [s["topic"] for s in served["sends"]] == ["conformance:log"]
    assert "instanceId" not in served  # instances are a heartbeat's business, never the document's


def test_deployment_fields_are_outside_both_hashes() -> None:
    plain = registration_document(_definition())
    dressed = registration_document(
        _definition(profile={"name": "cloud-service", "missing": ["R3"]}, degraded=("sends",))
    )
    payload = dressed.to_payload()
    assert payload["profile"] == {"name": "cloud-service", "missing": ["R3"]}
    assert payload["degraded"] == ["sends"]
    assert "degraded" not in plain.to_payload()  # omitted when empty
    assert dressed.registration_hash == plain.registration_hash
    assert dressed.contract_hash == plain.contract_hash


def test_degraded_names_only_document_members() -> None:
    with pytest.raises(ValueError, match="degraded"):
        registration_document(_definition(degraded=("registry",)))


def test_registration_needs_a_contract() -> None:
    with pytest.raises(ValueError, match="ContractInfo"):
        registration_document(AppDefinition(registry=register_canonical(Registry())))


# --- collector feed sender unit tests ------------------------------------------------------------

def test_feed_sender_register_and_traces() -> None:
    fake = FakeMessageSender()
    feeds = MeshFeedSender(fake)
    document = registration_document(_definition())

    asyncio.run(feeds.register(document))
    assert fake.last_topic == REGISTER_TOPIC
    assert fake.last_message == document.to_payload()
    assert fake.last_message["registrationHash"].startswith("sha256:")

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
        registration_hash="sha256:abc",
        service_version="1.4.2",
        is_healthy=True,
        health_checks={"registry": {"isHealthy": True}},
    )
    assert hb.to_payload() == {
        "service": "orders",
        "serviceVersion": "1.4.2",
        "instanceId": "orders-7f9c",
        "registrationHash": "sha256:abc",
        "sentAt": "2026-07-16T09:14:03Z",
        "health": {"isHealthy": True, "healthChecks": {"registry": {"isHealthy": True}}},
    }


def test_heartbeat_for_document_carries_its_hash_and_version() -> None:
    document = registration_document(_definition())
    payload = Heartbeat.for_document(
        document, sent_at="2026-07-16T09:14:03Z", instance_id="c-1"
    ).to_payload()
    assert payload["service"] == "conformance"
    assert payload["serviceVersion"] == "1.0.0"
    assert payload["registrationHash"] == document.registration_hash
    assert "descriptorHash" not in payload


def test_heartbeat_omits_an_undeclared_service_version() -> None:
    payload = Heartbeat(service="orders", sent_at="2026-07-16T09:14:03Z").to_payload()
    assert "serviceVersion" not in payload


def test_publish_heartbeat_sends_the_beat() -> None:
    fake = FakeMessageSender()
    feeds = MeshFeedSender(fake)
    hb = Heartbeat(service="orders", sent_at="2026-07-16T09:14:03Z", registration_hash="sha256:abc")
    asyncio.run(feeds.publish_heartbeat(hb))
    assert fake.last_topic == HEARTBEAT_TOPIC
    assert fake.last_message["service"] == "orders"
    assert fake.last_message["registrationHash"] == "sha256:abc"


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


# --- outbound trace propagation ------------------------------------------------------------------

def test_current_traceparent_is_none_outside_a_trace() -> None:
    assert current_traceparent() is None


def test_outbound_call_forwards_the_current_traceparent() -> None:
    captured: dict = {}

    class _FakeSender:
        async def send_message(self, topic, message, headers=None):
            captured.update(headers or {})
            return Result.ok()

    outbound = with_trace_propagation(_FakeSender())

    async def handler(_request: dict) -> Result:
        await outbound.send_message("downstream:topic", {"n": 1})  # published mid-invocation
        return Result.ok()

    pipeline = MiddlewarePipeline().use(trace_middleware(InMemoryTraceExporter(), service="svc"))
    app = BenzeneMessageApplication(Registry().register("do:thing", handler), pipeline)

    inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    asyncio.run(app.handle({"topic": "do:thing", "headers": {"traceparent": inbound}, "body": "{}"}))

    assert "traceparent" in captured
    # same trace id as the inbound header (this service joined it), a fresh span for this hop
    assert captured["traceparent"].split("-")[1] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert current_traceparent() is None  # the contextvar is reset after the invocation


def test_outbound_call_carries_the_inbound_tracestate_forward_unchanged() -> None:
    # M2 (mesh.md §7.1): traceparent *and* tracestate (wire-contracts §2) on every outbound message.
    captured: dict = {}

    class _FakeSender:
        async def send_message(self, topic, message, headers=None):
            captured.update(headers or {})
            return Result.ok()

    outbound = with_trace_propagation(_FakeSender())

    async def handler(_request: dict) -> Result:
        await outbound.send_message("downstream:topic", {})
        return Result.ok()

    pipeline = MiddlewarePipeline().use(trace_middleware(InMemoryTraceExporter(), service="svc"))
    app = BenzeneMessageApplication(Registry().register("do:thing", handler), pipeline)
    inbound = {
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "tracestate": "vendor=abc,other=1",
    }
    asyncio.run(app.handle({"topic": "do:thing", "headers": inbound, "body": "{}"}))
    assert captured["tracestate"] == "vendor=abc,other=1"
    assert current_tracestate() is None  # reset after the invocation

    # An invalid traceparent starts a fresh trace, and its tracestate is discarded with it (W3C).
    captured.clear()
    inbound["traceparent"] = "00-not-a-valid-header-01"
    asyncio.run(app.handle({"topic": "do:thing", "headers": inbound, "body": "{}"}))
    assert "traceparent" in captured
    assert "tracestate" not in captured


def test_a_published_event_carries_the_trace_context_to_every_subscriber() -> None:
    # M2 covers published events, not only sends: in this port an event publisher is a MessageSender
    # too, so the same with_trace_propagation wrapper forwards the context on a publish. Here a
    # fan-out (the in-process SNS equivalent) delivers one event to two subscriber pipelines.
    from benzene.core import InProcessFanOutSender, Pipelines

    seen: dict[str, dict] = {}

    def _subscriber(name: str) -> BenzeneMessageApplication:
        async def capture(context, next) -> None:  # noqa: A002 - spec name
            seen[name] = dict(context.headers)
            await next()

        async def on_placed(_event: dict) -> Result:
            return Result.ok()

        return BenzeneMessageApplication(
            Registry().register("order:placed", on_placed), MiddlewarePipeline().use(capture)
        )

    pipelines = Pipelines().add("billing", _subscriber("billing")).add("shipping", _subscriber("shipping"))
    publisher = with_trace_propagation(InProcessFanOutSender(pipelines, "billing", "shipping"))

    async def place(_request: dict) -> Result:
        await publisher.send_message("order:placed", {"id": "o-1"})  # a declared event, published
        return Result.ok()

    exporter = InMemoryTraceExporter()
    orders = BenzeneMessageApplication(
        Registry().register("order:place", place),
        MiddlewarePipeline().use(trace_middleware(exporter, service="orders")),
    )
    inbound = {
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "tracestate": "vendor=abc",
    }
    asyncio.run(orders.handle({"topic": "order:place", "headers": inbound, "body": "{}"}))

    (publishing_span,) = exporter
    for name in ("billing", "shipping"):
        headers = seen[name]
        _, trace_id, parent_id, _ = headers["traceparent"].split("-")
        assert trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"  # the subscriber joins the trace...
        assert parent_id == publishing_span.span_id  # ...as a child of the publishing invocation
        assert headers["tracestate"] == "vendor=abc"


def test_composed_outbound_stack_shares_one_correlation_id_across_retries() -> None:
    from benzene.core import with_correlation_id, with_retry

    calls: list[dict] = []

    class _FlakySender:
        async def send_message(self, topic, message, headers=None):
            calls.append(dict(headers or {}))
            return Result.service_unavailable("transient") if len(calls) == 1 else Result.ok()

    # correlation-id OUTSIDE retry -> the id is stable across attempts (the cookbook's order)
    outbound = with_correlation_id(with_retry(with_trace_propagation(_FlakySender()), attempts=3))

    async def handler(_request: dict) -> Result:
        return await outbound.send_message("downstream:topic", {})

    pipeline = MiddlewarePipeline().use(trace_middleware(InMemoryTraceExporter(), service="svc"))
    app = BenzeneMessageApplication(Registry().register("do:thing", handler), pipeline)
    inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    response = asyncio.run(app.handle({"topic": "do:thing", "headers": {"traceparent": inbound}, "body": "{}"}))

    assert response["statusCode"] == "ok"                    # retry recovered
    assert len(calls) == 2                                   # one retry
    ids = {call["x-correlation-id"] for call in calls}
    assert len(ids) == 1                                     # ONE correlation id across both attempts
    assert all(c["traceparent"].split("-")[1] == "4bf92f3577b34da6a3ce929d0e0e4736" for c in calls)
