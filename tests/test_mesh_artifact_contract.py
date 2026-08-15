"""Contract-shape guard: the Python artifacts carry exactly the cross-language read-model fields.

The Mesh UI is one page across every port, fed by artifacts whose shapes are fixed by the read-model
contract (main repo ``docs/guides/mesh-ui.md`` §2, pinned by ``website/demos/mesh/``). If the Python
aggregator's output grows or drops a field, the shared page silently misrenders for Python only. These
tests pin the exact key set of every artifact and item, so any drift from the contract fails here —
the port's cross-language-compatibility promise, enforced without needing the other ports present.
"""

from __future__ import annotations

from benzene.mesh import MeshCollector, build_artifacts

_AT = "2026-08-08T00:00:00+00:00"

# The exact top-level and per-item field sets the read-model contract defines (mesh-ui.md §2). The UI
# "must not invent fields"; symmetrically, the aggregator must emit exactly these — no more, no less.
_MANIFEST = {"generatedAtUtc", "services"}
_MANIFEST_SERVICE = {"name", "status", "contractDrift", "specUrl", "healthUrl"}
_SERVICE = {
    "name",
    "fetchedAtUtc",
    "specJson",
    "specHash",
    "previousSpecHash",
    "contractDrift",
    "health",
    "error",
}
_TOPICS = {"generatedAtUtc", "topics", "removedTopics"}
_TOPIC = {
    "topic",
    "version",
    "reserved",
    "consumers",
    "producers",
    "status",
    "requestSchema",
    "responseSchema",
    "messageSchema",
    "schemaMismatch",
    "changes",
}
_TOPOLOGY = {"generatedAtUtc", "edges"}
_EDGE = {
    "client",
    "server",
    "source",
    "requestsPerMinute",
    "errorRate",
    "p50LatencyMs",
    "p95LatencyMs",
    "p99LatencyMs",
}
_USAGE = {"generatedAtUtc", "windowStartUtc", "windowEndUtc", "entries"}
_USAGE_ENTRY = {
    "topic",
    "version",
    "service",
    "transport",
    "status",
    "count",
    "avgDurationMs",
    "source",
}
_ASYNCAPI = {"asyncapi", "info", "channels", "operations", "components"}
_ANNOTATIONS = {"generatedAtUtc", "annotations"}


def _artifacts() -> dict:
    c = MeshCollector()
    c.ingest_register(
        {
            "service": "orders",
            "topics": [{"id": "order:create", "requestSchema": {"type": "object"}}],
            "produces": [{"id": "stock:reserve"}],
            "descriptorHash": "h1",
        }
    )
    c.ingest_register(
        {"service": "inventory", "topics": [{"id": "stock:reserve"}], "descriptorHash": "h2"}
    )
    c.ingest_heartbeat({"service": "orders", "instanceId": "i1", "health": {"isHealthy": True}})
    c.ingest_traces(
        {
            "events": [
                {
                    "traceId": "t",
                    "spanId": "s1",
                    "service": "orders",
                    "topic": "order:create",
                    "status": "ok",
                },
                {
                    "traceId": "t",
                    "spanId": "s2",
                    "parentSpanId": "s1",
                    "service": "inventory",
                    "topic": "stock:reserve",
                    "status": "ok",
                },
            ]
        }
    )
    return build_artifacts(c, generated_at=_AT)


def test_manifest_fields_match_the_contract() -> None:
    arts = _artifacts()
    assert set(arts["manifest"]) == _MANIFEST
    assert arts["manifest"]["services"], "expected at least one service to check"
    for service in arts["manifest"]["services"]:
        assert set(service) == _MANIFEST_SERVICE


def test_service_document_fields_match_the_contract() -> None:
    for document in _artifacts()["services"].values():
        assert set(document) == _SERVICE


def test_topics_fields_match_the_contract() -> None:
    topics = _artifacts()["topics"]
    assert set(topics) == _TOPICS
    assert topics["topics"], "expected at least one topic to check"
    for topic in topics["topics"]:
        assert set(topic) == _TOPIC


def test_topology_edge_fields_match_the_contract() -> None:
    topology = _artifacts()["topology"]
    assert set(topology) == _TOPOLOGY
    assert topology["edges"], "expected at least one edge to check"
    for edge in topology["edges"]:
        assert set(edge) == _EDGE


def test_usage_fields_match_the_contract() -> None:
    usage = _artifacts()["usage"]
    assert set(usage) == _USAGE
    assert usage["entries"], "expected at least one usage entry to check"
    for entry in usage["entries"]:
        assert set(entry) == _USAGE_ENTRY


def test_asyncapi_and_annotations_fields_match_the_contract() -> None:
    arts = _artifacts()
    assert set(arts["asyncapi"]) == _ASYNCAPI
    assert set(arts["annotations"]) == _ANNOTATIONS
