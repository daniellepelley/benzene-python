"""The mesh-ui artifact projection — MeshCollector catalog → the cross-language read-model artifacts.

Shapes follow the main repo's docs/guides/mesh-ui.md and its website/demos/mesh fixtures. These pin
what the Python aggregator derives (estate, topology, functional map, per-service) and that the fields
it can't derive degrade to null/empty rather than being invented.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from benzene.mesh import (
    CallableServiceSource,
    HttpServiceSource,
    MeshCollector,
    MeshPoller,
    build_artifacts,
    write_artifacts,
)

_AT = "2026-08-08T00:00:00+00:00"


_ORDER_SCHEMA = {"type": "object", "properties": {"sku": {"type": "string"}}}
_RESERVE_SCHEMA = {"type": "object", "properties": {"qty": {"type": "integer"}}}


def _fleet() -> MeshCollector:
    c = MeshCollector()
    # Three services; one healthy, one unhealthy + drifted, one registered-but-never-beat (unreachable).
    c.ingest_register(
        {
            "service": "orders",
            "topics": [{"id": "order:create", "version": "1", "requestSchema": _ORDER_SCHEMA}],
            "descriptorHash": "h-ord",
        }
    )
    c.ingest_register(
        {
            "service": "inventory",
            "topics": [{"id": "stock:reserve", "responseSchema": _RESERVE_SCHEMA}],
            "descriptorHash": "h-inv",
        }
    )
    c.ingest_register(
        {"service": "notifications", "topics": [{"id": "notify:send"}], "descriptorHash": "h-not"}
    )
    c.ingest_heartbeat(
        {
            "service": "orders",
            "instanceId": "i1",
            "descriptorHash": "h-ord",
            "health": {"isHealthy": True, "healthChecks": {"db": {"status": "ok", "type": "sql"}}},
        }
    )
    # inventory's live instance runs a different descriptor hash -> unhealthy + contract drift.
    c.ingest_heartbeat(
        {
            "service": "inventory",
            "instanceId": "i1",
            "descriptorHash": "h-OLD",
            "health": {"isHealthy": False},
        }
    )
    # orders calls inventory (its span parents the inventory event) -> consumer edge; the call errored.
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
                    "status": "service-unavailable",
                },
            ]
        }
    )
    return c


def _sources() -> list[HttpServiceSource]:
    return [
        HttpServiceSource("orders", "https://orders.example"),
        HttpServiceSource("inventory", "https://inventory.example"),
    ]


# --- manifest ----------------------------------------------------------------------------------


def test_manifest_reports_estate_status_drift_and_links() -> None:
    arts = build_artifacts(_fleet(), sources=_sources(), generated_at=_AT)
    manifest = arts["manifest"]
    assert manifest["generatedAtUtc"] == _AT
    by_name = {s["name"]: s for s in manifest["services"]}

    assert by_name["orders"]["status"] == "healthy"
    assert by_name["orders"]["contractDrift"] is False
    assert by_name["orders"]["specUrl"] == "https://orders.example/benzene/spec"
    assert by_name["orders"]["healthUrl"] == "https://orders.example/benzene/health"

    assert by_name["inventory"]["status"] == "unhealthy"
    assert by_name["inventory"]["contractDrift"] is True  # instance hash != registered hash

    # Registered but never sent a heartbeat -> no health signal reached us -> unreachable, no links.
    assert by_name["notifications"]["status"] == "unreachable"
    assert by_name["notifications"]["specUrl"] is None


# --- topology ----------------------------------------------------------------------------------


def test_topology_edges_come_from_trace_parentage_with_error_rate() -> None:
    arts = build_artifacts(_fleet(), generated_at=_AT)
    edges = arts["topology"]["edges"]
    assert len(edges) == 1
    edge = edges[0]
    assert edge["client"] == "orders" and edge["server"] == "inventory"
    assert edge["source"] == "collector"
    assert edge["errorRate"] == 1.0  # the one traced call failed
    # Rate/latency need a metrics feed the collector doesn't have -> degraded, not invented.
    assert edge["requestsPerMinute"] is None
    assert edge["p95LatencyMs"] is None


# --- topics (functional map) -------------------------------------------------------------------


def test_topics_carry_consumers_producers_and_reserved_flag() -> None:
    c = _fleet()
    c.ingest_register({"service": "collector", "topics": [{"id": "benzene:mesh:query:fleet"}]})
    arts = build_artifacts(c, generated_at=_AT)
    by_topic = {t["topic"]: t for t in arts["topics"]["topics"]}

    reserve = by_topic["stock:reserve"]
    assert reserve["producers"] == [{"service": "inventory"}]
    assert reserve["consumers"] == [{"service": "orders"}]
    assert reserve["reserved"] is False
    assert reserve["responseSchema"] == _RESERVE_SCHEMA  # retained from the descriptor feed
    assert reserve["requestSchema"] is None  # not declared -> absent, not fabricated

    assert by_topic["benzene:mesh:query:fleet"]["reserved"] is True  # benzene:* is plumbing
    assert arts["topics"]["removedTopics"] == []


def test_dropped_topic_moves_to_removed_topics() -> None:
    c = MeshCollector()
    c.ingest_register(
        {"service": "orders", "topics": [{"id": "order:create"}], "descriptorHash": "h1"}
    )
    # Re-register dropping order:create for order:cancel — nobody provides order:create anymore.
    c.ingest_register(
        {"service": "orders", "topics": [{"id": "order:cancel"}], "descriptorHash": "h2"}
    )
    topics = build_artifacts(c, generated_at=_AT)["topics"]
    active = {t["topic"] for t in topics["topics"]}
    assert "order:cancel" in active
    assert "order:create" not in active  # dropped from the active functional map...
    assert topics["removedTopics"] == [
        "order:create"
    ]  # ...and surfaced as removed (deprecation view)


def test_topics_carry_schemas_version_and_schema_mismatch() -> None:
    arts = build_artifacts(_fleet(), generated_at=_AT)
    create = {t["topic"]: t for t in arts["topics"]["topics"]}["order:create"]
    assert create["requestSchema"] == _ORDER_SCHEMA
    assert create["version"] == "1"
    assert create["schemaMismatch"] is False


def test_schema_mismatch_when_two_providers_disagree() -> None:
    c = MeshCollector()
    c.ingest_register(
        {"service": "a", "topics": [{"id": "shared:topic", "responseSchema": _ORDER_SCHEMA}]}
    )
    c.ingest_register(
        {"service": "b", "topics": [{"id": "shared:topic", "responseSchema": _RESERVE_SCHEMA}]}
    )
    topic = {t["topic"]: t for t in build_artifacts(c, generated_at=_AT)["topics"]["topics"]}[
        "shared:topic"
    ]
    assert {p["service"] for p in topic["producers"]} == {"a", "b"}
    assert topic["schemaMismatch"] is True  # two providers, different contracts


# --- per-service ------------------------------------------------------------------------------


def test_service_document_reports_hash_drift_and_health() -> None:
    arts = build_artifacts(_fleet(), generated_at=_AT)
    inventory = arts["services"]["inventory"]
    assert inventory["name"] == "inventory"
    assert inventory["fetchedAtUtc"] == _AT
    assert inventory["specHash"] == "h-inv"
    assert inventory["previousSpecHash"] is None
    assert inventory["contractDrift"] is True
    assert inventory["health"]["isHealthy"] is False
    assert inventory["error"] is None


def test_service_document_carries_spec_and_per_check_health() -> None:
    orders = build_artifacts(_fleet(), generated_at=_AT)["services"]["orders"]
    # The reconstructed spec document names the topic + its retained schema.
    spec = json.loads(orders["specJson"])
    assert spec["service"] == "orders"
    assert spec["topics"][0]["id"] == "order:create"
    assert spec["topics"][0]["requestSchema"] == _ORDER_SCHEMA
    # Per-check health detail from the heartbeat surfaces on the service page.
    assert orders["health"]["healthChecks"] == {"db": {"status": "ok", "type": "sql"}}


def test_topic_schema_change_is_flagged_in_changes() -> None:
    c = MeshCollector()
    c.ingest_register(
        {"service": "orders", "topics": [{"id": "order:create", "requestSchema": _ORDER_SCHEMA}]}
    )
    by_topic = {t["topic"]: t for t in build_artifacts(c, generated_at=_AT)["topics"]["topics"]}
    assert by_topic["order:create"]["changes"] == []  # first registration — nothing changed yet
    # Re-register the same topic with a different schema -> schema-changed.
    c.ingest_register(
        {"service": "orders", "topics": [{"id": "order:create", "requestSchema": _RESERVE_SCHEMA}]}
    )
    change = {t["topic"]: t for t in build_artifacts(c, generated_at=_AT)["topics"]["topics"]}[
        "order:create"
    ]["changes"]
    assert len(change) == 1 and change[0]["kind"] == "schema-changed"
    assert "order:create" in change[0]["description"]


def test_previous_spec_hash_records_a_contract_change() -> None:
    c = MeshCollector()
    c.ingest_register(
        {"service": "orders", "topics": [{"id": "order:create"}], "descriptorHash": "h1"}
    )
    c.ingest_register(
        {"service": "orders", "topics": [{"id": "order:create"}], "descriptorHash": "h2"}
    )
    orders = build_artifacts(c, generated_at=_AT)["services"]["orders"]
    assert orders["specHash"] == "h2"
    assert orders["previousSpecHash"] == "h1"  # the hash before the change


def test_asyncapi_export_projects_the_domain_topics() -> None:
    doc = build_artifacts(_fleet(), generated_at=_AT)["asyncapi"]
    assert doc["asyncapi"] == "3.0.0"
    # order:create is a domain channel with its schema as the message payload...
    assert "order_create" in doc["channels"]
    assert doc["channels"]["order_create"]["address"] == "order:create"
    assert doc["components"]["messages"]["order_createMessage"]["payload"] == _ORDER_SCHEMA
    # ...and benzene:* plumbing is excluded from the domain export.
    assert not any(c.startswith("benzene") for c in doc["channels"])
    # stock:reserve has a provider (receive) and a consumer (send) operation.
    assert doc["operations"]["stock_reserve_receive"]["action"] == "receive"
    assert doc["operations"]["stock_reserve_send"]["action"] == "send"


def test_annotations_is_an_honest_empty_read_model() -> None:
    doc = build_artifacts(_fleet(), generated_at=_AT)["annotations"]
    assert doc == {"generatedAtUtc": _AT, "annotations": []}


def test_usage_counts_come_from_the_traces() -> None:
    usage = build_artifacts(_fleet(), generated_at=_AT)["usage"]
    assert usage["generatedAtUtc"] == _AT
    assert usage["windowStartUtc"] is None  # no time window in the trace catalog -> degraded
    by_key = {(e["topic"], e["service"], e["status"]): e for e in usage["entries"]}
    reserve = by_key[("stock:reserve", "inventory", "service-unavailable")]
    assert reserve["count"] == 1
    assert reserve["source"] == "collector"
    assert reserve["transport"] is None  # trace catalog has no transport -> degraded


# --- write to disk ----------------------------------------------------------------------------


def test_write_artifacts_lays_out_the_files_the_ui_fetches(tmp_path: Path) -> None:
    write_artifacts(tmp_path, _fleet(), sources=_sources(), generated_at=_AT)
    assert (tmp_path / "manifest.json").exists()
    assert (tmp_path / "topology.json").exists()
    assert (tmp_path / "topics.json").exists()
    assert (tmp_path / "usage.json").exists()
    assert (tmp_path / "asyncapi.json").exists()
    assert (tmp_path / "annotations.json").exists()
    # One services/{name}.json per service, each valid JSON with the contract's top-level name.
    for name in ("orders", "inventory", "notifications"):
        doc = json.loads((tmp_path / "services" / f"{name}.json").read_text())
        assert doc["name"] == name
    # No stray temp siblings from the atomic writes.
    assert not list(tmp_path.glob(".*.tmp"))
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert {s["name"] for s in manifest["services"]} == {"orders", "inventory", "notifications"}


# --- pull path: a polled fleet enriches the artifacts (schemas + per-check health) --------------


def test_polled_fleet_enriches_topics_and_service_health() -> None:
    # A pull source exposes /benzene/spec (schemas) and /benzene/health (per-check detail); the
    # poller folds both into the collector, so the artifacts carry them just like the push path.
    async def spec() -> dict:
        return {
            "service": "inventory",
            "topics": [{"id": "stock:reserve", "requestSchema": _RESERVE_SCHEMA}],
        }

    async def health() -> dict:
        return {"isHealthy": True, "healthChecks": {"db": {"status": "ok", "type": "sql"}}}

    collector = MeshCollector()
    poller = MeshPoller(collector, [CallableServiceSource("inventory", spec=spec, health=health)])
    asyncio.run(poller.poll_once())

    arts = build_artifacts(collector, generated_at=_AT)
    topic = {t["topic"]: t for t in arts["topics"]["topics"]}["stock:reserve"]
    assert topic["requestSchema"] == _RESERVE_SCHEMA  # schema pulled from /benzene/spec
    service = arts["services"]["inventory"]
    assert service["health"]["healthChecks"] == {"db": {"status": "ok", "type": "sql"}}
    assert service["health"]["isHealthy"] is True


def test_anonymous_service_known_only_via_traces_degrades_cleanly() -> None:
    # A service the collector only ever saw as a trace emitter (never registered) has no descriptor:
    # its per-service doc carries no spec/hash, and the estate lists it as unreachable.
    c = MeshCollector()
    c.ingest_traces(
        {
            "events": [
                {"traceId": "t", "spanId": "s", "service": "ghost", "topic": "x:y", "status": "ok"}
            ]
        }
    )
    arts = build_artifacts(c, generated_at=_AT)
    ghost = arts["services"]["ghost"]
    assert ghost["specJson"] is None  # no descriptor -> no reconstructed spec
    assert ghost["specHash"] is None
    assert ghost["health"]["healthChecks"] == {}
    status = {s["name"]: s["status"] for s in arts["manifest"]["services"]}
    assert status["ghost"] == "unreachable"


def test_a_failed_write_leaves_no_partial_or_orphan_temp_file(tmp_path: Path) -> None:
    # Atomic write: the artifact only appears via os.replace once fully written. If the write fails
    # mid-flight, the temp file is cleaned up and the target never appears half-written — so the UI
    # (or a reader) never fetches a truncated artifact. A set is not JSON-serialisable, so json.dump
    # raises inside the write and exercises the cleanup path.
    from benzene.mesh.artifacts import _write_json

    target = tmp_path / "manifest.json"
    with pytest.raises(TypeError):
        _write_json(target, {"bad": {1, 2, 3}})

    assert not target.exists()  # no half-written artifact
    assert list(tmp_path.iterdir()) == []  # and no orphaned .tmp file left behind


def test_topology_never_draws_a_service_calling_itself() -> None:
    # Two services provide the same topic (alpha, beta both serve shared:work); alpha also *calls* it,
    # load-balanced to beta. alpha is then both a provider and a consumer of the topic — the edge
    # builder must skip the alpha->alpha self-loop and keep only alpha->beta.
    c = MeshCollector()
    c.ingest_register(
        {"service": "alpha", "topics": [{"id": "shared:work"}], "descriptorHash": "a"}
    )
    c.ingest_register({"service": "beta", "topics": [{"id": "shared:work"}], "descriptorHash": "b"})
    c.ingest_traces(
        {
            "events": [
                # alpha's calling span, then beta handling shared:work as its child.
                {
                    "traceId": "t",
                    "spanId": "sa",
                    "service": "alpha",
                    "topic": "a:enter",
                    "status": "ok",
                },
                {
                    "traceId": "t",
                    "spanId": "sb",
                    "parentSpanId": "sa",
                    "service": "beta",
                    "topic": "shared:work",
                    "status": "ok",
                },
            ]
        }
    )
    edges = {
        (e["client"], e["server"])
        for e in build_artifacts(c, generated_at=_AT)["topology"]["edges"]
    }
    assert ("alpha", "alpha") not in edges  # the self-loop is skipped
    assert ("alpha", "beta") in edges
