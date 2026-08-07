"""The collector persistence seam: the store contract, the JSON-file store, and durable round-trips."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from benzene.mesh import (
    CollectorStore,
    JsonFileCollectorStore,
    MeshCollector,
    NullCollectorStore,
)

# --- the store implementations ---------------------------------------------------------------


def test_null_store_keeps_nothing() -> None:
    store = NullCollectorStore()
    store.save({"anything": 1})
    assert store.load() is None
    assert isinstance(store, CollectorStore)  # satisfies the runtime-checkable protocol


def test_json_file_store_round_trips(tmp_path: Path) -> None:
    store = JsonFileCollectorStore(tmp_path / "state.json")
    assert store.load() is None  # nothing saved yet is a first boot, not an error
    store.save({"version": 1, "services": ["orders"]})
    assert store.load() == {"version": 1, "services": ["orders"]}
    assert isinstance(store, CollectorStore)


def test_json_file_store_creates_missing_parent_dirs(tmp_path: Path) -> None:
    store = JsonFileCollectorStore(tmp_path / "nested" / "deeper" / "state.json")
    store.save({"version": 1})
    assert store.load() == {"version": 1}


def test_json_file_store_replaces_previous_snapshot(tmp_path: Path) -> None:
    store = JsonFileCollectorStore(tmp_path / "state.json")
    store.save({"version": 1, "n": 1})
    store.save({"version": 1, "n": 2})
    assert store.load() == {"version": 1, "n": 2}


def test_json_file_store_writes_atomically_leaving_no_temp_files(tmp_path: Path) -> None:
    store = JsonFileCollectorStore(tmp_path / "state.json")
    store.save({"version": 1})
    # The atomic write must leave exactly the target file behind — no stray temp siblings.
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]


def test_json_file_store_tolerates_a_corrupt_file(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text("{ this is not json", encoding="utf-8")
    # A truncated/corrupt snapshot must not crash the host — treat it as a first boot.
    assert JsonFileCollectorStore(path).load() is None


def test_json_file_store_ignores_a_non_object_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    assert JsonFileCollectorStore(path).load() is None


# --- the collector wired to a store ----------------------------------------------------------


def test_collector_snapshot_round_trips_the_full_catalog() -> None:
    source = MeshCollector()
    source.ingest_register(
        {"service": "orders", "topics": [{"id": "order:create"}], "descriptorHash": "h1"}
    )
    source.ingest_heartbeat(
        {
            "service": "orders",
            "instanceId": "i1",
            "health": {"isHealthy": True},
            "descriptorHash": "h1",
        }
    )
    source.ingest_traces(
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
    source.ingest_issues(
        {
            "service": "orders",
            "issues": [
                {"fingerprint": "f1", "topic": "order:create", "status": "bad-request", "count": 2}
            ],
        }
    )

    restored = MeshCollector()
    restored.restore(source.snapshot())

    # Every query read model comes back identical after a snapshot/restore.
    assert restored.query_fleet({}) == source.query_fleet({})
    assert restored.query_service({"service": "orders"}) == source.query_service(
        {"service": "orders"}
    )
    # Consumer edges (derived from the rebuilt span-owner index) survive the round-trip.
    assert restored.query_topic({"topic": "stock:reserve"})["consumers"] == ["orders"]


def test_collector_snapshot_is_json_serializable() -> None:
    c = MeshCollector()
    c.ingest_register({"service": "orders", "topics": [{"id": "order:create"}]})
    c.ingest_traces(
        {
            "events": [
                {
                    "traceId": "t",
                    "spanId": "s",
                    "service": "orders",
                    "topic": "order:create",
                    "status": "ok",
                }
            ]
        }
    )
    json.dumps(c.snapshot())  # must not raise — a store moves this dict to durable bytes


def test_collector_rehydrates_from_its_store_on_construction(tmp_path: Path) -> None:
    store = JsonFileCollectorStore(tmp_path / "state.json")
    first = MeshCollector(store=store)
    first.ingest_register({"service": "orders", "topics": [{"id": "order:create"}]})

    # A brand-new collector pointed at the same store comes up already knowing the fleet.
    second = MeshCollector(store=store)
    assert "orders" in {s["service"] for s in second.query_fleet({})["services"]}


def test_collector_persists_after_every_mutating_ingest(tmp_path: Path) -> None:
    store = JsonFileCollectorStore(tmp_path / "state.json")
    c = MeshCollector(store=store)
    # All four mutating ingests must write through the store, not just register/heartbeat.
    c.ingest_register({"service": "orders", "topics": [{"id": "order:create"}]})
    c.ingest_heartbeat({"service": "orders", "instanceId": "i1", "health": {"isHealthy": True}})
    c.ingest_traces(
        {"events": [{"traceId": "t", "spanId": "s", "service": "orders", "topic": "order:create"}]}
    )
    c.ingest_issues({"service": "orders", "issues": [{"fingerprint": "f1", "count": 1}]})
    # A fresh collector on the same store sees every feed reflected — proof all four saved.
    reloaded = MeshCollector(store=store)
    saved = store.load()
    assert saved is not None
    assert {s["name"] for s in saved["services"]} == {"orders"}
    assert saved["services"][0]["instances"][0]["instanceId"] == "i1"
    assert saved["events"] and saved["events"][0]["spanId"] == "s"
    assert "f1" in saved["issues"]
    assert reloaded.query_fleet({})["issues"][0]["fingerprint"] == "f1"


def test_restore_ignores_an_incompatible_snapshot_version() -> None:
    c = MeshCollector()
    c.ingest_register({"service": "orders", "topics": [{"id": "order:create"}]})
    c.restore({"version": 999, "services": [{"name": "ghost", "provided": []}]})
    # A future/foreign snapshot is ignored, leaving the existing catalog untouched.
    assert "orders" in {s["service"] for s in c.query_fleet({})["services"]}
    assert "ghost" not in {s["service"] for s in c.query_fleet({})["services"]}


# --- snapshot isolation (a snapshot is a detached copy, not a live view) ----------------------


def test_snapshot_is_isolated_from_later_mutation() -> None:
    c = MeshCollector()
    c.ingest_issues({"service": "o", "issues": [{"fingerprint": "f1", "count": 1}]})
    snap = c.snapshot()
    # A later ingest that merges the same fingerprint must not reach back into a handed-out snapshot.
    c.ingest_issues({"service": "o", "issues": [{"fingerprint": "f1", "count": 5}]})
    assert snap["issues"]["f1"]["count"] == 1


def test_restore_does_not_share_issue_state_with_the_source() -> None:
    source = MeshCollector()
    source.ingest_issues({"service": "o", "issues": [{"fingerprint": "g", "count": 1}]})
    restored = MeshCollector()
    restored.restore(source.snapshot())
    # Mutating the restored collector must not corrupt the source's issue records (no shared dicts).
    restored.ingest_issues({"service": "o", "issues": [{"fingerprint": "g", "count": 9}]})
    assert source.query_fleet({})["issues"][0]["count"] == 1


def test_json_file_store_leaves_no_partial_file_when_save_fails(tmp_path: Path) -> None:
    store = JsonFileCollectorStore(tmp_path / "state.json")
    store.save({"version": 1, "ok": True})  # a good prior snapshot
    with pytest.raises(TypeError):
        store.save({"bad": object()})  # json.dump raises mid-write
    # The atomic write must clean up its temp sibling and never replace the good file with a partial.
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]
    assert store.load() == {"version": 1, "ok": True}
