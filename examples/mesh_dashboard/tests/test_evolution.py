"""The mesh's contract-evolution signals, end to end: a mid-rollout fleet → the governance views.

Each assertion pins one signal the mesh-ui highlights — schema-changed, removedTopics, contractDrift,
schemaMismatch — proving the demo actually reproduces it through the public ingest API (not by hand-
setting artifact fields), so the example keeps demonstrating a real capability of the collector.
"""

from __future__ import annotations

import json
from pathlib import Path

from mesh_dashboard.evolution import build_evolution_artifacts, write_evolution_artifacts


def test_schema_changed_is_flagged_on_the_redeployed_topic() -> None:
    artifacts = build_evolution_artifacts()
    topics = {t["topic"]: t for t in artifacts["topics"]["topics"]}

    place = topics["orders:place"]
    kinds = {c["kind"] for c in place["changes"]}
    assert "schema-changed" in kinds
    # The added coupon field shows in the current request contract.
    assert "coupon" in place["requestSchema"]["properties"]


def test_retired_topic_moves_to_removed() -> None:
    artifacts = build_evolution_artifacts()
    active = {t["topic"] for t in artifacts["topics"]["topics"]}
    assert artifacts["topics"]["removedTopics"] == ["orders:cancel"]
    assert "orders:cancel" not in active  # dropped from the active functional map


def test_straggler_instance_marks_the_service_drifted() -> None:
    artifacts = build_evolution_artifacts()
    drift = {s["name"]: s["contractDrift"] for s in artifacts["manifest"]["services"]}
    # orders-1 still runs the old descriptor; orders-2 the new one — the mismatch is drift.
    assert drift["orders"] is True
    assert drift["inventory"] is False

    orders = artifacts["services"]["orders"]
    assert orders["contractDrift"] is True
    # The redeploy left a prior spec hash to compare against.
    assert orders["specHash"] != orders["previousSpecHash"]
    assert orders["previousSpecHash"] is not None


def test_divergent_consumers_flag_a_schema_mismatch() -> None:
    artifacts = build_evolution_artifacts()
    topics = {t["topic"]: t for t in artifacts["topics"]["topics"]}

    reserve = topics["inventory:reserve"]
    assert reserve["schemaMismatch"] is True
    consumers = {c["service"] for c in reserve["consumers"]}
    assert consumers == {"inventory", "inventory-legacy"}


def test_write_evolution_artifacts_round_trips_to_disk(tmp_path: Path) -> None:
    artifacts = write_evolution_artifacts(tmp_path)
    assert (tmp_path / "topics.json").is_file()
    on_disk = json.loads((tmp_path / "topics.json").read_text())
    assert on_disk["removedTopics"] == artifacts["topics"]["removedTopics"]
