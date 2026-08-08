"""Project a :class:`MeshCollector` catalog into the mesh-ui artifact set.

The language-neutral **Benzene Mesh UI** (``mesh-ui.html``, one canonical page across every port) is
data-driven from a fixed set of static JSON artifacts an aggregator publishes — ``manifest.json``,
``services/{name}.json``, ``topics.json``, ``topology.json``, ``usage.json`` (``annotations.json`` needs
a write-plane this static floor doesn't have). Their shapes are the cross-language read-model contract
documented in the main repo's ``docs/guides/mesh-ui.md`` and pinned by the ``website/demos/mesh/``
fixtures. This module is the Python aggregator's projection into that contract.

The UI **must not invent fields and degrades gracefully when any is absent** (mesh.md §6), so this
emits exactly what the collector's catalog knows. From the descriptor/spec feed it derives the estate
(names, health, contract-drift + spec-hash history), the functional map (topics with consumers/producers,
**request/response schemas, version, and schema-mismatch**), and per-service **spec + per-check health**;
from the trace feed, the topology (who calls whom) and **usage** (exercise counts per topic/service/
status). What genuinely needs feeds this collector doesn't have — latency/rate metrics, a usage time
window, transports, and annotations — stays ``null``/empty rather than being fabricated.

Pure and transport-neutral: :func:`build_artifacts` returns plain dicts (inject ``generated_at`` for a
deterministic result); :func:`write_artifacts` lays them out on disk for the UI to fetch by relative
path. See ``deploy/mesh`` for the wiring that serves them.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Protocol

from .collector import MeshCollector

# The collector's health aggregate → the manifest status vocabulary the UI counts (healthy /
# unhealthy / unreachable, per the reference fixtures). ``degraded`` (some instances unhealthy)
# belongs in the unhealthy worklist; ``unknown`` (no heartbeats reached us) reads as unreachable.
_HEALTH_TO_STATUS = {
    "healthy": "healthy",
    "degraded": "unhealthy",
    "unhealthy": "unhealthy",
    "unknown": "unreachable",
}


class ServiceEndpoint(Protocol):
    """A pull source that can name a service's spec/health URLs (e.g. ``HttpServiceSource``)."""

    name: str

    @property
    def spec_url(self) -> str: ...

    @property
    def health_url(self) -> str: ...


def _is_reserved(topic: str) -> bool:
    """Benzene's own plumbing topics (``benzene:*``) — hidden by default in the UI."""
    return topic.startswith("benzene:")


def _endpoints(sources: Iterable[ServiceEndpoint]) -> dict[str, tuple[str, str]]:
    endpoints: dict[str, tuple[str, str]] = {}
    for source in sources:
        name = getattr(source, "name", None)
        spec_url = getattr(source, "spec_url", None)
        health_url = getattr(source, "health_url", None)
        if name and spec_url and health_url:
            endpoints[name] = (spec_url, health_url)
    return endpoints


def build_artifacts(
    collector: MeshCollector,
    *,
    sources: Iterable[ServiceEndpoint] = (),
    generated_at: str,
) -> dict[str, Any]:
    """Project the catalog into the mesh-ui artifacts.

    Returns ``{"manifest", "topology", "topics", "services": {name: doc}}`` — the ``manifest`` /
    ``topology`` / ``topics`` documents plus one per-service document. ``generated_at`` is stamped as
    each artifact's ``generatedAtUtc`` / ``fetchedAtUtc`` (inject it for a deterministic result).
    ``sources`` supplies each service's ``specUrl`` / ``healthUrl`` for the manifest links.
    """
    fleet = collector.query_fleet({})
    snapshot = collector.snapshot()
    services = {entry["name"]: entry for entry in snapshot["services"]}
    endpoints = _endpoints(sources)
    names = [entry["service"] for entry in fleet["services"]]
    return {
        "manifest": _manifest(collector, fleet, endpoints, generated_at),
        "topology": _topology(collector, fleet, generated_at),
        "topics": _topics(collector, fleet, services, generated_at),
        "services": {name: _service(collector, name, services, generated_at) for name in names},
        "usage": _usage(snapshot, generated_at),
    }


def _drifted(service_detail: dict[str, Any]) -> bool:
    # Any live instance whose descriptor hash differs from the registered one is contract drift.
    return any(not inst["hashMatches"] for inst in service_detail.get("instances", []))


def _manifest(
    collector: MeshCollector,
    fleet: dict[str, Any],
    endpoints: dict[str, tuple[str, str]],
    generated_at: str,
) -> dict[str, Any]:
    services = []
    for entry in fleet["services"]:
        name = entry["service"]
        spec_url, health_url = endpoints.get(name, (None, None))
        services.append(
            {
                "name": name,
                "status": _HEALTH_TO_STATUS.get(entry["health"], "unreachable"),
                "contractDrift": _drifted(collector.query_service({"service": name})),
                "specUrl": spec_url,
                "healthUrl": health_url,
            }
        )
    return {"generatedAtUtc": generated_at, "services": services}


def _topology(collector: MeshCollector, fleet: dict[str, Any], generated_at: str) -> dict[str, Any]:
    # A consumer of a topic is a client of that topic's provider(s). Aggregate invocations/errors
    # across the topics a pair shares into one client -> server edge (rate/latency need a metrics
    # feed this collector doesn't have, so they stay null — the UI renders them as reduced).
    pairs: dict[tuple[str, str], dict[str, int]] = {}
    for topic_entry in fleet["topics"]:
        detail = collector.query_topic({"topic": topic_entry["topic"]})
        providers = detail.get("providers", [])
        consumers = detail.get("consumers", [])
        invocations = detail.get("invocations", 0)
        errors = detail.get("errors", 0)
        for client in consumers:
            for server in providers:
                if client == server:
                    continue
                acc = pairs.setdefault((client, server), {"invocations": 0, "errors": 0})
                acc["invocations"] += invocations
                acc["errors"] += errors
    edges = [
        {
            "client": client,
            "server": server,
            "source": "collector",
            "requestsPerMinute": None,
            "errorRate": (acc["errors"] / acc["invocations"]) if acc["invocations"] else None,
            "p50LatencyMs": None,
            "p95LatencyMs": None,
            "p99LatencyMs": None,
        }
        for (client, server), acc in sorted(pairs.items())
    ]
    return {"generatedAtUtc": generated_at, "edges": edges}


def _topics(
    collector: MeshCollector,
    fleet: dict[str, Any],
    services: dict[str, dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    topics = []
    for topic_entry in fleet["topics"]:
        topic = topic_entry["topic"]
        detail = collector.query_topic({"topic": topic})
        providers = detail.get("providers", [])
        # Each provider's retained contract for this topic (from its descriptor/spec feed).
        provider_specs = [services.get(p, {}).get("topicSpecs", {}).get(topic) for p in providers]
        present = [spec for spec in provider_specs if spec]
        representative = present[0] if present else {}
        # Two providers of one topic declaring different contracts is a real mismatch signal.
        schema_mismatch = any(spec != representative for spec in present[1:])
        topics.append(
            {
                "topic": topic,
                "version": representative.get("version", ""),
                "reserved": _is_reserved(topic),
                "consumers": [{"service": c} for c in detail.get("consumers", [])],
                "producers": [{"service": p} for p in providers],
                "status": None,
                "requestSchema": representative.get("requestSchema"),
                "responseSchema": representative.get("responseSchema"),
                "messageSchema": representative.get("messageSchema"),
                "schemaMismatch": schema_mismatch,
                "changes": [],
            }
        )
    return {"generatedAtUtc": generated_at, "topics": topics, "removedTopics": []}


def _service(
    collector: MeshCollector,
    name: str,
    services: dict[str, dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    detail = collector.query_service({"service": name})
    entry = services.get(name, {})
    return {
        "name": name,
        "fetchedAtUtc": generated_at,
        "specJson": _spec_json(name, entry),
        "specHash": entry.get("descriptorHash"),
        "previousSpecHash": entry.get("previousDescriptorHash"),
        "contractDrift": _drifted(detail),
        "health": _service_health(detail, entry),
        "error": None,
    }


def _spec_json(name: str, entry: dict[str, Any]) -> str | None:
    """Reconstruct the service's spec document (as a JSON string) from its retained topic contracts."""
    if not entry.get("hasDescriptor"):
        return None
    specs = entry.get("topicSpecs", {})
    topics = []
    for topic_id in sorted(entry.get("provided", [])):
        spec = specs.get(topic_id, {})
        topics.append({"id": topic_id, **spec})
    return json.dumps({"service": name, "topics": topics}, ensure_ascii=False)


def _service_health(detail: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
    # The aggregate isHealthy (from the query read model) plus the per-check detail retained from the
    # heartbeats — merged across instances, latest-wins, so the service page can render each check.
    checks: dict[str, Any] = {}
    for instance in entry.get("instances", []):
        checks.update(instance.get("healthChecks", {}))
    return {"isHealthy": detail.get("health") == "healthy", "healthChecks": checks}


def _usage(snapshot: dict[str, Any], generated_at: str) -> dict[str, Any]:
    """Derive usage from the observed traces: exercise counts per (topic, service, status).

    The pull+trace catalog has no time window, transport, or durations, so those fields are null —
    but the counts are enough for the UI's usage view and its zero-traffic deprecation signal.
    """
    counts: Counter[tuple[str, str, str]] = Counter()
    for event in snapshot.get("events", []):
        counts[(event["topic"], event["service"], event["status"])] += 1
    entries = [
        {
            "topic": topic,
            "version": None,
            "service": service,
            "transport": None,
            "status": status,
            "count": count,
            "avgDurationMs": None,
            "source": "collector",
        }
        for (topic, service, status), count in sorted(counts.items())
    ]
    return {
        "generatedAtUtc": generated_at,
        "windowStartUtc": None,
        "windowEndUtc": None,
        "entries": entries,
    }


def write_artifacts(
    directory: str | os.PathLike[str],
    collector: MeshCollector,
    *,
    sources: Iterable[ServiceEndpoint] = (),
    generated_at: str,
) -> dict[str, Any]:
    """Build the artifacts and lay them out under ``directory`` for the UI to fetch by relative path.

    Writes ``manifest.json`` / ``topology.json`` / ``topics.json`` at the root and one
    ``services/{name}.json`` per service. Each file is written atomically (temp + ``os.replace``) so a
    reader — or the UI mid-fetch — never sees a half-written artifact. Returns the built artifacts.
    """
    artifacts = build_artifacts(collector, sources=sources, generated_at=generated_at)
    root = Path(directory)
    _write_json(root / "manifest.json", artifacts["manifest"])
    _write_json(root / "topology.json", artifacts["topology"])
    _write_json(root / "topics.json", artifacts["topics"])
    _write_json(root / "usage.json", artifacts["usage"])
    services_dir = root / "services"
    for name, document in artifacts["services"].items():
        _write_json(services_dir / f"{name}.json", document)
    return artifacts


def _write_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(document, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_name)
        raise
