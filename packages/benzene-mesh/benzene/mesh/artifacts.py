"""Project a :class:`MeshCollector` catalog into the mesh-ui artifact set.

The language-neutral **Benzene Mesh UI** (``mesh-ui.html``, one canonical page across every port) is
data-driven from a fixed set of static JSON artifacts an aggregator publishes — ``manifest.json``,
``services/{name}.json``, ``topics.json``, ``topology.json`` (plus ``usage.json`` / ``annotations.json``
that need feeds this collector doesn't have). Their shapes are the cross-language read-model contract
documented in the main repo's ``docs/guides/mesh-ui.md`` and pinned by the ``website/demos/mesh/``
fixtures. This module is the Python aggregator's projection into that contract.

The UI **must not invent fields and degrades gracefully when any is absent** (mesh.md §6), so this
emits exactly what the collector's catalog knows and leaves the rest ``null``/empty rather than
fabricating it: payload schemas, per-check health detail, spec history, and latency/rate metrics are
not in the pull+trace catalog, so those fields render as reduced. What *is* derivable — the estate
(names, health, contract-drift), the functional map (topics with consumers/producers), the topology
(who calls whom, from trace parentage), and per-service health/drift — is emitted in full.

Pure and transport-neutral: :func:`build_artifacts` returns plain dicts (inject ``generated_at`` for a
deterministic result); :func:`write_artifacts` lays them out on disk for the UI to fetch by relative
path. See ``deploy/mesh`` for the wiring that serves them.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
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
    hashes = {s["name"]: s.get("descriptorHash") for s in collector.snapshot()["services"]}
    endpoints = _endpoints(sources)
    names = [entry["service"] for entry in fleet["services"]]
    return {
        "manifest": _manifest(collector, fleet, endpoints, generated_at),
        "topology": _topology(collector, fleet, generated_at),
        "topics": _topics(collector, fleet, generated_at),
        "services": {name: _service(collector, name, hashes, generated_at) for name in names},
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


def _topics(collector: MeshCollector, fleet: dict[str, Any], generated_at: str) -> dict[str, Any]:
    topics = []
    for topic_entry in fleet["topics"]:
        topic = topic_entry["topic"]
        detail = collector.query_topic({"topic": topic})
        topics.append(
            {
                "topic": topic,
                "version": "",
                "reserved": _is_reserved(topic),
                "consumers": [{"service": c} for c in detail.get("consumers", [])],
                "producers": [{"service": p} for p in detail.get("providers", [])],
                "status": None,
                # Payload schemas aren't retained in the pull+trace catalog (the descriptor's
                # per-topic schemas are not stored), so these degrade rather than being invented.
                "requestSchema": None,
                "responseSchema": None,
                "messageSchema": None,
                "schemaMismatch": False,
                "changes": [],
            }
        )
    return {"generatedAtUtc": generated_at, "topics": topics, "removedTopics": []}


def _service(
    collector: MeshCollector,
    name: str,
    hashes: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    detail = collector.query_service({"service": name})
    return {
        "name": name,
        "fetchedAtUtc": generated_at,
        "specJson": None,  # the collector retains the hash, not the spec document text
        "specHash": hashes.get(name),
        "previousSpecHash": None,  # no per-service hash history in the catalog
        "contractDrift": _drifted(detail),
        "health": {"isHealthy": detail.get("health") == "healthy", "healthChecks": {}},
        "error": None,
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
