"""The mesh **artifact emitter** — project the fleet into the mesh-UI catalog artifacts (mesh.md §9).

The :doc:`mesh UI </guides/mesh-ui>` is a single, language-neutral web page that renders a Benzene
estate from a fixed set of static JSON artifacts, resolved by *relative path*. It never knows which
port produced them; the shapes are pinned by ``Benzene.Mesh.Contracts`` and are byte-compatible
across ports. This module is the Python **aggregator**: it derives those artifacts from two sources
and writes them to a directory the UI can be served next to.

The two data planes (mesh-ui guide §2):

1. **The catalog spine** — each service's derived spec (``/benzene/spec`` →
   :class:`~benzene.core.ServiceSpec`) and health aggregate (``/benzene/health`` →
   ``{isHealthy, healthChecks}``). This is the structural map: topics, schemas, HTTP mappings, and
   per-service health/drift. A service the aggregator can't reach contributes a spine entry with
   ``error`` set (its live feeds may still describe it — see below).
2. **Live enrichment from the collector** — a :class:`~benzene.mesh.MeshCollector` answers
   ``query_fleet`` / ``query_topic``. From what services *report* (register / heartbeat / traces) it
   derives providers, **consumer edges from trace parentage**, and per-topic invocation/status
   counts. This is what fills in *who calls whom* (topology) and *how much* (usage) — signals no
   single service can know about itself.

The six artifacts and where each field comes from:

======================  ==============================================================================
Artifact                Derivation
======================  ==============================================================================
``manifest.json``       one entry per spine service — ``status`` (health), ``contractDrift`` (spec hash)
``services/<n>.json``   the spine snapshot: ``specJson`` + ``specHash`` + ``health`` + ``error``
``topics.json``         union of the fleet's topics; **consumers** = handlers (spec + collector
                        providers) with HTTP mappings; **producers** = collector trace-parent callers
``topology.json``       ``client → server`` edges from the collector's trace-parentage (``source`` =
                        ``"collector"``, latency/RPM ``null``); an optional external **topology source**
                        (X-Ray) overlays richer edges (real RPM + p50/p95/p99 + error rate) per pair
``usage.json``          per-topic invocation/status counts from the collector (``source`` =
                        ``"collector"``); an optional external **usage source** (CloudWatch) supplies
                        real per-(topic, transport, status) counts + ``avgDurationMs`` per topic it covers
``annotations.json``    human discussion threads — not derived; emitted empty unless seeded
======================  ==============================================================================

Everything here is pure and deterministic given its inputs (pass ``generated_at`` in), so the
:meth:`~MeshArtifactEmitter.build_manifest` / ``build_topics`` / … projections are unit-testable
against the reference fixtures without touching the filesystem; :meth:`~MeshArtifactEmitter.emit`
is the thin I/O wrapper that writes the six files.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


def spec_hash(spec_json: str) -> str:
    """Hash a spec document for drift detection (``Benzene.Mesh.Contracts.MeshHashing``).

    HMAC-SHA256 with an empty key over the UTF-8 spec text, lowercase hex. This is the aggregator's
    *artifact-level* spec hash (distinct from the mesh :class:`~benzene.mesh.ServiceDescriptor`'s
    ``descriptorHash``), matched to the .NET aggregator so a spec hashed by any port compares equal.
    """
    digest = hmac.new(b"", spec_json.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest


def _iso(moment: datetime) -> str:
    """An ISO-8601 UTC string with a ``Z`` suffix — the timestamp form the .NET aggregator emits."""
    return moment.isoformat().replace("+00:00", "Z")


def _schema_or_none(schema: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """A payload schema, or ``None`` when it is absent/open (``{}``) — the fixtures null an absent schema."""
    return dict(schema) if schema else None


def _canonical(value: Any) -> str:
    """A key-order-independent serialization, for comparing two consumers' schemas (schema-mismatch)."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class HttpMapping:
    """One HTTP ``(method, path)`` a service exposes for a topic — the ``consumers[].httpMappings`` shape."""

    method: str
    path: str

    def to_payload(self) -> dict[str, str]:
        return {"method": self.method, "path": self.path}


@dataclass(frozen=True)
class TopologyEdge:
    """One enriched ``client → server`` edge an external **topology source** contributes.

    The ``topology.json`` edge shape, but sourced from an observability backend (X-Ray) rather than the
    collector's trace-parentage — so it carries real timing the collector plane can't: a call rate
    (:attr:`requests_per_minute`), an error ratio (:attr:`error_rate`), and latency percentiles. Its
    :attr:`source` tag (e.g. ``"xray"``) rides with it so a merged ``topology.json`` keeps attribution,
    mirroring .NET's ``Benzene.Mesh.Contracts.TopologyEdge``. Any metric the source can't supply is
    ``None`` (rendered ``–`` by the mesh UI) — never guessed.
    """

    client: str
    server: str
    source: str
    requests_per_minute: float | None = None
    error_rate: float | None = None
    p50_latency_ms: float | None = None
    p95_latency_ms: float | None = None
    p99_latency_ms: float | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "client": self.client,
            "server": self.server,
            "source": self.source,
            "requestsPerMinute": self.requests_per_minute,
            "errorRate": self.error_rate,
            "p50LatencyMs": self.p50_latency_ms,
            "p95LatencyMs": self.p95_latency_ms,
            "p99LatencyMs": self.p99_latency_ms,
        }


@dataclass(frozen=True)
class UsageEntry:
    """One enriched usage count an external **usage source** contributes (the ``usage.json`` entry shape).

    A count at the finest dimension combination the backend (CloudWatch) can supply — split by
    :attr:`transport` and :attr:`status`, optionally with a mean :attr:`avg_duration_ms` — carrying its
    own :attr:`source` tag (e.g. ``"cloudwatch"``) so a merged feed keeps attribution. A ``None``
    dimension means the backend genuinely lacks it (not "all"), matching .NET's
    ``Benzene.Mesh.Contracts.MeshUsageEntry``.
    """

    topic: str
    count: int
    source: str
    version: str | None = None
    service: str | None = None
    transport: str | None = None
    status: str | None = None
    avg_duration_ms: float | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "version": self.version,
            "service": self.service,
            "transport": self.transport,
            "status": self.status,
            "count": self.count,
            "avgDurationMs": self.avg_duration_ms,
            "source": self.source,
        }


class TopologySource(Protocol):
    """An external source of enriched topology edges (X-Ray) the emitter overlays on the collector plane."""

    def topology(self) -> Sequence[TopologyEdge]: ...


class UsageSource(Protocol):
    """An external source of enriched usage entries (CloudWatch) the emitter overlays on the collector plane."""

    def usage(self) -> Sequence[UsageEntry]: ...


@dataclass(frozen=True)
class ServiceCatalog:
    """One service's **catalog-spine** input: what the aggregator fetched from it.

    ``spec`` is the ``/benzene/spec`` document (a :meth:`~benzene.core.ServiceSpec.to_payload`
    ``dict``) and ``health`` the ``/benzene/health`` aggregate (``{isHealthy, healthChecks}``); either
    is ``None`` when the aggregator could not reach the service, in which case ``error`` explains why.
    ``http_mappings`` carries the routes the ServiceSpec itself does not (the derived spec is
    transport-neutral) — a ``topic id → [HttpMapping]`` map the caller reads off its HTTP router.
    ``previous_spec_hash`` is last run's ``specHash`` for this service (drift is hash-vs-previous-hash).
    """

    name: str
    spec: Mapping[str, Any] | None
    health: Mapping[str, Any] | None
    spec_url: str
    health_url: str
    previous_spec_hash: str | None = None
    error: str | None = None
    http_mappings: Mapping[str, Sequence[HttpMapping]] = field(default_factory=dict)

    @property
    def spec_json(self) -> str | None:
        """The spec serialized to the exact text that is stored and hashed, or ``None`` if unreachable."""
        if self.spec is None:
            return None
        return json.dumps(self.spec, separators=(",", ":"), sort_keys=True)

    @property
    def spec_hash(self) -> str | None:
        text = self.spec_json
        return spec_hash(text) if text is not None else None

    @property
    def contract_drift(self) -> bool:
        current = self.spec_hash
        return (
            current is not None
            and self.previous_spec_hash is not None
            and current != self.previous_spec_hash
        )

    @property
    def status(self) -> str:
        """``healthy`` / ``unhealthy`` / ``unreachable`` — health is the primary signal (mesh-ui §3.1)."""
        if self.health is None:
            return "unreachable"
        return "healthy" if self.health.get("isHealthy") else "unhealthy"


class _CollectorView(Protocol):
    """The slice of :class:`~benzene.mesh.MeshCollector` the emitter reads — its public read models."""

    def query_fleet(self, body: dict[str, Any]) -> dict[str, Any]: ...
    def query_topic(self, body: dict[str, Any]) -> dict[str, Any]: ...


# A topic id starting with this prefix is Benzene's own plumbing — the UI hides it behind a toggle.
_RESERVED_PREFIX = "benzene:"


class MeshArtifactEmitter:
    """Projects the catalog spine + a :class:`~benzene.mesh.MeshCollector` into the six mesh-UI artifacts.

    Construct it with the fleet's :class:`ServiceCatalog` entries, the collector, and a
    ``generated_at`` stamp; call the ``build_*`` methods for the in-memory artifact dicts (pure, so
    they test straight against the reference fixtures) or :meth:`emit` to write all six to a directory.
    """

    def __init__(
        self,
        services: Sequence[ServiceCatalog],
        collector: _CollectorView,
        *,
        generated_at: datetime,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        annotations: Sequence[Mapping[str, Any]] = (),
        topology_source: TopologySource | None = None,
        usage_source: UsageSource | None = None,
    ) -> None:
        self._services = list(services)
        self._collector = collector
        self._generated_at = generated_at
        self._window_start = window_start
        self._window_end = window_end
        self._annotations = [dict(annotation) for annotation in annotations]
        # One fleet query drives topics/topology/usage — providers + invocation counts per topic.
        self._fleet = collector.query_fleet({})
        self._fleet_topics: dict[str, dict[str, Any]] = {
            str(entry["topic"]): entry for entry in self._fleet.get("topics", [])
        }
        # Optional external enrichment planes (mesh.md §9 / .NET Benzene.Mesh.Aggregator layering): an
        # X-Ray topology source and a CloudWatch usage source, each queried once here (as the collector
        # is) so the build_* projections stay pure. Absent → the collector plane is used verbatim.
        self._external_edges: list[TopologyEdge] = (
            list(topology_source.topology()) if topology_source is not None else []
        )
        self._external_usage: list[UsageEntry] = (
            list(usage_source.usage()) if usage_source is not None else []
        )

    # --- manifest.json ------------------------------------------------------------------------
    def build_manifest(self) -> dict[str, Any]:
        """One entry per service: identity, health status, contract-drift flag, and the spine URLs."""
        return {
            "generatedAtUtc": _iso(self._generated_at),
            "services": [
                {
                    "name": service.name,
                    "status": service.status,
                    "contractDrift": service.contract_drift,
                    "specUrl": service.spec_url,
                    "healthUrl": service.health_url,
                }
                for service in self._services
            ],
        }

    # --- services/<name>.json -----------------------------------------------------------------
    def build_service_snapshots(self) -> dict[str, dict[str, Any]]:
        """The per-service detail snapshot, keyed by service name (one file each under ``services/``)."""
        return {service.name: self._service_snapshot(service) for service in self._services}

    def _service_snapshot(self, service: ServiceCatalog) -> dict[str, Any]:
        return {
            "name": service.name,
            "fetchedAtUtc": _iso(self._generated_at),
            "specJson": service.spec_json,
            "specHash": service.spec_hash,
            "previousSpecHash": service.previous_spec_hash,
            "contractDrift": service.contract_drift,
            "health": dict(service.health) if service.health is not None else None,
            "error": service.error,
        }

    # --- topics.json --------------------------------------------------------------------------
    def build_topics(self) -> dict[str, Any]:
        """The functional map: every topic, its consumers (handlers) + producers (senders), schemas."""
        aggregates = self._aggregate_topics()
        topics = [self._topic_entry(key, agg) for key, agg in aggregates.items()]
        # Domain topics first, utilities last; then by topic id, then version (the .NET catalog order).
        topics.sort(key=lambda entry: (entry["reserved"], entry["topic"], entry["version"]))
        return {"generatedAtUtc": _iso(self._generated_at), "topics": topics, "removedTopics": []}

    def _aggregate_topics(self) -> dict[tuple[str, str], _TopicAggregate]:
        aggregates: dict[tuple[str, str], _TopicAggregate] = {}

        def aggregate(topic_id: str, version: str) -> _TopicAggregate:
            key = (topic_id, version)
            existing = aggregates.get(key)
            if existing is None:
                existing = _TopicAggregate()
                aggregates[key] = existing
            return existing

        # Spine handlers: each service that serves a topic is a consumer of it, with its HTTP mappings.
        for service in self._services:
            if service.spec is None:
                continue
            for topic in service.spec.get("topics", []):
                topic_id = str(topic["id"])
                version = str(topic.get("version", "") or "")
                agg = aggregate(topic_id, version)
                mappings = [m.to_payload() for m in service.http_mappings.get(topic_id, ())]
                agg.consumers[service.name] = mappings
                agg.request_schemas.append(_schema_or_none(topic.get("requestSchema")))
                agg.response_schemas.append(_schema_or_none(topic.get("responseSchema")))

        # Live enrichment: collector providers are handlers too (a service unreachable on its spec
        # endpoint can still have registered), and trace-parent callers are the topic's producers.
        for topic_id in self._fleet_topics:
            keys = [key for key in aggregates if key[0] == topic_id] or [(topic_id, "")]
            live = self._topic_live(topic_id)
            for key in keys:
                agg = aggregate(*key)
                for provider in live["providers"]:
                    agg.consumers.setdefault(provider, [])
                for producer in live["producers"]:
                    agg.producers.add(producer)

        return aggregates

    def _topic_entry(self, key: tuple[str, str], agg: _TopicAggregate) -> dict[str, Any]:
        topic_id, version = key
        reserved = topic_id.startswith(_RESERVED_PREFIX)
        consumers = [
            {"service": name, "httpMappings": agg.consumers[name]}
            for name in sorted(agg.consumers)
        ]
        producers = [{"service": name} for name in sorted(agg.producers)]
        return {
            "topic": topic_id,
            "version": version,
            "reserved": reserved,
            "consumers": consumers,
            "producers": producers,
            "status": _topic_status(reserved, consumers, producers),
            "requestSchema": _first_schema(agg.request_schemas),
            "responseSchema": _first_schema(agg.response_schemas),
            "messageSchema": None,
            "schemaMismatch": (not reserved)
            and (_has_conflict(agg.request_schemas) or _has_conflict(agg.response_schemas)),
        }

    # --- topology.json ------------------------------------------------------------------------
    def build_topology(self) -> dict[str, Any]:
        """``client → server`` edges: the collector's trace-parentage baseline, overlaid by an X-Ray source.

        The collector plane derives edges from trace parentage (``source: "collector"``, timing ``null``).
        When a :class:`TopologySource` is wired, its richer edges (real RPM + latency percentiles, tagged
        ``"xray"``) **take precedence per ``(client, server)`` pair**, and collector edges survive only for
        pairs the external source did not observe — mirroring .NET's aggregator layering a fleet source over
        the baseline (the richer edge wins; the baseline fills gaps). Deterministic: sorted by pair.
        """
        edges: dict[tuple[str, str], _Edge] = {}
        for topic_id in self._fleet_topics:
            live = self._topic_live(topic_id)
            unambiguous = len(live["producers"]) == 1 and len(live["providers"]) == 1
            for client in live["producers"]:  # trace-parent caller
                for server in live["providers"]:  # the handler that registered the topic
                    if client == server:
                        continue
                    edge = edges.setdefault((client, server), _Edge())
                    if unambiguous:
                        edge.invocations += live["invocations"]
                        edge.errors += live["errors"]
                    else:
                        edge.ambiguous = True
        merged: dict[tuple[str, str], dict[str, Any]] = {
            (client, server): self._edge_payload(client, server, edge)
            for (client, server), edge in edges.items()
        }
        for external in self._external_edges:  # the external (X-Ray) edge wins its pair
            merged[(external.client, external.server)] = external.to_payload()
        ordered = sorted(merged.items(), key=lambda item: item[0])
        return {
            "generatedAtUtc": _iso(self._generated_at),
            "edges": [payload for _key, payload in ordered],
        }

    def _edge_payload(self, client: str, server: str, edge: _Edge) -> dict[str, Any]:
        # No trace-source timing in this MVP, so RPM + latency percentiles stay null (mesh-ui renders
        # them as "–"); an error rate is honest only when every carried topic maps unambiguously to
        # this one edge, so an ambiguous edge blanks it rather than showing a wrong number.
        error_rate = (
            edge.errors / edge.invocations
            if (not edge.ambiguous and edge.invocations > 0)
            else None
        )
        return {
            "client": client,
            "server": server,
            "source": "collector",
            "requestsPerMinute": None,
            "errorRate": error_rate,
            "p50LatencyMs": None,
            "p95LatencyMs": None,
            "p99LatencyMs": None,
        }

    # --- usage.json ---------------------------------------------------------------------------
    def build_usage(self) -> dict[str, Any]:
        """Per-topic exercise counts (collector), overlaid per-topic by an external CloudWatch usage source.

        The collector plane counts observed invocations per (topic, status) (``source: "collector"``,
        no transport/service/duration). When a :class:`UsageSource` is wired, it supplies richer entries
        (real counts split by transport + status, with ``avgDurationMs``, tagged ``"cloudwatch"``): **for
        every topic the external source reports, its entries replace the collector's** for that topic;
        topics the external source doesn't cover keep their collector entries. This mirrors the .NET
        aggregator merging usage adapters over the baseline, and keeps each entry's ``source`` attribution
        (so one ``usage.json`` carries both planes). Deterministic: topics sorted, external order preserved.
        """
        collector_by_topic: dict[str, list[dict[str, Any]]] = {}
        for topic_id in sorted(self._fleet_topics):
            status_counts = self._topic_live(topic_id)["statusCounts"]
            for status in sorted(status_counts):
                collector_by_topic.setdefault(topic_id, []).append(
                    {
                        "topic": topic_id,
                        "version": None,
                        "service": None,
                        "transport": None,
                        "status": status,
                        "count": status_counts[status],
                        "avgDurationMs": None,
                        "source": "collector",
                    }
                )
        external_by_topic: dict[str, list[dict[str, Any]]] = {}
        for entry in self._external_usage:
            external_by_topic.setdefault(entry.topic, []).append(entry.to_payload())

        entries: list[dict[str, Any]] = []
        for topic_id in sorted(set(collector_by_topic) | set(external_by_topic)):
            entries.extend(external_by_topic.get(topic_id) or collector_by_topic.get(topic_id, []))
        return {
            "generatedAtUtc": _iso(self._generated_at),
            "windowStartUtc": _iso(self._window_start) if self._window_start else None,
            "windowEndUtc": _iso(self._window_end) if self._window_end else None,
            "entries": entries,
        }

    # --- annotations.json ---------------------------------------------------------------------
    def build_annotations(self) -> dict[str, Any]:
        """Human discussion threads — not derived; the seeded set (empty by default) plus the stamp."""
        return {
            "generatedAtUtc": _iso(self._generated_at),
            "annotations": [dict(annotation) for annotation in self._annotations],
        }

    # --- collector read-through ---------------------------------------------------------------
    def _topic_live(self, topic_id: str) -> dict[str, Any]:
        """The collector's view of one topic: providers, producers (callers), counts. Empty if unknown."""
        fleet_entry = self._fleet_topics.get(topic_id, {})
        providers = list(fleet_entry.get("providers", []))
        invocations = int(fleet_entry.get("invocations", 0))
        producers: list[str] = []
        errors = 0
        status_counts: dict[str, int] = {}
        if topic_id in self._fleet_topics:
            detail = self._collector.query_topic({"topic": topic_id})
            producers = list(detail.get("consumers", []))  # trace-parent callers = the topic's producers
            errors = int(detail.get("errors", 0))
            status_counts = dict(detail.get("statusCounts", {}))
        return {
            "providers": providers,
            "producers": producers,
            "invocations": invocations,
            "errors": errors,
            "statusCounts": status_counts,
        }

    # --- emit ---------------------------------------------------------------------------------
    def emit(self, out_dir: str) -> dict[str, Any]:
        """Write all six artifacts under ``out_dir`` (creating ``services/``). Returns the manifest."""
        os.makedirs(os.path.join(out_dir, "services"), exist_ok=True)
        manifest = self.build_manifest()
        _write_json(os.path.join(out_dir, "manifest.json"), manifest)
        for name, snapshot in self.build_service_snapshots().items():
            _write_json(os.path.join(out_dir, "services", f"{name}.json"), snapshot)
        _write_json(os.path.join(out_dir, "topics.json"), self.build_topics())
        _write_json(os.path.join(out_dir, "topology.json"), self.build_topology())
        _write_json(os.path.join(out_dir, "usage.json"), self.build_usage())
        _write_json(os.path.join(out_dir, "annotations.json"), self.build_annotations())
        return manifest


@dataclass
class _TopicAggregate:
    consumers: dict[str, list[dict[str, str]]] = field(default_factory=dict)  # service -> httpMappings
    producers: set[str] = field(default_factory=set)
    request_schemas: list[dict[str, Any] | None] = field(default_factory=list)
    response_schemas: list[dict[str, Any] | None] = field(default_factory=list)


@dataclass
class _Edge:
    invocations: int = 0
    errors: int = 0
    ambiguous: bool = False


def _first_schema(schemas: Sequence[dict[str, Any] | None]) -> dict[str, Any] | None:
    """The first declared (non-null) schema — the representative payload for the topic entry."""
    for schema in schemas:
        if schema is not None:
            return schema
    return None


def _has_conflict(schemas: Sequence[dict[str, Any] | None]) -> bool:
    """True when two consumers declare *different* (non-null) schemas for the same topic (a likely bug)."""
    declared = {_canonical(schema) for schema in schemas if schema is not None}
    return len(declared) > 1


def _topic_status(
    reserved: bool, consumers: Sequence[Mapping[str, Any]], producers: Sequence[Mapping[str, Any]]
) -> str | None:
    """A domain topic's informational status (mesh-ui value/deprecation view); reserved topics get none.

    Producers but nobody consumes it is a deprecation candidate; consumers exist but nobody produces
    it and none are HTTP-reachable is a gap (an HTTP endpoint's producer is an external caller, not a
    fleet declaration). Anything else is left unflagged rather than guessed at.
    """
    if reserved:
        return None
    has_consumers = len(consumers) > 0
    has_producers = len(producers) > 0
    if has_producers and not has_consumers:
        return "deprecation-candidate"
    if has_consumers and not has_producers and all(
        not consumer["httpMappings"] for consumer in consumers
    ):
        return "gap"
    return None


def _write_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
