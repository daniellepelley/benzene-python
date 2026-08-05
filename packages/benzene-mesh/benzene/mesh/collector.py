"""The mesh collector — the receiving side that ingests feeds and renders the fleet (mesh.md §§4–6).

A collector is *an ordinary Benzene service* serving the four ingest topics
(``benzene:mesh:register`` / ``heartbeat`` / ``traces`` / ``issues``) plus a set of ``mesh:query:*``
read models. :class:`MeshCollector` holds the in-memory catalog and derives the fleet view from what
services report; :func:`collector_registry` wires it onto a :class:`~benzene.core.Registry` so the
collector runs through the normal pipeline (it dogfoods the framework it collects for).

Derivation rules that are normative (mesh.md §4):

- ``service`` is required on register and heartbeat → ``bad-request`` when missing; a traces batch of
  any size (including empty) is accepted.
- Re-registration **replaces** a service's registration wholesale — a redeploy that drops a topic
  drops its provider edge.
- **Consumer edges are derived from trace parentage**: an event whose parent span belongs to a
  *different* service makes that service a consumer of the event's topic (never declared).

The ``benzene:mesh:query:*`` shapes follow the reference collector and are pinned by
``mesh-collector-cases.json`` as the observable surface for the ingest/derivation rules.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from benzene.core import Handler, Registry
from benzene.results import Result, is_successful

from .feeds import HEARTBEAT_TOPIC, ISSUES_TOPIC, REGISTER_TOPIC, TRACES_TOPIC

# Query topics (the collector's read models — one collector's shapes, pinned by the fixtures).
QUERY_FLEET_TOPIC = "benzene:mesh:query:fleet"
QUERY_SERVICE_TOPIC = "benzene:mesh:query:service"
QUERY_TOPIC_TOPIC = "benzene:mesh:query:topic"
QUERY_TRACE_TOPIC = "benzene:mesh:query:trace"

# The feeds a service can report, in the order they are listed in `missingFeeds`. `issues` is special:
# it is only "missing" when a failure needs explaining (see `_missing_feeds`).
_FEEDS = ("descriptor", "health", "traces", "issues")


class CollectorBadRequest(Exception):
    """A malformed ingest/query (e.g. a required identifier missing) → ``bad-request``."""


class CollectorNotFound(Exception):
    """A query for an unknown service / topic / trace → ``not-found``."""


@dataclass
class _Instance:
    healthy: bool
    descriptor_hash: str | None


@dataclass
class _Service:
    name: str
    has_descriptor: bool = False
    descriptor_hash: str | None = None
    provided: list[str] = field(default_factory=list)  # topic ids this service currently provides
    instances: dict[str, _Instance] = field(default_factory=dict)
    reported_issues: bool = False  # has sent any issues batch (even empty) — the feed's liveness


@dataclass
class _Event:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    service: str
    topic: str
    status: str


class MeshCollector:
    """In-memory catalog: ingest the feeds, derive the fleet. One instance per collector."""

    def __init__(self) -> None:
        self._services: dict[str, _Service] = {}
        self._topics: set[str] = set()  # every topic ever seen (registered or traced); grows only
        self._events: list[_Event] = []
        self._span_owner: dict[str, str] = {}  # span id -> the service that emitted it
        self._issues: dict[str, dict[str, Any]] = {}  # fingerprint -> merged issue

    # --- ingest ----------------------------------------------------------------------------
    def ingest_register(self, body: dict[str, Any]) -> dict[str, Any]:
        service = _require(body, "service")
        record = self._service(service)
        # Re-registration replaces wholesale: drop this service's old provider edges first.
        record.provided = [str(t["id"]) for t in body.get("topics", []) if "id" in t]
        record.has_descriptor = True
        record.descriptor_hash = body.get("descriptorHash")
        self._topics.update(record.provided)
        return {"accepted": 1}

    def ingest_heartbeat(self, body: dict[str, Any]) -> dict[str, Any]:
        service = _require(body, "service")
        record = self._service(service)
        instance_id = str(body.get("instanceId", ""))
        health = body.get("health") or {}
        record.instances[instance_id] = _Instance(
            healthy=bool(health.get("isHealthy", False)),
            descriptor_hash=body.get("descriptorHash"),
        )
        return {"accepted": 1}

    def ingest_traces(self, body: dict[str, Any]) -> dict[str, Any]:
        events = body.get("events") or []
        for raw in events:
            event = _Event(
                trace_id=str(raw.get("traceId", "")),
                span_id=str(raw.get("spanId", "")),
                parent_span_id=raw.get("parentSpanId"),
                service=str(raw.get("service", "")),
                topic=str(raw.get("topic", "")),
                status=str(raw.get("status", "")),
            )
            self._events.append(event)
            self._span_owner[event.span_id] = event.service
            self._topics.add(event.topic)
            self._service(event.service)  # a traced service becomes known (possibly anonymous)
        return {"accepted": len(events)}

    def ingest_issues(self, body: dict[str, Any]) -> dict[str, Any]:
        service = _require(body, "service")
        self._service(service).reported_issues = True  # even an empty batch is a liveness assertion
        accepted = 0
        for issue in body.get("issues") or []:
            fingerprint = issue.get("fingerprint")
            if not fingerprint:
                continue  # skip an unidentifiable entry, never reject the batch for it
            self._merge_issue(str(fingerprint), issue)
            accepted += 1
        return {"accepted": accepted}

    def _merge_issue(self, fingerprint: str, issue: dict[str, Any]) -> None:
        existing = self._issues.get(fingerprint)
        if existing is None:
            merged = dict(issue)
            merged["count"] = int(issue.get("count", 0))
            merged["exemplarTraceIds"] = list(issue.get("exemplarTraceIds", []))
            self._issues[fingerprint] = merged
            return
        # Merge by fingerprint (mesh.md §4.1): count is a delta, exemplars accrue, timestamps span.
        existing["count"] += int(issue.get("count", 0))
        for trace_id in issue.get("exemplarTraceIds", []):
            if trace_id not in existing["exemplarTraceIds"]:
                existing["exemplarTraceIds"].append(trace_id)
        if "firstSeen" in issue:
            existing["firstSeen"] = min(existing.get("firstSeen", issue["firstSeen"]), issue["firstSeen"])
        if "lastSeen" in issue:
            existing["lastSeen"] = max(existing.get("lastSeen", issue["lastSeen"]), issue["lastSeen"])

    # --- queries ---------------------------------------------------------------------------
    def query_fleet(self, _body: dict[str, Any]) -> dict[str, Any]:
        services = [self._fleet_service_entry(record) for record in self._services.values()]
        topics = [
            {
                "topic": topic,
                "providers": self._providers_of(topic),
                "invocations": self._invocations_on(topic),
            }
            for topic in sorted(self._topics)
        ]
        return {"services": services, "topics": topics, "issues": list(self._issues.values())}

    def query_service(self, body: dict[str, Any]) -> dict[str, Any]:
        name = _require(body, "service")
        record = self._services.get(name)
        if record is None:
            raise CollectorNotFound(f"No service {name!r} in the catalog")
        response: dict[str, Any] = {"service": name}
        if record.instances:
            response["health"] = self._health(record)
            response["instances"] = [
                {
                    "instanceId": instance_id,
                    "healthy": instance.healthy,
                    "hashMatches": instance.descriptor_hash == record.descriptor_hash,
                }
                for instance_id, instance in record.instances.items()
            ]
        invocations = self._invocations_by(name)
        if invocations:
            response["invocations"] = invocations
        missing = self._missing_feeds(record)
        if missing:
            response["missingFeeds"] = missing
        return response

    def query_topic(self, body: dict[str, Any]) -> dict[str, Any]:
        topic = _require(body, "topic")
        if topic not in self._topics:
            raise CollectorNotFound(f"No topic {topic!r} in the catalog")
        events = [event for event in self._events if event.topic == topic]
        response: dict[str, Any] = {"topic": topic, "providers": self._providers_of(topic)}
        consumers = self._consumers_of(topic, events)
        if consumers:
            response["consumers"] = consumers
        if events:
            response["invocations"] = len(events)
            response["errors"] = sum(1 for event in events if not is_successful(event.status))
            response["statusCounts"] = dict(Counter(event.status for event in events))
        return response

    def query_trace(self, body: dict[str, Any]) -> dict[str, Any]:
        trace_id = _require(body, "traceId")
        events = [event for event in self._events if event.trace_id == trace_id]
        if not events:
            raise CollectorNotFound(f"No trace {trace_id!r} in the catalog")
        return {
            "traceId": trace_id,
            "events": [{"spanId": event.span_id, "service": event.service} for event in events],
        }

    # --- derivation helpers ----------------------------------------------------------------
    def _service(self, name: str) -> _Service:
        record = self._services.get(name)
        if record is None:
            record = _Service(name)
            self._services[name] = record
        return record

    def _providers_of(self, topic: str) -> list[str]:
        return sorted(name for name, record in self._services.items() if topic in record.provided)

    def _consumers_of(self, topic: str, events: list[_Event]) -> list[str]:
        consumers: set[str] = set()
        for event in events:
            parent = event.parent_span_id
            owner = self._span_owner.get(parent) if parent else None
            if owner is not None and owner != event.service:
                consumers.add(owner)
        return sorted(consumers)

    def _invocations_on(self, topic: str) -> int:
        return sum(1 for event in self._events if event.topic == topic)

    def _invocations_by(self, service: str) -> int:
        return sum(1 for event in self._events if event.service == service)

    def _health(self, record: _Service) -> str:
        if not record.instances:
            return "unknown"
        healths = [instance.healthy for instance in record.instances.values()]
        if all(healths):
            return "healthy"
        if not any(healths):
            return "unhealthy"
        return "degraded"

    def _missing_feeds(self, record: _Service) -> list[str]:
        has_failure = any(
            event.service == record.name and not is_successful(event.status)
            for event in self._events
        )
        present = {
            "descriptor": record.has_descriptor,
            "health": bool(record.instances),
            "traces": self._invocations_by(record.name) > 0,
            # `issues` is only "missing" when a failure needs explaining and no issue feed arrived.
            "issues": record.reported_issues or not has_failure,
        }
        return [feed for feed in _FEEDS if not present[feed]]

    def _fleet_service_entry(self, record: _Service) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "service": record.name,
            "topics": len(record.provided),
            "health": self._health(record),
            "missingFeeds": self._missing_feeds(record),
        }
        if record.instances:
            entry["instances"] = len(record.instances)
        return entry


def _require(body: dict[str, Any], key: str) -> str:
    value = body.get(key)
    if not value:
        raise CollectorBadRequest(f"{key!r} is required")
    return str(value)


def _ingest_handler(method) -> Handler:
    async def handler(request: dict[str, Any]) -> Result:
        try:
            return Result.ok(method(request))
        except CollectorBadRequest as exc:
            return Result.bad_request(str(exc))

    return handler


def _query_handler(method) -> Handler:
    async def handler(request: dict[str, Any]) -> Result:
        try:
            return Result.ok(method(request))
        except CollectorBadRequest as exc:
            return Result.bad_request(str(exc))
        except CollectorNotFound as exc:
            return Result.not_found(str(exc))

    return handler


def collector_registry(collector: MeshCollector | None = None) -> Registry:
    """Wire a :class:`MeshCollector` onto a registry — the collector as an ordinary Benzene service.

    Drive it through a :class:`~benzene.core.BenzeneMessageApplication`: push the ingest feeds and
    query it with ``benzene:mesh:query:*``.
    """
    collector = collector or MeshCollector()
    registry = Registry()
    registry.register(REGISTER_TOPIC, _ingest_handler(collector.ingest_register))
    registry.register(HEARTBEAT_TOPIC, _ingest_handler(collector.ingest_heartbeat))
    registry.register(TRACES_TOPIC, _ingest_handler(collector.ingest_traces))
    registry.register(ISSUES_TOPIC, _ingest_handler(collector.ingest_issues))
    registry.register(QUERY_FLEET_TOPIC, _query_handler(collector.query_fleet))
    registry.register(QUERY_SERVICE_TOPIC, _query_handler(collector.query_service))
    registry.register(QUERY_TOPIC_TOPIC, _query_handler(collector.query_topic))
    registry.register(QUERY_TRACE_TOPIC, _query_handler(collector.query_trace))
    return registry
