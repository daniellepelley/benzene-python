"""The mesh collector — the receiving side that ingests feeds and renders the fleet (mesh.md §§4–6).

A collector is *an ordinary Benzene service* serving the four ingest topics
(``benzene:mesh:register`` / ``heartbeat`` / ``traces`` / ``issues``) plus a set of ``mesh:query:*``
read models. :class:`MeshCollector` holds the in-memory catalog and derives the fleet view from what
services report; :func:`collector_registry` wires it onto a :class:`~benzene.core.Registry` so the
collector runs through the normal pipeline (it dogfoods the framework it collects for).

Derivation rules that are normative (mesh.md §4):

- ``service`` is required on register, heartbeat, and issues → ``bad-request`` when missing; a traces
  or issues batch of any size (including empty) is accepted.
- Re-registration **replaces** a service's registration wholesale — a redeploy that drops a topic
  drops its provider edge, and one that drops a ``consumes`` entry drops its consumer edge the same way.
- **The producer/consumer graph is built from the latest registered descriptor alone**: ``topics`` gives
  provider edges, ``consumes`` gives consumer edges. Trace parentage MUST NOT be used to admit an edge
  into this graph — it feeds invocation counts and status stats only.
- **Declared vs. observed** (mesh.md §4.2): a trace never changes the graph, but it *is* the only signal
  for two collector-derived read models layered on top of it — **liveness** (has a declared edge ever
  been exercised, so an unexercised one is a decommission *candidate*, never a fact) and **drift** (a
  traced edge nobody declared, filed as a ``contract-drift`` issue the moment it's first observed on a
  service that *has* registered a descriptor — an anonymous/never-registered service has nothing to
  diverge from, so it is never flagged).

The ``benzene:mesh:query:*`` shapes follow the reference collector and are pinned by
``mesh-collector-cases.json`` as the observable surface for the ingest/derivation rules.
"""

from __future__ import annotations

import asyncio
import copy
from collections import Counter, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from benzene.core import Handler, Registry
from benzene.results import Result, is_successful

from .feeds import HEARTBEAT_TOPIC, ISSUES_TOPIC, REGISTER_TOPIC, TRACES_TOPIC
from .issues import issue_fingerprint
from .store import CollectorStore, NullCollectorStore

# Bumped when the snapshot shape changes incompatibly; an older/newer snapshot is ignored on load.
_SNAPSHOT_VERSION = 2

# Query topics (the collector's read models — one collector's shapes, pinned by the fixtures).
QUERY_FLEET_TOPIC = "benzene:mesh:query:fleet"
QUERY_SERVICE_TOPIC = "benzene:mesh:query:service"
QUERY_TOPIC_TOPIC = "benzene:mesh:query:topic"
QUERY_TRACE_TOPIC = "benzene:mesh:query:trace"

# The feeds a service can report, in the order they are listed in `missingFeeds`. `issues` is special:
# it is only "missing" when a failure needs explaining (see `_missing_feeds`).
_FEEDS = ("descriptor", "health", "traces", "issues")

# How many trace events one collector retains by default — the newest N, oldest evicted. Far above any
# fixture's or ordinary burst's event count, but bounded, so a long-lived collector's memory, snapshot
# size, and full-scan queries stop growing with uptime.
_DEFAULT_MAX_EVENTS = 10_000

# Merged issue exemplars keep the newest few (mesh.md §4.1).
_MAX_EXEMPLARS = 3
# Issue fields the collector aggregates itself — everything else is "latest-wins" on merge.
_AGGREGATED_ISSUE_FIELDS = frozenset({"count", "exemplarTraceIds", "firstSeen", "lastSeen"})


class CollectorError(Exception):
    """Base for a collector ingest/query failure — catch this to handle either kind at once."""


class CollectorBadRequest(CollectorError):
    """A malformed ingest/query (e.g. a required identifier missing) → ``bad-request``."""


class CollectorNotFound(CollectorError):
    """A query for an unknown service / topic / trace → ``not-found``."""


@dataclass
class _Instance:
    healthy: bool
    descriptor_hash: str | None
    health_checks: dict[str, Any] = field(default_factory=dict)  # the heartbeat's per-check detail


@dataclass
class _Service:
    name: str
    has_descriptor: bool = False
    descriptor_hash: str | None = None
    previous_descriptor_hash: str | None = None  # the hash before the last contract change (drift)
    provided: list[str] = field(default_factory=list)  # topic ids this service currently provides
    consumed: list[str] = field(default_factory=list)  # topic ids this service currently consumes
    # Per-topic contract detail from the descriptor/spec feed: id -> {version, requestSchema, responseSchema}.
    topic_specs: dict[str, dict[str, Any]] = field(default_factory=dict)
    # The topic_specs from the register before the current one, for per-topic schema-change detection.
    previous_topic_specs: dict[str, dict[str, Any]] = field(default_factory=dict)
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
    started_at: str | None = None


class MeshCollector:
    """The catalog: ingest the feeds, derive the fleet. One instance per collector.

    In-memory by default. Pass a :class:`~benzene.mesh.CollectorStore` to make the catalog durable:
    the collector restores from it on construction and writes a fresh snapshot after every mutating
    ingest, so a restarted host rehydrates the fleet it already knew. The default store keeps nothing,
    so tests and single runs behave exactly as before and pay nothing.

    The trace-event log is a **bounded window**: ``max_events`` (default 10 000) newest events are
    retained and the oldest are evicted, the way :class:`~benzene.mesh.QueueTraceExporter` bounds the
    sending side. Everything derived from a *declaration* (the service catalog, the producer/consumer
    graph, merged issues) is unbounded and permanent — only the raw event log, and the trace-derived
    stats scanned out of it, are windowed.
    """

    def __init__(
        self, *, store: CollectorStore | None = None, max_events: int = _DEFAULT_MAX_EVENTS
    ) -> None:
        self._services: dict[str, _Service] = {}
        self._topics: set[str] = set()  # every topic ever seen (registered or traced); grows only
        self._ever_provided: set[str] = (
            set()
        )  # topics some service ever declared (for removed-topic detection)
        self._max_events = max_events
        # Bounded: the newest `max_events` trace events, oldest evicted (see the class docstring).
        self._events: deque[_Event] = deque(maxlen=max_events)
        # span id -> the service that emitted it. Used only for the §4.2 observed-signal (liveness,
        # drift) below — NEVER for graph membership (`_providers_of`/`_consumers_of` are declared-only).
        # Tolerant of eviction by construction: every lookup is a `.get`, and a parent span whose event
        # has aged out of the window simply yields no observed caller.
        self._span_owner: dict[str, str] = {}
        self._issues: dict[str, dict[str, Any]] = {}  # fingerprint -> merged issue
        self._store: CollectorStore = store or NullCollectorStore()
        # Set while an ingest runs under `persist_off_loop`, so the blocking store write is hoisted out
        # of the mutation and awaited on a worker thread instead of running on the event loop.
        self._defer_save = False
        self._save_pending = False
        self._save_lock: asyncio.Lock | None = None
        self._save_loop: asyncio.AbstractEventLoop | None = None
        saved = self._store.load()
        if saved is not None:
            self.restore(saved)

    # --- ingest ----------------------------------------------------------------------------
    def ingest_register(self, body: dict[str, Any]) -> dict[str, Any]:
        service = _require(body, "service")
        record = self._service(service)
        new_hash = body.get("descriptorHash")
        # A changed contract hash on a service we already knew is drift — remember the prior hash.
        if record.has_descriptor and record.descriptor_hash and new_hash != record.descriptor_hash:
            record.previous_descriptor_hash = record.descriptor_hash
        # Re-registration replaces wholesale: drop this service's old provider/consumer edges + specs first.
        topics = [t for t in body.get("topics", []) if isinstance(t, dict) and "id" in t]
        record.provided = [str(t["id"]) for t in topics]
        consumes = [t for t in body.get("consumes", []) if isinstance(t, dict) and "id" in t]
        record.consumed = [str(t["id"]) for t in consumes]
        # Keep the prior contracts so the artifact writer can flag which topics changed schema.
        record.previous_topic_specs = record.topic_specs
        record.topic_specs = {str(t["id"]): _topic_spec(t) for t in topics}
        record.has_descriptor = True
        record.descriptor_hash = new_hash
        self._topics.update(record.provided)
        self._topics.update(record.consumed)
        self._ever_provided.update(
            record.provided
        )  # remember it was declared, even if later dropped
        return self._persisted({"accepted": 1})

    def ingest_heartbeat(self, body: dict[str, Any]) -> dict[str, Any]:
        service = _require(body, "service")
        record = self._service(service)
        instance_id = str(body.get("instanceId", ""))
        health = body.get("health") or {}
        checks = health.get("healthChecks")
        record.instances[instance_id] = _Instance(
            healthy=bool(health.get("isHealthy", False)),
            descriptor_hash=body.get("descriptorHash"),
            health_checks=dict(checks) if isinstance(checks, dict) else {},
        )
        return self._persisted({"accepted": 1})

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
                started_at=raw.get("startedAt"),
            )
            self._events.append(event)
            self._span_owner[event.span_id] = event.service
            self._topics.add(event.topic)
            callee = self._service(event.service)  # a traced service becomes known (possibly anonymous)
            self._flag_drift_if_undeclared(callee, event.topic, callee.provided, event.trace_id)
            parent = event.parent_span_id
            caller_name = self._span_owner.get(parent) if parent else None
            if caller_name and caller_name != event.service:
                caller = self._services.get(caller_name)
                if caller is not None:  # only a registered caller has a `consumes` to diverge from
                    self._flag_drift_if_undeclared(caller, event.topic, caller.consumed, event.trace_id)
        return self._persisted({"accepted": len(events)})

    def _flag_drift_if_undeclared(
        self, record: _Service, topic: str, declared: list[str], trace_id: str
    ) -> None:
        """File a ``contract-drift`` issue the first time a trace observes ``record`` on a topic it
        hasn't declared — either as a provider (``topic`` not in its ``topics``) or as a consumer
        (``topic`` not in its ``consumes``), mesh.md §4.2's "Undeclared" signal. A service that has
        never registered a descriptor has no declared contract to diverge from, so it is never flagged.
        """
        if not record.has_descriptor or topic in declared:
            return
        fingerprint = issue_fingerprint(record.name, topic, "", "contract-drift", "undeclared-edge")
        self._merge_issue(
            fingerprint,
            {
                "fingerprint": fingerprint,
                "classification": "contract-drift",
                "service": record.name,
                "topic": topic,
                "status": "",
                "count": 1,
                "exemplarTraceIds": [trace_id] if trace_id else [],
            },
        )

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
        return self._persisted({"accepted": accepted})

    def _merge_issue(self, fingerprint: str, issue: dict[str, Any]) -> None:
        # A malformed aggregated field (e.g. an explicit null count/firstSeen/lastSeen from a
        # hand-rolled or cross-language POST) must not crash the merge and drop the whole batch —
        # the contract is skip-the-bad-part, accept the rest. Coerce count and span only over
        # present, non-null timestamps.
        incoming_exemplars = list(issue.get("exemplarTraceIds", []))
        existing = self._issues.get(fingerprint)
        if existing is None:
            merged = dict(issue)
            merged["count"] = _safe_int(issue.get("count", 0))
            merged["exemplarTraceIds"] = incoming_exemplars[-_MAX_EXEMPLARS:]  # keep the newest
            self._issues[fingerprint] = merged
            return
        # Merge by fingerprint (mesh.md §4.1): count is a delta; firstSeen/lastSeen span; exemplars keep
        # the newest ≤3; every other field is latest-wins (identity fields are fingerprint-pinned).
        for key, value in issue.items():
            if key not in _AGGREGATED_ISSUE_FIELDS:
                existing[key] = value
        existing["count"] = _safe_int(existing.get("count", 0)) + _safe_int(issue.get("count", 0))
        combined = existing["exemplarTraceIds"] + [
            trace_id
            for trace_id in incoming_exemplars
            if trace_id not in existing["exemplarTraceIds"]
        ]
        existing["exemplarTraceIds"] = combined[-_MAX_EXEMPLARS:]
        first = issue.get("firstSeen")
        if first is not None:
            existing["firstSeen"] = min(existing.get("firstSeen") or first, first)
        last = issue.get("lastSeen")
        if last is not None:
            existing["lastSeen"] = max(existing.get("lastSeen") or last, last)

    # --- persistence -----------------------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        """The whole catalog as a JSON-able dict — what a :class:`CollectorStore` persists.

        ``events`` holds the retained window only — at most ``max_events`` (default 10 000) newest
        trace events, oldest first — so a snapshot has a bounded size no matter how long the collector
        has been up. Services, the declared graph, and merged issues are complete, never windowed.
        """
        return {
            "version": _SNAPSHOT_VERSION,
            "services": [
                {
                    "name": record.name,
                    "hasDescriptor": record.has_descriptor,
                    "descriptorHash": record.descriptor_hash,
                    "previousDescriptorHash": record.previous_descriptor_hash,
                    "provided": record.provided,
                    "consumed": record.consumed,
                    "topicSpecs": copy.deepcopy(record.topic_specs),
                    "previousTopicSpecs": copy.deepcopy(record.previous_topic_specs),
                    "reportedIssues": record.reported_issues,
                    "instances": [
                        {
                            "instanceId": instance_id,
                            "healthy": instance.healthy,
                            "descriptorHash": instance.descriptor_hash,
                            "healthChecks": copy.deepcopy(instance.health_checks),
                        }
                        for instance_id, instance in record.instances.items()
                    ],
                }
                for record in self._services.values()
            ],
            "topics": sorted(self._topics),
            "everProvided": sorted(self._ever_provided),
            "events": [
                {
                    "traceId": event.trace_id,
                    "spanId": event.span_id,
                    "parentSpanId": event.parent_span_id,
                    "service": event.service,
                    "topic": event.topic,
                    "status": event.status,
                    "startedAt": event.started_at,
                }
                for event in self._events
            ],
            # Deep-copied: issue records are nested mutable dicts, and later ingests must not mutate a
            # snapshot already handed out (nor, via restore, one collector leak into another).
            "issues": copy.deepcopy(self._issues),
        }

    def restore(self, snapshot: dict[str, Any]) -> None:
        """Replace the catalog with a snapshot from :meth:`snapshot`.

        A snapshot from an incompatible ``version`` is ignored (the catalog is left empty and refills
        from the fleet) rather than half-loaded into an inconsistent state.
        """
        if snapshot.get("version") != _SNAPSHOT_VERSION:
            return
        self._services = {}
        for entry in snapshot.get("services", []):
            record = _Service(
                name=str(entry["name"]),
                has_descriptor=bool(entry.get("hasDescriptor", False)),
                descriptor_hash=entry.get("descriptorHash"),
                previous_descriptor_hash=entry.get("previousDescriptorHash"),
                provided=list(entry.get("provided", [])),
                consumed=list(entry.get("consumed", [])),
                topic_specs=copy.deepcopy(entry.get("topicSpecs", {})),
                previous_topic_specs=copy.deepcopy(entry.get("previousTopicSpecs", {})),
                reported_issues=bool(entry.get("reportedIssues", False)),
                instances={
                    str(inst["instanceId"]): _Instance(
                        healthy=bool(inst.get("healthy", False)),
                        descriptor_hash=inst.get("descriptorHash"),
                        health_checks=copy.deepcopy(inst.get("healthChecks", {})),
                    )
                    for inst in entry.get("instances", [])
                },
            )
            self._services[record.name] = record
        self._topics = set(snapshot.get("topics", []))
        self._ever_provided = set(snapshot.get("everProvided", []))
        # A snapshot written by a collector with a larger (or no) cap is trimmed to this one's window:
        # a bounded deque built from an iterable keeps the *last* `maxlen` items — the newest events.
        self._events = deque(
            (
                _Event(
                    trace_id=str(raw.get("traceId", "")),
                    span_id=str(raw.get("spanId", "")),
                    parent_span_id=raw.get("parentSpanId"),
                    service=str(raw.get("service", "")),
                    topic=str(raw.get("topic", "")),
                    status=str(raw.get("status", "")),
                    started_at=raw.get("startedAt"),
                )
                for raw in snapshot.get("events", [])
            ),
            maxlen=self._max_events,
        )
        # Rebuild the span-owner index from the restored events — a pure index over `_events`, not
        # itself persisted, needed again after restart for the §4.2 observed-signal derivation.
        self._span_owner = {event.span_id: event.service for event in self._events}
        # Deep-copied so the restored collector never shares nested issue dicts with the snapshot it
        # was given (the JSON-file store hands back fresh parses; an in-memory snapshot would not).
        self._issues = copy.deepcopy(snapshot.get("issues", {}))

    def _persisted(self, result: dict[str, Any]) -> dict[str, Any]:
        """Write the catalog through the store after a mutating ingest, then return ``result``.

        A store save is blocking file/S3 I/O over the *whole* catalog. Called directly — a poller
        sweep, a batch aggregation pass, a test — that is exactly right: the write happens before the
        ingest returns and there is no event loop to protect. Called through the registry's ingest
        handlers, :meth:`persist_off_loop` sets ``_defer_save`` so the write is hoisted out of the
        mutation and awaited on a worker thread instead of stalling the loop.
        """
        if self._defer_save:
            self._save_pending = True
            return result
        self._store.save(self.snapshot())
        return result

    async def persist_off_loop(
        self, ingest: Callable[[dict[str, Any]], dict[str, Any]], body: dict[str, Any]
    ) -> dict[str, Any]:
        """Run one mutating ``ingest`` on the loop, then persist the catalog on a worker thread.

        The mutation itself is synchronous and await-free, so concurrent ingests still apply one at a
        time (no interleaving); only the blocking ``store.save`` moves off the event loop, where a
        synchronous S3 PUT or fsync would otherwise block every other invocation the collector serves.
        Saves are serialized behind a lock and snapshot *inside* it, so the last write is always the
        latest state.
        """
        self._defer_save = True
        try:
            result = ingest(body)
        finally:
            self._defer_save = False
        if self._save_pending:
            self._save_pending = False
            async with self._save_gate():
                await asyncio.to_thread(self._store.save, self.snapshot())
        return result

    def _save_gate(self) -> asyncio.Lock:
        """The lock serializing store writes, (re)bound to the loop currently running.

        An :class:`asyncio.Lock` binds to the first loop that *contends* for it, and a collector can
        easily outlive one loop — a warm Lambda container reuses the instance across invocations, each
        with its own :func:`asyncio.run`. Rebinding when the loop changes keeps the second invocation
        from failing on a lock owned by the first one's dead loop.
        """
        loop = asyncio.get_running_loop()
        if self._save_lock is None or self._save_loop is not loop:
            self._save_lock = asyncio.Lock()
            self._save_loop = loop
        return self._save_lock

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
        providers = self._providers_of(topic)
        consumers = self._consumers_of(topic)
        response: dict[str, Any] = {
            "topic": topic,
            "providers": providers,
            "consumers": consumers,
            "invocations": len(events),
        }
        if events:
            response["errors"] = sum(1 for event in events if not is_successful(event.status))
            response["statusCounts"] = dict(Counter(event.status for event in events))
        # mesh.md §4.2: per declared edge, report last-observed-at (or its absence) rather than
        # collapsing liveness to a boolean — an absent entry is a decommission *candidate*, not a fact.
        if providers:
            response["providerActivity"] = self._edge_activity(self._observed_providers(topic), providers)
        if consumers:
            response["consumerActivity"] = self._edge_activity(self._observed_consumers(topic), consumers)
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

    def _consumers_of(self, topic: str) -> list[str]:
        return sorted(name for name, record in self._services.items() if topic in record.consumed)

    def _observed_providers(self, topic: str) -> dict[str, str | None]:
        """{service: last-observed-at or None} for every service traced handling ``topic`` — the §4.2
        observed signal for provider liveness (never used to admit a provider edge into the graph)."""
        observed: dict[str, str | None] = {}
        for event in self._events:
            if event.topic == topic:
                self._note_observed(observed, event.service, event.started_at)
        return observed

    def _observed_consumers(self, topic: str) -> dict[str, str | None]:
        """{service: last-observed-at or None} for every service whose span parented a traced call to
        ``topic`` by a different service — the §4.2 observed signal for consumer liveness."""
        observed: dict[str, str | None] = {}
        for event in self._events:
            if event.topic != topic or not event.parent_span_id:
                continue
            caller = self._span_owner.get(event.parent_span_id)
            if caller and caller != event.service:
                self._note_observed(observed, caller, event.started_at)
        return observed

    @staticmethod
    def _note_observed(observed: dict[str, str | None], service: str, started_at: str | None) -> None:
        current = observed.get(service) if service in observed else None
        if started_at and (current is None or started_at > current):
            observed[service] = started_at
        elif service not in observed:
            observed[service] = None

    @staticmethod
    def _edge_activity(
        observed: dict[str, str | None], declared: list[str]
    ) -> dict[str, dict[str, str]]:
        """Every declared name -> ``{"lastObservedAt": ...}`` when known, else ``{}`` (observed with no
        timestamp, or never observed at all — mesh.md §4.2 deliberately treats "unobserved" as a
        decommission *candidate*, not a removal, so an undeclared entry is never synthesized here)."""
        activity: dict[str, dict[str, str]] = {}
        for name in declared:
            last_observed_at = observed.get(name)
            activity[name] = {"lastObservedAt": last_observed_at} if last_observed_at else {}
        return activity

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


def _safe_int(value: Any) -> int:
    """Coerce a count to int, treating a null/garbage value as 0 (skip-the-bad-part, don't crash)."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _topic_spec(topic: dict[str, Any]) -> dict[str, Any]:
    """The contract detail on a descriptor/spec topic entry: version + non-empty payload schemas.

    An empty schema (``{}``) means "no schema" in the descriptor, so it is omitted rather than stored —
    the artifact projection then renders it as absent rather than an empty object.
    """
    spec: dict[str, Any] = {}
    version = topic.get("version")
    if version:
        spec["version"] = str(version)
    for key in ("requestSchema", "responseSchema", "messageSchema"):
        if topic.get(key):
            spec[key] = topic[key]
    return spec


_CollectorMethod = Callable[[dict[str, Any]], dict[str, Any]]


def _ingest_handler(collector: MeshCollector, method: _CollectorMethod) -> Handler:
    async def handler(request: dict[str, Any]) -> Result:
        try:
            return Result.ok(await collector.persist_off_loop(method, request))
        except CollectorBadRequest as exc:
            return Result.bad_request(str(exc))

    return handler


def _query_handler(method: _CollectorMethod) -> Handler:
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
    registry.register(REGISTER_TOPIC, _ingest_handler(collector, collector.ingest_register))
    registry.register(HEARTBEAT_TOPIC, _ingest_handler(collector, collector.ingest_heartbeat))
    registry.register(TRACES_TOPIC, _ingest_handler(collector, collector.ingest_traces))
    registry.register(ISSUES_TOPIC, _ingest_handler(collector, collector.ingest_issues))
    registry.register(QUERY_FLEET_TOPIC, _query_handler(collector.query_fleet))
    registry.register(QUERY_SERVICE_TOPIC, _query_handler(collector.query_service))
    registry.register(QUERY_TOPIC_TOPIC, _query_handler(collector.query_topic))
    registry.register(QUERY_TRACE_TOPIC, _query_handler(collector.query_trace))
    return registry
