"""The mesh collector — the receiving side that ingests feeds and renders the fleet (mesh.md §§4–6).

A collector is *an ordinary Benzene service* serving the four ingest topics
(``benzene:mesh:register`` / ``heartbeat`` / ``traces`` / ``issues``) plus a set of ``mesh:query:*``
read models. :class:`MeshCollector` holds the in-memory catalog and derives the fleet view from what
services report; :func:`collector_registry` wires it onto a :class:`~benzene.core.Registry` so the
collector runs through the normal pipeline (it dogfoods the framework it collects for).

Derivation rules that are normative (mesh.md §4):

- ``service`` is required on register, heartbeat, and issues → ``bad-request`` when missing; a traces
  or issues batch of any size (including empty) is accepted.
- **The register body is the service's Contract Document** (mesh.md §2). Only ``service`` is
  required: a member that is absent, or named in ``degraded``, is unknown — it contributes no edges and
  its emptiness asserts nothing.
- Re-registration **replaces** a service's registration wholesale — a redeploy that drops a topic from
  ``requests`` drops its consumer edge, and one that drops it from ``events`` or ``sends`` drops its
  provider edge the same way.
- **The producer/consumer graph is built from the latest registration alone**: ``requests`` gives
  consumer edges (a handler registration is that topic's consumer); ``events`` ∪ ``sends`` gives
  provider edges (a service publishing an event is its provider exactly as one sending to a topic is).
  ``benzene:`` entries are ignored. Trace parentage MUST NOT be used to admit an edge into this graph —
  it feeds invocation counts and status stats only.
- **Heartbeats carry ``registrationHash``** (mesh.md §5). This collector keys its catalog by
  ``service`` alone (it does not implement Service Versions), so it compares the hash with the
  service's one registration.
- **Declared vs. observed** (mesh.md §4.2): a trace never changes the graph, but it *is* the only signal
  for two collector-derived read models layered on top of it — **liveness** (has a declared edge ever
  been exercised, so an unexercised one is a decommission *candidate*, never a fact) and **drift** (a
  traced edge nobody declared, filed as a ``contract-drift`` issue the moment it's first observed on a
  service that *has* registered — an anonymous/never-registered service has nothing to diverge from,
  so it is never flagged, and neither is a member its registration left unknown).

The ``benzene:mesh:query:*`` shapes follow the reference collector and are pinned by
``mesh-collector-cases.json`` as the observable surface for the ingest/derivation rules.
"""

from __future__ import annotations

import copy
import math
import re
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from benzene.core import Handler, Registry
from benzene.results import Result, is_successful

from .feeds import HEARTBEAT_TOPIC, ISSUES_TOPIC, REGISTER_TOPIC, TRACES_TOPIC
from .issues import decode_issue_errors, issue_fingerprint, merge_issue_errors
from .specdoc import (
    is_reserved_topic,
    known_members,
    resolve_schema,
    schema_catalogue,
    spec_entries,
    spec_topic_ids,
)
from .store import CollectorStore, NullCollectorStore

# Bumped when the snapshot shape changes incompatibly; an older/newer snapshot is ignored on load.
# 4: the registration is the Contract Document (registrationHash, known members, the document itself).
_SNAPSHOT_VERSION = 4

# Query topics (the collector's read models — one collector's shapes, pinned by the fixtures).
QUERY_FLEET_TOPIC = "benzene:mesh:query:fleet"
QUERY_SERVICE_TOPIC = "benzene:mesh:query:service"
QUERY_TOPIC_TOPIC = "benzene:mesh:query:topic"
QUERY_TRACE_TOPIC = "benzene:mesh:query:trace"

# The feeds a service can report, in the order they are listed in `missingFeeds`. `descriptor` is the
# read models' name for the registration (mesh.md §6). `issues` is special: it is only "missing" when a
# failure needs explaining (see `_missing_feeds`).
_FEEDS = ("descriptor", "health", "traces", "issues")

# Merged issue exemplars keep the newest few (mesh.md §4.1).
_MAX_EXEMPLARS = 3
# Issue fields the collector aggregates itself — everything else is "latest-wins" on merge.
_AGGREGATED_ISSUE_FIELDS = frozenset({"count", "errors", "exemplarTraceIds", "firstSeen", "lastSeen"})
# The fleet view's recent flows, newest first.
_MAX_FLEET_TRACES = 20


class CollectorError(Exception):
    """Base for a collector ingest/query failure — catch this to handle either kind at once."""


class CollectorBadRequest(CollectorError):
    """A malformed ingest/query (e.g. a required identifier missing) → ``bad-request``."""


class CollectorNotFound(CollectorError):
    """A query for an unknown service / topic / trace → ``not-found``."""


@dataclass
class _Instance:
    healthy: bool
    registration_hash: str | None
    service_version: str | None = None
    health_checks: dict[str, Any] = field(default_factory=dict)  # the heartbeat's per-check detail


@dataclass
class _Service:
    name: str
    registered: bool = False
    registration_hash: str | None = None
    previous_registration_hash: str | None = None  # the hash before the last contract change (drift)
    consumed: list[str] = field(default_factory=list)  # topics it currently consumes (`requests`)
    produced: list[str] = field(default_factory=list)  # topics it currently provides (`events` ∪ `sends`)
    # The members the registration actually asserts (absent or `degraded` ones are unknown).
    known: list[str] = field(default_factory=list)
    # The registered Contract Document, as it arrived — the service's own self-description.
    document: dict[str, Any] = field(default_factory=dict)
    # Per-topic contract detail from `requests`: topic -> {version, requestSchema, responseSchema},
    # schemas dereferenced against the document's components.
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
    duration_ms: float = 0.0


class MeshCollector:
    """The catalog: ingest the feeds, derive the fleet. One instance per collector.

    In-memory by default. Pass a :class:`~benzene.mesh.CollectorStore` to make the catalog durable:
    the collector restores from it on construction and writes a fresh snapshot after every mutating
    ingest, so a restarted host rehydrates the fleet it already knew. The default store keeps nothing,
    so tests and single runs behave exactly as before and pay nothing.
    """

    def __init__(self, *, store: CollectorStore | None = None) -> None:
        self._services: dict[str, _Service] = {}
        self._topics: set[str] = set()  # every topic ever seen (registered or traced); grows only
        self._ever_consumed: set[str] = (
            set()
        )  # topics some service ever declared as a handler (for removed-topic detection)
        self._events: list[_Event] = []
        # span id -> the service that emitted it. Used only for the §4.2 observed-signal (liveness,
        # drift) below — NEVER for graph membership (`_providers_of`/`_consumers_of` are declared-only).
        self._span_owner: dict[str, str] = {}
        self._issues: dict[str, dict[str, Any]] = {}  # fingerprint -> merged issue
        self._store: CollectorStore = store or NullCollectorStore()
        saved = self._store.load()
        if saved is not None:
            self.restore(saved)

    # --- ingest ----------------------------------------------------------------------------
    def ingest_register(self, body: dict[str, Any]) -> dict[str, Any]:
        """Ingest a registration — a Contract Document (mesh.md §2), reduced to ``service`` at least."""
        service = _require(body, "service")
        record = self._service(service)
        new_hash = body.get("registrationHash")
        # A changed contract hash on a service we already knew is drift — remember the prior hash.
        if record.registered and record.registration_hash and new_hash != record.registration_hash:
            record.previous_registration_hash = record.registration_hash
        # Re-registration replaces wholesale: this service's old provider/consumer edges and specs go.
        record.known = sorted(known_members(body))
        record.consumed = spec_topic_ids(body, "requests")
        record.produced = spec_topic_ids(body, "events", "sends")
        # Keep the prior contracts so the artifact writer can flag which topics changed schema.
        record.previous_topic_specs = record.topic_specs
        catalogue = schema_catalogue(body)
        record.topic_specs = {
            str(entry["topic"]): _topic_spec(entry, catalogue)
            for entry in spec_entries(body, "requests")
        }
        record.document = copy.deepcopy(body)
        record.registered = True
        record.registration_hash = str(new_hash) if new_hash else None
        self._topics.update(record.consumed)
        self._topics.update(record.produced)
        self._ever_consumed.update(record.consumed)  # remember it was declared, even if later dropped
        return self._persisted({"accepted": 1})

    def ingest_heartbeat(self, body: dict[str, Any]) -> dict[str, Any]:
        service = _require(body, "service")
        record = self._service(service)
        instance_id = str(body.get("instanceId", ""))
        health = body.get("health") or {}
        checks = health.get("healthChecks")
        version = body.get("serviceVersion")
        record.instances[instance_id] = _Instance(
            healthy=bool(health.get("isHealthy", False)),
            registration_hash=body.get("registrationHash"),
            service_version=str(version) if version else None,
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
                duration_ms=_duration_ms(raw.get("durationMs")),
            )
            self._events.append(event)
            self._span_owner[event.span_id] = event.service
            self._topics.add(event.topic)
            callee = self._service(event.service)  # a traced service becomes known (possibly anonymous)
            if "requests" in callee.known:
                self._flag_drift_if_undeclared(callee, event.topic, callee.consumed, event.trace_id)
            parent = event.parent_span_id
            caller_name = self._span_owner.get(parent) if parent else None
            if caller_name and caller_name != event.service:
                caller = self._services.get(caller_name)
                # Only a registered caller that asserted both its events and its sends has a provider
                # side to diverge from: an unknown member may hold the topic.
                if caller is not None and {"events", "sends"} <= set(caller.known):
                    self._flag_drift_if_undeclared(caller, event.topic, caller.produced, event.trace_id)
        return self._persisted({"accepted": len(events)})

    def _flag_drift_if_undeclared(
        self, record: _Service, topic: str, declared: list[str], trace_id: str
    ) -> None:
        """File a ``contract-drift`` issue the first time a trace observes ``record`` on a topic it
        hasn't declared — either as a consumer (``topic`` not in its ``requests``) or as a provider
        (``topic`` in neither its ``events`` nor its ``sends``), mesh.md §4.2's "Undeclared" signal. A
        service that has never registered has no declared contract to diverge from, so it is never
        flagged.
        """
        if not record.registered or topic in declared or is_reserved_topic(topic):
            return  # a benzene: topic is never declared (contract-document §1), so never undeclared
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
            merged.pop("errors", None)
            _merge_issue_errors(merged, issue.get("errors"))
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
        _merge_issue_errors(existing, issue.get("errors"))
        first = issue.get("firstSeen")
        if first is not None:
            existing["firstSeen"] = min(existing.get("firstSeen") or first, first)
        last = issue.get("lastSeen")
        if last is not None:
            existing["lastSeen"] = max(existing.get("lastSeen") or last, last)

    # --- persistence -----------------------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        """The whole catalog as a JSON-able dict — what a :class:`CollectorStore` persists."""
        return {
            "version": _SNAPSHOT_VERSION,
            "services": [
                {
                    "name": record.name,
                    "registered": record.registered,
                    "registrationHash": record.registration_hash,
                    "previousRegistrationHash": record.previous_registration_hash,
                    "consumed": record.consumed,
                    "produced": record.produced,
                    "known": record.known,
                    "document": copy.deepcopy(record.document),
                    "topicSpecs": copy.deepcopy(record.topic_specs),
                    "previousTopicSpecs": copy.deepcopy(record.previous_topic_specs),
                    "reportedIssues": record.reported_issues,
                    "instances": [
                        {
                            "instanceId": instance_id,
                            "healthy": instance.healthy,
                            "registrationHash": instance.registration_hash,
                            "serviceVersion": instance.service_version,
                            "healthChecks": copy.deepcopy(instance.health_checks),
                        }
                        for instance_id, instance in record.instances.items()
                    ],
                }
                for record in self._services.values()
            ],
            "topics": sorted(self._topics),
            "everConsumed": sorted(self._ever_consumed),
            "events": [
                {
                    "traceId": event.trace_id,
                    "spanId": event.span_id,
                    "parentSpanId": event.parent_span_id,
                    "service": event.service,
                    "topic": event.topic,
                    "status": event.status,
                    "startedAt": event.started_at,
                    "durationMs": event.duration_ms,
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
                registered=bool(entry.get("registered", False)),
                registration_hash=entry.get("registrationHash"),
                previous_registration_hash=entry.get("previousRegistrationHash"),
                consumed=list(entry.get("consumed", [])),
                produced=list(entry.get("produced", [])),
                known=list(entry.get("known", [])),
                document=copy.deepcopy(entry.get("document", {})),
                topic_specs=copy.deepcopy(entry.get("topicSpecs", {})),
                previous_topic_specs=copy.deepcopy(entry.get("previousTopicSpecs", {})),
                reported_issues=bool(entry.get("reportedIssues", False)),
                instances={
                    str(inst["instanceId"]): _Instance(
                        healthy=bool(inst.get("healthy", False)),
                        registration_hash=inst.get("registrationHash"),
                        service_version=inst.get("serviceVersion"),
                        health_checks=copy.deepcopy(inst.get("healthChecks", {})),
                    )
                    for inst in entry.get("instances", [])
                },
            )
            self._services[record.name] = record
        self._topics = set(snapshot.get("topics", []))
        self._ever_consumed = set(snapshot.get("everConsumed", []))
        self._events = [
            _Event(
                trace_id=str(raw.get("traceId", "")),
                span_id=str(raw.get("spanId", "")),
                parent_span_id=raw.get("parentSpanId"),
                service=str(raw.get("service", "")),
                topic=str(raw.get("topic", "")),
                status=str(raw.get("status", "")),
                started_at=raw.get("startedAt"),
                duration_ms=_duration_ms(raw.get("durationMs")),
            )
            for raw in snapshot.get("events", [])
        ]
        # Rebuild the span-owner index from the restored events — a pure index over `_events`, not
        # itself persisted, needed again after restart for the §4.2 observed-signal derivation.
        self._span_owner = {event.span_id: event.service for event in self._events}
        # Deep-copied so the restored collector never shares nested issue dicts with the snapshot it
        # was given (the JSON-file store hands back fresh parses; an in-memory snapshot would not).
        self._issues = copy.deepcopy(snapshot.get("issues", {}))

    def _persisted(self, result: dict[str, Any]) -> dict[str, Any]:
        """Write the catalog through the store after a mutating ingest, then return ``result``."""
        self._store.save(self.snapshot())
        return result

    # --- queries ---------------------------------------------------------------------------
    def query_fleet(self, _body: dict[str, Any]) -> dict[str, Any]:
        services = [self._fleet_service_entry(record) for record in self._services.values()]
        # The fleet summary shows one role per topic for brevity (full provider/consumer detail is
        # `query_topic`'s job): consumers (handler registrations), since that's the side essentially
        # every registered service populates, unlike the rarer `events`/`sends` declarations.
        topics = [
            {
                "topic": topic,
                "consumers": self._consumers_of(topic),
                "invocations": self._invocations_on(topic),
            }
            for topic in sorted(self._topics)
        ]
        return {
            "services": services,
            "topics": topics,
            "traces": self._trace_summaries(),
            "issues": list(self._issues.values()),
        }

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
                    # One registration per service here (no Service Versions), so that is what every
                    # instance's heartbeat hash is compared with (mesh.md §5).
                    "hashMatches": instance.registration_hash == record.registration_hash,
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
        # Providers are the `events`/`sends` (publisher/caller) role, so their observed signal is who
        # was traced as the *caller*; consumers are the `requests` (handler) role, observed as who was
        # traced actually *handling* the topic.
        if providers:
            response["providerActivity"] = self._edge_activity(self._observed_callers(topic), providers)
        if consumers:
            response["consumerActivity"] = self._edge_activity(self._observed_handlers(topic), consumers)
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
        # A provider is a service whose registration publishes (`events`) or sends to (`sends`) this
        # topic — the publisher/caller role (mesh.md §4).
        return sorted(name for name, record in self._services.items() if topic in record.produced)

    def _consumers_of(self, topic: str) -> list[str]:
        # A consumer is a service whose registration serves this topic (`requests`, its handlers) —
        # the receiver role (mesh.md §4).
        return sorted(name for name, record in self._services.items() if topic in record.consumed)

    def _observed_handlers(self, topic: str) -> dict[str, str | None]:
        """{service: last-observed-at or None} for every service traced handling ``topic`` — the §4.2
        observed signal for *consumer* (handler) liveness (never used to admit an edge into the graph)."""
        observed: dict[str, str | None] = {}
        for event in self._events:
            if event.topic == topic:
                self._note_observed(observed, event.service, event.started_at)
        return observed

    def _observed_callers(self, topic: str) -> dict[str, str | None]:
        """{service: last-observed-at or None} for every service whose span parented a traced call to
        ``topic`` by a different service — the §4.2 observed signal for *provider* (sender) liveness."""
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

    def _trace_summaries(self) -> list[dict[str, Any]]:
        """The fleet view's recent flows, newest first — one summary per trace id.

        A flow's ``status`` (mesh.md §4) is that of its earliest-started failing event when it failed,
        else of its earliest-started event; an event is failing exactly when it makes ``failed`` true,
        and equal start times keep receipt order. An event with no readable ``startedAt`` sorts after
        every timed one, and a flow whose deciding event carries no status omits ``status``.

        Every summary carries ``startedAt`` (the earliest readable start, as received) and
        ``durationMs`` (latest event end − earliest start, as benzene-go's meshd computes it). An
        event with no readable start cannot be placed on the timeline, so it moves neither; its own
        duration is still a lower bound on the flow's. A flow with no readable start at all has
        ``startedAt: ""`` — a string a reader can render, claiming no time — and sorts last.
        """
        flows: dict[str, list[_Event]] = {}
        for event in self._events:  # receipt order
            flows.setdefault(event.trace_id, []).append(event)
        summaries: list[tuple[datetime | None, dict[str, Any]]] = []
        for trace_id, events in flows.items():
            ordered = sorted(events, key=_start_order)  # stable: ties keep receipt order
            failing = [event for event in ordered if not is_successful(event.status)]
            summary: dict[str, Any] = {
                "traceId": trace_id,
                "events": len(events),
                "services": sorted({event.service for event in events if event.service}),
                "failed": bool(failing),
            }
            started = _instant(ordered[0].started_at)
            summary["startedAt"] = ordered[0].started_at if started is not None else ""
            summary["durationMs"] = _flow_duration_ms(events, started)
            status = (failing or ordered)[0].status
            if status:
                summary["status"] = status
            summaries.append((started, summary))
        # Newest first (a stable sort), a flow with no known start time after every timed one.
        timed = sorted(
            (pair for pair in summaries if pair[0] is not None),
            key=lambda pair: pair[0] or _EPOCH,
            reverse=True,
        )
        untimed = [pair for pair in summaries if pair[0] is None]
        return [summary for _, summary in (timed + untimed)[:_MAX_FLEET_TRACES]]

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
            "descriptor": record.registered,  # the read models' name for the registration
            "health": bool(record.instances),
            "traces": self._invocations_by(record.name) > 0,
            # `issues` is only "missing" when a failure needs explaining and no issue feed arrived.
            "issues": record.reported_issues or not has_failure,
        }
        return [feed for feed in _FEEDS if not present[feed]]

    def _fleet_service_entry(self, record: _Service) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "service": record.name,
            "topics": len(record.consumed),
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


def _merge_issue_errors(merged: dict[str, Any], incoming: Any) -> None:
    """Merge an incoming issue's ``errors`` delta into ``merged`` by (field, code) identity, bounded
    and ordered as mesh.md §4.1 says; an invalid entry is skipped, and ``errors`` is reported only when
    the issue holds any."""
    existing = decode_issue_errors(merged.get("errors"))
    combined = merge_issue_errors(existing, decode_issue_errors(incoming))
    if combined:
        merged["errors"] = [entry.to_payload() for entry in combined]
    else:
        merged.pop("errors", None)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MILLISECOND = timedelta(milliseconds=1)


def _duration_ms(value: Any) -> float:
    """An event's ``durationMs``; anything but a finite, non-negative number (a JSON ``true``
    included) reads as 0 rather than failing the batch."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    duration = float(value)
    return duration if math.isfinite(duration) and duration >= 0 else 0.0


def _flow_duration_ms(events: list[_Event], started: datetime | None) -> float:
    """(latest event end) − (earliest start) in milliseconds, over the events with a readable start;
    never shorter than the longest single event, since each one ran inside the flow."""
    longest = max(event.duration_ms for event in events)
    if started is None:
        return longest
    end = started
    for event in events:
        instant = _instant(event.started_at)
        if instant is None:
            continue
        try:
            end = max(end, instant + timedelta(milliseconds=event.duration_ms))
        except OverflowError:  # an absurd duration past datetime's range: its own length still counts
            continue
    return max((end - started) / _MILLISECOND, longest)
_FRACTION = re.compile(r"\.(\d+)")


def _instant(started_at: str | None) -> datetime | None:
    """``startedAt`` as an aware instant, or ``None`` when absent or unreadable.

    A flow's events are ordered by *time*, not by text: ``…09:00:00Z`` precedes ``…09:00:00.005Z``,
    which a string comparison gets backwards. Any fractional precision is accepted (``fromisoformat``
    before Python 3.11 takes only three or six digits), and an offset-less stamp is read as UTC.
    """
    if not isinstance(started_at, str) or not started_at:
        return None
    text = _FRACTION.sub(lambda m: "." + (m.group(1) + "000000")[:6], started_at, count=1)
    text = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _start_order(event: _Event) -> tuple[bool, datetime]:
    """Sort key for a flow's events: by start instant, an unreadable one after every timed one."""
    instant = _instant(event.started_at)
    return (instant is None, instant or _EPOCH)


def _safe_int(value: Any) -> int:
    """Coerce a count to int, treating a null/garbage value as 0 (skip-the-bad-part, don't crash)."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _topic_spec(entry: dict[str, Any], catalogue: dict[str, Any]) -> dict[str, Any]:
    """The contract detail of a ``requests`` entry: version + non-empty, dereferenced schemas.

    An empty schema (``{}``) constrains nothing, so it is omitted rather than stored — the artifact
    projection then renders it as absent rather than an empty object.
    """
    spec: dict[str, Any] = {}
    version = entry.get("version")
    if version:
        spec["version"] = str(version)
    for member, key in (("request", "requestSchema"), ("response", "responseSchema")):
        schema = resolve_schema(entry.get(member), catalogue)
        if schema:
            spec[key] = schema
    return spec


_CollectorMethod = Callable[[dict[str, Any]], dict[str, Any]]


def _ingest_handler(method: _CollectorMethod) -> Handler:
    async def handler(request: dict[str, Any]) -> Result:
        try:
            return Result.ok(method(request))
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
    registry.register(REGISTER_TOPIC, _ingest_handler(collector.ingest_register))
    registry.register(HEARTBEAT_TOPIC, _ingest_handler(collector.ingest_heartbeat))
    registry.register(TRACES_TOPIC, _ingest_handler(collector.ingest_traces))
    registry.register(ISSUES_TOPIC, _ingest_handler(collector.ingest_issues))
    registry.register(QUERY_FLEET_TOPIC, _query_handler(collector.query_fleet))
    registry.register(QUERY_SERVICE_TOPIC, _query_handler(collector.query_service))
    registry.register(QUERY_TOPIC_TOPIC, _query_handler(collector.query_topic))
    registry.register(QUERY_TRACE_TOPIC, _query_handler(collector.query_trace))
    return registry
