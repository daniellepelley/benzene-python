"""The mesh **aggregator** — poll a registry of services, project the fleet, emit the UI artifacts.

Where :class:`~benzene.mesh.MeshArtifactEmitter` is the pure projection (catalog spine + a collector →
the six mesh-UI artifacts), :class:`MeshAggregator` is the *live driver* around it: on each pass it
HTTP-fetches every registered service's ``/benzene/spec`` and ``/benzene/health``, tolerates the ones
it can't reach (an ``error`` snapshot, not a failed run), builds the :class:`ServiceCatalog` spine,
reads the co-hosted :class:`~benzene.mesh.MeshCollector` (queried in-process — the collector ingests
its feeds over HTTP, but the aggregator shares the object and queries it directly), and writes the
artifacts to a directory the host serves.

This mirrors .NET's ``Benzene.Mesh.Aggregator.MeshAggregator.RunOnceAsync(registry)`` — one pass over a
``MeshServiceRegistry`` — kept deliberately smaller: the Python port's aggregator emits exactly the six
language-neutral mesh-UI artifacts the :class:`~benzene.mesh.MeshArtifactEmitter` owns (no AsyncAPI
composite / usage-adapter fan-out yet).

Two seams keep the local/compose case and a hosted (scheduled-trigger) case apart, as in .NET:

- :func:`run_poll_loop` is the timer-driven background loop (mirrors ``MeshPollBackgroundService``) —
  a failed pass is logged and never crashes the host; the dashboard shows stale data until the next
  good pass.
- :func:`aggregate_handler` wraps :meth:`MeshAggregator.run_once` as a Benzene handler for a
  ``mesh:aggregate`` topic, so a hosted deployment triggers a pass from a scheduler instead
  (``MeshAggregateMessageHandler``'s shape) — the same one pass, a different clock.

The aggregator's HTTP dependency is why this module (and :mod:`benzene.mesh.host`) live behind the
``benzene-mesh[host]`` extra and are *not* imported from :mod:`benzene.mesh`'s top level — importing
``benzene.mesh`` for descriptors/tracing never pulls in ``benzene-http``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from benzene.core import Handler
from benzene.http import HttpGet, stdlib_get_transport
from benzene.results import Result

from .artifacts import (
    HttpMapping,
    MeshArtifactEmitter,
    ServiceCatalog,
    TopologySource,
    UsageSource,
)
from .collector import MeshCollector
from .registry import MeshServiceEntry, MeshServiceRegistry

# MeshServiceEntry / MeshServiceRegistry are defined in the dependency-light .registry module (no
# benzene-http) so a discovery source can produce a registry without importing this HTTP-fetching
# aggregator; they are re-exported here so ``from benzene.mesh.aggregator import MeshServiceEntry`` keeps
# working for every existing caller.
__all__ = [
    "AGGREGATE_TOPIC",
    "MeshAggregator",
    "MeshServiceEntry",
    "MeshServiceRegistry",
    "SpecHealthSource",
    "aggregate_handler",
    "run_poll_loop",
]

_LOGGER = logging.getLogger("benzene.mesh.aggregator")

#: The reserved topic a hosted deployment triggers to run one aggregation pass (scheduled seam).
AGGREGATE_TOPIC = "mesh:aggregate"


@dataclass
class _Fetched:
    """One service's fetched spine: parsed spec/health docs, or an error explaining the miss."""

    spec: dict[str, Any] | None
    health: dict[str, Any] | None
    error: str | None


class SpecHealthSource:
    """Fetches a service's ``/benzene/spec`` + ``/benzene/health`` over HTTP (the default source).

    Injectable (:class:`MeshAggregator` takes any object with ``fetch``), so a test drives the
    aggregator with a fake fleet and no network. The default uses the stdlib GET transport, bounding
    each fetch by ``timeout`` seconds so one hung service can't stall a pass.

    Reachability rules mirror .NET's ``HttpMeshServiceSource``:

    - **spec** — a non-200 or a connection error means "unreachable" (spec ``None``, ``error`` set).
    - **health** — the body is read *regardless of status code*, so a service that maps an unhealthy
      aggregate to HTTP 503 (Benzene's health middleware does) still contributes a real ``unhealthy``
      snapshot; only a connection-level failure makes health ``None`` (unreachable).
    """

    def __init__(self, *, timeout: float = 10.0, get: HttpGet | None = None) -> None:
        self._get = get or stdlib_get_transport(timeout=timeout)

    async def fetch(self, entry: MeshServiceEntry) -> _Fetched:
        spec, spec_error = await self._fetch_spec(entry)
        health, health_error = await self._fetch_health(entry)
        # A service reachable for neither doc is "unreachable"; one reachable doc still describes it.
        error = None
        if spec is None and health is None:
            error = spec_error or health_error or "unreachable"
        elif spec is None:
            error = spec_error
        return _Fetched(spec=spec, health=health, error=error)

    async def _fetch_spec(self, entry: MeshServiceEntry) -> tuple[dict[str, Any] | None, str | None]:
        url = entry.resolved_spec_url()
        try:
            reply = await self._get(url)
        except Exception as exc:  # a connection-level failure — genuinely unreachable
            return None, f"{type(exc).__name__}: {exc}"
        if reply.status_code != 200:
            return None, f"spec fetch returned HTTP {reply.status_code}"
        try:
            parsed = json.loads(reply.body)
        except (ValueError, TypeError):
            return None, "spec body is not valid JSON"
        return (parsed if isinstance(parsed, dict) else None), (
            None if isinstance(parsed, dict) else "spec body is not a JSON object"
        )

    async def _fetch_health(
        self, entry: MeshServiceEntry
    ) -> tuple[dict[str, Any] | None, str | None]:
        url = entry.resolved_health_url()
        try:
            reply = await self._get(url)
        except Exception as exc:
            return None, f"{type(exc).__name__}: {exc}"
        # 404 (health surface not exposed) is unreachable-for-health; a 200/503 with a JSON body is a
        # real aggregate. Read the body regardless of the status code (503 carries a valid one).
        try:
            parsed = json.loads(reply.body)
        except (ValueError, TypeError):
            return None, f"health fetch returned HTTP {reply.status_code}"
        if not isinstance(parsed, dict) or "isHealthy" not in parsed:
            return None, f"health fetch returned HTTP {reply.status_code}"
        return parsed, None


class MeshAggregator:
    """Runs one aggregation pass over a :class:`MeshServiceRegistry`, emitting the six mesh-UI artifacts.

    Construct it with the co-hosted :class:`~benzene.mesh.MeshCollector` (the same object the host's
    collector service ingests HTTP feeds into) and call :meth:`run_once`. Contract-drift is computed
    against the previous pass's spec hash, remembered per service across passes (the .NET aggregator
    reads it back from the artifact store; a single long-running host keeps it in memory instead) —
    seed :paramref:`previous_hashes` to surface drift on the very first pass.

    A per-service spec/health fetch failing never fails the pass (that service becomes an ``error``
    snapshot but stays in the catalog if its live feeds describe it); :func:`run_poll_loop` additionally
    guards the whole pass so nothing here can crash the host.

    ``topology_source`` / ``usage_source`` are the optional enrichment hooks passed straight through to
    the :class:`~benzene.mesh.MeshArtifactEmitter` on every pass: a hosted deployment constructs the AWS
    sources (:class:`~benzene.mesh.aws.XRayTopologySource`,
    :class:`~benzene.mesh.aws.CloudWatchUsageSource`) once and hands them here, so each aggregation pass
    fills the topology plane's real latency percentiles + the usage feed from X-Ray/CloudWatch. Left
    unset, the pass emits exactly what the collector plane provides (today's behaviour, unchanged).
    """

    def __init__(
        self,
        collector: MeshCollector,
        *,
        source: SpecHealthSource | None = None,
        clock: Callable[[], datetime] | None = None,
        previous_hashes: Mapping[str, str] | None = None,
        annotations: Sequence[Mapping[str, Any]] = (),
        topology_source: TopologySource | None = None,
        usage_source: UsageSource | None = None,
    ) -> None:
        self._collector = collector
        self._source = source or SpecHealthSource()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._previous_hashes: dict[str, str] = dict(previous_hashes or {})
        self._annotations = [dict(annotation) for annotation in annotations]
        self._topology_source = topology_source
        self._usage_source = usage_source

    async def run_once(
        self,
        registry: MeshServiceRegistry,
        *,
        out_dir: str,
        generated_at: datetime | None = None,
    ) -> dict[str, Any]:
        """Poll every service once, project the fleet, and write the six artifacts under ``out_dir``.

        Returns the emitted ``manifest.json`` dict. Services are fetched concurrently, so one slow
        service adds only its own timeout to the pass, not everyone else's.
        """
        moment = generated_at or self._clock()
        entries = list(registry.services)
        fetched = await asyncio.gather(*(self._source.fetch(entry) for entry in entries))

        catalogs = [
            self._catalog(entry, result) for entry, result in zip(entries, fetched, strict=True)
        ]
        emitter = MeshArtifactEmitter(
            catalogs,
            self._collector,
            generated_at=moment,
            window_start=moment,
            window_end=moment,
            annotations=self._annotations,
            topology_source=self._topology_source,
            usage_source=self._usage_source,
        )
        manifest = emitter.emit(out_dir)

        # Remember this pass's hashes so the next pass can compute drift against them.
        for catalog in catalogs:
            if catalog.spec_hash is not None:
                self._previous_hashes[catalog.name] = catalog.spec_hash
        return manifest

    def _catalog(self, entry: MeshServiceEntry, fetched: _Fetched) -> ServiceCatalog:
        return ServiceCatalog(
            name=entry.name,
            spec=fetched.spec,
            health=fetched.health,
            spec_url=entry.resolved_spec_url(),
            health_url=entry.resolved_health_url(),
            previous_spec_hash=self._previous_hashes.get(entry.name),
            error=fetched.error,
            http_mappings=_http_mappings_from_spec(fetched.spec),
        )


def _http_mappings_from_spec(
    spec: Mapping[str, Any] | None,
) -> dict[str, list[HttpMapping]]:
    """Read the optional per-topic HTTP routes off a fetched ``/benzene/spec`` into the emitter's shape.

    The transport-neutral spec (topics + schemas) carries no route table, so a service's HTTP
    ``(method, path)`` mappings never reached the distributed aggregator — ``topics.json``
    ``consumers[].httpMappings`` came back empty for it (the "producer gap"). The Python HTTP binding now
    *optionally* rides those mappings along on ``/benzene/spec`` as ``topics[].http: [{method, path}]``
    (see :mod:`benzene.http.standard`); this reads them back into the ``topic id → [HttpMapping]`` map
    :class:`~benzene.mesh.ServiceCatalog` already threads into the emitter — so the distributed path fills
    the same HTTP column the in-process demo derives from its router. Purely additive: a spec without the
    field (every other port, a non-HTTP service) yields an empty map, exactly as before.
    """
    if not spec:
        return {}
    mappings: dict[str, list[HttpMapping]] = {}
    for topic in spec.get("topics", []) or []:
        if not isinstance(topic, Mapping):
            continue
        routes = topic.get("http")
        if not routes:
            continue
        topic_id = str(topic.get("id") or "")
        if not topic_id:
            continue
        parsed = [
            HttpMapping(str(route["method"]), str(route["path"]))
            for route in routes
            if isinstance(route, Mapping) and "method" in route and "path" in route
        ]
        if parsed:
            mappings[topic_id] = parsed
    return mappings


async def run_poll_loop(
    aggregator: MeshAggregator,
    registry: MeshServiceRegistry,
    *,
    out_dir: str,
    interval_seconds: float,
    stop: asyncio.Event | None = None,
    logger: logging.Logger | None = None,
    registry_provider: Callable[[], MeshServiceRegistry] | None = None,
) -> None:
    """Drive :meth:`MeshAggregator.run_once` on a timer until ``stop`` is set (mirrors ``MeshPollBackgroundService``).

    A failed pass is logged and swallowed — one bad pass must never stop future passes or crash the
    host; the dashboard simply shows stale data until the next good pass. This is the local/compose
    seam (a bare deployment has no external scheduler); a hosted deployment triggers
    :func:`aggregate_handler` from a scheduled invocation instead.

    ``registry_provider``, when given, is called **each pass** to obtain the registry (falling back to
    the static ``registry`` when it is ``None``) — this is the re-discovery seam: a hosted host wraps its
    discovery source (e.g. :class:`~benzene.mesh.aws.AwsLambdaDiscoveryProvider`) here so the fleet is
    re-enumerated every pass, picking up services that were created after the host booted and dropping
    ones that went away. A provider that raises is treated as a failed pass (logged, swallowed).
    """
    log = logger or _LOGGER
    stop = stop or asyncio.Event()
    interval = max(0.05, interval_seconds)
    while not stop.is_set():
        try:
            current = registry_provider() if registry_provider is not None else registry
            await aggregator.run_once(current, out_dir=out_dir)
        except Exception:  # noqa: BLE001 - a failed pass must never crash the host
            log.exception("Mesh aggregation pass failed")
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval)  # the interval elapsed → next pass


def aggregate_handler(
    aggregator: MeshAggregator, registry: MeshServiceRegistry, *, out_dir: str
) -> Handler:
    """A Benzene handler that runs one aggregation pass — the scheduled ``mesh:aggregate`` seam.

    Register it on ``mesh:aggregate`` so a hosted deployment triggers a pass from a scheduler (an EventBridge
    rule, a Cloud Scheduler job) instead of the in-process poll loop. Returns ``ok`` with the emitted
    manifest so the trigger's caller can observe the pass.
    """

    async def handler(_request: Any) -> Result:
        manifest = await aggregator.run_once(registry, out_dir=out_dir)
        return Result.ok(manifest)

    return handler
