"""Reports this service into the mesh collector — register, heartbeat, and pushed traces.

Mirrors .NET's ``WithCollector(...)`` on ``UseBenzeneCloudService``: best-effort, on an interval, never
affecting request traffic. Unlike the Lambda fleet demo (``deploy/mesh/fleet/service.py``, which pushes
traces once per invocation because a Lambda only runs *during* an invocation), a Kubernetes pod is a
long-running process, so this runs its own periodic loop — closer to .NET's Cloud Service collector
integration, which also beats on a timer from an always-on host.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone

from benzene.core import ContractDocument, HealthChecks
from benzene.mesh import Heartbeat, MeshFeedSender, QueueTraceExporter


class MeshReporter:
    """Periodically registers its Contract Document, heartbeats, and drains+pushes traces to a mesh
    collector (mesh.md §§2–5)."""

    def __init__(
        self,
        feeds: MeshFeedSender,
        document: ContractDocument,
        exporter: QueueTraceExporter,
        *,
        instance_id: str,
        health: HealthChecks | None = None,
        interval_seconds: float = 15.0,
    ) -> None:
        self._feeds = feeds
        self._document = document
        self._exporter = exporter
        self._instance_id = instance_id
        self._health = health or HealthChecks().add("self", lambda: True)
        self._interval = interval_seconds

    async def report_once(self) -> None:
        """Register + heartbeat + drain-and-push traces once. Never raises: a collector hiccup
        must never affect the service it observes (mesh.md, Degradation)."""
        with contextlib.suppress(Exception):
            await self._feeds.register(self._document)
        report = await self._health.run()
        with contextlib.suppress(Exception):
            await self._feeds.publish_heartbeat(
                # registrationHash + serviceVersion straight off the registered document (mesh.md §5)
                Heartbeat.for_document(
                    self._document,
                    sent_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    instance_id=self._instance_id,
                    is_healthy=report.is_healthy,
                    health_checks={name: r.to_payload() for name, r in report.checks.items()},
                )
            )
        events = self._exporter.drain()
        if events:
            with contextlib.suppress(Exception):
                await self._feeds.publish_traces(events)

    async def run_forever(self) -> None:
        """The background loop the host runs alongside the HTTP server."""
        while True:
            await self.report_once()
            await asyncio.sleep(self._interval)
