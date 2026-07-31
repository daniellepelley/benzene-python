"""Service-side mesh feeds — reporting a service into a collector (mesh.md §§1–3).

A mesh-enabled service pushes four independent, optional feeds to a collector over any
envelope-capable transport (an outbound :class:`~benzene.core.MessageSender` — Pub/Sub, SNS/SQS,
Service Bus, or an HTTP POST of the wire envelope):

======================  ======================  ====================
Topic                   Body                    Success response
======================  ======================  ====================
``benzene:mesh:register``   ServiceDescriptor       ``{"accepted": 1}``
``benzene:mesh:heartbeat``  :class:`Heartbeat`      ``{"accepted": 1}``
``benzene:mesh:traces``     ``{"events": [...]}``   ``{"accepted": <count>}``
``benzene:mesh:issues``     IssueBatch              ``{"accepted": <count>}``
======================  ======================  ====================

**Every feed is independent and optional on both sides** (mesh.md §, Degradation): an unreachable
collector or a failing feed must never affect service traffic. :class:`MeshFeedSender` returns the
outbound :class:`~benzene.results.Result` so a caller can log a failed feed, but sending is
fire-and-report — it does not raise.

This is the *sender* half (a Python service reporting in). The collector that ingests these feeds
and answers ``mesh:query:*`` is a separate implementation concern (mesh.md collector conformance).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from benzene.core import MessageSender
from benzene.results import Result

from .descriptor import ServiceDescriptor
from .issues import IssueBatch
from .trace import TraceEvent

REGISTER_TOPIC = "benzene:mesh:register"
HEARTBEAT_TOPIC = "benzene:mesh:heartbeat"
TRACES_TOPIC = "benzene:mesh:traces"
ISSUES_TOPIC = "benzene:mesh:issues"


@dataclass(frozen=True)
class Heartbeat:
    """A liveness beat: identity + descriptor hash + the health aggregate (mesh.md §, Heartbeat).

    ``descriptor_hash`` lets the collector notice a descriptor change it hasn't learned yet (a hash
    mismatch means "re-register").
    """

    service: str
    sent_at: str
    instance_id: str | None = None
    descriptor_hash: str | None = None
    is_healthy: bool = True
    health_checks: Mapping[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"service": self.service, "sentAt": self.sent_at}
        if self.instance_id is not None:
            payload["instanceId"] = self.instance_id
        if self.descriptor_hash is not None:
            payload["descriptorHash"] = self.descriptor_hash
        payload["health"] = {
            "isHealthy": self.is_healthy,
            "healthChecks": dict(self.health_checks or {}),
        }
        return payload


class MeshFeedSender:
    """Pushes a service's mesh feeds to a collector over an outbound :class:`MessageSender`.

    One method per feed: :meth:`register` announces the service's descriptor once; the ongoing
    telemetry feeds — :meth:`publish_heartbeat`, :meth:`publish_traces`, :meth:`publish_issues` —
    are pushed periodically. Each returns the outbound :class:`~benzene.results.Result` so a caller
    can log a failed feed; sending never raises.
    """

    def __init__(self, sender: MessageSender) -> None:
        self._sender = sender

    async def register(self, descriptor: ServiceDescriptor) -> Result:
        """Announce the ServiceDescriptor on ``benzene:mesh:register``."""
        return await self._sender.send_message(REGISTER_TOPIC, descriptor.to_payload())

    async def publish_heartbeat(self, heartbeat: Heartbeat) -> Result:
        """Send a :class:`Heartbeat` to ``benzene:mesh:heartbeat``."""
        return await self._sender.send_message(HEARTBEAT_TOPIC, heartbeat.to_payload())

    async def publish_traces(self, events: Iterable[TraceEvent]) -> Result:
        """Send a batch of :class:`TraceEvent`s to ``benzene:mesh:traces`` as ``{"events": [...]}``."""
        body = {"events": [event.to_payload() for event in events]}
        return await self._sender.send_message(TRACES_TOPIC, body)

    async def publish_issues(self, batch: IssueBatch) -> Result:
        """Send an :class:`~benzene.mesh.IssueBatch` to ``benzene:mesh:issues``."""
        return await self._sender.send_message(ISSUES_TOPIC, batch.to_payload())
