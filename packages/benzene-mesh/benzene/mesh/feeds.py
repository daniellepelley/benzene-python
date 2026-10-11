"""Service-side mesh feeds — reporting a service into a collector (mesh.md §§2–5).

A mesh-enabled service pushes independent, optional feeds to a collector over any envelope-capable
transport (an outbound :class:`~benzene.core.MessageSender` — Pub/Sub, SNS/SQS, Service Bus, or an
HTTP POST of the wire envelope):

==========================  ==========================  ==========================
Topic                       Body                        Success response
==========================  ==========================  ==========================
``benzene:mesh:register``   the Contract Document       ``{"accepted": 1}``
``benzene:mesh:heartbeat``  :class:`Heartbeat`          ``{"accepted": 1}``
``benzene:mesh:traces``     ``{"events": [...]}``       ``{"accepted": <count>}``
``benzene:mesh:issues``     IssueBatch                  ``{"accepted": <count>}``
==========================  ==========================  ==========================

**The register body is the Contract Document** (mesh.md §2) — the very document ``benzene:spec``
serves, derived once at the composition root, plus the deployment-side fields the service knows
(``runtime``, ``placement``, ``profile``, ``degraded``). :func:`registration_document` builds it from
an :class:`~benzene.core.AppDefinition`; :meth:`Heartbeat.for_document` stamps a heartbeat with that
document's ``registrationHash`` and ``serviceVersion``, so the collector can tell a stale instance.

**Every feed is independent and optional on both sides** (mesh.md §6): an unreachable collector or a
failing feed must never affect service traffic. :class:`MeshFeedSender` returns the outbound
:class:`~benzene.results.Result` so a caller can log a failed feed, but sending is fire-and-report —
it does not raise.

This is the *sender* half (a Python service reporting in). The receiving
:class:`~benzene.mesh.MeshCollector` ingests these feeds and answers ``benzene:mesh:query:*`` (see
:mod:`benzene.mesh.collector`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from benzene.core import AppDefinition, ContractDocument, MessageSender
from benzene.results import Result

from .issues import IssueBatch
from .trace import TraceEvent

REGISTER_TOPIC = "benzene:mesh:register"
HEARTBEAT_TOPIC = "benzene:mesh:heartbeat"
TRACES_TOPIC = "benzene:mesh:traces"
ISSUES_TOPIC = "benzene:mesh:issues"


def registration_document(definition: AppDefinition) -> ContractDocument:
    """The register body (mesh.md §2): the Contract Document the composition root derives.

    It is the document ``benzene:spec`` serves for ``definition`` — same entries, same hashes, same
    deployment-side fields (``runtime``, ``placement``, ``profile``, ``degraded``, declared on its
    :class:`~benzene.core.ContractInfo`; :func:`~benzene.mesh.declare_cloud_service_profile` puts the
    profile self-check there). Raises ``ValueError`` when the definition declares no ``contract``: a
    service with no self-description has nothing to register.
    """
    document = definition.contract_document()
    if document is None:
        raise ValueError(
            "A mesh registration is the service's Contract Document: declare "
            "AppDefinition(contract=ContractInfo(...)) to derive it (mesh.md §2)."
        )
    return document


@dataclass(frozen=True)
class Heartbeat:
    """A liveness beat: identity, the registered document's hash, and the health aggregate (mesh.md §5).

    ``registration_hash`` is the ``registrationHash`` of the document this instance registered; a
    collector comparing it with its registration spots an instance running a contract it has not
    learned. ``service_version`` scopes that comparison to one release. Build one from the registered
    document with :meth:`for_document` so the two never disagree.
    """

    service: str
    sent_at: str
    instance_id: str | None = None
    registration_hash: str | None = None
    service_version: str | None = None
    is_healthy: bool = True
    health_checks: Mapping[str, Any] | None = None

    @classmethod
    def for_document(
        cls,
        document: ContractDocument,
        *,
        sent_at: str,
        instance_id: str | None = None,
        is_healthy: bool = True,
        health_checks: Mapping[str, Any] | None = None,
    ) -> Heartbeat:
        """A heartbeat for the instance that registered ``document``: its ``service``,
        ``serviceVersion`` and ``registrationHash`` come from the document itself."""
        return cls(
            service=document.service,
            sent_at=sent_at,
            instance_id=instance_id,
            registration_hash=document.registration_hash,
            service_version=document.service_version or None,
            is_healthy=is_healthy,
            health_checks=health_checks,
        )

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"service": self.service}
        if self.service_version:  # omitted when the instance declares none (mesh.md §5)
            payload["serviceVersion"] = self.service_version
        if self.instance_id is not None:
            payload["instanceId"] = self.instance_id
        if self.registration_hash is not None:
            payload["registrationHash"] = self.registration_hash
        payload["sentAt"] = self.sent_at
        payload["health"] = {
            "isHealthy": self.is_healthy,
            "healthChecks": dict(self.health_checks or {}),
        }
        return payload


class MeshFeedSender:
    """Pushes a service's mesh feeds to a collector over an outbound :class:`MessageSender`.

    One method per feed: :meth:`register` announces the service's Contract Document once; the ongoing
    telemetry feeds — :meth:`publish_heartbeat`, :meth:`publish_traces`, :meth:`publish_issues` —
    are pushed periodically. Each returns the outbound :class:`~benzene.results.Result` so a caller
    can log a failed feed; sending never raises.
    """

    def __init__(self, sender: MessageSender) -> None:
        self._sender = sender

    async def register(self, document: ContractDocument) -> Result:
        """Announce the Contract Document on ``benzene:mesh:register`` (see :func:`registration_document`)."""
        return await self._sender.send_message(REGISTER_TOPIC, document.to_payload())

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
