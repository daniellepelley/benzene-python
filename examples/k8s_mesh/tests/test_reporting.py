"""The service -> collector feeds, in memory: the reporter registers the very Contract Document the
service serves on ``/benzene/spec`` (mesh.md §2), and heartbeats with that document's
``registrationHash`` (mesh.md §5) — so the collector's graph exists, and its hash check holds, with no
traffic at all.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from benzene.core import BenzeneMessageApplication, MessageSender, build_application, use_instance
from benzene.http import BenzeneHttpApp
from benzene.mesh import (
    MeshCollector,
    MeshFeedSender,
    QueueTraceExporter,
    collector_registry,
    registration_document,
)
from benzene.results import Result
from benzene.testing import FakeMessageSender

from k8s_mesh.service.domain import PAYMENT_TAKE_TOPIC
from k8s_mesh.service.reporting import MeshReporter
from k8s_mesh.service.startup import ServiceStartUp


class _CollectorSender:
    """A MessageSender that delivers each feed envelope to an in-memory collector app."""

    def __init__(self, collector: MeshCollector) -> None:
        self._app = BenzeneMessageApplication(collector_registry(collector))
        self.topics: list[str] = []

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        self.topics.append(topic)
        response = await self._app.handle({"topic": topic, "headers": {}, "body": json.dumps(message)})
        return Result.ok() if response["statusCode"] == "ok" else Result.bad_request()


def test_the_reporter_registers_the_served_document_and_its_hash_matches() -> None:
    definition, _ = build_application(
        ServiceStartUp("orders"), overrides=[use_instance(MessageSender, FakeMessageSender())]
    )
    collector = MeshCollector()
    sender = _CollectorSender(collector)
    document = registration_document(definition)
    reporter = MeshReporter(
        MeshFeedSender(sender), document, QueueTraceExporter(), instance_id="orders-pod-1"
    )

    asyncio.run(reporter.report_once())

    assert sender.topics == ["benzene:mesh:register", "benzene:mesh:heartbeat"]
    # The registered document is the served one.
    served = asyncio.run(BenzeneHttpApp.from_definition(definition).handle("GET", "/benzene/spec"))
    registered = collector.snapshot()["services"][0]["document"]
    assert registered == json.loads(served.body)
    assert registered["placement"] == {"cloud": "self-hosted"}
    # orders sends payment:take: a provider edge with zero traffic.
    assert collector.query_topic({"topic": PAYMENT_TAKE_TOPIC})["providers"] == ["orders"]
    # The heartbeat carried the document's registrationHash, so the instance matches.
    instance = collector.query_service({"service": "orders"})["instances"][0]
    assert instance == {"instanceId": "orders-pod-1", "healthy": True, "hashMatches": True}
