"""Wires ``ServiceStartUp`` onto the ASGI HTTP binding, driven entirely by env vars — the container's
composition boundary. Deployment and tests build the app from the *same* ``ServiceStartUp``; only the
outbound edge (a real ``EnvelopeHttpMessageSender`` here, a ``FakeMessageSender`` in a test) differs.

Env vars (matching .NET's ``examples/K8sMesh/Service`` manifests):

- ``MESH_SERVICE`` — which domain to run: ``orders`` / ``payments`` / ``shipping`` (default ``orders``).
- ``DOWNSTREAM_MSG_URL`` — the next service's ``/benzene/invoke`` URL (e.g.
  ``http://payments/benzene/invoke``); unset for a terminal service (``shipping``) or a standalone run.
- ``MESH_COLLECTOR_ENVELOPE_URL`` — the mesh's ``/benzene/invoke`` URL (e.g.
  ``http://mesh/benzene/invoke``); unset disables reporting (register/heartbeat/traces) entirely.
- ``PORT`` — the HTTP port (default ``8080``).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from benzene.core import MessageSender, build_application, use_instance
from benzene.http import BenzeneHttpApp
from benzene.mesh import (
    MeshFeedSender,
    QueueTraceExporter,
    ServiceDescriptor,
    ServiceInfo,
)

from ..envelope_client import EnvelopeHttpMessageSender
from .domain import NullMessageSender
from .reporting import MeshReporter
from .startup import ServiceStartUp


@dataclass
class ServiceHost:
    """The wired service: the ASGI app to serve, and an optional mesh reporter to run alongside it."""

    app: BenzeneHttpApp
    reporter: MeshReporter | None


def build_service_host(env: Mapping[str, str] | None = None) -> ServiceHost:
    env = os.environ if env is None else env
    name = env.get("MESH_SERVICE", "orders")
    downstream_url = env.get("DOWNSTREAM_MSG_URL") or None
    collector_url = env.get("MESH_COLLECTOR_ENVELOPE_URL") or None
    instance_id = env.get("HOSTNAME", name)  # Kubernetes sets HOSTNAME to the pod name

    sender: MessageSender = (
        EnvelopeHttpMessageSender(downstream_url) if downstream_url else NullMessageSender()
    )
    definition, scope = build_application(
        ServiceStartUp(name), overrides=[use_instance(MessageSender, sender)]
    )
    app = BenzeneHttpApp.from_definition(definition)

    reporter: MeshReporter | None = None
    if collector_url:
        exporter = scope.get_service(QueueTraceExporter)
        feeds = MeshFeedSender(EnvelopeHttpMessageSender(collector_url))
        # What each service sends downstream - orders -> payments -> shipping (terminal) - is declared
        # on the startup's outbound registry (definition.outbound): that is what puts provider edges in
        # the mesh's graph; nothing here is derived from tracing a live call.
        # ServiceStartUp.configure always sets registry (it's the router-derived registry, never left
        # None) — assert rather than silently deriving from an empty Registry.
        assert definition.registry is not None
        descriptor = ServiceDescriptor.derive(
            definition.registry,
            ServiceInfo(
                service=name,
                instance_id=instance_id,
                placement={"platform": "kubernetes", "zone": "in-cluster"},
            ),
            definition.outbound,
        )
        reporter = MeshReporter(feeds, descriptor, exporter, instance_id=instance_id)

    return ServiceHost(app=app, reporter=reporter)
