"""AWS host wiring: boot the shared ``ServiceStartUp`` and specialize it to Lambda.

One Lambda image serves any of the six domains (mirrors ``examples/k8s_mesh``'s ``MESH_SERVICE``
convention, and this repo's ``deploy/mesh/fleet/service.py``): the environment picks the identity
(``SERVICE_NAME``) and where each produced topic actually goes. Terraform deploys the same zip six
times with a different ``SERVICE_NAME`` (and, for the three producers, different queue/topic/bus env
vars) — only this file is AWS- and deployment-specific; ``service/domain.py`` and ``startup.py`` are
unchanged between "deployed to Lambda" and "driven in memory in a test".

Deployment and tests build the app from the *same* composition root (``ServiceStartUp``); only the
outbound ``MessageSender`` differs — :class:`TopicRoutingMessageSender` over the real SQS/SNS/
EventBridge clients here, a ``FakeMessageSender`` in tests (via ``create_test_host``).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from benzene.aws import (
    AwsLambdaApp,
    EventBridgeMessageSender,
    SnsMessageSender,
    SqsMessageSender,
)
from benzene.core import Container, MessageSender, build_application
from benzene.mesh import MeshFeedSender, QueueTraceExporter, S3TraceInbox, with_trace_propagation
from benzene.results import Result, Status

from .startup import ServiceStartUp

#: Per-service, per-topic outbound targets: which env var names the queue URL / topic ARN / bus name
#: for each topic this service produces. Mirrors TS's ``functions/shared.ts`` (``targetEnvVar``) and
#: the Terraform ``local.service_env`` map (``deploy/main.tf``) — the two must stay in step.
_SQS_TARGETS: dict[str, dict[str, str]] = {
    "orders": {"payments:capture": "PAYMENTS_QUEUE_URL"},
    "payments": {"shipping:book": "SHIPPING_QUEUE_URL"},
}
_SNS_TARGETS: dict[str, dict[str, str]] = {
    "orders": {"order:placed": "ORDER_PLACED_TOPIC_ARN"},
}
_EVENTBRIDGE_TARGETS: dict[str, dict[str, str]] = {
    "payments": {"payment:captured": "EVENT_BUS_NAME"},
    "shipping": {"shipment:dispatched": "EVENT_BUS_NAME"},
}


class TopicRoutingMessageSender:
    """Routes each send to the :class:`~benzene.core.MessageSender` registered for its topic.

    A Cloud Service Lambda in this estate sends different topics over different transports (orders:
    SQS *and* SNS; payments: SQS *and* EventBridge) — this is the small, example-local seam that picks
    the right outbound client per topic, mirroring TS's per-service ``target()`` outbound wiring
    (``functions/shared.ts``). An unrouted topic is a configuration error (a producer wired to a topic
    with no Terraform-provisioned target), reported as a failed send rather than a crash.
    """

    def __init__(self, senders: Mapping[str, MessageSender]) -> None:
        self._senders = dict(senders)

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        sender = self._senders.get(topic)
        if sender is None:
            return Result.failure(
                Status.NOT_FOUND, f"no outbound route configured for topic {topic!r}"
            )
        return await sender.send_message(topic, message, headers)


def _production_sender(service_name: str, env: Mapping[str, str]) -> MessageSender:
    """Build the real per-topic outbound router from the env vars Terraform sets for this service,
    wrapped so a downstream hop joins the caller's trace (mesh.md §3) — the collector then derives the
    consumer edge from the ``traceparent`` it carries, exactly as :func:`trace_middleware` on the
    receiving service reads it back out."""
    senders: dict[str, MessageSender] = {}
    for topic, env_var in _SQS_TARGETS.get(service_name, {}).items():
        senders[topic] = SqsMessageSender(env[env_var])
    for topic, env_var in _SNS_TARGETS.get(service_name, {}).items():
        senders[topic] = SnsMessageSender(env[env_var])
    for topic, env_var in _EVENTBRIDGE_TARGETS.get(service_name, {}).items():
        senders[topic] = EventBridgeMessageSender(env[env_var])
    return with_trace_propagation(TopicRoutingMessageSender(senders))


@dataclass
class ServiceLambda:
    """A wired service Lambda: the app, its trace exporter, and an optional mesh feed sender —
    mirrors ``deploy/mesh/fleet/service.py``'s ``FleetService`` (same reason: a Lambda only runs
    *during* an invocation, so traces are pushed once per invocation rather than on a background loop
    a long-lived host would use)."""

    app: AwsLambdaApp
    exporter: QueueTraceExporter
    feeds: MeshFeedSender | None


def build_service(env: Mapping[str, str] | None = None) -> ServiceLambda:
    """Boot ``ServiceStartUp`` for ``SERVICE_NAME`` and specialize it to Lambda.

    When ``MESH_ARTIFACT_BUCKET`` is set (Terraform wires it on every service Lambda — see
    ``deploy/main.tf``), the returned :class:`ServiceLambda` also carries a :class:`MeshFeedSender`
    that pushes trace batches into the mesh's :class:`~benzene.mesh.S3TraceInbox`
    (``MESH_TRACE_PREFIX``, default ``"_state/traces"``), so
    :func:`~aws_lambda_mesh.service.main.handler` can drain and push after each invocation. This is a
    direct S3 write, not a Lambda invoke — deliberately, so that several service Lambdas fanned out
    from one SNS/EventBridge message and pushing at the same time never contend over shared state (see
    ``mesh/discovery_service.py``'s module docstring for the race a load-mutate-save push had).
    """
    env = os.environ if env is None else env
    service_name = env.get("SERVICE_NAME")
    if not service_name:
        raise RuntimeError(
            "Set SERVICE_NAME to run the AWS host (tests use create_test_host instead)."
        )

    def use_production_sender(services: Container) -> None:
        services.add_instance(MessageSender, _production_sender(service_name, env))

    definition, scope = build_application(ServiceStartUp(service_name), overrides=[use_production_sender])
    app = AwsLambdaApp.from_definition(definition)
    exporter = scope.get_service(QueueTraceExporter)

    feeds: MeshFeedSender | None = None
    bucket = env.get("MESH_ARTIFACT_BUCKET")
    if bucket:
        feeds = MeshFeedSender(S3TraceInbox(bucket, env.get("MESH_TRACE_PREFIX", "_state/traces")))

    return ServiceLambda(app=app, exporter=exporter, feeds=feeds)
