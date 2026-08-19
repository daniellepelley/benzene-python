"""Azure host wiring: boot the shared ``ServiceStartUp`` and specialize it to Azure Functions.

One Function App image serves any of the six domains (mirrors ``examples/aws_lambda_mesh``'s
``SERVICE_NAME`` convention): the environment picks the identity (``SERVICE_NAME``) and where each
produced topic actually goes. Terraform deploys the same zip six times with a different
``SERVICE_NAME`` (and, for the three producers, different queue/event-hub/topic env vars) — only this
file and ``function_app.py`` are Azure- and deployment-specific; ``domain.py`` and ``startup.py`` are
unchanged between "deployed to Azure Functions" and "driven in memory in a test".

Deployment and tests build the app from the *same* composition root (``ServiceStartUp``); only the
outbound ``MessageSender`` differs — :class:`TopicRoutingMessageSender` over the real Service
Bus/Event Hub/Event Grid clients here, a ``FakeMessageSender`` in tests (via ``create_test_host``).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from benzene.azure import (
    AzureFunctionsApp,
    EventGridMessageSender,
    EventHubMessageSender,
    ServiceBusMessageSender,
)
from benzene.core import MessageSender, build_application, use_instance
from benzene.results import Result, Status

from .startup import ServiceStartUp

#: Per-service, per-topic outbound targets: which env var names the Service Bus queue / Event Hub name
#: for each topic this service produces over that transport. Mirrors ``examples/aws_lambda_mesh``'s
#: ``_SQS_TARGETS``/``_SNS_TARGETS``/``_EVENTBRIDGE_TARGETS`` and the Terraform
#: ``local.service_app_settings`` map (``deploy/main.tf``) — the two must stay in step.
_SERVICE_BUS_TARGETS: dict[str, dict[str, str]] = {
    "orders": {"payment:take": "PAYMENTS_QUEUE"},
    "payments": {"shipment:book": "SHIPPING_QUEUE"},
}
_EVENT_HUB_TARGETS: dict[str, dict[str, str]] = {
    "orders": {"order:placed": "ORDER_PLACED_EVENTHUB"},
}
#: Every producer of an Event Grid topic shares the one topic endpoint/key — no per-topic env var.
_EVENT_GRID_TOPICS: dict[str, tuple[str, ...]] = {
    "payments": ("payment:captured",),
    "shipping": ("shipment:dispatched",),
}


class TopicRoutingMessageSender:
    """Routes each send to the :class:`~benzene.core.MessageSender` registered for its topic.

    A Cloud Service Function App in this estate sends different topics over different transports
    (orders: Service Bus *and* Event Hub; payments: Service Bus *and* Event Grid) — this is the small,
    example-local seam that picks the right outbound client per topic, mirroring
    ``examples/aws_lambda_mesh``'s per-service ``TopicRoutingMessageSender``. An unrouted topic is a
    configuration error (a producer wired to a topic with no Terraform-provisioned target), reported
    as a failed send rather than a crash.
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


def _require(env: Mapping[str, str], key: str) -> str:
    value = env.get(key)
    if not value:
        raise RuntimeError(f"Set {key} to run the Azure host (tests use create_test_host instead).")
    return value


def _production_sender(service_name: str, env: Mapping[str, str]) -> MessageSender:
    """Build the real per-topic outbound router from the env vars Terraform sets for this service."""
    senders: dict[str, MessageSender] = {}

    for topic, env_var in _SERVICE_BUS_TARGETS.get(service_name, {}).items():
        senders[topic] = ServiceBusMessageSender(
            connection_string=_require(env, "BENZENE_SERVICEBUS_CONNECTION"),
            entity_name=_require(env, env_var),
        )
    for topic, env_var in _EVENT_HUB_TARGETS.get(service_name, {}).items():
        senders[topic] = EventHubMessageSender(
            connection_string=_require(env, "BENZENE_EVENTHUB_CONNECTION"),
            eventhub_name=_require(env, env_var),
        )
    if service_name in _EVENT_GRID_TOPICS:
        grid_sender = EventGridMessageSender(
            topic_endpoint=_require(env, "BENZENE_EVENTGRID_ENDPOINT"),
            key=_require(env, "BENZENE_EVENTGRID_KEY"),
        )
        for topic in _EVENT_GRID_TOPICS[service_name]:
            senders[topic] = grid_sender

    return TopicRoutingMessageSender(senders)


def build_service_app(env: Mapping[str, str] | None = None) -> AzureFunctionsApp:
    """Boot ``ServiceStartUp`` for ``SERVICE_NAME`` and specialize it to Azure Functions."""
    env = os.environ if env is None else env
    service_name = env.get("SERVICE_NAME")
    if not service_name:
        raise RuntimeError(
            "Set SERVICE_NAME to run the Azure host (tests use create_test_host instead)."
        )

    definition, _ = build_application(
        ServiceStartUp(service_name),
        overrides=[use_instance(MessageSender, _production_sender(service_name, env))],
    )
    return AzureFunctionsApp.from_definition(definition)
