"""GCP host wiring: boot the shared ``OrdersStartUp`` and specialize it to Cloud Functions.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — the real Pub/Sub client here, a fake in tests (via
``create_test_host``). Only this file is GCP-specific.
"""

from __future__ import annotations

import os

from benzene.core import Container, MessageSender, build_application
from benzene.gcp import GcpFunctionsApp, PubSubMessageSender
from orders_domain import OrdersStartUp


def build_gcp_orders_app() -> GcpFunctionsApp:
    """Boot ``OrdersStartUp`` and specialize it to Cloud Functions, publishing to the Pub/Sub topic."""
    topic = os.environ.get("BENZENE_PUBSUB_TOPIC")
    if not topic:
        raise RuntimeError(
            "Set BENZENE_PUBSUB_TOPIC (projects/<project>/topics/<topic>) to run the GCP host "
            "(tests use create_test_host instead)."
        )

    def use_pubsub(services: Container) -> None:
        services.add_instance(MessageSender, PubSubMessageSender(topic))

    definition, _ = build_application(OrdersStartUp, overrides=[use_pubsub])
    return GcpFunctionsApp.from_definition(definition)
