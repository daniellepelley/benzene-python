"""GCP host wiring: boot the shared ``OrdersStartUp`` and specialize it to Cloud Functions.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — the real Pub/Sub client here, a fake in tests. Only this file
is GCP-specific.
"""

from __future__ import annotations

from benzene.core import Container, MessageSender, build_application
from benzene.gcp import GcpFunctionsApp, PubSubMessageSender

from orders_domain import ORDER_EVENTS_KEY, OrderService, OrdersStartUp


def build_gcp_orders_app(
    service: OrderService | None = None,
    sender: MessageSender | None = None,
    seen: list[str] | None = None,
) -> GcpFunctionsApp:
    """Build the GCP Functions app for the order domain, booting from ``OrdersStartUp``.

    Pass a store / outbound client / event log to override the defaults (tests do this via the test
    harness instead); in production the default publishes ``orders:created`` to the Pub/Sub topic
    named by ``BENZENE_PUBSUB_TOPIC``.
    """

    def overrides(services: Container) -> None:
        if service is not None:
            services.add_instance(OrderService, service)
        if seen is not None:
            services.add_instance(ORDER_EVENTS_KEY, seen)
        if sender is not None:
            services.add_instance(MessageSender, sender)
        else:
            import os

            topic = os.environ.get("BENZENE_PUBSUB_TOPIC")
            if not topic:
                raise RuntimeError(
                    "Set BENZENE_PUBSUB_TOPIC (projects/<project>/topics/<topic>) to run the GCP "
                    "host with a real Pub/Sub client, or pass sender=... in tests."
                )
            services.add_instance(MessageSender, PubSubMessageSender(topic))

    definition, _ = build_application(OrdersStartUp, overrides=[overrides])
    return GcpFunctionsApp(http_router=definition.router, registry=definition.registry)
