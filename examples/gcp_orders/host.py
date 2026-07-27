"""GCP host wiring: mount the shared order domain onto Cloud Functions HTTP + Pub/Sub triggers.

Compare with ``aws_orders`` / ``azure_orders``: only this file changes between clouds — the domain
(``orders_domain``) is identical.
"""

from __future__ import annotations

from benzene.core import MessageSender
from benzene.gcp import GcpFunctionsApp, PubSubMessageSender

from orders_domain import OrderService, build_orders


def build_gcp_orders_app(
    service: OrderService | None = None,
    sender: MessageSender | None = None,
    seen: list[str] | None = None,
) -> GcpFunctionsApp:
    """Build the GCP Functions app for the order domain.

    Pass a store / outbound client / event log for tests; in production the defaults create a real
    :class:`~benzene.gcp.PubSubMessageSender` (its Pub/Sub topic comes from configuration).
    """
    service = service or OrderService()
    if sender is None:
        import os

        topic = os.environ.get("BENZENE_PUBSUB_TOPIC")
        if not topic:
            raise RuntimeError(
                "Set BENZENE_PUBSUB_TOPIC (projects/<project>/topics/<topic>) to run the GCP host "
                "with a real Pub/Sub client, or pass sender=... (e.g. a FakeMessageSender) in tests."
            )
        sender = PubSubMessageSender(topic)
    wiring = build_orders(service, sender, seen if seen is not None else [])
    return GcpFunctionsApp(http_router=wiring.router, registry=wiring.registry)
