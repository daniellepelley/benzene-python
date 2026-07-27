"""Azure host wiring: mount the shared order domain onto Azure Functions (HTTP + Service Bus +
Event Hub + egress).

Only this file differs from the GCP and AWS hosts — the domain (``orders_domain``) is identical. The
``orders.created`` subscriber handles the event whether it arrives over Service Bus or Event Hub.
"""

from __future__ import annotations

from benzene.azure import AzureFunctionsApp, ServiceBusMessageSender
from benzene.core import MessageSender

from orders_domain import OrderService, build_orders


def build_azure_orders_app(
    service: OrderService | None = None,
    sender: MessageSender | None = None,
    seen: list[str] | None = None,
) -> AzureFunctionsApp:
    """Build the Azure Functions app for the order domain.

    Pass a store / outbound client / event log for tests; in production the default publishes
    ``orders.created`` to the Service Bus entity named by the ``BENZENE_SERVICEBUS_*`` settings.
    """
    service = service or OrderService()
    if sender is None:
        import os

        sender = ServiceBusMessageSender(
            connection_string=os.environ["BENZENE_SERVICEBUS_CONNECTION"],
            entity_name=os.environ["BENZENE_SERVICEBUS_ENTITY"],
        )
    wiring = build_orders(service, sender, seen if seen is not None else [])
    return AzureFunctionsApp(http_router=wiring.router, registry=wiring.registry)
