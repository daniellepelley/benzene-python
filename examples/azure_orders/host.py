"""Azure host wiring: boot the shared ``OrdersStartUp`` and specialize it to Azure Functions.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — the real Service Bus client here, a fake in tests. Only this
file is Azure-specific. The ``orders:created`` subscriber handles the event whether it arrives over
Service Bus or Event Hub.
"""

from __future__ import annotations

from benzene.azure import AzureFunctionsApp, ServiceBusMessageSender
from benzene.core import Container, MessageSender, build_application

from orders_domain import ORDER_EVENTS_KEY, OrderService, OrdersStartUp


def build_azure_orders_app(
    service: OrderService | None = None,
    sender: MessageSender | None = None,
    seen: list[str] | None = None,
) -> AzureFunctionsApp:
    """Build the Azure Functions app for the order domain, booting from ``OrdersStartUp``.

    In production the default publishes ``orders:created`` to the Service Bus entity named by the
    ``BENZENE_SERVICEBUS_*`` settings; tests override the sender via the test harness.
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

            connection = os.environ.get("BENZENE_SERVICEBUS_CONNECTION")
            entity = os.environ.get("BENZENE_SERVICEBUS_ENTITY")
            if not connection or not entity:
                raise RuntimeError(
                    "Set BENZENE_SERVICEBUS_CONNECTION and BENZENE_SERVICEBUS_ENTITY to run the "
                    "Azure host with a real Service Bus client, or pass sender=... in tests."
                )
            services.add_instance(
                MessageSender,
                ServiceBusMessageSender(connection_string=connection, entity_name=entity),
            )

    definition, _ = build_application(OrdersStartUp, overrides=[overrides])
    return AzureFunctionsApp(http_router=definition.router, registry=definition.registry)
