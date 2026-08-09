"""Azure host wiring: boot the shared ``OrdersStartUp`` and specialize it to Azure Functions.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — the real Service Bus client here, a fake in tests (via
``create_test_host``). Only this file is Azure-specific.
"""

from __future__ import annotations

import os

from benzene.azure import AzureFunctionsApp, ServiceBusMessageSender
from benzene.core import Container, MessageSender, build_application
from orders_domain import OrdersStartUp


def build_azure_orders_app() -> AzureFunctionsApp:
    """Boot ``OrdersStartUp`` and specialize it to Azure Functions, publishing to the Service Bus entity."""
    connection = os.environ.get("BENZENE_SERVICEBUS_CONNECTION")
    entity = os.environ.get("BENZENE_SERVICEBUS_ENTITY")
    if not connection or not entity:
        raise RuntimeError(
            "Set BENZENE_SERVICEBUS_CONNECTION and BENZENE_SERVICEBUS_ENTITY to run the Azure host "
            "(tests use create_test_host instead)."
        )

    def use_service_bus(services: Container) -> None:
        services.add_instance(
            MessageSender,
            ServiceBusMessageSender(connection_string=connection, entity_name=entity),
        )

    definition, _ = build_application(OrdersStartUp, overrides=[use_service_bus])
    return AzureFunctionsApp.from_definition(definition)
