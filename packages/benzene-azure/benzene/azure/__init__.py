"""``benzene.azure`` — the Azure Functions host and its transport bindings.

Distribution ``benzene-azure``: host the same Benzene handlers behind Azure Functions HTTP, Service
Bus, and Event Hub triggers, plus a Service Bus outbound client. Depends on ``benzene-core`` and
``benzene-http``. Mirrors .NET's ``Benzene.Azure.Function.*``.

    from benzene.azure import AzureFunctionsApp

``azure-servicebus`` is only needed for the real outbound client and is an optional extra
(``pip install benzene-azure[servicebus]``); the inbound bindings and the test host need no Azure
SDK. Contributes the ``benzene.azure`` subpackage to the shared ``benzene`` namespace.
"""

from __future__ import annotations

from .app import (
    AzureFunctionsApp,
    AzureHttpResponse,
    event_hub_function,
    http_function,
    service_bus_function,
)
from .clients import ServiceBusMessageSender
from .events import TOPIC_PROPERTY, decode_event_hub_event, decode_service_bus

__all__ = [
    "AzureFunctionsApp",
    "AzureHttpResponse",
    "ServiceBusMessageSender",
    "TOPIC_PROPERTY",
    "decode_event_hub_event",
    "decode_service_bus",
    "event_hub_function",
    "http_function",
    "service_bus_function",
]
