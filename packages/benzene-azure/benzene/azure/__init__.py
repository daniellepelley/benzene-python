"""``benzene.azure`` — the Azure Functions host and its transport bindings.

Distribution ``benzene-azure``: host the same Benzene handlers behind Azure Functions HTTP, Service
Bus, Event Hub, Queue Storage, Blob Storage, Cosmos DB change feed, Timer, and Event Grid (native +
CloudEvents 1.0) triggers, plus Service Bus, Queue Storage, and Event Grid outbound clients. Depends
on ``benzene-core`` and ``benzene-http``. Mirrors .NET's ``Benzene.Azure.Function.*`` and
``Benzene.Clients.Azure.*``.

    from benzene.azure import AzureFunctionsApp

The Azure SDKs are only needed for the real outbound clients and are optional extras
(``pip install benzene-azure[servicebus,storage,eventgrid]``); the inbound bindings and the test host
need no Azure SDK. Contributes the ``benzene.azure`` subpackage to the shared ``benzene`` namespace.
"""

from __future__ import annotations

from .app import (
    AzureFunctionsApp,
    AzureHttpResponse,
    blob_function,
    cosmos_function,
    event_grid_function,
    event_hub_function,
    http_function,
    queue_storage_function,
    service_bus_function,
    timer_function,
)
from .clients import (
    EventGridMessageSender,
    QueueStorageMessageSender,
    ServiceBusMessageSender,
)
from .events import (
    DEFAULT_BLOB_TOPIC,
    DEFAULT_COSMOS_TOPIC,
    DEFAULT_TIMER_TOPIC,
    TOPIC_PROPERTY,
    decode_blob_created,
    decode_cosmos_document,
    decode_event_grid,
    decode_event_hub_event,
    decode_queue_storage,
    decode_service_bus,
    decode_timer,
    is_cloud_event,
)

__all__ = [
    "DEFAULT_BLOB_TOPIC",
    "DEFAULT_COSMOS_TOPIC",
    "DEFAULT_TIMER_TOPIC",
    "AzureFunctionsApp",
    "AzureHttpResponse",
    "EventGridMessageSender",
    "QueueStorageMessageSender",
    "ServiceBusMessageSender",
    "TOPIC_PROPERTY",
    "blob_function",
    "cosmos_function",
    "decode_blob_created",
    "decode_cosmos_document",
    "decode_event_grid",
    "decode_event_hub_event",
    "decode_queue_storage",
    "decode_service_bus",
    "decode_timer",
    "event_grid_function",
    "event_hub_function",
    "http_function",
    "is_cloud_event",
    "queue_storage_function",
    "service_bus_function",
    "timer_function",
]
