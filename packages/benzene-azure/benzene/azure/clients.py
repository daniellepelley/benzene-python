"""Azure outbound clients implementing the ``benzene.core.MessageSender`` port.

Each forwards the Benzene topic + headers onto whatever channel the transport offers and maps a send
failure to ``service-unavailable`` (never raising for a domain outcome). The Azure SDKs are optional
dependencies, imported lazily inside the methods, so the module (and its tests) load with no SDK:

* :class:`ServiceBusMessageSender` — topic + headers on ``application_properties`` (a native channel).
* :class:`EventHubMessageSender` — topic + headers on ``properties`` (a native channel — the producer
  side of :func:`~benzene.azure.decode_event_hub_event`'s ``properties``/``application_properties`` read).
* :class:`QueueStorageMessageSender` — a Storage Queue has *no* attribute channel, so topic + headers
  are embedded in the payload as a Benzene envelope (mirrors ``Benzene.Clients.Azure`` QueueStorage).
* :class:`EventGridMessageSender` — publishes an Event Grid event (native schema by default, or
  CloudEvents 1.0), the Benzene topic in ``eventType``/``type`` (mirrors ``Benzene.Clients.Azure``).

Each ``send_message`` runs its blocking Azure SDK call via :func:`asyncio.to_thread`, so an
``await sender.send_message(...)`` never blocks the event loop (matching the consumer loops and the
other transports' clients).
"""

from __future__ import annotations

import asyncio
import base64
import json
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from benzene.core import encode_body, to_jsonable
from benzene.results import Result, Status

from .events import TOPIC_PROPERTY


class ServiceBusMessageSender:
    """Sends to a Service Bus queue/topic, Benzene topic carried in ``application_properties``.

    ``sender`` (an ``azure.servicebus.ServiceBusSender``) may be injected for testing; otherwise a
    client is created lazily from ``connection_string`` + ``entity_name``.
    """

    def __init__(
        self,
        connection_string: str | None = None,
        entity_name: str | None = None,
        sender: Any | None = None,
        serializer: Callable[[Any], str] | None = None,
    ) -> None:
        self._connection_string = connection_string
        self._entity_name = entity_name
        self._sender = sender
        self._serialize = serializer or encode_body

    def _make_message(self, topic: str, message: Any, headers: dict[str, str] | None) -> Any:
        from azure.servicebus import ServiceBusMessage  # lazy: optional dependency

        properties = {str(k): str(v) for k, v in (headers or {}).items()}
        properties[TOPIC_PROPERTY] = topic
        return ServiceBusMessage(self._serialize(message), application_properties=properties)

    def _get_sender(self) -> Any:
        if self._sender is None:
            from azure.servicebus import ServiceBusClient  # lazy: optional dependency

            client = ServiceBusClient.from_connection_string(self._connection_string)
            self._sender = client.get_queue_sender(queue_name=self._entity_name)
        return self._sender

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            await asyncio.to_thread(
                self._get_sender().send_messages, self._make_message(topic, message, headers)
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()


class EventHubMessageSender:
    """Sends to an Event Hub, Benzene topic carried in the event's ``properties`` (a native channel).

    The producer counterpart of :func:`~benzene.azure.decode_event_hub_event`, which reads the topic
    from an event's ``properties``/``application_properties`` — the same convention
    :class:`ServiceBusMessageSender` uses for Service Bus. ``producer`` (an
    ``azure.eventhub.EventHubProducerClient``) may be injected for testing; otherwise a client is
    created lazily from ``connection_string`` + ``eventhub_name``. The whole build-batch-and-send
    sequence runs in one :func:`asyncio.to_thread` hop, mirroring the other senders' single blocking
    call.
    """

    def __init__(
        self,
        connection_string: str | None = None,
        eventhub_name: str | None = None,
        producer: Any | None = None,
        serializer: Callable[[Any], str] | None = None,
    ) -> None:
        self._connection_string = connection_string
        self._eventhub_name = eventhub_name
        self._producer = producer
        self._serialize = serializer or encode_body

    def _get_producer(self) -> Any:
        if self._producer is None:
            from azure.eventhub import EventHubProducerClient  # lazy: optional dependency

            self._producer = EventHubProducerClient.from_connection_string(
                conn_str=self._connection_string, eventhub_name=self._eventhub_name
            )
        return self._producer

    def _send_sync(self, topic: str, message: Any, headers: dict[str, str] | None) -> None:
        from azure.eventhub import EventData  # lazy: optional dependency

        properties = {str(k): str(v) for k, v in (headers or {}).items()}
        properties[TOPIC_PROPERTY] = topic
        event = EventData(self._serialize(message))
        event.properties = properties

        producer = self._get_producer()
        batch = producer.create_batch()
        batch.add(event)
        producer.send_batch(batch)

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            await asyncio.to_thread(self._send_sync, topic, message, headers)
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()


class QueueStorageMessageSender:
    """Enqueues to an Azure Storage Queue, the Benzene topic + headers embedded in the payload.

    A Storage Queue message is opaque text with no attribute channel, so — unlike Service Bus — the
    topic can't ride alongside the body. The sender instead serializes a Benzene envelope
    ``{topic, headers, body}`` (the same shape the wire entry point speaks), which
    :func:`~benzene.azure.decode_queue_storage` lifts straight back on the inbound side. Mirrors the
    ``Benzene.Clients.Azure`` QueueStorage client.

    ``client`` (an ``azure.storage.queue.QueueClient``) may be injected for testing; otherwise one is
    created lazily from ``queue_url`` or from ``connection_string`` + ``queue_name``. ``base64_encode``
    matches the classic Storage Queue convention (the .NET SDK's default); the decoder auto-detects it.
    """

    def __init__(
        self,
        queue_url: str | None = None,
        *,
        queue_name: str | None = None,
        connection_string: str | None = None,
        client: Any | None = None,
        serializer: Callable[[Any], str] | None = None,
        base64_encode: bool = False,
    ) -> None:
        self._queue_url = queue_url
        self._queue_name = queue_name
        self._connection_string = connection_string
        self._client = client
        self._serialize = serializer or encode_body
        self._base64_encode = base64_encode

    def _make_message(self, topic: str, message: Any, headers: dict[str, str] | None) -> str:
        envelope = {
            "topic": topic,
            "headers": {str(k): str(v) for k, v in (headers or {}).items()},
            "body": self._serialize(message),
        }
        text = json.dumps(envelope)
        if self._base64_encode:
            return base64.b64encode(text.encode("utf-8")).decode("ascii")
        return text

    def _get_client(self) -> Any:
        if self._client is None:
            from azure.storage.queue import QueueClient  # lazy: optional dependency

            if self._connection_string is not None:
                self._client = QueueClient.from_connection_string(
                    self._connection_string, self._queue_name
                )
            else:
                self._client = QueueClient.from_queue_url(self._queue_url)
        return self._client

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            await asyncio.to_thread(
                self._get_client().send_message, self._make_message(topic, message, headers)
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()


class EventGridMessageSender:
    """Publishes an Event Grid event, the Benzene topic carried in ``eventType`` (or CloudEvents ``type``).

    Native Event Grid schema is used by default (``cloud_events=True`` switches to CloudEvents 1.0).
    Because native schema has no free-form attribute channel, the Benzene headers travel in a ``headers``
    field of the event; in CloudEvents mode they travel as *extension attributes*. Either way
    :func:`~benzene.azure.decode_event_grid` reverses the mapping. Mirrors the ``Benzene.Clients.Azure``
    Event Grid client.

    ``client`` (an ``azure.eventgrid.EventGridPublisherClient``) may be injected for testing; otherwise
    one is created lazily from ``topic_endpoint`` + ``key``.
    """

    def __init__(
        self,
        topic_endpoint: str | None = None,
        *,
        key: str | None = None,
        client: Any | None = None,
        subject: str = "benzene",
        source: str = "benzene",
        data_version: str = "1.0",
        cloud_events: bool = False,
    ) -> None:
        self._topic_endpoint = topic_endpoint
        self._key = key
        self._client = client
        self._subject = subject
        self._source = source
        self._data_version = data_version
        self._cloud_events = cloud_events

    def _make_event(
        self, topic: str, message: Any, headers: dict[str, str] | None
    ) -> dict[str, Any]:
        data = to_jsonable(message)
        now = datetime.now(timezone.utc).isoformat()
        if self._cloud_events:
            event: dict[str, Any] = {
                "specversion": "1.0",
                "id": str(uuid.uuid4()),
                "source": self._source,
                "type": topic,
                "subject": self._subject,
                "time": now,
                "data": data,
            }
            # Benzene headers ride as CloudEvents extension attributes (topic already owns ``type``).
            event.update({str(k).lower(): str(v) for k, v in (headers or {}).items()})
            return event
        return {
            "id": str(uuid.uuid4()),
            "eventType": topic,
            "subject": self._subject,
            "eventTime": now,
            "dataVersion": self._data_version,
            "data": data,
            "headers": {str(k): str(v) for k, v in (headers or {}).items()},
        }

    def _get_client(self) -> Any:
        if self._client is None:
            from azure.core.credentials import AzureKeyCredential  # lazy: optional dependency
            from azure.eventgrid import EventGridPublisherClient  # lazy: optional dependency

            self._client = EventGridPublisherClient(
                self._topic_endpoint, AzureKeyCredential(self._key)
            )
        return self._client

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            await asyncio.to_thread(
                self._get_client().send, self._make_event(topic, message, headers)
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()
