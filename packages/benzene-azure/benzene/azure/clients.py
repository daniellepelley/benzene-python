"""Azure outbound clients implementing the ``benzene.core.MessageSender`` port.

Each forwards the Benzene topic + headers onto whatever channel the transport offers and maps a send
failure to ``service-unavailable`` (never raising for a domain outcome). The Azure SDKs are optional
dependencies, imported lazily inside the methods, so the module (and its tests) load with no SDK:

* :class:`ServiceBusMessageSender` — topic + headers on ``application_properties`` (a native channel).
* :class:`QueueStorageMessageSender` — a Storage Queue has *no* attribute channel, so topic + headers
  are embedded in the payload as a Benzene envelope (mirrors ``Benzene.Clients.Azure`` QueueStorage).
* :class:`EventGridMessageSender` — publishes an Event Grid event (native schema by default, or
  CloudEvents 1.0), the Benzene topic in ``eventType``/``type`` (mirrors ``Benzene.Clients.Azure``).

A *missing* SDK raises an ImportError naming that class's extra (``benzene-azure[servicebus]`` /
``[storage]`` / ``[eventgrid]``) straight out of ``send_message`` — a forgotten extra is a deployment
error, not a message outcome, so it is never mapped to ``service-unavailable`` for retries and circuit
breakers to hammer.

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


def _service_bus_message(body: str | bytes, properties: dict[str, str]) -> Any:
    """The default :class:`ServiceBusMessageSender` ``message_factory``: the real SDK object.

    A module-level function so it is exactly the seam an injected ``message_factory`` replaces —
    the wire shape is identical either way, only the construction moves. A missing SDK is a
    *deployment* error, not a message outcome, so it surfaces as an ImportError naming the extra
    (the same guard :mod:`benzene.grpc` uses) rather than a ``service-unavailable`` result that
    retry middleware and circuit breakers would hammer forever.
    """
    try:
        from azure.servicebus import ServiceBusMessage  # lazy: optional dependency
    except ImportError as exc:
        raise ImportError(
            "ServiceBusMessageSender requires azure-servicebus — install it with "
            "'pip install benzene-azure[servicebus]'."
        ) from exc
    return ServiceBusMessage(body, application_properties=properties)


class ServiceBusMessageSender:
    """Sends to a Service Bus queue/topic, Benzene topic carried in ``application_properties``.

    ``sender`` (an ``azure.servicebus.ServiceBusSender``) may be injected for testing; otherwise a
    client is created lazily from ``connection_string`` + ``entity_name``.

    ``message_factory`` builds the object handed to ``send_messages`` from
    ``(body, application_properties)``; it defaults to :func:`_service_bus_message` (the real
    ``azure.servicebus.ServiceBusMessage``, constructed lazily so the SDK stays optional). Injecting
    a duck-typed factory lets the egress contract — topic tagging, header propagation, serialization,
    failure mapping — be exercised without the SDK, exactly as every other sender already is; what
    goes on the wire is unchanged.
    """

    def __init__(
        self,
        connection_string: str | None = None,
        entity_name: str | None = None,
        sender: Any | None = None,
        serializer: Callable[[Any], str] | None = None,
        message_factory: Callable[[str | bytes, dict[str, str]], Any] | None = None,
    ) -> None:
        self._connection_string = connection_string
        self._entity_name = entity_name
        self._sender = sender
        self._serialize = serializer or encode_body
        self._message_factory = message_factory or _service_bus_message

    def _make_message(self, topic: str, message: Any, headers: dict[str, str] | None) -> Any:
        properties = {str(k): str(v) for k, v in (headers or {}).items()}
        properties[TOPIC_PROPERTY] = topic
        return self._message_factory(self._serialize(message), properties)

    def _get_sender(self) -> Any:
        if self._sender is None:
            try:
                from azure.servicebus import ServiceBusClient  # lazy: optional dependency
            except ImportError as exc:
                raise ImportError(
                    "ServiceBusMessageSender requires azure-servicebus — install it with "
                    "'pip install benzene-azure[servicebus]'."
                ) from exc

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
        except ImportError:
            raise  # a missing SDK is a deployment error, never a service-unavailable result
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
            try:
                from azure.storage.queue import QueueClient  # lazy: optional dependency
            except ImportError as exc:
                raise ImportError(
                    "QueueStorageMessageSender requires azure-storage-queue — install it with "
                    "'pip install benzene-azure[storage]'."
                ) from exc

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
        except ImportError:
            raise  # a missing SDK is a deployment error, never a service-unavailable result
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
            try:
                from azure.core.credentials import AzureKeyCredential  # lazy: optional dependency
                from azure.eventgrid import EventGridPublisherClient  # lazy: optional dependency
            except ImportError as exc:
                raise ImportError(
                    "EventGridMessageSender requires azure-eventgrid — install it with "
                    "'pip install benzene-azure[eventgrid]'."
                ) from exc

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
        except ImportError:
            raise  # a missing SDK is a deployment error, never a service-unavailable result
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()
