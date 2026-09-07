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

A *missing* SDK raises an ImportError naming that class's extra (``benzene-azure[servicebus]`` /
``[storage]`` / ``[eventgrid]``) straight out of ``send_message`` — a forgotten extra is a deployment
error, not a message outcome, so it is never mapped to ``service-unavailable`` for retries and circuit
breakers to hammer.

Each ``send_message`` runs its blocking Azure SDK call via :func:`asyncio.to_thread`, so an
``await sender.send_message(...)`` never blocks the event loop (matching the consumer loops and the
other transports' clients).

**Batches.** Every sender here also implements :class:`~benzene.core.BatchMessageSender` —
``send_batch([(topic, message), ...])``, reporting a per-message outcome — but Azure's batching
model is not AWS's, and the difference is visible in the results:

* :class:`ServiceBusMessageSender` and :class:`EventHubMessageSender` batch by **size**: the SDK
  hands out a batch object that refuses a message once it is full, so messages are packed until it
  says stop and each full batch is sent. The send is **atomic per batch** — the broker takes all of
  it or none — so a failed send fails exactly that batch's messages, at their caller indices, and
  the batches around it are unaffected. A single message too large for an *empty* batch is its own
  ``bad-request`` failure (it cannot get smaller on a retry) and does not abort the rest.
* :class:`EventGridMessageSender` publishes up to 100 events per ``send`` call, likewise atomic per
  chunk.
* :class:`QueueStorageMessageSender` has **no batch API at all** — a Storage Queue takes one message
  per call — so its ``send_batch`` is a documented *sequential fallback*: N round trips, never
  presented as one, with each message's own outcome reported. Nothing here fakes atomicity.

Every batch entry is built by the same ``_make_message`` / ``_make_event`` helper the single send
uses, so batching changes how messages are transmitted and never what a message is.
"""

from __future__ import annotations

import asyncio
import base64
import json
import uuid
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import Any

from benzene.core import (
    BatchResult,
    FailedMessage,
    chunked,
    encode_body,
    send_batch_sequentially,
    to_jsonable,
)
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
    # ``application_properties`` is typed with the SDK's own wider key/value types, and dicts are
    # invariant, so the dict[str, str] this seam declares does not satisfy the parameter (visible
    # only when the optional SDK is installed to typecheck against). Widen it here rather than at
    # the seam, which would leak the SDK's types into an optional import.
    application_properties: Any = properties
    return ServiceBusMessage(body, application_properties=application_properties)


def _event_hub_event(body: str | bytes, properties: dict[str, str]) -> Any:
    """The default :class:`EventHubMessageSender` ``event_factory``: a real ``EventData``.

    The Event Hub twin of :func:`_service_bus_message`, and for the same two reasons: it is the seam
    an injected factory replaces (so the egress contract is testable with no SDK installed), and it
    turns a missing extra into an ImportError naming it rather than a ``service-unavailable`` result
    that retry middleware and circuit breakers would hammer forever.
    """
    try:
        from azure.eventhub import EventData  # lazy: optional dependency
    except ImportError as exc:
        raise ImportError(
            "EventHubMessageSender requires azure-eventhub — install it with "
            "'pip install benzene-azure[eventhub]'."
        ) from exc
    event = EventData(body)
    event.properties = dict(properties)
    return event


def _require_config(
    *, sender: str, injected: str, injected_value: Any, **required: str | None
) -> None:
    """Fail at construction when a sender has neither an injected client nor the config to build one.

    Every sender here can be built two ways: hand it an already-constructed SDK client (the seam
    tests use), or hand it the connection details and let it build one lazily on first send. Supply
    neither - overwhelmingly because an environment variable was unset and its ``None`` went
    straight through - and the Azure SDK used to raise from somewhere inside itself, on the *message
    path*, with a message naming neither the Benzene class nor the argument that was missing.

    Checking here instead makes it a start-up failure, which is the house style: a misconfigured
    service should refuse to boot rather than accept traffic and fail every message with
    ``service-unavailable``. ``send_message`` keeps its never-raise contract for everything that
    happens after construction, because a sender that raises mid-loop would take a worker down for
    what may be a transient broker outage.
    """
    if injected_value is not None:
        return
    missing = [name for name, value in required.items() if value is None]
    if not missing:
        return
    needed = ", ".join(f"{name}=" for name in sorted(missing))
    raise ValueError(
        f"{sender} is missing {needed} and no {injected}= was injected, so it cannot build a "
        f"client. Pass the missing argument(s) - if they come from environment variables, check "
        f"those are actually set - or inject an already-built client with {injected}=."
    )


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
        _require_config(
            sender="ServiceBusMessageSender",
            injected="sender",
            injected_value=sender,
            connection_string=connection_string,
            entity_name=entity_name,
        )
        self._connection_string = connection_string
        self._entity_name = entity_name
        self._sender = sender
        self._serialize = serializer or encode_body
        self._message_factory = message_factory or _service_bus_message

    def _make_message(self, topic: str, message: Any, headers: dict[str, str] | None) -> Any:
        # The SDK object is built by ``self._message_factory`` (default: ``_service_bus_message``),
        # so this stays a plain dict[str, str] and the lazy SDK import lives in the factory.
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

            client = ServiceBusClient.from_connection_string(str(self._connection_string))
            self._sender = client.get_queue_sender(queue_name=str(self._entity_name))
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

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """Pack messages into ``ServiceBusMessageBatch`` objects and send each one.

        Service Bus batches by size, not by count: ``create_message_batch()`` hands out a batch that
        raises ``ValueError`` once the next message would not fit, which is the signal to send what
        is packed and start another. The send is **atomic per batch**, so a failure is reported
        against every caller index that batch carried — and no others.
        """
        if not messages:
            return BatchResult()  # nothing to send is no broker call at all
        sender = self._get_sender()  # ImportError here: a missing SDK is never a message outcome
        failures = await asyncio.to_thread(
            self._send_batches, sender, list(enumerate(messages)), headers
        )
        return BatchResult(tuple(failures))

    def _send_batches(
        self,
        sender: Any,
        pending: list[tuple[int, tuple[str, Any]]],
        headers: dict[str, str] | None,
    ) -> list[FailedMessage]:
        failures: list[FailedMessage] = []
        position = 0
        while position < len(pending):
            batch = sender.create_message_batch()
            packed: list[int] = []
            while position < len(pending):
                index, (topic, message) = pending[position]
                try:
                    wire_message = self._make_message(topic, message, headers)
                except Exception as ex:  # one unserializable payload is that entry's failure alone
                    failures.append(FailedMessage(index, Status.BAD_REQUEST, str(ex)))
                    position += 1
                    continue
                try:
                    batch.add_message(wire_message)
                except ValueError as ex:
                    if not packed:
                        # It does not fit an *empty* batch, so no batch will ever take it; a retry
                        # cannot make it smaller. Fail this one message and carry on with the rest.
                        failures.append(FailedMessage(index, Status.BAD_REQUEST, str(ex)))
                        position += 1
                        continue
                    break  # the batch is full: send it and start another with this message
                packed.append(index)
                position += 1
            if not packed:
                continue
            try:
                sender.send_messages(batch)
            except Exception as ex:  # atomic: the broker took none of this batch
                failures.extend(
                    FailedMessage(index, Status.SERVICE_UNAVAILABLE, str(ex)) for index in packed
                )
        return failures


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
        event_factory: Callable[[str | bytes, dict[str, str]], Any] | None = None,
    ) -> None:
        # eventhub_name is NOT required: a Service Bus/Event Hub connection string may carry the
        # entity in its own EntityPath, and the SDK accepts None for the name in that case.
        _require_config(
            sender="EventHubMessageSender",
            injected="producer",
            injected_value=producer,
            connection_string=connection_string,
        )
        self._connection_string = connection_string
        self._eventhub_name = eventhub_name
        self._producer = producer
        self._serialize = serializer or encode_body
        self._event_factory = event_factory or _event_hub_event

    def _get_producer(self) -> Any:
        if self._producer is None:
            from azure.eventhub import EventHubProducerClient  # lazy: optional dependency

            self._producer = EventHubProducerClient.from_connection_string(
                conn_str=str(self._connection_string),
                eventhub_name=self._eventhub_name,
            )
        return self._producer

    def _make_event(self, topic: str, message: Any, headers: dict[str, str] | None) -> Any:
        # The SDK object is built by ``self._event_factory`` (default: ``_event_hub_event``), so
        # this stays a plain dict[str, str] and the lazy SDK import lives in the factory.
        properties = {str(k): str(v) for k, v in (headers or {}).items()}
        properties[TOPIC_PROPERTY] = topic
        return self._event_factory(self._serialize(message), properties)

    def _send_sync(self, topic: str, message: Any, headers: dict[str, str] | None) -> None:
        event = self._make_event(topic, message, headers)
        producer = self._get_producer()
        batch = producer.create_batch()
        batch.add(event)
        producer.send_batch(batch)

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            await asyncio.to_thread(self._send_sync, topic, message, headers)
        except ImportError:
            raise  # a missing SDK is a deployment error, never a service-unavailable result
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """Pack events into ``EventDataBatch`` objects and send each one.

        Like Service Bus, an Event Hub batch is bounded by *size*: ``create_batch()`` hands out a
        batch whose ``add`` raises ``ValueError`` once the next event will not fit, and the send is
        **atomic per batch**, so a failure is reported against exactly the caller indices that batch
        carried. All events go into unkeyed batches: an ``EventDataBatch``'s partition key is fixed
        at creation and this sender does not expose one, so there is nothing to group by (a keyed
        producer would need per-key batches, and .NET's client groups for exactly that reason).
        """
        if not messages:
            return BatchResult()  # nothing to send is no broker call at all
        producer = self._get_producer()
        failures = await asyncio.to_thread(
            self._send_batches, producer, list(enumerate(messages)), headers
        )
        return BatchResult(tuple(failures))

    def _send_batches(
        self,
        producer: Any,
        pending: list[tuple[int, tuple[str, Any]]],
        headers: dict[str, str] | None,
    ) -> list[FailedMessage]:
        failures: list[FailedMessage] = []
        position = 0
        while position < len(pending):
            batch = producer.create_batch()
            packed: list[int] = []
            while position < len(pending):
                index, (topic, message) = pending[position]
                try:
                    event = self._make_event(topic, message, headers)
                except ImportError:
                    raise  # a missing SDK is a deployment error, never a per-message outcome
                except Exception as ex:  # one unserializable payload fails alone
                    failures.append(FailedMessage(index, Status.BAD_REQUEST, str(ex)))
                    position += 1
                    continue
                try:
                    batch.add(event)
                except ValueError as ex:
                    if not packed:
                        # Too large for an *empty* batch: no batch will ever take it, and a retry
                        # cannot make it smaller. Fail this one event and carry on with the rest.
                        failures.append(FailedMessage(index, Status.BAD_REQUEST, str(ex)))
                        position += 1
                        continue
                    break  # the batch is full: send it and start another with this event
                packed.append(index)
                position += 1
            if not packed:
                continue
            try:
                producer.send_batch(batch)
            except Exception as ex:  # atomic: the hub took none of this batch
                failures.extend(
                    FailedMessage(index, Status.SERVICE_UNAVAILABLE, str(ex)) for index in packed
                )
        return failures


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
        # Two valid config shapes rather than one, so this cannot use _require_config's
        # all-of-these rule: either a queue_url on its own, or a connection_string plus the
        # queue_name to look up inside that account.
        if client is None and queue_url is None and not (connection_string and queue_name):
            raise ValueError(
                "QueueStorageMessageSender needs either queue_url=, or both connection_string= "
                "and queue_name=, and no client= was injected, so it cannot build a client. If "
                "these come from environment variables, check those are actually set."
            )
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
                    self._connection_string, str(self._queue_name)
                )
            else:
                self._client = QueueClient.from_queue_url(str(self._queue_url))
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

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """**Sequential fallback**: a Storage Queue takes one message per call, so this is N calls.

        ``QueueClient`` exposes no batch-send API — unlike Service Bus, there is no batch object to
        fill — so rather than dress a loop up as one atomic send, this says what it is. The seam's
        contract still holds: every message is attempted, one failure aborts nothing, and each
        failure is reported at the caller's index with the status ``send_message`` returned. Reach
        for Service Bus or Event Hub when the throughput of a real batch API is what you need.
        """
        return await send_batch_sequentially(self, messages, headers)


EVENT_GRID_BATCH_LIMIT = 100
"""Events per ``EventGridPublisherClient.send`` call — the chunk size .NET's
``EventGridBatchMessageClient`` uses, and comfortably inside Event Grid's 1 MB per-request limit for
typical events. (The service reports an oversized request for the whole call, which arrives as that
chunk's failures; drop this if your events are large.)"""


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
        _require_config(
            sender="EventGridMessageSender",
            injected="client",
            injected_value=client,
            topic_endpoint=topic_endpoint,
            key=key,
        )
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
                str(self._topic_endpoint), AzureKeyCredential(str(self._key))
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

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """Publish up to :data:`EVENT_GRID_BATCH_LIMIT` events per ``send`` call.

        ``EventGridPublisherClient.send`` takes a list as readily as a single event, and a chunk is
        **atomic**: it is accepted whole or it raises, so a failure is reported against every caller
        index in that chunk and none outside it.
        """
        client = self._get_client()  # ImportError here: a missing SDK is never a message outcome
        failures: list[FailedMessage] = []
        for chunk in chunked(messages, EVENT_GRID_BATCH_LIMIT):
            failures.extend(await asyncio.to_thread(self._send_chunk, client, chunk, headers))
        return BatchResult(tuple(failures))

    def _send_chunk(
        self, client: Any, chunk: list[tuple[int, tuple[str, Any]]], headers: dict[str, str] | None
    ) -> list[FailedMessage]:
        events, failures, sent = [], [], []
        for index, (topic, message) in chunk:
            try:
                events.append(self._make_event(topic, message, headers))
            except Exception as ex:  # one unserializable payload is that entry's failure alone
                failures.append(FailedMessage(index, Status.BAD_REQUEST, str(ex)))
                continue
            sent.append(index)
        if not events:
            return failures
        try:
            client.send(events)
        except Exception as ex:  # atomic: the topic took none of this chunk
            failures.extend(
                FailedMessage(index, Status.SERVICE_UNAVAILABLE, str(ex)) for index in sent
            )
        return failures
