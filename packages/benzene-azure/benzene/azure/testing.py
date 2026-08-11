"""Native-message builders + test host for the Azure binding (mirrors .NET's ``*.TestHelpers``).

Drive an :class:`~benzene.azure.AzureFunctionsApp` with stand-ins for every Azure Functions trigger
input — HTTP, Service Bus, Event Hub, Queue Storage, Blob Storage, Cosmos DB change feed, Timer, and
Event Grid (native schema + CloudEvents 1.0) — in memory, no Azure SDK required.
"""

from __future__ import annotations

import base64
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from benzene.core import encode_body, to_jsonable

from .app import AzureFunctionsApp, AzureHttpResponse

if TYPE_CHECKING:
    from benzene.core import Scope


@dataclass
class FakeServiceBusMessage:
    """Stand-in for an ``azure.functions.ServiceBusMessage`` (``get_body`` + ``application_properties``)."""

    _body: bytes
    application_properties: dict[str, str] = field(default_factory=dict)

    def get_body(self) -> bytes:
        return self._body


@dataclass
class FakeEventHubEvent:
    """Stand-in for an ``azure.functions.EventHubEvent`` (``get_body`` + ``properties``)."""

    _body: bytes
    properties: dict[str, str] = field(default_factory=dict)

    def get_body(self) -> bytes:
        return self._body


def _with_topic(topic: str, headers: dict[str, str] | None) -> dict[str, str]:
    props = {str(k): str(v) for k, v in (headers or {}).items()}
    props["topic"] = topic
    return props


def service_bus_message(
    topic: str, body: Any, headers: dict[str, str] | None = None
) -> FakeServiceBusMessage:
    return FakeServiceBusMessage(encode_body(body).encode("utf-8"), _with_topic(topic, headers))


def event_hub_event(
    topic: str, body: Any, headers: dict[str, str] | None = None
) -> FakeEventHubEvent:
    return FakeEventHubEvent(encode_body(body).encode("utf-8"), _with_topic(topic, headers))


@dataclass
class FakeQueueMessage:
    """Stand-in for an ``azure.functions.QueueMessage`` (``get_body`` returns the raw queue text)."""

    _body: bytes

    def get_body(self) -> bytes:
        return self._body


@dataclass
class FakeBlobTrigger:
    """Stand-in for an ``azure.functions.InputStream`` blob trigger (``name`` / ``uri`` / ``metadata``)."""

    name: str
    uri: str = ""
    length: int | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    content_type: str | None = None


@dataclass
class FakeTimerRequest:
    """Stand-in for an ``azure.functions.TimerRequest`` (``past_due`` + schedule status)."""

    past_due: bool = False
    schedule_status: dict[str, str] = field(default_factory=dict)
    schedule: dict[str, Any] = field(default_factory=dict)


def queue_storage_message(
    topic: str,
    body: Any,
    headers: dict[str, str] | None = None,
    *,
    base64_encode: bool = False,
) -> FakeQueueMessage:
    """Build a Queue message carrying a Benzene envelope — the shape ``QueueStorageMessageSender`` writes."""
    envelope = {
        "topic": topic,
        "headers": {str(k): str(v) for k, v in (headers or {}).items()},
        "body": encode_body(body),
    }
    text = json.dumps(envelope)
    if base64_encode:
        text = base64.b64encode(text.encode("utf-8")).decode("ascii")
    return FakeQueueMessage(text.encode("utf-8"))


def queue_text_message(text: str, *, base64_encode: bool = False) -> FakeQueueMessage:
    """Build a *plain-text* Queue message (no envelope) — routed by the trigger's ``default_topic``."""
    if base64_encode:
        text = base64.b64encode(text.encode("utf-8")).decode("ascii")
    return FakeQueueMessage(text.encode("utf-8"))


def blob_event(
    name: str,
    uri: str = "",
    *,
    length: int | None = None,
    metadata: dict[str, str] | None = None,
    content_type: str | None = None,
) -> FakeBlobTrigger:
    """Build a blob-created trigger stand-in."""
    return FakeBlobTrigger(
        name=name,
        uri=uri,
        length=length,
        metadata=dict(metadata or {}),
        content_type=content_type,
    )


def cosmos_change_feed(*documents: Any) -> list[Any]:
    """Build a Cosmos DB change-feed batch — one changed document (a dict) per element."""
    return [to_jsonable(document) for document in documents]


def timer_request(
    *,
    past_due: bool = False,
    schedule_status: dict[str, str] | None = None,
    schedule: dict[str, Any] | None = None,
) -> FakeTimerRequest:
    """Build a Timer trigger stand-in (one tick)."""
    return FakeTimerRequest(
        past_due=past_due,
        schedule_status=dict(schedule_status or {}),
        schedule=dict(schedule or {}),
    )


def event_grid_event(
    event_type: str,
    data: Any,
    headers: dict[str, str] | None = None,
    *,
    subject: str = "benzene",
) -> dict[str, Any]:
    """Build a **native** Event Grid event ``{eventType, subject, data, headers, ...}``."""
    return {
        "id": str(uuid.uuid4()),
        "eventType": event_type,
        "subject": subject,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "dataVersion": "1.0",
        "data": to_jsonable(data),
        "headers": {str(k): str(v) for k, v in (headers or {}).items()},
    }


def event_grid_cloud_event(
    event_type: str,
    data: Any,
    headers: dict[str, str] | None = None,
    *,
    source: str = "benzene",
    subject: str = "benzene",
) -> dict[str, Any]:
    """Build a **CloudEvents 1.0** event ``{specversion, type, source, data, ...}``; headers as extensions."""
    event: dict[str, Any] = {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": source,
        "type": event_type,
        "subject": subject,
        "time": datetime.now(timezone.utc).isoformat(),
        "data": to_jsonable(data),
    }
    event.update({str(k).lower(): str(v) for k, v in (headers or {}).items()})
    return event


class AzureFunctionsTestHost:
    """Wraps an :class:`AzureFunctionsApp` for in-memory tests of each trigger."""

    #: The resolved root scope, set by ``create_test_host(...).build_azure()`` for assertions.
    scope: Scope | None = None

    def __init__(self, app: AzureFunctionsApp) -> None:
        self._app = app

    def send_http(
        self,
        method: str,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
        query: dict[str, str] | None = None,
    ) -> AzureHttpResponse:
        return self._app.handle_http(
            method=method,
            path=path,
            query_string=urlencode(query or {}),
            headers=headers or {},
            body="" if body is None else encode_body(body),
        )

    def send_service_bus(
        self, topic: str, body: Any, headers: dict[str, str] | None = None
    ) -> None:
        self._app.handle_service_bus(service_bus_message(topic, body, headers))

    def send_event_hub(self, topic: str, body: Any, headers: dict[str, str] | None = None) -> None:
        self._app.handle_event_hub([event_hub_event(topic, body, headers)])

    def send_event_hub_batch(self, events: list[FakeEventHubEvent]) -> None:
        self._app.handle_event_hub(events)

    def send_queue_storage(
        self,
        topic: str,
        body: Any,
        headers: dict[str, str] | None = None,
        *,
        default_topic: str = "",
    ) -> None:
        self._app.handle_queue_storage(
            queue_storage_message(topic, body, headers), default_topic=default_topic
        )

    def send_queue_text(self, text: str, *, default_topic: str = "") -> None:
        self._app.handle_queue_storage(queue_text_message(text), default_topic=default_topic)

    def send_blob(
        self,
        name: str,
        uri: str = "",
        *,
        default_topic: str = "blob:created",
        **fields: Any,
    ) -> None:
        self._app.handle_blob(blob_event(name, uri, **fields), default_topic=default_topic)

    def send_cosmos(self, documents: list[Any], *, default_topic: str = "cosmos:change") -> None:
        self._app.handle_cosmos(cosmos_change_feed(*documents), default_topic=default_topic)

    def send_timer(self, *, past_due: bool = False, default_topic: str = "timer:tick") -> None:
        self._app.handle_timer(timer_request(past_due=past_due), default_topic=default_topic)

    def send_event_grid(self, event: Any, *, default_topic: str = "") -> None:
        self._app.handle_event_grid(event, default_topic=default_topic)
