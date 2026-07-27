"""Azure outbound client (Service Bus) implementing the ``benzene.core.MessageSender`` port.

Forwards the Benzene topic and headers onto the message's ``application_properties`` (the ``topic``
property plus one per header). ``azure-servicebus`` is an optional dependency, imported lazily.
"""

from __future__ import annotations

import json
from typing import Any, Callable

from benzene.results import Result, Status

from .events import TOPIC_PROPERTY


def _default_serialize(message: Any) -> str:
    from dataclasses import asdict, is_dataclass

    if isinstance(message, str):
        return message
    if is_dataclass(message) and not isinstance(message, type):
        return json.dumps(asdict(message))
    return json.dumps(message)


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
        self._serialize = serializer or _default_serialize

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
            self._get_sender().send_messages(self._make_message(topic, message, headers))
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()
