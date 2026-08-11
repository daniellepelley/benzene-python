"""RabbitMQ outbound binding — :class:`RabbitMqMessageSender` (transport-bindings §"Outbound clients").

Implements the :class:`~benzene.core.MessageSender` port over a RabbitMQ channel: it serializes the
message to the JSON body, forwards the Benzene header dictionary onto the AMQP ``properties.headers``
(so correlation/trace propagation rides across the hop), and carries the Benzene **topic** in the
reserved ``topic`` header — the same convention the consumer reads. All Benzene topics are published to
one configured exchange, header-routed (the single-stream, header-routed pattern the Kafka/Pub/Sub
clients use too). Mirrors .NET's ``Benzene.RabbitMq``.

The ``pika`` client is an optional dependency imported lazily; inject any object exposing
``basic_publish(exchange, routing_key, body, properties)`` to test without a broker.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from benzene.core import encode_body
from benzene.results import Result, Status

#: Header name carrying the Benzene topic (the cross-port convention; the consumer resolves it).
TOPIC_HEADER = "topic"


@dataclass
class _AmqpProperties:
    """A minimal ``pika.BasicProperties`` stand-in — used when the SDK is absent (tests, no broker)."""

    headers: dict[str, str] = field(default_factory=dict)


class RabbitMqMessageSender:
    """Publishes Benzene messages to a RabbitMQ exchange, the Benzene topic tagged in ``topic``.

    ``exchange`` / ``routing_key`` are the physical AMQP destination every Benzene message is published
    to. A ``channel`` may be injected (tests, or a shared client); otherwise a ``pika`` blocking
    connection is opened lazily from ``host`` on first use. The AMQP ``properties.headers`` carry the
    Benzene headers plus the ``topic`` header, so a downstream consumer resolves the Benzene topic
    header-first, exactly as the inbound binding does.
    """

    def __init__(
        self,
        exchange: str = "",
        routing_key: str = "",
        channel: Any | None = None,
        *,
        host: str | None = None,
        serializer: Callable[[Any], str] | None = None,
    ) -> None:
        self._exchange = exchange
        self._routing_key = routing_key
        self._channel = channel
        self._host = host
        self._serialize = serializer or encode_body

    def _client(self) -> Any:
        if self._channel is None:
            import pika  # lazy: optional dependency

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._host or "localhost")
            )
            self._channel = connection.channel()
        return self._channel

    def _properties(self, headers: dict[str, str]) -> Any:
        """Build the AMQP properties carrying ``headers`` — real ``pika`` if present, else a stand-in."""
        try:
            import pika  # lazy: optional dependency

            return pika.BasicProperties(headers=headers)
        except ImportError:
            return _AmqpProperties(headers=headers)

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        amqp_headers: dict[str, str] = {str(k): str(v) for k, v in (headers or {}).items()}
        amqp_headers[TOPIC_HEADER] = topic
        data = self._serialize(message).encode("utf-8")

        try:
            channel = self._client()
            channel.basic_publish(
                exchange=self._exchange,
                routing_key=self._routing_key,
                body=data,
                properties=self._properties(amqp_headers),
            )
        except Exception as ex:  # a publish failure is service-unavailable, never a crash
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()
