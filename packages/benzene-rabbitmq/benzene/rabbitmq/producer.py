"""RabbitMQ outbound binding — :class:`RabbitMqMessageSender` (transport-bindings §"Outbound clients").

Implements the :class:`~benzene.core.MessageSender` port over a RabbitMQ channel: it serializes the
message to the JSON body, forwards the Benzene header dictionary onto the AMQP ``properties.headers``
(so correlation/trace propagation rides across the hop), and carries the Benzene **topic** in the
reserved ``topic`` header — the same convention the consumer reads. All Benzene topics are published to
one configured exchange, header-routed (the single-stream, header-routed pattern the Kafka/Pub/Sub
clients use too). Mirrors .NET's ``Benzene.RabbitMq``.

The ``pika`` client is an optional dependency imported lazily; inject any object exposing
``basic_publish(exchange, routing_key, body, properties)`` to test without a broker. A *missing* pika
is a deployment error, not a message outcome — the lazy import raises a teaching :class:`ImportError`
naming the extra rather than turning every publish into a ``service-unavailable`` result.
"""

from __future__ import annotations

import asyncio
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
    connection is opened lazily from ``host`` on first use — *inside* the worker thread, since opening
    it is blocking network I/O that must not run on the event loop. The AMQP ``properties.headers``
    carry the Benzene headers plus the ``topic`` header, so a downstream consumer resolves the Benzene
    topic header-first, exactly as the inbound binding does.

    A ``pika`` connection and its channels are **not thread-safe**, and every publish runs on a worker
    thread: concurrent ``send_message`` calls would otherwise interleave AMQP frames on the one shared
    channel. An :class:`asyncio.Lock` is therefore held across lazy client creation *and* the publish,
    so one sender issues one publish at a time (use several senders — hence several channels — for
    parallel throughput).
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
        #: Serializes client creation + publish: one pika channel, many ``to_thread`` workers.
        self._lock = asyncio.Lock()

    def _client(self) -> Any:
        """Return the channel, opening the blocking pika connection on first use (worker thread only).

        Called from inside the threaded publish — ``pika.BlockingConnection`` is a blocking network
        round-trip and must never run on the event loop — and always under :attr:`_lock`.
        """
        if self._channel is None:
            try:
                import pika  # lazy: optional dependency
            except ImportError as exc:  # a forgotten extra is a deployment error, not a send outcome
                raise ImportError(
                    "RabbitMqMessageSender requires pika — install it with "
                    "'pip install benzene-rabbitmq[rabbitmq]'."
                ) from exc

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
        properties = self._properties(amqp_headers)

        def _publish() -> None:
            # Connect (first call) and publish on the worker thread: both are blocking pika network
            # calls, so an ``await send_message(...)`` never stalls the event loop.
            channel = self._client()
            channel.basic_publish(
                exchange=self._exchange,
                routing_key=self._routing_key,
                body=data,
                properties=properties,
            )

        try:
            # One channel is not thread-safe: hold the lock across creation + publish so two workers
            # never interleave frames on it.
            async with self._lock:
                await asyncio.to_thread(_publish)
        except ImportError:
            raise  # a missing pika is a deployment error, not a per-message failure result
        except Exception as ex:  # a publish failure is service-unavailable, never a crash
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()
