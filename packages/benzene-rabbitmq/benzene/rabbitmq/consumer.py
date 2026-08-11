"""RabbitMQ inbound binding — a self-hosted consumer (transport-bindings §"RabbitMQ").

A RabbitMQ delivery carries the Benzene **topic** in a ``topic`` header inside its AMQP
``properties.headers`` (the cross-port convention shared with SQS/SNS/Pub/Sub/Service Bus/Kafka,
wire-contracts §2); the remaining application headers are the Benzene headers (UTF-8 decoded), and the
message body is the JSON body. There is **no response channel** — per the spec, result mapping is
*acknowledge/log only*: one delivery is one pipeline invocation and one DI scope, and the handler's
result governs whether the loop acks the delivery (success) or nacks/leaves it for redelivery, never a
reply.

Everything here is duck-typed against the ``pika`` shapes (a delivery's ``method`` /
``properties.headers`` / ``body`` triple; ``channel.basic_get()`` / ``channel.basic_ack()`` /
``channel.basic_nack()``), so the binding — decode, per-delivery dispatch, and the consumer loop — is
exercised in memory with fakes and needs neither a broker nor the SDK. Only the real client is an
optional dependency. Mirrors .NET's ``Benzene.RabbitMq``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import Any

from benzene.core import (
    AppDefinition,
    BenzeneMessageApplication,
    application_from,
    read_message_metadata,
)
from benzene.results import Result


def _decode_headers(raw: Mapping[str, Any] | None) -> dict[str, str]:
    """AMQP application headers (a ``dict`` of ``str`` → bytes/str) → the flat string metadata dict."""
    attributes: dict[str, str] = {}
    for key, value in (raw or {}).items():
        if isinstance(value, (bytes, bytearray)):
            attributes[key] = bytes(value).decode("utf-8", "replace")
        else:
            attributes[key] = "" if value is None else str(value)
    return attributes


def decode_rabbitmq_message(method: Any, properties: Any, body: Any) -> dict[str, Any]:
    """Decode a RabbitMQ delivery into a Benzene envelope ``{topic, headers, body}``.

    The Benzene topic is resolved out of the ``topic`` header carried in AMQP ``properties.headers``
    (the reserved metadata key); every other header becomes a Benzene header. The message body (bytes)
    is decoded as the UTF-8 JSON body. ``method`` (the delivery frame) is unused by the decode — the
    loop reads its ``delivery_tag`` to ack — but is accepted so the signature matches pika's
    ``(method, properties, body)`` delivery triple.
    """
    raw_headers = getattr(properties, "headers", None)
    topic, headers = read_message_metadata(_decode_headers(raw_headers))
    if body is None:
        decoded = ""
    elif isinstance(body, (bytes, bytearray)):
        decoded = bytes(body).decode("utf-8")
    else:
        decoded = str(body)
    return {"topic": topic, "headers": headers, "body": decoded}


class RabbitMqConsumerApp:
    """Runs one RabbitMQ delivery through the Benzene pipeline (one delivery → one invocation → scope).

    :meth:`handle_message` never raises — the entry point always returns a response envelope (a
    malformed body is a ``bad-request``, a handler fault a ``service-unavailable``), so a poison
    delivery can never crash the consumer loop (the binding's failure rule). Since RabbitMQ has no
    response channel, the mapped :class:`~benzene.results.Result` is returned for the loop to *act on*
    (ack / nack / log), not sent back to a caller.
    """

    def __init__(self, application: BenzeneMessageApplication) -> None:
        self._application = application

    @classmethod
    def from_definition(cls, definition: AppDefinition) -> RabbitMqConsumerApp:
        """Build the consumer app from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(application_from(definition))

    async def handle_message(self, method: Any, properties: Any, body: Any) -> Result:
        envelope = decode_rabbitmq_message(method, properties, body)
        response = await self._application.handle(envelope)
        return Result(response["statusCode"])


async def run_consumer_loop(
    app: RabbitMqConsumerApp,
    channel: Any,
    *,
    queue: str = "",
    should_continue: Callable[[], bool] = lambda: True,
    ack: bool = True,
    requeue: bool = True,
    on_result: Callable[[Any, Result], None] | None = None,
) -> None:
    """Drive a self-hosted consumer: pull a delivery, dispatch it, ack on success (at-least-once).

    ``channel`` is duck-typed (``basic_get(queue)`` → a ``(method, properties, body)`` triple, or
    ``(None, None, None)`` when the queue is empty; ``basic_ack(delivery_tag=...)`` /
    ``basic_nack(delivery_tag=..., requeue=...)``) — pika's callback model is flattened to a poll so
    the loop mirrors the Kafka binding. With ``ack=True`` (the default, at-least-once) a **successful**
    result acks the delivery and a failed one is nacked with ``requeue`` (redelivered rather than
    silently dropped); a caller wanting manual acknowledgement passes ``ack=False`` and acts from
    ``on_result``. ``should_continue`` bounds the loop (a real worker loops forever; a test stops after
    N pulls).

    ``basic_get``/``basic_ack``/``basic_nack`` are plain synchronous ``pika`` calls (each a blocking
    network round-trip), run via :func:`asyncio.to_thread` rather than called directly on the event
    loop. Called directly, an idle queue means ``basic_get`` returning ``(None, None, None)`` in a tight
    loop with **no** ``await`` point at all — starving every other coroutine sharing the loop (e.g. an
    HTTP server hosted alongside this consumer); the ``to_thread`` hop is that missing await point and
    keeps the network calls off the loop (matching the Kafka/SQS consumer loops).
    """
    while should_continue():
        method, properties, body = await asyncio.to_thread(channel.basic_get, queue)
        if method is None:
            continue
        result = await app.handle_message(method, properties, body)
        if on_result is not None:
            on_result(method, result)
        if not ack:
            continue
        # At-least-once: ack only a successful outcome; a failed delivery is nacked for redelivery.
        if result.is_successful:
            await asyncio.to_thread(channel.basic_ack, delivery_tag=method.delivery_tag)
        else:
            await asyncio.to_thread(
                channel.basic_nack, delivery_tag=method.delivery_tag, requeue=requeue
            )
