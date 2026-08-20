"""RabbitMQ inbound binding — a self-hosted consumer (transport-bindings §"RabbitMQ").

A RabbitMQ delivery carries the Benzene **topic** in a ``topic`` header inside its AMQP
``properties.headers`` (the cross-port convention shared with SQS/SNS/Pub/Sub/Service Bus/Kafka,
wire-contracts §2); the remaining application headers are the Benzene headers (UTF-8 decoded), and the
message body is the JSON body. There is **no response channel** — per the spec, result mapping is
*acknowledge/log only*: one delivery is one pipeline invocation and one DI scope, and the handler's
result governs whether the loop acks the delivery (success), requeues it for redelivery (a transient
failure), or drops it to the queue's dead-letter exchange (a final one) — never a reply.

Everything here is duck-typed against the ``pika`` shapes (a delivery's ``method`` /
``properties.headers`` / ``body`` triple; ``channel.basic_get()`` / ``channel.basic_ack()`` /
``channel.basic_nack()``), so the binding — decode, per-delivery dispatch, and the consumer loop — is
exercised in memory with fakes and needs neither a broker nor the SDK. Only the real client is an
optional dependency. Mirrors .NET's ``Benzene.RabbitMq``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from typing import Any

from benzene.core import (
    DEFAULT_RETRYABLE,
    AppDefinition,
    BenzeneMessageApplication,
    application_from,
    read_message_metadata,
)
from benzene.results import Result

logger = logging.getLogger(__name__)


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
    queue: str,
    should_continue: Callable[[], bool] = lambda: True,
    ack: bool = True,
    requeue: bool = True,
    idle_sleep: float = 1.0,
    on_result: Callable[[Any, Result], None] | None = None,
) -> None:
    """Drive a self-hosted consumer: pull a delivery, dispatch it, ack on success (at-least-once).

    ``queue`` (required) is the AMQP queue to pull from — the binding never guesses a name. ``channel``
    is duck-typed (``basic_get(queue)`` → a ``(method, properties, body)`` triple, or
    ``(None, None, None)`` when the queue is empty; ``basic_ack(delivery_tag=...)`` /
    ``basic_nack(delivery_tag=..., requeue=...)``) — pika's callback model is flattened to a poll so
    the loop mirrors the Kafka binding. With ``ack=True`` (the default, at-least-once) a **successful**
    result acks the delivery; a failed one is nacked, and *how* depends on the status:

    - a **transient** failure (:data:`~benzene.core.DEFAULT_RETRYABLE`: ``service-unavailable``,
      ``timeout``, ``too-many-requests``) is nacked with ``requeue`` (the ``requeue=False`` argument
      turns even these into drops), so it is redelivered rather than silently dropped;
    - any **other** failure is deterministic — a malformed body (``bad-request``) or an unknown topic
      (``not-found``) will fail identically on every redelivery — so it is nacked with
      ``requeue=False``. The delivery leaves the queue instead of returning to its head and spinning
      the loop at full speed. Declare the queue with a **dead-letter exchange**
      (``x-dead-letter-exchange``) and the broker routes these dropped deliveries there for
      inspection/replay; without a DLX they are discarded.

    A caller wanting manual acknowledgement passes ``ack=False`` and acts from ``on_result``.
    ``should_continue`` bounds the loop (a real worker loops forever; a test stops after N pulls), and
    ``idle_sleep`` is the backoff awaited after an empty pull — ``basic_get`` on an empty queue returns
    immediately, so without it an idle consumer busy-polls the broker (RabbitMQ has no equivalent of
    Kafka's ``poll_timeout`` or SQS's long poll).

    ``basic_get``/``basic_ack``/``basic_nack`` are plain synchronous ``pika`` calls (each a blocking
    network round-trip), run via :func:`asyncio.to_thread` rather than called directly on the event
    loop. Called directly, they would block every other coroutine sharing the loop (e.g. an HTTP server
    hosted alongside this consumer); the ``to_thread`` hop — plus the ``idle_sleep`` await on an empty
    pull — keeps the loop responsive (matching the Kafka/SQS consumer loops).
    """
    while should_continue():
        method, properties, body = await asyncio.to_thread(channel.basic_get, queue)
        if method is None:
            await asyncio.sleep(idle_sleep)  # an empty queue: back off instead of busy-polling
            continue
        result = await app.handle_message(method, properties, body)
        if on_result is not None:
            on_result(method, result)
        if not ack:
            continue
        # At-least-once: ack only a successful outcome. A failed delivery is nacked — requeued while
        # the failure looks transient, dropped (to the queue's dead-letter exchange) when it is final.
        if result.is_successful:
            await asyncio.to_thread(channel.basic_ack, delivery_tag=method.delivery_tag)
        else:
            redeliver = requeue and result.status in DEFAULT_RETRYABLE
            topic, _ = read_message_metadata(_decode_headers(getattr(properties, "headers", None)))
            if redeliver:
                logger.warning(
                    "delivery %s on topic %r failed with status %s; requeued for redelivery",
                    method.delivery_tag,
                    topic,
                    result.status,
                )
            else:
                logger.warning(
                    "delivery %s on topic %r failed with status %s; dropped without requeue "
                    "(the queue's dead-letter exchange receives it, if one is configured)",
                    method.delivery_tag,
                    topic,
                    result.status,
                )
            await asyncio.to_thread(
                channel.basic_nack, delivery_tag=method.delivery_tag, requeue=redeliver
            )
