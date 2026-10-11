"""RabbitMQ inbound binding — a self-hosted consumer (transport-bindings §"RabbitMQ").

A RabbitMQ delivery carries the Benzene **topic** in a ``topic`` header inside its AMQP
``properties.headers`` (the cross-port convention shared with SQS/SNS/Pub/Sub/Service Bus/Kafka,
wire-contracts §2) — or, when no such header is present, in its AMQP routing key, so a non-Benzene
producer still routes; the remaining application headers are the Benzene headers (UTF-8 decoded), and the
message body is the JSON body. There is **no response channel** — one delivery is one pipeline
invocation and one DI scope, and the outcome **settles** the delivery (transport-bindings §1,
obligation 8): acked when its invocation ended ``isSuccessful`` (``ignored`` included), otherwise
nacked — a failure, a missing result, an unmatched topic — with ``requeue`` (redelivered) or without
(to the queue's dead-letter exchange). The unit of settlement is the one delivery.
``RabbitMqConsumerApp(..., settlement=Settlement(at_most_once=True))`` acks whatever the outcome;
``Settlement(ignore_unmatched_topics=True)`` acks a topic no handler serves as ``ignored``.

Everything here is duck-typed against the ``pika`` shapes (a delivery's ``method`` /
``properties.headers`` / ``body`` triple; ``channel.basic_get()`` / ``channel.basic_ack()`` /
``channel.basic_nack()``), so the binding — decode, per-delivery dispatch, and the consumer loop — is
exercised in memory with fakes and needs neither a broker nor the SDK. Only the real client is an
optional dependency. Mirrors .NET's ``Benzene.RabbitMq``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import Any, ClassVar

from benzene.core import (
    QUEUE,
    AppDefinition,
    BenzeneMessageApplication,
    Settlement,
    SettlementDefaults,
    application_from,
    dispatch_one_way,
    read_message_metadata,
    redelivers,
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
    (the reserved metadata key); every other header becomes a Benzene header. A delivery with no topic
    header falls back to the delivery's AMQP **routing key** (``method.routing_key``), so a message from
    a non-Benzene producer still routes (``topicSources`` in ``transport-metadata-cases.json``); the
    header wins whenever it is present. The message body (bytes) is decoded as the UTF-8 JSON body.
    ``method`` is the delivery frame — the loop also reads its ``delivery_tag`` to ack.
    """
    raw_headers = getattr(properties, "headers", None)
    topic, headers = read_message_metadata(_decode_headers(raw_headers))
    if not topic:
        topic = _routing_key(method)
    if body is None:
        decoded = ""
    elif isinstance(body, (bytes, bytearray)):
        decoded = bytes(body).decode("utf-8")
    else:
        decoded = str(body)
    return {"topic": topic, "headers": headers, "body": decoded}


def _routing_key(method: Any) -> str:
    """The delivery's AMQP routing key (pika's ``Basic.Deliver`` / ``Basic.GetOk``), or ``""``."""
    key = getattr(method, "routing_key", None)
    if isinstance(key, (bytes, bytearray)):
        return bytes(key).decode("utf-8", "replace")
    return "" if key is None else str(key)


class RabbitMqConsumerApp:
    """Runs one RabbitMQ delivery through the Benzene pipeline (one delivery → one invocation → scope).

    :meth:`handle_message` returns the invocation's :class:`~benzene.results.Result` — decoded from
    the response envelope with its ``isSuccessful`` authoritative, never re-derived from the status
    text — for the loop to *settle* (ack / nack), not to send back to a caller. A malformed body is a
    ``bad-request`` and a handler fault a ``service-unavailable``, so content never crashes the loop.
    """

    SETTLEMENT_DEFAULTS: ClassVar[tuple[SettlementDefaults, ...]] = (redelivers("rabbitmq", QUEUE),)

    def __init__(
        self, application: BenzeneMessageApplication, *, settlement: Settlement = Settlement()
    ) -> None:
        self._application = application
        self._settlement = settlement

    @classmethod
    def from_definition(
        cls, definition: AppDefinition, *, settlement: Settlement = Settlement()
    ) -> RabbitMqConsumerApp:
        """Build the consumer app from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(application_from(definition), settlement=settlement)

    @property
    def settlement(self) -> Settlement:
        """How the loop settles each delivery: acked iff ``settlement.acknowledges(result)``."""
        return self._settlement

    async def handle_message(self, method: Any, properties: Any, body: Any) -> Result:
        outcome = await dispatch_one_way(
            self._application, self._settlement, decode_rabbitmq_message, method, properties, body
        )
        return outcome.result


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
    the loop mirrors the Kafka binding. With ``ack=True`` (the default) a delivery ``app.settlement``
    acknowledges — by default a **successful** one — is acked and any other is nacked with ``requeue``
    (redelivered rather than silently dropped; ``requeue=False`` sends it to the dead-letter exchange);
    a caller wanting manual acknowledgement passes ``ack=False`` and acts from ``on_result``. ``should_continue`` bounds the loop (a real worker loops forever; a test stops after
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
        # Settlement: ack what the app's Settlement acknowledges (by default a successful outcome);
        # nack the rest for redelivery, or for the dead-letter exchange with requeue=False.
        if app.settlement.acknowledges(result):
            await asyncio.to_thread(channel.basic_ack, delivery_tag=method.delivery_tag)
        else:
            await asyncio.to_thread(
                channel.basic_nack, delivery_tag=method.delivery_tag, requeue=requeue
            )
