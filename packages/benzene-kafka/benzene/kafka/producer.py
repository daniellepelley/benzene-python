"""Kafka outbound binding — :class:`KafkaMessageSender` (transport-bindings §"Outbound clients").

Implements the :class:`~benzene.core.MessageSender` port over a Kafka producer: it serializes the
message to the JSON body, forwards the Benzene header dictionary onto the record's Kafka headers (so
correlation/trace propagation rides across the hop), and carries the Benzene **topic** in the reserved
``topic`` header — the same convention the consumer reads. All Benzene topics are produced to one
configured Kafka topic, header-routed (the single-stream, header-routed pattern the Pub/Sub client
uses too).

The ``confluent-kafka`` producer is an optional dependency imported lazily; inject any object exposing
``produce(topic, value, headers, on_delivery)`` + ``flush(timeout)`` to test without a broker.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from benzene.core import encode_body
from benzene.results import Result, Status

#: Header name carrying the Benzene topic (the cross-port convention; the consumer resolves it).
TOPIC_HEADER = "topic"


class KafkaMessageSender:
    """Produces Benzene messages to a Kafka topic, the Benzene topic tagged in the ``topic`` header.

    ``kafka_topic`` is the physical Kafka topic every Benzene message is produced to. A ``producer``
    may be injected (tests, or a shared client); otherwise a ``confluent_kafka.Producer`` is created
    lazily from ``bootstrap_servers`` on first use. ``flush_timeout`` bounds the wait for the broker
    ack that turns a fire-and-buffer ``produce`` into a synchronous :class:`~benzene.results.Result`.
    """

    def __init__(
        self,
        kafka_topic: str,
        producer: Any | None = None,
        *,
        bootstrap_servers: str | None = None,
        serializer: Callable[[Any], str] | None = None,
        flush_timeout: float = 10.0,
    ) -> None:
        self._kafka_topic = kafka_topic
        self._producer = producer
        self._bootstrap_servers = bootstrap_servers
        self._serialize = serializer or encode_body
        self._flush_timeout = flush_timeout

    def _client(self) -> Any:
        if self._producer is None:
            from confluent_kafka import Producer  # lazy: optional dependency

            self._producer = Producer({"bootstrap.servers": self._bootstrap_servers or "localhost"})
        return self._producer

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        header_list: list[tuple[str, bytes]] = [
            (str(k), str(v).encode("utf-8")) for k, v in (headers or {}).items()
        ]
        header_list.append((TOPIC_HEADER, topic.encode("utf-8")))
        data = self._serialize(message).encode("utf-8")

        delivery_errors: list[Any] = []

        def _on_delivery(error: Any, _message: Any) -> None:
            if error is not None:
                delivery_errors.append(error)

        def _publish() -> None:
            producer = self._client()
            producer.produce(
                self._kafka_topic, value=data, headers=header_list, on_delivery=_on_delivery
            )
            # ``flush`` blocks up to ``flush_timeout`` for the broker ack; run the whole publish on a
            # worker thread so it never stalls the event loop (e.g. a co-hosted ASGI server).
            producer.flush(self._flush_timeout)

        try:
            await asyncio.to_thread(_publish)
        except Exception as ex:  # a produce/flush failure is service-unavailable, never a crash
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        if delivery_errors:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(delivery_errors[0]))
        return Result.ok()
