"""Native-event builder + test host for the Kafka binding (mirrors the cloud ``*.testing`` modules).

Drive a :class:`~benzene.kafka.KafkaConsumerApp` exactly as a broker delivery would — a record whose
``topic`` header carries the Benzene topic, its other headers the Benzene headers, and its value the
JSON body — in memory, no broker, so an example's tests dogfood the real binding through the same
one-specialization-step setup (``create_test_host(StartUp).build_kafka()``) as every other host.

Kafka has no response channel, so :meth:`KafkaTestHost.send_kafka` returns the mapped
:class:`~benzene.results.Result` the consumer loop would act on (commit / log) rather than a reply.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from benzene.core import encode_body
from benzene.results import Result

from .consumer import KafkaConsumerApp
from .producer import TOPIC_HEADER

if TYPE_CHECKING:
    from benzene.core import Scope


@dataclass
class FakeKafkaMessage:
    """A stand-in for a ``confluent_kafka.Message``.

    Exposes the accessors the binding reads — ``headers()`` / ``value()`` / ``error()`` for the
    decode, and ``topic()`` / ``partition()`` / ``offset()`` for the loop's per-partition offset
    bookkeeping (the seek-back that keeps a failed record from being leapfrogged).
    """

    _headers: list[tuple[str, bytes]]
    _value: bytes
    _error: Any = None
    _topic: str = "benzene"
    _partition: int = 0
    _offset: int = 0

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers

    def value(self) -> bytes:
        return self._value

    def error(self) -> Any:
        return self._error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class KafkaMessageBuilder:
    """Builds a :class:`FakeKafkaMessage` for a Benzene ``topic`` + body, headers as Kafka headers."""

    def __init__(
        self,
        topic: str,
        *,
        kafka_topic: str = "benzene",
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        self._topic = topic
        self._kafka_topic = kafka_topic
        self._partition = partition
        self._offset = offset
        self._headers: dict[str, str] = {}
        self._body: str = ""

    def with_header(self, key: str, value: str) -> KafkaMessageBuilder:
        self._headers[key] = value
        return self

    def with_body(self, body: Any) -> KafkaMessageBuilder:
        self._body = encode_body(body)
        return self

    def build(self) -> FakeKafkaMessage:
        headers: list[tuple[str, bytes]] = [
            (str(k), str(v).encode("utf-8")) for k, v in self._headers.items()
        ]
        headers.append((TOPIC_HEADER, self._topic.encode("utf-8")))
        return FakeKafkaMessage(
            _headers=headers,
            _value=self._body.encode("utf-8"),
            _topic=self._kafka_topic,
            _partition=self._partition,
            _offset=self._offset,
        )


@dataclass
class RecordingKafkaConsumer:
    """An in-memory consumer that replays a fixed list of records, then reports empty polls.

    Records committed via :meth:`commit` are appended to :attr:`committed` and every
    :meth:`seek` target to :attr:`seeks`, so a test can assert the loop's at-least-once behaviour (a
    failed record is *not* committed, and the loop seeks back to it) without a broker.
    """

    records: list[Any]
    committed: list[Any] = field(default_factory=list)
    seeks: list[Any] = field(default_factory=list)

    def poll(self, _timeout: float) -> Any:
        return self.records.pop(0) if self.records else None

    def commit(self, *, message: Any) -> None:
        self.committed.append(message)

    def seek(self, partition: Any) -> None:
        self.seeks.append(partition)


class KafkaTestHost:
    """Wraps a :class:`KafkaConsumerApp` for in-memory tests of the Kafka ingress."""

    #: The resolved root scope, set by ``create_test_host(...).build_kafka()`` for assertions.
    scope: Scope | None = None

    def __init__(self, app: KafkaConsumerApp) -> None:
        self._app = app

    async def send_kafka(
        self, topic: str, body: Any = None, headers: dict[str, str] | None = None
    ) -> Result:
        """Deliver one record on ``topic`` (``headers`` become Kafka headers); returns the result."""
        builder = KafkaMessageBuilder(topic)
        for key, value in (headers or {}).items():
            builder.with_header(key, value)
        if body is not None:
            builder.with_body(body)
        return await self._app.handle_message(builder.build())
