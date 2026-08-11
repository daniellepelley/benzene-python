"""Native-delivery builder + test host for the RabbitMQ binding (mirrors the ``*.testing`` modules).

Drive a :class:`~benzene.rabbitmq.RabbitMqConsumerApp` exactly as a broker delivery would — a delivery
whose ``topic`` header carries the Benzene topic, its other headers the Benzene headers, and its body
the JSON body — in memory, no broker, so an example's tests dogfood the real binding through the same
one-specialization-step setup (``create_test_host(StartUp).build_rabbitmq()``) as every other host.

RabbitMQ has no response channel, so :meth:`RabbitMqTestHost.send_rabbitmq` returns the mapped
:class:`~benzene.results.Result` the consumer loop would act on (ack / nack / log) rather than a reply.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from benzene.core import encode_body
from benzene.results import Result

from .consumer import RabbitMqConsumerApp
from .producer import TOPIC_HEADER

if TYPE_CHECKING:
    from benzene.core import Scope


@dataclass
class FakeRabbitMqMethod:
    """A stand-in for a pika delivery ``method`` frame — carries the ``delivery_tag`` the loop acks."""

    delivery_tag: int = 1


@dataclass
class FakeRabbitMqProperties:
    """A stand-in for ``pika.BasicProperties`` — the AMQP application ``headers`` the decode reads."""

    headers: dict[str, Any] | None = None


@dataclass
class FakeRabbitMqMessage:
    """A stand-in for a pika delivery: the ``(method, properties, body)`` triple a ``basic_get`` yields."""

    method: FakeRabbitMqMethod
    properties: FakeRabbitMqProperties
    body: bytes


class RabbitMqMessageBuilder:
    """Builds a :class:`FakeRabbitMqMessage` for a Benzene ``topic`` + body, headers as AMQP headers."""

    def __init__(self, topic: str, delivery_tag: int = 1) -> None:
        self._topic = topic
        self._delivery_tag = delivery_tag
        self._headers: dict[str, str] = {}
        self._body: str = ""

    def with_header(self, key: str, value: str) -> RabbitMqMessageBuilder:
        self._headers[key] = value
        return self

    def with_body(self, body: Any) -> RabbitMqMessageBuilder:
        self._body = encode_body(body)
        return self

    def build(self) -> FakeRabbitMqMessage:
        headers: dict[str, Any] = {str(k): str(v) for k, v in self._headers.items()}
        headers[TOPIC_HEADER] = self._topic
        return FakeRabbitMqMessage(
            method=FakeRabbitMqMethod(delivery_tag=self._delivery_tag),
            properties=FakeRabbitMqProperties(headers=headers),
            body=self._body.encode("utf-8"),
        )


@dataclass
class RecordingRabbitMqChannel:
    """An in-memory channel that replays a fixed list of deliveries, then reports an empty queue.

    Deliveries acked via :meth:`basic_ack` / nacked via :meth:`basic_nack` append their delivery tag to
    :attr:`acked` / :attr:`nacked`, and outbound :meth:`basic_publish` calls are recorded on
    :attr:`published`, so a test can assert the loop's at-least-once behaviour (a failed delivery is
    *not* acked) and the sender's forwarded headers without a broker.
    """

    deliveries: list[FakeRabbitMqMessage] = field(default_factory=list)
    published: list[dict[str, Any]] = field(default_factory=list)
    acked: list[int] = field(default_factory=list)
    nacked: list[int] = field(default_factory=list)

    def basic_get(self, queue: str = "", *, auto_ack: bool = False) -> tuple[Any, Any, Any]:
        if self.deliveries:
            delivery = self.deliveries.pop(0)
            return delivery.method, delivery.properties, delivery.body
        return None, None, None

    def basic_ack(self, *, delivery_tag: int, multiple: bool = False) -> None:
        self.acked.append(delivery_tag)

    def basic_nack(
        self, *, delivery_tag: int, multiple: bool = False, requeue: bool = True
    ) -> None:
        self.nacked.append(delivery_tag)

    def basic_publish(
        self,
        *,
        exchange: str,
        routing_key: str,
        body: Any,
        properties: Any = None,
    ) -> None:
        self.published.append(
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "body": body,
                "properties": properties,
            }
        )


class RabbitMqTestHost:
    """Wraps a :class:`RabbitMqConsumerApp` for in-memory tests of the RabbitMQ ingress."""

    #: The resolved root scope, set by ``create_test_host(...).build_rabbitmq()`` for assertions.
    scope: Scope | None = None

    def __init__(self, app: RabbitMqConsumerApp) -> None:
        self._app = app

    async def send_rabbitmq(
        self, topic: str, body: Any = None, headers: dict[str, str] | None = None
    ) -> Result:
        """Deliver one message on ``topic`` (``headers`` become AMQP headers); returns the result."""
        builder = RabbitMqMessageBuilder(topic)
        for key, value in (headers or {}).items():
            builder.with_header(key, value)
        if body is not None:
            builder.with_body(body)
        message = builder.build()
        return await self._app.handle_message(message.method, message.properties, message.body)
