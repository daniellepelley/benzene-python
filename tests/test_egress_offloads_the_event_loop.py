"""Every outbound ``MessageSender`` runs its blocking SDK call via ``asyncio.to_thread``.

A handler that publishes while an ASGI server is co-hosted in the same process (the ``k8s_orders``
"one process, three transports" story) must not stall that server: ``await sender.send_message(...)``
has to keep the event loop free, exactly as the consumer loops now do. This pins that contract for
each transport package at once — patch the module's ``asyncio.to_thread`` with a spy that still runs
the callable, drive a send against an in-memory fake client, and assert the send both **dispatched
through ``to_thread``** and still recorded the operation on the fake. Remove the ``to_thread`` wrapping
in any sender and its case here fails.

``benzene-http`` and ``benzene-grpc`` already offload (and have their own coverage); this guards the
five packages whose senders were moved off-loop: AWS, Kafka, RabbitMQ, GCP, Azure.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from benzene.results import Status


class _SpyToThread:
    """A drop-in for ``asyncio.to_thread`` that records each dispatched callable, then really runs it."""

    def __init__(self, real: Any) -> None:
        self._real = real  # the genuine asyncio.to_thread, captured before patching
        self.dispatched: list[Any] = []

    async def __call__(self, func: Any, /, *args: Any, **kwargs: Any) -> Any:
        self.dispatched.append(func)
        return await self._real(func, *args, **kwargs)


def _spy_on(monkeypatch: pytest.MonkeyPatch) -> _SpyToThread:
    # Every sender module does ``import asyncio`` and calls ``asyncio.to_thread``, so patching the
    # attribute on the shared asyncio module is seen by all of them; monkeypatch restores it after.
    spy = _SpyToThread(asyncio.to_thread)
    monkeypatch.setattr(asyncio, "to_thread", spy)
    return spy


# --- AWS (direct-call pattern) -----------------------------------------------------------------


def test_sns_sender_offloads_the_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    from benzene.aws import SnsMessageSender

    class _FakeSns:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def publish(self, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(kwargs)
            return {}

    spy = _spy_on(monkeypatch)
    fake = _FakeSns()
    result = asyncio.run(SnsMessageSender("arn:topic", client=fake).send_message("t", {"a": 1}))

    assert result.status == Status.OK
    assert fake.calls, "the SDK publish must still run"
    assert spy.dispatched, "the publish must be dispatched via asyncio.to_thread"


# --- Kafka (closure pattern: produce + flush) --------------------------------------------------


def test_kafka_sender_offloads_produce_and_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    from benzene.kafka import KafkaMessageSender

    class _FakeProducer:
        def __init__(self) -> None:
            self.produced: list[Any] = []

        def produce(self, topic: Any, value: Any, headers: Any, on_delivery: Any) -> None:
            self.produced.append(value)
            on_delivery(None, None)

        def flush(self, _timeout: Any) -> None:
            return None

    spy = _spy_on(monkeypatch)
    producer = _FakeProducer()
    result = asyncio.run(
        KafkaMessageSender("orders-events", producer=producer).send_message("orders:created", {})
    )

    assert result.status == Status.OK
    assert producer.produced, "produce must still run"
    assert spy.dispatched, "produce/flush must be dispatched via asyncio.to_thread"


# --- RabbitMQ (direct-call pattern) ------------------------------------------------------------


def test_rabbitmq_sender_offloads_the_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    from benzene.rabbitmq import RabbitMqMessageSender

    class _FakeChannel:
        def __init__(self) -> None:
            self.published: list[dict[str, Any]] = []

        def basic_publish(
            self, *, exchange: Any, routing_key: Any, body: Any, properties: Any = None
        ) -> None:
            self.published.append({"body": body, "properties": properties})

    spy = _spy_on(monkeypatch)
    channel = _FakeChannel()
    result = asyncio.run(
        RabbitMqMessageSender("orders-events", "orders", channel=channel).send_message(
            "orders:created", {}
        )
    )

    assert result.status == Status.OK
    assert channel.published, "basic_publish must still run"
    assert spy.dispatched, "basic_publish must be dispatched via asyncio.to_thread"


# --- GCP (closure pattern: publish + future.result) --------------------------------------------


def test_pubsub_sender_offloads_the_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    from benzene.gcp import PubSubMessageSender

    class _FakeFuture:
        def result(self) -> str:
            return "id-1"

    class _FakePublisher:
        def __init__(self) -> None:
            self.published: list[Any] = []

        def publish(self, topic_path: Any, data: Any, **attributes: Any) -> _FakeFuture:
            self.published.append(data)
            return _FakeFuture()

    spy = _spy_on(monkeypatch)
    publisher = _FakePublisher()
    result = asyncio.run(
        PubSubMessageSender("projects/p/topics/t", publisher=publisher).send_message("t", {})
    )

    assert result.status == Status.OK
    assert publisher.published, "publish must still run"
    assert spy.dispatched, "publish/result must be dispatched via asyncio.to_thread"


# --- Azure (direct-call pattern) ---------------------------------------------------------------


def test_service_bus_sender_offloads_the_send(monkeypatch: pytest.MonkeyPatch) -> None:
    from benzene.azure import ServiceBusMessageSender

    class _FakeServiceBusSender:
        def __init__(self) -> None:
            self.sent: list[Any] = []

        def send_messages(self, message: Any) -> None:
            self.sent.append(message)

    spy = _spy_on(monkeypatch)
    fake = _FakeServiceBusSender()
    # The sender builds an azure.servicebus.ServiceBusMessage in _make_message, which needs the SDK;
    # inject a serializer-free path by passing the fake sender and a simple body — _make_message only
    # runs if the SDK import succeeds, so skip cleanly when azure-servicebus isn't installed.
    pytest.importorskip("azure.servicebus")
    result = asyncio.run(ServiceBusMessageSender(sender=fake).send_message("orders:created", {}))

    assert result.status == Status.OK
    assert fake.sent, "send_messages must still run"
    assert spy.dispatched, "send_messages must be dispatched via asyncio.to_thread"
