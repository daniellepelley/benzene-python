"""Azure outbound client (Service Bus) — the egress wire contract, the mirror of the inbound decode.

The sender forwards the Benzene topic + headers onto ``application_properties`` and serializes the
body through the shared wire policy, mapping a send failure to ``service-unavailable``. Unlike
SNS/SQS/Pub/Sub (plain dicts/bytes), it constructs a real ``azure.servicebus.ServiceBusMessage`` in
``_make_message``, so these need that optional SDK — the injected fake sender keeps it credentialless.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("benzene.azure")
pytest.importorskip("azure.servicebus")

from benzene.azure import TOPIC_PROPERTY, ServiceBusMessageSender
from benzene.core import encode_body
from benzene.results import Status, is_successful


class _FakeServiceBusSender:
    def __init__(self) -> None:
        self.sent: list = []

    def send_messages(self, message) -> None:
        self.sent.append(message)


def test_service_bus_sender_tags_topic_propagates_headers_and_serializes_body() -> None:
    fake = _FakeServiceBusSender()
    result = asyncio.run(
        ServiceBusMessageSender(sender=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    message = fake.sent[0]  # a real azure.servicebus.ServiceBusMessage
    props = message.application_properties
    assert props[TOPIC_PROPERTY] == "orders:created"
    assert props["traceparent"] == "tp"
    assert str(message) == encode_body({"id": "1"})


def test_service_bus_sender_maps_a_send_failure_to_service_unavailable() -> None:
    class Boom:
        def send_messages(self, message):
            raise RuntimeError("bus down")

    result = asyncio.run(ServiceBusMessageSender(sender=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "bus down" in " ".join(result.errors)
