"""Azure outbound client (Event Hub) — the egress wire contract, the mirror of the inbound decode.

The sender forwards the Benzene topic + headers onto the event's ``properties`` and serializes the
body through the shared wire policy, mapping a send failure to ``service-unavailable``. Like
``ServiceBusMessageSender``, it constructs a real ``azure.eventhub.EventData`` in ``_send_sync``, so
these need that optional SDK — the injected fake producer keeps it credentialless.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("benzene.azure")
pytest.importorskip("azure.eventhub")

from benzene.azure import TOPIC_PROPERTY, EventHubMessageSender, decode_event_hub_event
from benzene.core import encode_body
from benzene.results import Status, is_successful


class _FakeBatch:
    def __init__(self) -> None:
        self.events: list = []

    def add(self, event) -> None:
        self.events.append(event)


class _FakeEventHubProducer:
    def __init__(self) -> None:
        self.sent: list = []

    def create_batch(self) -> _FakeBatch:
        return _FakeBatch()

    def send_batch(self, batch: _FakeBatch) -> None:
        self.sent.extend(batch.events)


def test_event_hub_sender_tags_topic_propagates_headers_and_serializes_body() -> None:
    fake = _FakeEventHubProducer()
    result = asyncio.run(
        EventHubMessageSender(producer=fake).send_message(
            "order:placed", {"orderId": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    event = fake.sent[0]  # a real azure.eventhub.EventData
    assert event.properties[TOPIC_PROPERTY] == "order:placed"
    assert event.properties["traceparent"] == "tp"
    assert bytes(event.body_as_str(), "utf-8") == encode_body({"orderId": "1"}).encode("utf-8")


def test_event_hub_sender_round_trips_through_the_inbound_decoder() -> None:
    fake = _FakeEventHubProducer()
    asyncio.run(
        EventHubMessageSender(producer=fake).send_message(
            "order:placed", {"orderId": "1"}, headers={"traceparent": "tp"}
        )
    )
    envelope = decode_event_hub_event(fake.sent[0])
    assert envelope["topic"] == "order:placed"
    assert envelope["headers"]["traceparent"] == "tp"


def test_event_hub_sender_maps_a_send_failure_to_service_unavailable() -> None:
    class Boom:
        def create_batch(self):
            raise RuntimeError("hub down")

    result = asyncio.run(EventHubMessageSender(producer=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "hub down" in " ".join(result.errors)
