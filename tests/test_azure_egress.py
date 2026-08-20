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
    assert "bus down" in " ".join(result.messages)


# --- missing connection details fail at construction, naming what is missing --------------------


def test_service_bus_sender_missing_connection_string_names_the_class_and_the_argument() -> None:
    # The overwhelmingly common cause is an unset environment variable passed straight through as
    # None. That used to surface as an SDK error on the MESSAGE path, naming neither the Benzene
    # class nor the argument. It is now a start-up failure naming both, plus the injected
    # alternative - the same "refuse to boot rather than fail every message" rule the rest of
    # Benzene applies to misconfiguration.
    with pytest.raises(ValueError) as caught:
        ServiceBusMessageSender(entity_name="q")

    message = str(caught.value)
    assert "ServiceBusMessageSender" in message
    assert "connection_string=" in message
    assert "sender=" in message


def test_service_bus_sender_missing_entity_name_names_that_argument_instead() -> None:
    with pytest.raises(ValueError) as caught:
        ServiceBusMessageSender(connection_string="Endpoint=sb://x/;")

    assert "entity_name=" in str(caught.value)


def test_service_bus_sender_missing_everything_names_both_arguments() -> None:
    with pytest.raises(ValueError) as caught:
        ServiceBusMessageSender()

    message = str(caught.value)
    assert "connection_string=" in message
    assert "entity_name=" in message


def test_service_bus_sender_with_an_injected_sender_needs_no_connection_details_at_all() -> None:
    # The other half of the contract: injecting a client must stay free of the config the lazy path
    # needs, or the check above would make the testable seam unusable.
    fake = _FakeServiceBusSender()

    result = asyncio.run(ServiceBusMessageSender(sender=fake).send_message("orders:created", {}))

    assert is_successful(result.status)
    assert len(fake.sent) == 1


def test_a_fully_configured_sender_constructs_without_touching_the_broker() -> None:
    # Construction must stay lazy: the check reads arguments only, it does not dial the broker.
    ServiceBusMessageSender(connection_string="Endpoint=sb://x/;", entity_name="q")


def test_send_failures_after_construction_are_still_results_not_exceptions() -> None:
    # The never-raise contract still holds for everything on the message path - a broker outage
    # must not take a worker down. Only misconfiguration, which is known at construction, raises.
    class Boom:
        def send_messages(self, message):
            raise RuntimeError("broker down")

    result = asyncio.run(ServiceBusMessageSender(sender=Boom()).send_message("t", {}))

    assert result.status == Status.SERVICE_UNAVAILABLE
