"""Azure outbound client (Service Bus) — the egress wire contract, the mirror of the inbound decode.

The sender forwards the Benzene topic + headers onto ``application_properties`` and serializes the
body through the shared wire policy, mapping a send failure to ``service-unavailable``. Unlike
SNS/SQS/Pub/Sub (plain dicts/bytes), the wire object is an ``azure.servicebus.ServiceBusMessage`` —
so ``ServiceBusMessageSender`` takes an injectable ``message_factory`` and these tests pass a
duck-typed stub (``.body``/``.application_properties``, exactly what the SDK type exposes). That
keeps the whole contract — topic tagging, header propagation, serialization, failure mapping —
enforced in default CI with no Azure SDK installed; the one test below that pins the *real* SDK
object is the only thing that needs it, and a stub ``azure.servicebus`` module pins the default
factory's call into the SDK even without it.
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
from dataclasses import dataclass, field
from typing import Any

import pytest

pytest.importorskip("benzene.azure")

from benzene.azure import TOPIC_PROPERTY, ServiceBusMessageSender
from benzene.core import encode_body
from benzene.results import Status, is_successful


def _has_service_bus() -> bool:
    """Is azure-servicebus importable? (``find_spec`` raises when the ``azure`` parent is absent.)"""
    try:
        return importlib.util.find_spec("azure.servicebus") is not None
    except ModuleNotFoundError:
        return False


@dataclass
class _StubMessage:
    """A duck-typed stand-in for ``azure.servicebus.ServiceBusMessage`` (same two attributes)."""

    body: str | bytes
    application_properties: dict[str, str] = field(default_factory=dict)


def _stub_factory(body: str | bytes, properties: dict[str, str]) -> _StubMessage:
    return _StubMessage(body, dict(properties))


class _FakeServiceBusSender:
    def __init__(self) -> None:
        self.sent: list[Any] = []

    def send_messages(self, message: Any) -> None:
        self.sent.append(message)


def test_service_bus_sender_tags_topic_propagates_headers_and_serializes_body() -> None:
    fake = _FakeServiceBusSender()
    result = asyncio.run(
        ServiceBusMessageSender(sender=fake, message_factory=_stub_factory).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    message = fake.sent[0]
    props = message.application_properties
    assert props[TOPIC_PROPERTY] == "orders:created"
    assert props["traceparent"] == "tp"
    assert message.body == encode_body({"id": "1"})


def test_service_bus_sender_maps_a_send_failure_to_service_unavailable() -> None:
    class Boom:
        def send_messages(self, message: Any) -> None:
            raise RuntimeError("bus down")

    result = asyncio.run(
        ServiceBusMessageSender(sender=Boom(), message_factory=_stub_factory).send_message("t", {})
    )
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "bus down" in " ".join(result.messages)


def test_the_default_message_factory_builds_the_sdk_message_from_body_and_properties(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default factory's call *into* the SDK, pinned without installing it.

    ``ServiceBusMessage(body, application_properties=...)`` is the constructor shape the real path
    depends on; a stub module in ``sys.modules`` records it, so a typo there can't hide behind a
    skip in an environment with no azure-servicebus.
    """
    calls: list[tuple[Any, dict[str, str]]] = []

    class _RecordingMessage:
        def __init__(self, body: Any, application_properties: dict[str, str] | None = None) -> None:
            calls.append((body, dict(application_properties or {})))
            self.body = body
            self.application_properties = application_properties or {}

    stub = types.ModuleType("azure.servicebus")
    stub.ServiceBusMessage = _RecordingMessage  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "azure.servicebus", stub)

    fake = _FakeServiceBusSender()
    result = asyncio.run(  # no message_factory: the default (lazy SDK construction) path
        ServiceBusMessageSender(sender=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )

    assert is_successful(result.status)
    body, properties = calls[0]
    assert body == encode_body({"id": "1"})
    assert properties == {"traceparent": "tp", TOPIC_PROPERTY: "orders:created"}


@pytest.mark.skipif(not _has_service_bus(), reason="azure-servicebus is not installed")
def test_the_default_message_factory_constructs_a_real_service_bus_message() -> None:
    """The one real-SDK test: the default factory really does produce a ``ServiceBusMessage``."""
    from azure.servicebus import ServiceBusMessage

    fake = _FakeServiceBusSender()
    result = asyncio.run(
        ServiceBusMessageSender(sender=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    message = fake.sent[0]
    assert isinstance(message, ServiceBusMessage)
    assert message.application_properties[TOPIC_PROPERTY] == "orders:created"
    assert message.application_properties["traceparent"] == "tp"
    assert str(message) == encode_body({"id": "1"})


# --- a missing SDK is a deployment error, not a message outcome (D1) ----------------------------


def test_a_missing_service_bus_sdk_in_the_message_factory_raises_a_teaching_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Without the guard the lazy ``from azure.servicebus import ServiceBusMessage`` is swallowed by
    # ``send_message``'s ``except Exception`` mapper and every send quietly becomes
    # service-unavailable — which retry middleware and circuit breakers then hammer.
    monkeypatch.setitem(sys.modules, "azure.servicebus", None)
    with pytest.raises(ImportError) as excinfo:
        asyncio.run(
            ServiceBusMessageSender(sender=_FakeServiceBusSender()).send_message(
                "orders:created", {"id": "1"}
            )
        )
    message = str(excinfo.value)
    assert "ServiceBusMessageSender" in message
    assert "azure-servicebus" in message
    assert "pip install benzene-azure[servicebus]" in message


def test_a_missing_service_bus_sdk_building_the_client_raises_a_teaching_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "azure.servicebus", None)
    with pytest.raises(ImportError) as excinfo:
        asyncio.run(
            ServiceBusMessageSender(
                connection_string="Endpoint=sb://example/", entity_name="orders"
            ).send_message("orders:created", {"id": "1"})
        )
    assert "pip install benzene-azure[servicebus]" in str(excinfo.value)


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

    # Through the injectable factory, so the config contract is enforced with no Azure SDK present
    # (the default factory's own SDK call is pinned by the two tests above).
    result = asyncio.run(
        ServiceBusMessageSender(sender=fake, message_factory=_stub_factory).send_message(
            "orders:created", {}
        )
    )

    assert is_successful(result.status)
    assert len(fake.sent) == 1


def test_a_fully_configured_sender_constructs_without_touching_the_broker() -> None:
    # Construction must stay lazy: the check reads arguments only, it does not dial the broker.
    ServiceBusMessageSender(connection_string="Endpoint=sb://x/;", entity_name="q")


def test_send_failures_after_construction_are_still_results_not_exceptions() -> None:
    # The never-raise contract still holds for everything on the message path - a broker outage
    # must not take a worker down. Only misconfiguration, which is known at construction, raises.
    class Boom:
        def send_messages(self, message: Any) -> None:
            raise RuntimeError("broker down")

    result = asyncio.run(
        ServiceBusMessageSender(sender=Boom(), message_factory=_stub_factory).send_message("t", {})
    )

    assert result.status == Status.SERVICE_UNAVAILABLE
