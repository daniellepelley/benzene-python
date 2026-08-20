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
    assert "bus down" in " ".join(result.errors)


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
