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

from benzene.azure import TOPIC_PROPERTY, EventHubMessageSender, ServiceBusMessageSender
from benzene.core import BatchResult, FailedMessage, encode_body
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


# --- batch sends (T2.1) -------------------------------------------------------------------------
#
# Service Bus and Event Hub batch by *size*, not by count: the SDK hands out a batch object that
# refuses a message once it is full (``ValueError``), so the fakes below model exactly that. Both
# are **per-batch atomic** — a failed ``send_messages``/``send_batch`` fails every message in that
# batch and only that batch — which is the opposite of AWS's per-entry model and the reason the seam
# reports outcomes per message rather than per call.


def _failure(result: BatchResult, index: int) -> FailedMessage:
    """The failure recorded at ``index``, asserting there is one (and narrowing it for mypy)."""
    failure = result.failure_for(index)
    assert failure is not None, f"expected message {index} to have failed"
    return failure


class _FakeSizedBatch:
    """A stand-in for ``ServiceBusMessageBatch``: ``add_message`` raises once ``capacity`` is hit."""

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.messages: list[Any] = []

    def add_message(self, message: Any) -> None:
        if getattr(message, "oversized", False):
            raise ValueError("message too large for an empty batch")
        if len(self.messages) >= self.capacity:
            raise ValueError("batch is full")
        self.messages.append(message)


class _FakeBatchingServiceBusSender:
    def __init__(self, capacity: int = 2, fail_batches: set[int] | None = None) -> None:
        self.batches: list[_FakeSizedBatch] = []
        self.sent: list[list[Any]] = []
        self._capacity = capacity
        self._fail = fail_batches or set()

    def create_message_batch(self) -> _FakeSizedBatch:
        batch = _FakeSizedBatch(self._capacity)
        self.batches.append(batch)
        return batch

    def send_messages(self, batch: Any) -> None:
        if len(self.sent) in self._fail:
            self.sent.append([])
            raise RuntimeError("service bus down")
        self.sent.append(list(batch.messages))


def _sb_sender(fake: Any) -> ServiceBusMessageSender:
    return ServiceBusMessageSender(sender=fake, message_factory=_stub_factory)


def _pairs(count: int) -> list[tuple[str, dict]]:
    return [("orders:created", {"id": str(n)}) for n in range(count)]


def test_service_bus_batch_fills_batches_until_the_sdk_says_they_are_full() -> None:
    fake = _FakeBatchingServiceBusSender(capacity=2)
    result = asyncio.run(_sb_sender(fake).send_batch(_pairs(5)))
    assert result.all_succeeded
    assert [len(sent) for sent in fake.sent] == [2, 2, 1]  # size-bounded, not count-bounded
    topics = [m.application_properties[TOPIC_PROPERTY] for m in fake.sent[0]]
    assert topics == ["orders:created"] * 2


def test_service_bus_batch_failure_is_atomic_for_that_batch_only() -> None:
    # Azure's model: the send either takes the whole batch or none of it. Messages 2 and 3 are the
    # second batch, so they alone fail — 0, 1, 4 landed and must not be resent.
    fake = _FakeBatchingServiceBusSender(capacity=2, fail_batches={1})
    result = asyncio.run(_sb_sender(fake).send_batch(_pairs(5)))
    assert result.failed_indexes == (2, 3)
    assert _failure(result, 2).status == Status.SERVICE_UNAVAILABLE
    assert "service bus down" in (_failure(result, 3).detail or "")


def test_service_bus_batch_message_too_big_for_an_empty_batch_fails_alone() -> None:
    fake = _FakeBatchingServiceBusSender(capacity=5)

    def factory(body, properties):
        message = _StubMessage(body, dict(properties))
        message.oversized = properties.get("size") == "huge"  # type: ignore[attr-defined]
        return message

    sender = ServiceBusMessageSender(sender=fake, message_factory=factory)
    messages = [("orders:created", {"id": "0"}), ("orders:created", {"id": "1"})]
    result = asyncio.run(sender.send_batch(messages, headers={"size": "huge"}))
    assert result.failed_indexes == (0, 1)
    assert _failure(result, 0).status == Status.BAD_REQUEST  # never retried: it can't get smaller
    assert fake.sent == []  # nothing was sendable, and nothing hung on trying


def test_service_bus_batch_puts_the_same_bytes_on_the_wire_as_a_single_send() -> None:
    single, batched = _FakeServiceBusSender(), _FakeBatchingServiceBusSender(capacity=10)
    asyncio.run(
        _sb_sender(single).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    asyncio.run(
        _sb_sender(batched).send_batch(
            [("orders:created", {"id": "1"})], headers={"traceparent": "tp"}
        )
    )
    assert batched.sent[0][0].body == single.sent[0].body
    assert batched.sent[0][0].application_properties == single.sent[0].application_properties


def test_service_bus_empty_batch_creates_nothing() -> None:
    fake = _FakeBatchingServiceBusSender()
    assert asyncio.run(_sb_sender(fake).send_batch([])).all_succeeded
    assert fake.batches == [] and fake.sent == []


class _FakeSizedEventBatch:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.events: list[Any] = []

    def add(self, event: Any) -> None:
        if len(self.events) >= self.capacity:
            raise ValueError("EventDataBatch is full")
        self.events.append(event)


class _FakeBatchingEventHubProducer:
    def __init__(self, capacity: int = 2, fail_batches: set[int] | None = None) -> None:
        self.sent: list[list[Any]] = []
        self._capacity = capacity
        self._fail = fail_batches or set()

    def create_batch(self) -> _FakeSizedEventBatch:
        return _FakeSizedEventBatch(self._capacity)

    def send_batch(self, batch: Any) -> None:
        if len(self.sent) in self._fail:
            self.sent.append([])
            raise RuntimeError("event hub down")
        self.sent.append(list(batch.events))


def _eh_sender(fake: Any) -> EventHubMessageSender:
    return EventHubMessageSender(producer=fake, event_factory=_stub_event_factory)


def _stub_event_factory(body: str | bytes, properties: dict[str, str]) -> Any:
    """A duck-typed stand-in for ``azure.eventhub.EventData`` (the two attributes the sender sets)."""
    return _StubMessage(body, dict(properties))


def test_event_hub_batch_fills_event_data_batches_until_full() -> None:
    fake = _FakeBatchingEventHubProducer(capacity=2)
    result = asyncio.run(_eh_sender(fake).send_batch(_pairs(5)))
    assert result.all_succeeded
    assert [len(sent) for sent in fake.sent] == [2, 2, 1]
    assert fake.sent[0][0].application_properties[TOPIC_PROPERTY] == "orders:created"


def test_event_hub_batch_failure_is_atomic_for_that_batch_only() -> None:
    fake = _FakeBatchingEventHubProducer(capacity=2, fail_batches={0})
    result = asyncio.run(_eh_sender(fake).send_batch(_pairs(5)))
    assert result.failed_indexes == (0, 1)
    assert "event hub down" in (_failure(result, 1).detail or "")


def test_a_missing_event_hub_sdk_raises_a_teaching_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Same D1 rule as every other sender: a forgotten extra is a deployment error, and the batch
    # path must not launder it into per-message service-unavailable failures either.
    monkeypatch.setitem(sys.modules, "azure.eventhub", None)
    sender = EventHubMessageSender(producer=_FakeBatchingEventHubProducer())
    with pytest.raises(ImportError) as excinfo:
        asyncio.run(sender.send_batch(_pairs(2)))
    assert "azure-eventhub" in str(excinfo.value)
    assert "pip install benzene-azure[eventhub]" in str(excinfo.value)
