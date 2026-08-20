"""Azure transport parity (roadmap §20) — the Queue Storage, Blob, Cosmos, Timer, and Event Grid
triggers and the Queue Storage / Event Grid outbound clients.

Parallels ``test_azure_events`` / ``test_azure_egress`` but for the parity bindings that .NET carries
as ``Benzene.Azure.Function.{QueueStorage,BlobStorage,CosmosDb,Timer,EventGrid}`` and
``Benzene.Clients.Azure.*``. Everything runs on in-memory stand-ins — no Azure SDK, no network: the
inbound decoders lift the right ``{topic, headers, body}`` from a realistic fake event (Event Grid in
*both* native and CloudEvents 1.0 schema), the fan-out triggers (Cosmos, Queue) produce one invocation
per item, and each outbound sender forwards topic + headers and maps a client exception to
``service-unavailable``.
"""

from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("benzene.azure")

from benzene.azure import (
    AzureFunctionsApp,
    EventGridMessageSender,
    QueueStorageMessageSender,
    decode_blob_created,
    decode_cosmos_document,
    decode_event_grid,
    decode_queue_storage,
    decode_timer,
    is_cloud_event,
)
from benzene.azure.testing import (
    AzureFunctionsTestHost,
    FakeQueueMessage,
    blob_event,
    event_grid_cloud_event,
    event_grid_event,
    queue_storage_message,
    queue_text_message,
    timer_request,
)
from benzene.core import Registry, encode_body
from benzene.results import Result, Status, is_successful

# --- inbound decoders --------------------------------------------------------------------------


def test_decode_queue_storage_lifts_an_embedded_benzene_envelope() -> None:
    message = queue_storage_message("orders:place", {"sku": "A"}, headers={"x-corr": "c1"})
    envelope = decode_queue_storage(message, default_topic="fallback")
    assert envelope["topic"] == "orders:place"  # lifted from the embedded envelope, not the default
    assert envelope["headers"]["x-corr"] == "c1"
    assert json.loads(envelope["body"]) == {"sku": "A"}


def test_decode_queue_storage_plain_text_falls_back_to_default_topic() -> None:
    envelope = decode_queue_storage(queue_text_message("just text"), default_topic="queue:in")
    assert envelope["topic"] == "queue:in"  # no envelope -> configurable convention
    assert envelope["headers"] == {}
    assert envelope["body"] == "just text"


def test_decode_queue_storage_auto_detects_base64_payloads() -> None:
    message = queue_storage_message("orders:place", {"sku": "A"}, base64_encode=True)
    envelope = decode_queue_storage(message)
    assert envelope["topic"] == "orders:place"
    assert json.loads(envelope["body"]) == {"sku": "A"}


def test_decode_blob_created_uses_default_topic_and_descriptor_body() -> None:
    blob = blob_event("orders/1.json", "https://acct.blob.core.windows.net/c/1.json", length=12)
    envelope = decode_blob_created(blob)
    assert envelope["topic"] == "blob:created"
    body = json.loads(envelope["body"])
    assert body["name"] == "orders/1.json"
    assert body["uri"].endswith("/1.json")
    assert body["length"] == 12


def test_decode_cosmos_document_wraps_one_document() -> None:
    envelope = decode_cosmos_document({"id": "d1", "sku": "A"}, default_topic="cosmos:orders")
    assert envelope["topic"] == "cosmos:orders"
    assert json.loads(envelope["body"]) == {"id": "d1", "sku": "A"}


def test_decode_timer_reports_schedule_and_past_due() -> None:
    envelope = decode_timer(timer_request(past_due=True, schedule_status={"Last": "t0"}))
    assert envelope["topic"] == "timer:tick"
    body = json.loads(envelope["body"])
    assert body["isPastDue"] is True
    assert body["schedule_status"]["Last"] == "t0"


def test_decode_event_grid_native_schema_maps_eventtype_and_headers() -> None:
    event = event_grid_event("orders:created", {"id": "1"}, headers={"traceparent": "tp"})
    assert not is_cloud_event(event)
    envelope = decode_event_grid(event, default_topic="fallback")
    assert envelope["topic"] == "orders:created"  # from eventType
    assert envelope["headers"]["traceparent"] == "tp"  # from the native headers field
    assert json.loads(envelope["body"]) == {"id": "1"}


def test_decode_event_grid_cloudevents_maps_type_and_extension_headers() -> None:
    event = event_grid_cloud_event("orders:created", {"id": "1"}, headers={"traceparent": "tp"})
    assert is_cloud_event(event)  # detected by specversion
    envelope = decode_event_grid(event, default_topic="fallback")
    assert envelope["topic"] == "orders:created"  # from type
    assert envelope["headers"]["traceparent"] == "tp"  # from a CloudEvents extension attribute
    # A standard CloudEvents attribute (subject) is NOT leaked into headers.
    assert "subject" not in envelope["headers"]
    assert json.loads(envelope["body"]) == {"id": "1"}


def test_decode_event_grid_falls_back_to_default_topic_when_type_missing() -> None:
    envelope = decode_event_grid({"data": {"id": "1"}}, default_topic="eg:default")
    assert envelope["topic"] == "eg:default"


# --- host fan-out (one invocation per item) ----------------------------------------------------


def _recording_host(topic: str, seen: list[dict]) -> AzureFunctionsTestHost:
    async def record(request: dict) -> Result:
        seen.append(request)
        return Result.ok()

    return AzureFunctionsTestHost(AzureFunctionsApp(registry=Registry().register(topic, record)))


def test_cosmos_change_feed_produces_one_invocation_per_document() -> None:
    seen: list[dict] = []
    host = _recording_host("cosmos:change", seen)
    host.send_cosmos([{"id": "d1"}, {"id": "d2"}, {"id": "d3"}])
    assert [doc["id"] for doc in seen] == ["d1", "d2", "d3"]  # one scope apiece, in order


def test_queue_storage_produces_one_invocation() -> None:
    seen: list[dict] = []
    host = _recording_host("orders:place", seen)
    host.send_queue_storage("orders:place", {"sku": "A"})
    assert len(seen) == 1
    assert seen[0]["sku"] == "A"


def test_event_grid_batch_produces_one_invocation_per_event() -> None:
    seen: list[dict] = []
    host = _recording_host("orders:created", seen)
    host.send_event_grid(
        [
            event_grid_event("orders:created", {"id": "1"}),
            event_grid_cloud_event("orders:created", {"id": "2"}),
        ]
    )
    assert [doc["id"] for doc in seen] == ["1", "2"]  # native + CloudEvents, one scope each


def test_timer_tick_reaches_the_handler_once() -> None:
    seen: list[dict] = []
    host = _recording_host("timer:tick", seen)
    host.send_timer(past_due=True)
    assert len(seen) == 1
    assert seen[0]["isPastDue"] is True


# --- outbound senders --------------------------------------------------------------------------


class _FakeQueueClient:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def send_message(self, content: str) -> None:
        self.sent.append(content)


def test_queue_storage_sender_embeds_topic_headers_and_body_as_an_envelope() -> None:
    fake = _FakeQueueClient()
    result = asyncio.run(
        QueueStorageMessageSender(client=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    envelope = json.loads(fake.sent[0])
    assert envelope["topic"] == "orders:created"
    assert envelope["headers"]["traceparent"] == "tp"
    assert envelope["body"] == encode_body({"id": "1"})
    # Round-trips through the inbound decoder — the two directions are mirror images.
    decoded = decode_queue_storage(FakeQueueMessage(fake.sent[0].encode("utf-8")))
    assert decoded["topic"] == "orders:created"
    assert decoded["headers"]["traceparent"] == "tp"


def test_queue_storage_sender_maps_a_failure_to_service_unavailable() -> None:
    class Boom:
        def send_message(self, content: str) -> None:
            raise RuntimeError("queue down")

    result = asyncio.run(QueueStorageMessageSender(client=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "queue down" in " ".join(result.messages)


class _FakeEventGridClient:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    def send(self, event: dict) -> None:
        self.sent.append(event)


def test_event_grid_sender_native_schema_carries_topic_in_eventtype_and_headers() -> None:
    fake = _FakeEventGridClient()
    result = asyncio.run(
        EventGridMessageSender(client=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    event = fake.sent[0]
    assert event["eventType"] == "orders:created"  # native schema by default
    assert event["headers"]["traceparent"] == "tp"
    assert event["data"] == {"id": "1"}
    # Reversible: the inbound decoder recovers the same topic + headers.
    decoded = decode_event_grid(event)
    assert decoded["topic"] == "orders:created"
    assert decoded["headers"]["traceparent"] == "tp"


def test_event_grid_sender_cloudevents_carries_topic_in_type_and_extension_headers() -> None:
    fake = _FakeEventGridClient()
    result = asyncio.run(
        EventGridMessageSender(client=fake, cloud_events=True).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    event = fake.sent[0]
    assert event["specversion"] == "1.0"
    assert event["type"] == "orders:created"
    assert event["traceparent"] == "tp"  # header as a CloudEvents extension attribute
    decoded = decode_event_grid(event)
    assert decoded["topic"] == "orders:created"
    assert decoded["headers"]["traceparent"] == "tp"


def test_event_grid_sender_maps_a_failure_to_service_unavailable() -> None:
    class Boom:
        def send(self, event: dict) -> None:
            raise RuntimeError("grid down")

    result = asyncio.run(EventGridMessageSender(client=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "grid down" in " ".join(result.messages)
