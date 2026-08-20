"""GCP outbound client (Pub/Sub) — the egress wire contract, the mirror of the inbound decode.

The sender forwards the Benzene topic + headers onto the message attributes, encodes the body to
bytes through the shared wire policy, and maps a publish failure (surfaced via ``future.result()``)
to ``service-unavailable``. The injected fake publisher keeps this credential-free — no google-cloud
SDK needed.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

pytest.importorskip("benzene.gcp")

from benzene.core import encode_body
from benzene.gcp import TOPIC_ATTRIBUTE, PubSubMessageSender
from benzene.results import Status, is_successful


class _FakeFuture:
    def __init__(self, error: Exception | None = None) -> None:
        self._error = error

    def result(self):
        if self._error is not None:
            raise self._error
        return "message-id"


class _FakePublisher:
    def __init__(self, future: _FakeFuture | None = None) -> None:
        self.calls: list[dict] = []
        self._future = future or _FakeFuture()

    def publish(self, topic_path, data, **attributes) -> _FakeFuture:
        self.calls.append({"topic_path": topic_path, "data": data, "attributes": attributes})
        return self._future


def test_pubsub_sender_tags_topic_propagates_headers_and_encodes_body() -> None:
    fake = _FakePublisher()
    result = asyncio.run(
        PubSubMessageSender("projects/p/topics/t", publisher=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    call = fake.calls[0]
    assert call["topic_path"] == "projects/p/topics/t"
    assert call["data"] == encode_body({"id": "1"}).encode("utf-8")  # Pub/Sub data is bytes
    assert call["attributes"][TOPIC_ATTRIBUTE] == "orders:created"
    assert call["attributes"]["traceparent"] == "tp"


def test_pubsub_sender_maps_a_publish_failure_to_service_unavailable() -> None:
    # A failed publish surfaces via future.result(), which the sender must map — not crash.
    fake = _FakePublisher(future=_FakeFuture(error=RuntimeError("pubsub down")))
    result = asyncio.run(
        PubSubMessageSender("projects/p/topics/t", publisher=fake).send_message("t", {})
    )
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "pubsub down" in " ".join(result.errors)


# --- a missing SDK is a deployment error, not a message outcome (D1) ----------------------------


def test_a_missing_pubsub_sdk_raises_a_teaching_import_error_out_of_send_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Without the guard the lazy ``from google.cloud import pubsub_v1`` is swallowed by the sender's
    # ``except Exception`` mapper and every publish quietly becomes service-unavailable — which retry
    # middleware and circuit breakers then hammer. A missing extra must escape instead.
    monkeypatch.setitem(sys.modules, "google.cloud", None)
    with pytest.raises(ImportError) as excinfo:
        asyncio.run(
            PubSubMessageSender("projects/p/topics/t").send_message("orders:created", {"id": "1"})
        )
    message = str(excinfo.value)
    assert "PubSubMessageSender" in message
    assert "google-cloud-pubsub" in message
    assert "pip install benzene-gcp[pubsub]" in message
