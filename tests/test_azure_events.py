"""Azure Service Bus / Event Hub message extraction (duck-typed body + property shapes).

The native SDK message shapes vary — a body can be `str`, `bytes`, or an iterable of byte chunks (Event
Hub), and metadata lives under `application_properties` or `properties`. These decode helpers absorb
that variation into a Benzene envelope; the tests pin each shape with minimal stand-ins.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

pytest.importorskip("benzene.azure")

from benzene.azure.events import (
    body_to_text,
    decode_event_hub_event,
    decode_service_bus,
)


def test_body_to_text_absorbs_every_native_shape() -> None:
    assert body_to_text(None) == ""
    assert body_to_text("already text") == "already text"
    assert body_to_text(b"bytes body") == "bytes body"
    # Event Hub bodies can arrive as an iterable of byte chunks.
    assert body_to_text([b"chunk-", b"one"]) == "chunk-one"
    # a non-iterable, non-bytes value falls back to str()
    assert body_to_text(42) == "42"


@dataclass
class _ServiceBusMessage:
    """A Service Bus message whose body comes via get_body() and props via application_properties."""

    _body: bytes = b'{"sku": "A"}'
    application_properties: dict = field(default_factory=lambda: {"topic": "orders:place", "x-corr": "c1"})

    def get_body(self):
        return self._body


def test_decode_service_bus_reads_topic_and_headers_from_properties() -> None:
    envelope = decode_service_bus(_ServiceBusMessage())
    assert envelope["topic"] == "orders:place"
    assert envelope["headers"]["x-corr"] == "c1"
    assert envelope["body"] == '{"sku": "A"}'


@dataclass
class _EventHubEvent:
    """An Event Hub event whose body is a plain `.body` attribute and props are bytes-keyed."""

    body: bytes = b'{"id": "e1"}'
    properties: dict = field(default_factory=lambda: {b"topic": b"orders:created"})


def test_decode_event_hub_reads_body_attr_and_bytes_properties() -> None:
    envelope = decode_event_hub_event(_EventHubEvent())
    assert envelope["topic"] == "orders:created"  # bytes property key/value decoded to str
    assert envelope["body"] == '{"id": "e1"}'


def test_missing_properties_yield_no_topic() -> None:
    @dataclass
    class _Bare:
        body: str = "{}"

    envelope = decode_service_bus(_Bare())
    assert envelope["topic"] == ""      # no application_properties/properties -> empty topic
    assert envelope["headers"] == {}
