"""Decoders for the Azure Functions message shapes Benzene binds (Service Bus, Event Hub).

Each maps a native message/event into a Benzene envelope ``{topic, headers, body}``. Following the
cross-port convention, the Benzene topic is carried in the ``topic`` application/message property;
the remaining properties are the headers, and the message body (decoded to text) is the JSON body.
"""

from __future__ import annotations

from typing import Any

from benzene.core import read_message_metadata

TOPIC_PROPERTY = "topic"


def body_to_text(body: Any) -> str:
    """Decode a native message body (bytes / str / iterable of bytes) to text."""
    if body is None:
        return ""
    if isinstance(body, str):
        return body
    if isinstance(body, (bytes, bytearray)):
        return bytes(body).decode("utf-8")
    # Event Hub / Service Bus bodies can arrive as an iterable of byte chunks.
    try:
        return b"".join(bytes(chunk) for chunk in body).decode("utf-8")
    except TypeError:
        return str(body)


def _properties(message: Any, *attr_names: str) -> dict[str, str]:
    for name in attr_names:
        props = getattr(message, name, None)
        if props:
            return {_str(k): _str(v) for k, v in dict(props).items()}
    return {}


def _str(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    return str(value)


def _get_body(message: Any) -> Any:
    get_body = getattr(message, "get_body", None)
    if callable(get_body):
        return get_body()
    return getattr(message, "body", None)


def decode_service_bus(message: Any) -> dict[str, Any]:
    """Map a Service Bus message → a Benzene envelope. Topic from ``application_properties``."""
    metadata = _properties(message, "application_properties", "properties")
    topic, headers = read_message_metadata(metadata)
    return {"topic": topic, "headers": headers, "body": body_to_text(_get_body(message))}


def decode_event_hub_event(event: Any) -> dict[str, Any]:
    """Map an Event Hub event → a Benzene envelope. Topic from ``properties``."""
    metadata = _properties(event, "properties", "application_properties")
    topic, headers = read_message_metadata(metadata)
    return {"topic": topic, "headers": headers, "body": body_to_text(_get_body(event))}
