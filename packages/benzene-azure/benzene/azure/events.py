"""Decoders for the Azure Functions message shapes Benzene binds (Service Bus, Event Hub).

Each maps a native message/event into a Benzene envelope ``{topic, headers, body}``. Following the
cross-port convention, the Benzene topic is carried in the ``benzene-topic`` application/message
property (wire-contracts §2, tier A — prefixed because a property shares a namespace with the
application, unlike the envelope's own ``topic`` field);
the remaining properties are the headers, and the message body (decoded to text) is the JSON body.
"""

from __future__ import annotations

from typing import Any

from benzene.core import TOPIC_KEY, WireNames, take_topic

#: Alias of the core constant — one definition across every binding (wire-contracts §2).
TOPIC_PROPERTY = TOPIC_KEY


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


def decode_service_bus(message: Any, names: WireNames | None = None) -> dict[str, Any]:
    """Map a Service Bus message → a Benzene envelope. Topic from ``application_properties``."""
    topic, headers = take_topic(
        _properties(message, "application_properties", "properties"), names
    )
    return {"topic": topic, "headers": headers, "body": body_to_text(_get_body(message))}


def decode_event_hub_event(event: Any, names: WireNames | None = None) -> dict[str, Any]:
    """Map an Event Hub event → a Benzene envelope. Topic from ``properties``."""
    topic, headers = take_topic(
        _properties(event, "properties", "application_properties"), names
    )
    return {"topic": topic, "headers": headers, "body": body_to_text(_get_body(event))}
