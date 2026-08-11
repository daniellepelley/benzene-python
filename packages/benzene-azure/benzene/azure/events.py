"""Decoders for the Azure Functions message shapes Benzene binds.

Each maps a native message/event into a Benzene envelope ``{topic, headers, body}``, where ``body`` is
always a pre-serialized JSON string (what :class:`~benzene.core.BenzeneMessageApplication` decodes).

Two families live here, mirroring the .NET ``Benzene.Azure.Function.*`` triggers:

* **Metadata-carrying transports** (Service Bus, Event Hub) — the Benzene topic rides in the ``topic``
  application/message property; the rest of the properties are the headers (the cross-port convention).
* **Envelope-less / event-shaped triggers** (Queue Storage, Blob Storage, Cosmos DB change feed, Timer,
  Event Grid) — these have no native string→string header channel, so the topic comes from an injectable
  default (or is lifted from a Benzene envelope the producer embedded, e.g. a Queue message), and the
  binding-specific payload (blob descriptor, changed document, timer info, Event Grid ``data``) becomes
  the body. Event Grid is the exception that *does* carry a channel — its extension attributes.
"""

from __future__ import annotations

import base64
import binascii
import json
from typing import Any

from benzene.core import encode_body, read_message_metadata

TOPIC_PROPERTY = "topic"

#: Default Benzene topics for the triggers whose native shape carries no topic (override per binding).
DEFAULT_BLOB_TOPIC = "blob:created"
DEFAULT_COSMOS_TOPIC = "cosmos:change"
DEFAULT_TIMER_TOPIC = "timer:tick"

#: The CloudEvents 1.0 context attributes; anything else at the top level is an *extension* (→ header).
_CLOUD_EVENT_ATTRIBUTES = frozenset(
    {
        "specversion",
        "id",
        "source",
        "type",
        "subject",
        "time",
        "datacontenttype",
        "dataschema",
        "data",
        "data_base64",
    }
)


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


# --- Envelope-less / event-shaped triggers -------------------------------------------------------


def _field(source: Any, key: str) -> Any:
    """Read ``key`` from a native trigger input, whether it exposes fields as dict keys or attributes.

    The Azure Functions bindings hand a decoder either a plain ``dict`` (Cosmos documents, Event Grid
    events arrive as JSON) or an SDK object (``azure.functions.TimerRequest`` / ``InputStream``), so a
    decoder that duck-types over both stays SDK-free and testable with the same stand-in either way.
    """
    if isinstance(source, dict):
        return source.get(key)
    return getattr(source, key, None)


def _first_field(source: Any, *keys: str) -> Any:
    for key in keys:
        value = _field(source, key)
        if value is not None:
            return value
    return None


def _topic_or(value: Any, default_topic: str) -> str:
    """A present, non-empty native type/eventType wins; otherwise fall back to the injected default."""
    return _str(value) if value else default_topic


def _headers(raw: Any) -> dict[str, str]:
    """Coerce a mapping of header candidates to the ``str→str``, lower-cased channel Benzene uses."""
    if not raw:
        return {}
    return {str(key).lower(): _str(value) for key, value in dict(raw).items()}


def _body_text(body: Any) -> str:
    """Serialize a decoded payload to the JSON *string* the envelope carries (``str`` passes through)."""
    if body is None:
        return ""
    if isinstance(body, str):
        return body
    return encode_body(body)


def _decode_queue_text(text: str) -> str:
    """Base64-decode a Queue Storage payload when it round-trips; otherwise return it verbatim.

    Azure Storage Queue messages are frequently base64-encoded (the .NET/Java SDKs default to it), yet a
    Functions Queue trigger may hand plain text too. Decoding only when the text is *canonical* base64
    that decodes to valid UTF-8 keeps a plain-text body (a JSON envelope, a bare word) untouched — a
    JSON object starts with ``{``, which is never valid base64.
    """
    try:
        decoded = base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError):
        return text
    if base64.b64encode(decoded).decode("ascii") != text:
        return text
    try:
        return decoded.decode("utf-8")
    except UnicodeDecodeError:
        return text


def _benzene_envelope(text: str) -> dict[str, Any] | None:
    """Return the parsed object iff ``text`` is a JSON Benzene envelope (an object carrying ``topic``)."""
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return None
    if isinstance(parsed, dict) and TOPIC_PROPERTY in parsed:
        return parsed
    return None


def decode_queue_storage(message: Any, *, default_topic: str = "") -> dict[str, Any]:
    """Map an Azure Storage Queue message → a Benzene envelope (mirrors ``Function.QueueStorage``).

    A Storage Queue has no attribute channel, so there are two routing conventions, tried in order:
    if the (base64-decoded) message text is a Benzene envelope ``{topic, headers, body}`` — what
    :class:`~benzene.azure.QueueStorageMessageSender` writes — its topic/headers/body are lifted
    straight out; otherwise the whole message text becomes the body under the injectable
    ``default_topic`` (the "one queue → one topic" convention).
    """
    text = _decode_queue_text(body_to_text(_get_body(message)))
    envelope = _benzene_envelope(text)
    if envelope is not None:
        return {
            "topic": _topic_or(envelope.get(TOPIC_PROPERTY), default_topic),
            "headers": _headers(envelope.get("headers")),
            "body": _body_text(envelope.get("body")),
        }
    return {"topic": default_topic, "headers": {}, "body": text}


def decode_blob_created(blob: Any, *, default_topic: str = DEFAULT_BLOB_TOPIC) -> dict[str, Any]:
    """Map a Blob Storage trigger → a Benzene envelope (mirrors ``Function.BlobStorage``).

    A blob-created trigger is a notification, not a message: it names a blob rather than carrying an
    application payload, so the topic comes from ``default_topic`` and the body is the blob descriptor
    (``name`` / ``uri`` / ``length`` / ``metadata`` / ``content_type`` — whichever the binding exposes).
    """
    descriptor: dict[str, Any] = {}
    for key in ("name", "uri", "length", "metadata", "content_type"):
        value = _field(blob, key)
        if value is not None:
            descriptor[key] = value
    return {"topic": default_topic, "headers": {}, "body": encode_body(descriptor)}


def decode_cosmos_document(
    document: Any, *, default_topic: str = DEFAULT_COSMOS_TOPIC
) -> dict[str, Any]:
    """Map one changed Cosmos DB document → a Benzene envelope (mirrors ``Function.CosmosDb``).

    The change feed delivers a *batch* (list) of documents; the host fans it out to one invocation per
    document (as Event Hub does per event), so this decoder is single-document: the topic is the
    injectable ``default_topic`` and the body is the document itself.
    """
    return {"topic": default_topic, "headers": {}, "body": encode_body(document)}


def decode_timer(timer: Any, *, default_topic: str = DEFAULT_TIMER_TOPIC) -> dict[str, Any]:
    """Map a Timer trigger → a Benzene envelope (mirrors ``Function.Timer``).

    A timer tick carries no payload, only schedule metadata; one tick is one invocation. The body is
    the timer info (``isPastDue`` plus the schedule/last/next status), under the injectable
    ``default_topic``.
    """
    past_due = _first_field(timer, "past_due", "is_past_due")
    info: dict[str, Any] = {"isPastDue": bool(past_due)}
    for key in ("schedule_status", "schedule"):
        value = _field(timer, key)
        if value is not None:
            info[key] = value
    return {"topic": default_topic, "headers": {}, "body": encode_body(info)}


def is_cloud_event(event: Any) -> bool:
    """True when ``event`` is a CloudEvents 1.0 event (the ``specversion`` context attribute is present)."""
    return _field(event, "specversion") is not None


def _cloud_event_headers(event: Any) -> dict[str, str]:
    """Lift CloudEvents *extension attributes* (any top-level key outside the standard set) as headers."""
    items = event.items() if isinstance(event, dict) else vars(event).items()
    return {
        str(key).lower(): _str(value)
        for key, value in items
        if str(key).lower() not in _CLOUD_EVENT_ATTRIBUTES
    }


def decode_event_grid(event: Any, *, default_topic: str = "") -> dict[str, Any]:
    """Map an Event Grid event → a Benzene envelope, native schema **or** CloudEvents 1.0.

    Mirrors ``Function.EventGrid``. The two schemas are told apart by the CloudEvents ``specversion``
    attribute (:func:`is_cloud_event`):

    * **CloudEvents 1.0** — topic from ``type``; body from ``data``; headers are the *extension
      attributes* (every top-level key outside the CloudEvents context set), which is where
      :class:`~benzene.azure.EventGridMessageSender` puts the Benzene headers.
    * **Native Event Grid** — topic from ``eventType``; body from ``data``; headers from the ``headers``
      field the sender adds (native schema has no other free-form channel).

    Either way the Benzene topic falls back to ``default_topic`` when the event names no type.
    """
    data = _field(event, "data")
    if is_cloud_event(event):
        topic = _topic_or(_field(event, "type"), default_topic)
        headers = _cloud_event_headers(event)
    else:
        topic = _topic_or(_first_field(event, "eventType", "event_type"), default_topic)
        headers = _headers(_field(event, "headers"))
    return {"topic": topic, "headers": headers, "body": _body_text(data)}
