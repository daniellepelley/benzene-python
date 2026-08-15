"""Decoders for the AWS Lambda event shapes Benzene binds.

Each decoder maps a native event/record into a Benzene envelope ``{topic, headers, body}``. Per the
cross-port convention (transport-bindings §"AWS Lambda"), the topic for the *attribute-carrying*
transports (SQS, SNS, Kafka) is carried in the reserved ``topic`` metadata key and resolved through
:func:`~benzene.core.read_message_metadata`; API Gateway resolves its topic from the route instead
(handled by the HTTP binding).

The remaining native sources — **S3** object-created notifications, **EventBridge**, **DynamoDB
Streams**, and **Kinesis Data Streams** — have *no* Benzene metadata channel on the wire, so their
topic comes from an injectable convention (a configured default, or, for DynamoDB, the record's
``eventName``) rather than from the payload. Their bodies are the JSON projections described per
decoder. This mirrors .NET's ``Benzene.Aws.Lambda.{S3,EventBridge,DynamoDb,Kinesis,Kafka}``.
"""

from __future__ import annotations

import base64
from typing import Any
from urllib.parse import urlencode

from benzene.core import encode_body, read_message_metadata

TOPIC_ATTRIBUTE = "topic"

#: Default topic conventions for the sources with no native metadata channel (override per app).
DEFAULT_S3_TOPIC = "s3:object-created"
DEFAULT_KINESIS_TOPIC = "kinesis:record"
DEFAULT_EVENTBRIDGE_TOPIC = "eventbridge:event"


def is_api_gateway(event: dict[str, Any]) -> bool:
    # v1 proxy events carry httpMethod; v2 (HTTP API) carry requestContext.http.
    if "httpMethod" in event:
        return True
    return "http" in (event.get("requestContext") or {})


def event_source(event: dict[str, Any]) -> str | None:
    """Classify an event by its distinguishing shape.

    Returns one of ``"apigateway"``, ``"sqs"``, ``"sns"``, ``"s3"``, ``"eventbridge"``,
    ``"dynamodb"``, ``"kinesis"``, ``"kafka"``, ``"invoke"``, or ``None`` when the shape is
    unrecognised. The order matters: EventBridge (a bare ``detail-type`` at the top level) and Kafka
    (``aws:kafka`` with a lower-case ``records`` map, not the ``Records`` array the other sources use)
    are keyed off top-level markers before the ``Records[0].eventSource`` discriminator sorts the
    rest; ``"invoke"`` — a bare ``{"topic": ...}`` Payload, carrying no Lambda trigger markers at all —
    is checked last, since it is the most generic shape and every other source's own marker must be
    ruled out first.
    """
    if is_api_gateway(event):
        return "apigateway"
    if "detail-type" in event:
        return "eventbridge"
    if event.get("eventSource") == "aws:kafka" or isinstance(event.get("records"), dict):
        return "kafka"
    records = event.get("Records")
    if records:
        first = records[0]
        source = first.get("eventSource")
        if source == "aws:sqs":
            return "sqs"
        if source == "aws:s3":
            return "s3"
        if source == "aws:dynamodb":
            return "dynamodb"
        if source == "aws:kinesis":
            return "kinesis"
        if first.get("EventSource") == "aws:sns" or "Sns" in first:
            return "sns"
    if "topic" in event:
        return "invoke"
    return None


def api_gateway_request(event: dict[str, Any]) -> dict[str, Any]:
    """Extract ``(method, path, query_string, headers, body)`` from an API Gateway proxy event."""
    method = event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get(
        "method", "GET"
    )
    path = event.get("path") or event.get("rawPath") or "/"
    params = event.get("queryStringParameters") or {}
    query_string = urlencode(params) if params else event.get("rawQueryString", "")
    headers = {str(k): str(v) for k, v in (event.get("headers") or {}).items()}
    body = event.get("body") or ""
    return {
        "method": method,
        "path": path,
        "query_string": query_string,
        "headers": headers,
        "body": body,
    }


def sqs_record_envelope(record: dict[str, Any]) -> dict[str, Any]:
    """Map one SQS record to a Benzene envelope. Topic from the ``topic`` message attribute."""
    attributes = record.get("messageAttributes") or {}
    metadata = {k: v.get("stringValue", "") for k, v in attributes.items()}
    topic, headers = read_message_metadata(metadata)
    return {"topic": topic, "headers": headers, "body": record.get("body") or ""}


def sns_record_envelope(record: dict[str, Any]) -> dict[str, Any]:
    """Map one SNS record to a Benzene envelope. Topic from the ``topic`` message attribute."""
    sns = record.get("Sns") or {}
    attributes = sns.get("MessageAttributes") or {}
    metadata = {k: v.get("Value", "") for k, v in attributes.items()}
    topic, headers = read_message_metadata(metadata)
    return {"topic": topic, "headers": headers, "body": sns.get("Message") or ""}


def s3_record_envelope(record: dict[str, Any], topic: str = DEFAULT_S3_TOPIC) -> dict[str, Any]:
    """Map one S3 ``ObjectCreated`` record to a Benzene envelope (mirrors ``Benzene.Aws.Lambda.S3``).

    An S3 notification carries no message metadata, so the topic is a caller-supplied convention
    (``topic``) rather than a value read off the record; the bucket name, object key and the native
    ``eventName`` become the JSON body so a handler can fetch/inspect the object it names.
    """
    s3 = record.get("s3") or {}
    body = {
        "bucket": (s3.get("bucket") or {}).get("name", ""),
        "key": (s3.get("object") or {}).get("key", ""),
        "eventName": record.get("eventName", ""),
    }
    return {"topic": topic, "headers": {}, "body": encode_body(body)}


#: Must match ``benzene.aws.clients.EMBEDDED_HEADERS_KEY`` — the outbound sender's embedding key.
EMBEDDED_HEADERS_KEY = "_benzeneHeaders"


def eventbridge_envelope(
    event: dict[str, Any], default_topic: str = DEFAULT_EVENTBRIDGE_TOPIC
) -> dict[str, Any]:
    """Map an EventBridge event to a Benzene envelope (mirrors ``Benzene.Aws.Lambda.EventBridge``).

    Unlike the batch sources an EventBridge delivery is a *single* event (no ``Records`` array): the
    Benzene topic is the ``detail-type`` (falling back to ``default_topic`` when absent) and the body
    is the ``detail`` object, serialized through the shared wire policy — the reserved
    ``_benzeneHeaders`` key, when present, is left in the body verbatim (a request mapper's
    deserialization simply ignores it, same as .NET). EventBridge has no native per-message
    attributes, so headers are the ``eventbridge-``-prefixed envelope metadata plus any Benzene wire
    headers lifted from ``_benzeneHeaders`` — embedded headers win on key collision.
    """
    detail = event.get("detail")
    topic = event.get("detail-type") or default_topic
    body = detail if isinstance(detail, str) else encode_body(detail if detail is not None else {})

    headers: dict[str, str] = {}
    for key, value in (
        ("eventbridge-id", event.get("id")),
        ("eventbridge-source", event.get("source")),
        ("eventbridge-account", event.get("account")),
        ("eventbridge-region", event.get("region")),
        ("eventbridge-time", event.get("time")),
        ("eventbridge-detail-type", event.get("detail-type")),
    ):
        if value:
            headers[key] = str(value)
    if isinstance(detail, dict):
        embedded = detail.get(EMBEDDED_HEADERS_KEY)
        if isinstance(embedded, dict):
            for key, value in embedded.items():
                if isinstance(value, str):
                    headers[key] = value

    return {"topic": topic, "headers": headers, "body": body}


def dynamodb_record_envelope(record: dict[str, Any], topic: str | None = None) -> dict[str, Any]:
    """Map one DynamoDB Streams record to a Benzene envelope (mirrors ``Benzene.Aws.Lambda.DynamoDb``).

    With no metadata channel the topic defaults to a ``dynamodb:<eventname>`` convention derived from
    the record's ``eventName`` (``INSERT`` / ``MODIFY`` / ``REMOVE``); pass ``topic`` to override it.
    The body is the ``dynamodb`` projection (``Keys`` / ``NewImage`` / ``OldImage`` in DynamoDB's
    attribute-value encoding) so a handler sees exactly what the stream delivered.
    """
    event_name = record.get("eventName", "")
    resolved = topic if topic is not None else f"dynamodb:{event_name.lower()}"
    body = encode_body(record.get("dynamodb") or {})
    return {"topic": resolved, "headers": {}, "body": body}


def kinesis_record_envelope(
    record: dict[str, Any], topic: str = DEFAULT_KINESIS_TOPIC
) -> dict[str, Any]:
    """Map one Kinesis Data Streams record to a Benzene envelope (mirrors ``Benzene.Aws.Lambda.Kinesis``).

    ``kinesis.data`` is base64-encoded on the wire; it is decoded to the UTF-8 JSON body verbatim.
    Kinesis carries no metadata, so the topic is the caller-supplied ``topic`` convention.
    """
    data = (record.get("kinesis") or {}).get("data")
    body = _b64_to_text(data)
    return {"topic": topic, "headers": {}, "body": body}


def _b64_to_text(data: Any) -> str:
    """Decode a base64 wire field (str or bytes) to UTF-8 text; ``None`` / empty → ``""``."""
    if not data:
        return ""
    if isinstance(data, str):
        data = data.encode("ascii")
    return base64.b64decode(data).decode("utf-8")


def _decode_kafka_headers(raw: list[dict[str, Any]] | None) -> dict[str, str]:
    """Kafka Lambda headers (a list of ``{name: [byte, ...]}``) → the flat string metadata dict.

    Each header is a single-key object whose value is a byte array (a list of ints); mirrors the
    self-hosted consumer's header decode (``benzene.kafka``), only the Lambda envelope boxes each
    header as its own object and encodes the bytes as an int list rather than raw ``bytes``.
    """
    attributes: dict[str, str] = {}
    for header in raw or []:
        for key, value in header.items():
            if isinstance(value, (bytes, bytearray, list)):
                attributes[str(key)] = bytes(value).decode("utf-8", "replace")
            else:
                attributes[str(key)] = "" if value is None else str(value)
    return attributes


def kafka_records(event: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten the Kafka/MSK Lambda event's ``{"records": {"topic-partition": [record, ...]}}`` map.

    The Lambda Kafka event groups records by ``<topic>-<partition>``; the host processes them one at a
    time, so this collapses the grouping into a single in-order list.
    """
    grouped = event.get("records") or {}
    flat: list[dict[str, Any]] = []
    for group in grouped.values():
        flat.extend(group or [])
    return flat


def kafka_record_envelope(record: dict[str, Any]) -> dict[str, Any]:
    """Map one Kafka/MSK record to a Benzene envelope (mirrors ``Benzene.Aws.Lambda.Kafka``).

    Matches the self-hosted ``benzene.kafka`` consumer's contract: the Benzene topic is the ``topic``
    header when present (the cross-port convention), otherwise the record's own Kafka ``topic``; the
    remaining headers become Benzene headers, and the base64 ``value`` decodes to the UTF-8 JSON body.
    """
    metadata = _decode_kafka_headers(record.get("headers"))
    topic, headers = read_message_metadata(metadata)
    if not topic:
        topic = str(record.get("topic", ""))
    body = _b64_to_text(record.get("value"))
    return {"topic": topic, "headers": headers, "body": body}


def invoke_envelope(event: dict[str, Any]) -> dict[str, Any]:
    """Normalize a direct Lambda-invoke event to a Benzene envelope.

    A direct ``lambda.invoke()`` Payload *is* the transport-neutral Benzene envelope already
    (``{topic, headers, body}`` — the same shape :class:`~benzene.core.BenzeneMessageApplication`
    and the Cloud Service Profile's ``/benzene/invoke`` HTTP surface both accept), so unlike every
    other decoder here there is no native wire shape to translate *from* — this only fills in
    defaults for absent fields, the same normalisation :meth:`~benzene.core.BenzeneMessageApplication.
    handle` itself would apply. Unlike the channel-less sources (S3, EventBridge, DynamoDB, Kinesis),
    the caller's topic is trusted as-is: it travelled explicitly, not inferred from a convention.
    """
    return {
        "topic": event.get("topic") or "",
        "headers": event.get("headers") or {},
        "body": event.get("body") or "",
    }
