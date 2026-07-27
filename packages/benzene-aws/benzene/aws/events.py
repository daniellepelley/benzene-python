"""Decoders for the AWS Lambda event shapes Benzene binds (API Gateway, SQS, SNS).

Each decoder maps a native event/record into a Benzene envelope ``{topic, headers, body}``. Per the
cross-port convention (transport-bindings §"AWS Lambda"), the topic for the messaging transports is
carried in the ``benzene-topic`` message attribute; API Gateway resolves its topic from the route
instead (handled by the HTTP binding), so there is no ``benzene-topic`` attribute there.

Note the asymmetry with the envelope this builds: the *envelope's* field is plain ``topic``, because
the envelope is entirely Benzene's own namespace, while a message attribute shares a namespace with
the application and the transport and so carries the ``benzene-`` marker (wire-contracts §2).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from benzene.core import TOPIC_KEY, take_topic

#: Alias of the core constant — one definition across every binding (wire-contracts §2).
TOPIC_ATTRIBUTE = TOPIC_KEY


def is_api_gateway(event: dict[str, Any]) -> bool:
    # v1 proxy events carry httpMethod; v2 (HTTP API) carry requestContext.http.
    if "httpMethod" in event:
        return True
    return "http" in (event.get("requestContext") or {})


def event_source(event: dict[str, Any]) -> str | None:
    """Classify an event: ``"apigateway"``, ``"sqs"``, ``"sns"``, or ``None`` if unrecognised."""
    if is_api_gateway(event):
        return "apigateway"
    records = event.get("Records")
    if records:
        first = records[0]
        if first.get("eventSource") == "aws:sqs":
            return "sqs"
        if first.get("EventSource") == "aws:sns" or "Sns" in first:
            return "sns"
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
    topic, headers = take_topic(
        {k: (v or {}).get("stringValue", "") for k, v in attributes.items()}
    )
    return {"topic": topic, "headers": headers, "body": record.get("body") or ""}


def sns_record_envelope(record: dict[str, Any]) -> dict[str, Any]:
    """Map one SNS record to a Benzene envelope. Topic from the ``topic`` message attribute."""
    sns = record.get("Sns") or {}
    attributes = sns.get("MessageAttributes") or {}
    topic, headers = take_topic({k: (v or {}).get("Value", "") for k, v in attributes.items()})
    return {"topic": topic, "headers": headers, "body": sns.get("Message") or ""}
