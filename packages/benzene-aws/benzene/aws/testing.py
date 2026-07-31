"""Native-event builders + test host for the AWS binding (mirrors .NET's ``*.TestHelpers``).

Drive an :class:`~benzene.aws.AwsLambdaApp` with the exact event shapes API Gateway, SQS, and SNS
deliver, in memory, so an example's tests dogfood the real bindings.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .app import AwsLambdaApp
from .events import TOPIC_ATTRIBUTE


def _body(value: Any) -> str:
    from dataclasses import asdict, is_dataclass

    if isinstance(value, str):
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return json.dumps(asdict(value))
    return json.dumps(value)


class ApiGatewayRequestBuilder:
    """Builds an API Gateway (v1 proxy) event."""

    def __init__(self, method: str, path: str) -> None:
        self._method = method.upper()
        self._path = path
        self._headers: dict[str, str] = {}
        self._query: dict[str, str] = {}
        self._body: str | None = None

    def with_header(self, key: str, value: str) -> "ApiGatewayRequestBuilder":
        self._headers[key] = value
        return self

    def with_query(self, key: str, value: str) -> "ApiGatewayRequestBuilder":
        self._query[key] = value
        return self

    def with_body(self, body: Any) -> "ApiGatewayRequestBuilder":
        self._body = _body(body)
        return self

    def build(self) -> dict[str, Any]:
        return {
            "httpMethod": self._method,
            "path": self._path,
            "headers": dict(self._headers),
            "queryStringParameters": dict(self._query) or None,
            "body": self._body,
        }


class SqsEventBuilder:
    """Builds an SQS event; add one record per message on a topic."""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def with_message(
        self, topic: str, body: Any, message_id: str | None = None, headers: dict[str, str] | None = None
    ) -> "SqsEventBuilder":
        attrs = {TOPIC_ATTRIBUTE: {"stringValue": topic, "dataType": "String"}}
        for key, value in (headers or {}).items():
            attrs[str(key)] = {"stringValue": str(value), "dataType": "String"}
        self._records.append(
            {
                "eventSource": "aws:sqs",
                "messageId": message_id or f"msg-{len(self._records) + 1}",
                "body": _body(body),
                "messageAttributes": attrs,
            }
        )
        return self

    def build(self) -> dict[str, Any]:
        return {"Records": list(self._records)}


class SnsEventBuilder:
    """Builds an SNS event; add one record per message on a topic."""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def with_message(
        self, topic: str, body: Any, headers: dict[str, str] | None = None
    ) -> "SnsEventBuilder":
        attrs = {TOPIC_ATTRIBUTE: {"Type": "String", "Value": topic}}
        for key, value in (headers or {}).items():
            attrs[str(key)] = {"Type": "String", "Value": str(value)}
        self._records.append(
            {"EventSource": "aws:sns", "Sns": {"Message": _body(body), "MessageAttributes": attrs}}
        )
        return self

    def build(self) -> dict[str, Any]:
        return {"Records": list(self._records)}


@dataclass
class ApiGatewayResponse:
    status_code: int
    headers: dict[str, str]
    body: str


class AwsLambdaTestHost:
    """Wraps an :class:`AwsLambdaApp` for in-memory tests of each event source."""

    def __init__(self, app: AwsLambdaApp) -> None:
        self._app = app

    def send_http(
        self,
        method: str,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
        query: dict[str, str] | None = None,
    ) -> ApiGatewayResponse:
        builder = ApiGatewayRequestBuilder(method, path)
        for key, value in (headers or {}).items():
            builder.with_header(key, value)
        for key, value in (query or {}).items():
            builder.with_query(key, value)
        if body is not None:
            builder.with_body(body)
        result = self._app.handle(builder.build())
        return ApiGatewayResponse(result["statusCode"], result.get("headers", {}), result.get("body", ""))

    def send_sqs(
        self,
        topic: str,
        body: Any,
        headers: dict[str, str] | None = None,
        message_id: str | None = None,
    ) -> dict[str, Any]:
        """Send one SQS message; returns the partial-batch-response ``{"batchItemFailures": [...]}``."""
        event = (
            SqsEventBuilder()
            .with_message(topic, body, message_id=message_id, headers=headers)
            .build()
        )
        return self._app.handle(event)

    def send_sqs_event(self, event: dict[str, Any]) -> dict[str, Any]:
        return self._app.handle(event)

    def send_sns(self, topic: str, body: Any, headers: dict[str, str] | None = None) -> None:
        event = SnsEventBuilder().with_message(topic, body, headers=headers).build()
        self._app.handle(event)
