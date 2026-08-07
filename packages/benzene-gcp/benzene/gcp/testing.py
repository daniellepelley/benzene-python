"""Native-event builders + test host for the GCP binding (mirrors .NET's ``*.TestHelpers``).

Drive a :class:`~benzene.gcp.functions.GcpFunctionsApp` exactly as Cloud Functions would — an HTTP
request or a Pub/Sub CloudEvent — in memory, so an example's tests dogfood the real binding.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from benzene.core import encode_body

from .functions import GcpFunctionsApp
from .pubsub import TOPIC_ATTRIBUTE

if TYPE_CHECKING:
    from benzene.core import Scope


class HttpRequestBuilder:
    """Builds a minimal Functions-Framework/Flask-style HTTP request."""

    def __init__(self, method: str, path: str) -> None:
        self.method = method.upper()
        self.path = path
        self.headers: dict[str, str] = {}
        self.query_string: bytes = b""
        self._body: str = ""

    def with_header(self, key: str, value: str) -> HttpRequestBuilder:
        self.headers[key] = value
        return self

    def with_query(self, query_string: str) -> HttpRequestBuilder:
        self.query_string = query_string.encode("latin-1")
        return self

    def with_body(self, body: Any) -> HttpRequestBuilder:
        self._body = encode_body(body)
        return self

    def get_data(self, as_text: bool = False) -> Any:
        return self._body if as_text else self._body.encode("utf-8")

    def build(self) -> HttpRequestBuilder:
        return self


@dataclass
class FakeCloudEvent:
    """A stand-in for a CloudEvent whose ``.data`` is the Pub/Sub push payload."""

    data: dict[str, Any]


class PubSubEventBuilder:
    """Builds a Pub/Sub CloudEvent for a Benzene ``topic`` + body."""

    def __init__(self, topic: str) -> None:
        self._topic = topic
        self._attributes: dict[str, str] = {}
        self._body: str = ""

    def with_attribute(self, key: str, value: str) -> PubSubEventBuilder:
        self._attributes[key] = value
        return self

    def with_body(self, body: Any) -> PubSubEventBuilder:
        self._body = encode_body(body)
        return self

    def build(self) -> FakeCloudEvent:
        attributes = dict(self._attributes)
        attributes[TOPIC_ATTRIBUTE] = self._topic
        message = {
            "data": base64.b64encode(self._body.encode("utf-8")).decode("ascii"),
            "attributes": attributes,
            "messageId": "test-message-id",
        }
        return FakeCloudEvent(data={"message": message, "subscription": "test-subscription"})


@dataclass
class GcpHttpResponse:
    status_code: int
    headers: dict[str, str]
    body: str


class GcpFunctionsTestHost:
    """Wraps a :class:`GcpFunctionsApp` for in-memory tests of both triggers."""

    #: The resolved root scope, set by ``create_test_host(...).build_gcp()`` for assertions.
    scope: Scope | None = None

    def __init__(self, app: GcpFunctionsApp) -> None:
        self._app = app

    def send_http(
        self,
        method: str,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
        query: dict[str, str] | None = None,
    ) -> GcpHttpResponse:
        builder = HttpRequestBuilder(method, path)
        for key, value in (headers or {}).items():
            builder.with_header(key, value)
        if query:
            builder.with_query(urlencode(query))
        if body is not None:
            builder.with_body(body)
        result_body, status, result_headers = self._app.handle_http(builder.build())
        return GcpHttpResponse(status, result_headers, result_body)

    def send_pubsub(
        self, topic: str, body: Any = None, headers: dict[str, str] | None = None
    ) -> None:
        """Send a Pub/Sub message on ``topic``; ``headers`` become Pub/Sub message attributes."""
        builder = PubSubEventBuilder(topic)
        for key, value in (headers or {}).items():
            builder.with_attribute(key, value)
        if body is not None:
            builder.with_body(body)
        self._app.handle_pubsub(builder.build())
