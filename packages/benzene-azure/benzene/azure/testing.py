"""Native-message builders + test host for the Azure binding (mirrors .NET's ``*.TestHelpers``).

Drive an :class:`~benzene.azure.AzureFunctionsApp` with stand-ins for the Azure Functions HTTP,
Service Bus, and Event Hub trigger inputs, in memory — no Azure SDK required.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from .app import AzureFunctionsApp, AzureHttpResponse

if TYPE_CHECKING:
    from benzene.core import Scope


def _body_text(value: Any) -> str:
    from dataclasses import asdict, is_dataclass

    if isinstance(value, str):
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return json.dumps(asdict(value))
    return json.dumps(value)


@dataclass
class FakeServiceBusMessage:
    """Stand-in for an ``azure.functions.ServiceBusMessage`` (``get_body`` + ``application_properties``)."""

    _body: bytes
    application_properties: dict[str, str] = field(default_factory=dict)

    def get_body(self) -> bytes:
        return self._body


@dataclass
class FakeEventHubEvent:
    """Stand-in for an ``azure.functions.EventHubEvent`` (``get_body`` + ``properties``)."""

    _body: bytes
    properties: dict[str, str] = field(default_factory=dict)

    def get_body(self) -> bytes:
        return self._body


def _with_topic(topic: str, headers: dict[str, str] | None) -> dict[str, str]:
    props = {str(k): str(v) for k, v in (headers or {}).items()}
    props["topic"] = topic
    return props


def service_bus_message(topic: str, body: Any, headers: dict[str, str] | None = None) -> FakeServiceBusMessage:
    return FakeServiceBusMessage(_body_text(body).encode("utf-8"), _with_topic(topic, headers))


def event_hub_event(topic: str, body: Any, headers: dict[str, str] | None = None) -> FakeEventHubEvent:
    return FakeEventHubEvent(_body_text(body).encode("utf-8"), _with_topic(topic, headers))


class AzureFunctionsTestHost:
    """Wraps an :class:`AzureFunctionsApp` for in-memory tests of each trigger."""

    #: The resolved root scope, set by ``create_test_host(...).build_azure()`` for assertions.
    scope: "Scope | None" = None

    def __init__(self, app: AzureFunctionsApp) -> None:
        self._app = app

    def send_http(
        self,
        method: str,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
        query: dict[str, str] | None = None,
    ) -> AzureHttpResponse:
        return self._app.handle_http(
            method=method,
            path=path,
            query_string=urlencode(query or {}),
            headers=headers or {},
            body="" if body is None else _body_text(body),
        )

    def send_service_bus(self, topic: str, body: Any, headers: dict[str, str] | None = None) -> None:
        self._app.handle_service_bus(service_bus_message(topic, body, headers))

    def send_event_hub(self, topic: str, body: Any, headers: dict[str, str] | None = None) -> None:
        self._app.handle_event_hub([event_hub_event(topic, body, headers)])

    def send_event_hub_batch(self, events: list[FakeEventHubEvent]) -> None:
        self._app.handle_event_hub(events)
