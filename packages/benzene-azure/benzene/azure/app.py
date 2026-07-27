"""Azure Functions host (transport-bindings §1 — one host, inner bindings per trigger).

``AzureFunctionsApp`` hosts the same Benzene handlers behind Azure Functions triggers:

- **HTTP** — via the ``benzene.http`` binding (route → topic, status mapping).
- **Service Bus** — one message per invocation, one scope; topic from the message's application
  properties; a failure raises so the message is retried / dead-lettered.
- **Event Hub** — a batch of events; **one scope per event**, processed in order; a failure raises
  (stops at the first failure, matching checkpoint semantics).

The example's ``main.py`` adapts the ``azure-functions`` request/response types to these methods, so
the package itself (and its tests) need no Azure SDK.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from benzene.core import BenzeneMessageApplication, Registry
from benzene.http import BenzeneHttpApp, HttpRouter
from benzene.results import is_successful

from .events import decode_event_hub_event, decode_service_bus


@dataclass
class AzureHttpResponse:
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""


class AzureFunctionsApp:
    def __init__(
        self,
        http_router: HttpRouter | None = None,
        registry: Registry | None = None,
        application: BenzeneMessageApplication | None = None,
    ) -> None:
        if application is None:
            if registry is None:
                registry = Registry()
                for definition in (http_router.definitions() if http_router else []):
                    registry.add_definition(definition)
            application = BenzeneMessageApplication(registry)
        self._application = application
        self._http_app = (
            BenzeneHttpApp(http_router, application=application) if http_router else None
        )

    # --- HTTP trigger ----------------------------------------------------------------------
    def handle_http_request(
        self,
        method: str,
        path: str,
        query_string: str = "",
        headers: dict[str, str] | None = None,
        body: str = "",
    ) -> AzureHttpResponse:
        if self._http_app is None:
            return AzureHttpResponse(501, {"content-type": "application/json"},
                                     '{"status": "not-implemented"}')
        response = asyncio.run(
            self._http_app.handle(
                method=method,
                path=path,
                query_string=query_string,
                headers=headers or {},
                body=body,
            )
        )
        return AzureHttpResponse(response.status_code, dict(response.headers), response.body)

    # --- Service Bus trigger ---------------------------------------------------------------
    def handle_service_bus(self, message: Any) -> None:
        self._run_or_raise(decode_service_bus(message), "Service Bus")

    # --- Event Hub trigger -----------------------------------------------------------------
    def handle_event_hub(self, events: Any) -> None:
        # Cardinality 'many' delivers a list; 'one' a single event. Process each, one scope apiece.
        if _is_single_event(events):
            events = [events]
        for event in events:
            self._run_or_raise(decode_event_hub_event(event), "Event Hub")

    def _run_or_raise(self, envelope: dict[str, Any], transport: str) -> None:
        response = asyncio.run(self._application.handle_async(envelope))
        if not is_successful(response["statusCode"]):
            raise RuntimeError(
                f"{transport} handler for topic {envelope['topic']!r} failed with "
                f"status {response['statusCode']!r}: {response['body']}"
            )


def _is_single_event(events: Any) -> bool:
    if isinstance(events, (list, tuple)):
        return False
    return hasattr(events, "get_body") or hasattr(events, "body")
