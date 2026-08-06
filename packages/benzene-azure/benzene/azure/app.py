"""Azure Functions host (transport-bindings §1 — one host, inner bindings per trigger).

``AzureFunctionsApp`` hosts the same Benzene handlers behind Azure Functions triggers:

- **HTTP** — via the ``benzene.http`` binding (route → topic, status mapping).
- **Service Bus** — one message per invocation, one scope; topic from the message's application
  properties; a failure raises so the message is retried / dead-lettered.
- **Event Hub** — a batch of events; **one scope per event**, processed in order; a failure raises
  (stops at the first failure, matching checkpoint semantics).

The :func:`http_function` / :func:`service_bus_function` / :func:`event_hub_function` helpers wrap an
app as the plain callables an Azure Functions trigger invokes (adapting the ``azure-functions``
request/message types lazily), mirroring ``benzene.gcp.http_function`` and
``benzene.aws.to_lambda_handler``. The package itself (and its tests) need no Azure SDK — see the
example's ``function_app.py`` for the v2-model wiring.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from benzene.core import BenzeneMessageApplication, MessageHandlingError, Registry
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
    def handle_http(
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
        asyncio.run(self._run_or_raise(decode_service_bus(message)))

    # --- Event Hub trigger -----------------------------------------------------------------
    def handle_event_hub(self, events: Any) -> None:
        # Cardinality 'many' delivers a list; 'one' a single event. Process each, one scope apiece.
        if _is_single_event(events):
            events = [events]

        async def run() -> None:
            for event in events:
                await self._run_or_raise(decode_event_hub_event(event))

        asyncio.run(run())

    async def _run_or_raise(self, envelope: dict[str, Any]) -> None:
        response = await self._application.handle(envelope)
        if not is_successful(response["statusCode"]):
            raise MessageHandlingError(envelope["topic"], response["statusCode"], response["body"])


def http_function(app: AzureFunctionsApp):
    """Return an Azure Functions HTTP entry point adapting ``azure.functions.HttpRequest``.

    Mirrors ``benzene.gcp.http_function`` / ``benzene.aws.to_lambda_handler`` so the entry-point
    idiom is the same across clouds. ``azure-functions`` is imported lazily inside the callable.
    """

    def entry(req: Any):
        from urllib.parse import urlsplit

        import azure.functions as func  # lazy: only needed at deploy time

        parts = urlsplit(getattr(req, "url", "") or "")
        raw = req.get_body()
        response = app.handle_http(
            method=req.method,
            path=parts.path or "/",
            query_string=parts.query,
            headers=dict(req.headers),
            body=raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else (raw or ""),
        )
        return func.HttpResponse(
            response.body, status_code=response.status_code, headers=response.headers
        )

    return entry


def service_bus_function(app: AzureFunctionsApp):
    """Return a Service Bus entry point: ``def entry(message)``."""

    def entry(message: Any) -> None:
        app.handle_service_bus(message)

    return entry


def event_hub_function(app: AzureFunctionsApp):
    """Return an Event Hub entry point: ``def entry(events)`` (single event or a batch)."""

    def entry(events: Any) -> None:
        app.handle_event_hub(events)

    return entry


def _is_single_event(events: Any) -> bool:
    if isinstance(events, (list, tuple)):
        return False
    return hasattr(events, "get_body") or hasattr(events, "body")
