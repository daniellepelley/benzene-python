"""Google Cloud Functions host (transport-bindings §1 — one host, inner bindings per trigger).

``GcpFunctionsApp`` hosts the same Benzene handlers behind two Cloud Functions triggers:

- an **HTTP** function (delegating to the ``benzene.http`` ASGI binding's request handling), and
- a **Pub/Sub** function (topic from the ``topic`` message attribute; one pipeline invocation and
  one scope per message; a failed result is raised so Pub/Sub redelivers — the queue-transport
  failure rule).

Both triggers share one ``BenzeneMessageApplication`` (one pipeline, one registry). The free
functions :func:`http_function` / :func:`pubsub_function` produce the plain callables the Google
Functions Framework expects.
"""

from __future__ import annotations

import asyncio
from typing import Any

from benzene.core import BenzeneMessageApplication, Registry
from benzene.http import BenzeneHttpApp, HttpRouter
from benzene.results import is_successful

from .pubsub import decode_pubsub_message


class GcpFunctionsApp:
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
    def handle_http(self, request: Any) -> tuple[str, int, dict[str, str]]:
        """Handle a Functions-Framework/Flask-style HTTP request → ``(body, status, headers)``."""
        if self._http_app is None:
            return ('{"status": "not-implemented"}', 501, {"content-type": "application/json"})

        method = getattr(request, "method", "GET")
        path = getattr(request, "path", "/")
        headers = dict(getattr(request, "headers", {}) or {})
        query_string = _query_string(request)
        body = _request_body(request)

        response = asyncio.run(
            self._http_app.handle(
                method=method,
                path=path,
                query_string=query_string,
                headers=headers,
                body=body,
            )
        )
        return response.body, response.status_code, dict(response.headers)

    # --- Pub/Sub trigger -------------------------------------------------------------------
    def handle_pubsub(self, cloud_event: Any) -> None:
        """Handle a Pub/Sub CloudEvent. Raises on a failure result so Pub/Sub redelivers."""
        message = _pubsub_message(cloud_event)
        envelope = decode_pubsub_message(message)
        response = asyncio.run(self._application.handle_async(envelope))
        if not is_successful(response["statusCode"]):
            raise RuntimeError(
                f"Pub/Sub handler for topic {envelope['topic']!r} failed with "
                f"status {response['statusCode']!r}: {response['body']}"
            )


def http_function(app: GcpFunctionsApp):
    """Return the plain HTTP entry point the Functions Framework calls: ``def entry(request)``."""

    def entry(request: Any):
        return app.handle_http(request)

    return entry


def pubsub_function(app: GcpFunctionsApp):
    """Return the plain CloudEvent entry point: ``def entry(cloud_event)``."""

    def entry(cloud_event: Any):
        return app.handle_pubsub(cloud_event)

    return entry


# --- request/event extraction (duck-typed so tests can pass simple stand-ins) --------------
def _query_string(request: Any) -> str:
    qs = getattr(request, "query_string", b"")
    if isinstance(qs, (bytes, bytearray)):
        return bytes(qs).decode("latin-1")
    return str(qs or "")


def _request_body(request: Any) -> str:
    get_data = getattr(request, "get_data", None)
    if callable(get_data):
        data = get_data(as_text=True) if _accepts_as_text(get_data) else get_data()
        if isinstance(data, (bytes, bytearray)):
            return bytes(data).decode("utf-8")
        return data or ""
    data = getattr(request, "data", "")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data).decode("utf-8")
    return data or ""


def _accepts_as_text(fn: Any) -> bool:
    try:
        import inspect

        return "as_text" in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False


def _pubsub_message(cloud_event: Any) -> dict[str, Any]:
    # A CloudEvent's `.data` is {"message": {...}, "subscription": ...}; also accept a bare
    # {"message": {...}} dict or the message dict directly.
    data = getattr(cloud_event, "data", cloud_event)
    if isinstance(data, dict) and "message" in data:
        return data["message"]
    return data
