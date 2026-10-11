"""Google Cloud Functions host (transport-bindings §1 — one host, inner bindings per trigger).

``GcpFunctionsApp`` hosts the same Benzene handlers behind two Cloud Functions triggers:

- an **HTTP** function (delegating to the ``benzene.http`` ASGI binding's request handling), and
- a **Pub/Sub** function (topic from the ``topic`` message attribute; one pipeline invocation and
  one scope per message; a failed result is raised so Pub/Sub redelivers — the queue-transport
  failure rule).

**Settlement** (transport-bindings §1, obligation 8): a Pub/Sub message is acknowledged — the function
returns normally — only when its invocation ended ``isSuccessful``; a failure, a missing result, an
unmatched topic or an uncaught error raises, so Pub/Sub redelivers it (enable *retry on failure* on the
function; the subscription's dead-letter topic decides when to give up). The unit of settlement is the
one message. ``settlement=Settlement(at_most_once=True)`` returns normally whatever the outcome;
``Settlement(ignore_unmatched_topics=True)`` acknowledges a topic no handler serves as ``ignored``.

Both triggers share one ``BenzeneMessageApplication`` (one pipeline, one registry). The free
functions :func:`http_function` / :func:`pubsub_function` produce the plain callables the Google
Functions Framework expects.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, ClassVar

from benzene.core import (
    QUEUE,
    REQUEST_RESPONSE,
    AppDefinition,
    BenzeneMessageApplication,
    MessageHandlingError,
    Registry,
    Settlement,
    SettlementDefaults,
    application_from,
    dispatch_one_way,
    redelivers,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths

from .pubsub import decode_pubsub_message


class GcpFunctionsApp:
    #: Each trigger's settlement shape and defaults, keyed by its ``settlementDefaults`` binding id.
    SETTLEMENT_DEFAULTS: ClassVar[tuple[SettlementDefaults, ...]] = (
        redelivers("pubsub", QUEUE),
        redelivers("http", REQUEST_RESPONSE),
    )

    def __init__(
        self,
        http_router: HttpRouter | None = None,
        registry: Registry | None = None,
        application: BenzeneMessageApplication | None = None,
        *,
        standard_paths: StandardPaths | None = None,
        settlement: Settlement = Settlement(),
    ) -> None:
        if application is None:
            if registry is None:
                registry = Registry.from_definitions(http_router) if http_router else Registry()
            application = BenzeneMessageApplication(registry)
        self._application = application
        self._settlement = settlement
        self._http_app = (
            BenzeneHttpApp(http_router, application=application, standard_paths=standard_paths)
            if http_router
            else None
        )

    @classmethod
    def from_definition(
        cls, definition: AppDefinition, *, settlement: Settlement = Settlement()
    ) -> GcpFunctionsApp:
        """Build the Functions host from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(
            http_router=definition.router,
            application=application_from(definition),
            standard_paths=definition.standard_paths,
            settlement=settlement,
        )

    @property
    def settlement(self) -> Settlement:
        """How the Pub/Sub trigger settles each message (see the module docstring)."""
        return self._settlement

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
        """Handle a Pub/Sub CloudEvent. Raises unless it is acknowledged, so Pub/Sub redelivers.

        The envelope's ``isSuccessful`` is what says whether it failed (wire-contracts.md 1.2) -
        deriving that from the status text would redeliver an application-defined status forever.
        """
        outcome = asyncio.run(
            dispatch_one_way(self._application, self._settlement, _decode_cloud_event, cloud_event)
        )
        if not outcome.acknowledge:
            raise MessageHandlingError(outcome.topic, outcome.result.status, outcome.detail)


def http_function(app: GcpFunctionsApp) -> Callable[[Any], Any]:
    """Return the plain HTTP entry point the Functions Framework calls: ``def entry(request)``."""

    def entry(request: Any) -> Any:
        return app.handle_http(request)

    return entry


def pubsub_function(app: GcpFunctionsApp) -> Callable[[Any], None]:
    """Return the plain CloudEvent entry point: ``def entry(cloud_event)``."""

    def entry(cloud_event: Any) -> None:
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


def _decode_cloud_event(cloud_event: Any) -> dict[str, Any]:
    return decode_pubsub_message(_pubsub_message(cloud_event))


def _pubsub_message(cloud_event: Any) -> dict[str, Any]:
    # A CloudEvent's `.data` is {"message": {...}, "subscription": ...}; also accept a bare
    # {"message": {...}} dict or the message dict directly.
    data = getattr(cloud_event, "data", cloud_event)
    if isinstance(data, dict) and "message" in data:
        return data["message"]
    return data
