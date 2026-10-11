"""Azure Functions host (transport-bindings §1 — one host, inner bindings per trigger).

``AzureFunctionsApp`` hosts the same Benzene handlers behind Azure Functions triggers:

- **HTTP** — via the ``benzene.http`` binding (route → topic, status mapping).
- **Service Bus** — one message per invocation, one scope; topic from the message's application
  properties; a failure raises so the message is retried / dead-lettered.
- **Event Hub** — a batch of events; **one scope per event**, processed in order; a failure raises
  (stops at the first failure, matching checkpoint semantics).
- **Queue Storage / Blob Storage / Timer** — one invocation apiece; the topic comes from the payload
  (a Queue envelope) or an injectable default (blob-created / timer tick have no topic of their own).
- **Cosmos DB change feed** — a batch of changed documents; **one scope per document**, like Event Hub.
- **Event Grid** — a single event or a batch; **one scope per event**; native schema or CloudEvents 1.0.

**Settlement** (transport-bindings §1, obligation 8). Every trigger but HTTP is one-way, so a message
is acknowledged — the trigger function returns normally — only when its invocation ended
``isSuccessful``; a failure, a missing result, an unmatched topic or an uncaught error raises
:class:`~benzene.core.MessageHandlingError`, and the Functions host's own retry, poison-queue and
dead-letter configuration decides what happens next. The unit of settlement is one message (Service
Bus, Queue Storage), one notification (Blob), one event (Event Grid), one tick (Timer — nothing to
redeliver, the run is recorded as failed), and for the checkpointing triggers the whole delivered batch
(Event Hub, Cosmos DB change feed): the first failing item raises, the items after it do not run, and
the batch is redelivered from its checkpoint. ``settlement=Settlement(at_most_once=True)`` returns
normally whatever the outcome; ``Settlement(ignore_unmatched_topics=True)`` acknowledges a topic no
handler serves as ``ignored``.

The ``*_function`` helpers wrap an app as the plain callables an Azure Functions trigger invokes
(adapting the ``azure-functions`` request/message types lazily), mirroring ``benzene.gcp.http_function``
and ``benzene.aws.to_lambda_handler``. The package itself (and its tests) need no Azure SDK — see the
example's ``function_app.py`` for the v2-model wiring.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar

from benzene.core import (
    ONE_SHOT,
    QUEUE,
    REQUEST_RESPONSE,
    STREAM,
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

from .events import (
    DEFAULT_BLOB_TOPIC,
    DEFAULT_COSMOS_TOPIC,
    DEFAULT_TIMER_TOPIC,
    decode_blob_created,
    decode_cosmos_document,
    decode_event_grid,
    decode_event_hub_event,
    decode_queue_storage,
    decode_service_bus,
    decode_timer,
)


@dataclass
class AzureHttpResponse:
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""


class AzureFunctionsApp:
    #: Each trigger's settlement shape and defaults, keyed by its ``settlementDefaults`` binding id
    #: (transport-bindings §1, obligation 8). ``blob-storage`` is a binding this port ships that the
    #: fixture does not name yet (see the conformance runner).
    SETTLEMENT_DEFAULTS: ClassVar[tuple[SettlementDefaults, ...]] = (
        redelivers("servicebus", QUEUE),
        redelivers("queue-storage", QUEUE),
        redelivers("eventhub", STREAM),
        redelivers("cosmosdb-changefeed", STREAM),
        redelivers("event-grid", ONE_SHOT),
        redelivers("timer", ONE_SHOT),
        redelivers("blob-storage", ONE_SHOT),
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
    ) -> AzureFunctionsApp:
        """Build the Functions host from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(
            http_router=definition.router,
            application=application_from(definition),
            standard_paths=definition.standard_paths,
            settlement=settlement,
        )

    @property
    def settlement(self) -> Settlement:
        """How the non-HTTP triggers settle each message (see the module docstring)."""
        return self._settlement

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
            return AzureHttpResponse(
                501, {"content-type": "application/json"}, '{"status": "not-implemented"}'
            )
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
        asyncio.run(
            self._settle_or_raise(decode_service_bus, message, keys=self._application.metadata_keys)
        )

    # --- Event Hub trigger -----------------------------------------------------------------
    def handle_event_hub(self, events: Any) -> None:
        # Cardinality 'many' delivers a list; 'one' a single event. Process each, one scope apiece;
        # the batch is the unit of settlement - the first failure raises and the rest don't run.
        if _is_single_event(events):
            events = [events]

        async def run() -> None:
            for event in events:
                await self._settle_or_raise(
                    decode_event_hub_event, event, keys=self._application.metadata_keys
                )

        asyncio.run(run())

    # --- Queue Storage trigger -------------------------------------------------------------
    def handle_queue_storage(self, message: Any, *, default_topic: str = "") -> None:
        # One message per invocation; topic from the embedded envelope, else ``default_topic``.
        asyncio.run(
            self._settle_or_raise(decode_queue_storage, message, default_topic=default_topic)
        )

    # --- Blob Storage trigger --------------------------------------------------------------
    def handle_blob(self, blob: Any, *, default_topic: str = DEFAULT_BLOB_TOPIC) -> None:
        # One blob-created notification per invocation; the descriptor is the body.
        asyncio.run(self._settle_or_raise(decode_blob_created, blob, default_topic=default_topic))

    # --- Cosmos DB change-feed trigger -----------------------------------------------------
    def handle_cosmos(self, documents: Any, *, default_topic: str = DEFAULT_COSMOS_TOPIC) -> None:
        # A batch (list) of changed documents; one scope per document, in order (Event Hub
        # precedent), and the batch is the unit of settlement.
        if not isinstance(documents, (list, tuple)):
            documents = [documents]

        async def run() -> None:
            for document in documents:
                await self._settle_or_raise(
                    decode_cosmos_document, document, default_topic=default_topic
                )

        asyncio.run(run())

    # --- Timer trigger ---------------------------------------------------------------------
    def handle_timer(self, timer: Any, *, default_topic: str = DEFAULT_TIMER_TOPIC) -> None:
        # One tick, one invocation; the schedule info is the body. A failure fails the run, for the
        # host's retry policy - there is no message to redeliver.
        asyncio.run(self._settle_or_raise(decode_timer, timer, default_topic=default_topic))

    # --- Event Grid trigger ----------------------------------------------------------------
    def handle_event_grid(self, event: Any, *, default_topic: str = "") -> None:
        # A single event or a batch (list); one scope per event, in order. Native or CloudEvents 1.0.
        events = event if isinstance(event, (list, tuple)) else [event]

        async def run() -> None:
            for item in events:
                await self._settle_or_raise(decode_event_grid, item, default_topic=default_topic)

        asyncio.run(run())

    async def _settle_or_raise(
        self, decode: Callable[..., Mapping[str, Any]], *args: Any, **kwargs: Any
    ) -> None:
        # Unless the message is acknowledged, fault the trigger so the host redelivers it. The
        # envelope's isSuccessful is the authoritative failure signal (wire-contracts.md 1.2), never
        # a classification derived from the status text.
        outcome = await dispatch_one_way(
            self._application, self._settlement, decode, *args, **kwargs
        )
        if not outcome.acknowledge:
            raise MessageHandlingError(outcome.topic, outcome.result.status, outcome.detail)


def http_function(app: AzureFunctionsApp) -> Callable[[Any], Any]:
    """Return an Azure Functions HTTP entry point adapting ``azure.functions.HttpRequest``.

    Mirrors ``benzene.gcp.http_function`` / ``benzene.aws.to_lambda_handler`` so the entry-point
    idiom is the same across clouds. ``azure-functions`` is imported lazily inside the callable.
    """

    def entry(req: Any) -> Any:
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


def service_bus_function(app: AzureFunctionsApp) -> Callable[[Any], None]:
    """Return a Service Bus entry point: ``def entry(message)``."""

    def entry(message: Any) -> None:
        app.handle_service_bus(message)

    return entry


def event_hub_function(app: AzureFunctionsApp) -> Callable[[Any], None]:
    """Return an Event Hub entry point: ``def entry(events)`` (single event or a batch)."""

    def entry(events: Any) -> None:
        app.handle_event_hub(events)

    return entry


def queue_storage_function(
    app: AzureFunctionsApp, *, default_topic: str = ""
) -> Callable[[Any], None]:
    """Return a Queue Storage entry point: ``def entry(message)``. ``default_topic`` routes plain text."""

    def entry(message: Any) -> None:
        app.handle_queue_storage(message, default_topic=default_topic)

    return entry


def blob_function(
    app: AzureFunctionsApp, *, default_topic: str = DEFAULT_BLOB_TOPIC
) -> Callable[[Any], None]:
    """Return a Blob Storage entry point: ``def entry(blob)`` (a blob-created notification)."""

    def entry(blob: Any) -> None:
        app.handle_blob(blob, default_topic=default_topic)

    return entry


def cosmos_function(
    app: AzureFunctionsApp, *, default_topic: str = DEFAULT_COSMOS_TOPIC
) -> Callable[[Any], None]:
    """Return a Cosmos DB change-feed entry point: ``def entry(documents)`` (a batch of documents)."""

    def entry(documents: Any) -> None:
        app.handle_cosmos(documents, default_topic=default_topic)

    return entry


def timer_function(
    app: AzureFunctionsApp, *, default_topic: str = DEFAULT_TIMER_TOPIC
) -> Callable[[Any], None]:
    """Return a Timer entry point: ``def entry(timer)`` (one tick)."""

    def entry(timer: Any) -> None:
        app.handle_timer(timer, default_topic=default_topic)

    return entry


def event_grid_function(
    app: AzureFunctionsApp, *, default_topic: str = ""
) -> Callable[[Any], None]:
    """Return an Event Grid entry point: ``def entry(event)`` (single event or a batch, either schema)."""

    def entry(event: Any) -> None:
        app.handle_event_grid(event, default_topic=default_topic)

    return entry


def _is_single_event(events: Any) -> bool:
    if isinstance(events, (list, tuple)):
        return False
    return hasattr(events, "get_body") or hasattr(events, "body")
