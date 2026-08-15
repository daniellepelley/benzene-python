"""Azure Functions entry point for a service Function App — ``SERVICE_NAME`` (env) picks the domain.

Uses the v2 Python programming model, exactly like ``examples/azure_orders/function_app.py``, but
**one shared file backs all six deployables** (mirrors ``examples/aws_lambda_mesh``'s single
``service.main.handler`` image, adapted to Azure Functions' decorator-at-import-time model): every
service gets the catch-all HTTP trigger (``/benzene/*`` plus, for orders, ``POST /orders`` — both match
the same ``route="{*route}"`` pattern, so ``host.json``'s ``routePrefix: ""`` puts them at the site
root, exactly as the mesh Function's discovery-built URL expects), and ``SERVICE_NAME`` decides which
*additional* Service Bus / Event Hub / Event Grid trigger(s) this deployable also declares — Terraform
deploys this same zip six times, once per ``SERVICE_NAME`` (``deploy/main.tf``).

Local run (per domain):  ``SERVICE_NAME=orders func start``
"""

from __future__ import annotations

import os

import azure.functions as func
from benzene.azure import (
    event_grid_function,
    event_hub_function,
    http_function,
    service_bus_function,
)

from .host import build_service_app

_SERVICE_NAME = os.environ.get("SERVICE_NAME", "")

_app = build_service_app()

_http = http_function(_app)
_service_bus = service_bus_function(_app)
_event_hub = event_hub_function(_app)
_event_grid = event_grid_function(_app)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# --- HTTP: every service (the mesh interrogates /benzene/spec + /benzene/health here; orders also
# takes POST /orders, matched by the same catch-all route) --------------------------------------
@app.route(route="{*route}", methods=["GET", "POST"])
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    return _http(req)


# --- Service Bus: the point-to-point command chain (orders -> payments -> shipping) -------------
if _SERVICE_NAME == "payments":

    @app.function_name("payments-sb")
    @app.service_bus_queue_trigger(
        arg_name="message", queue_name="payments", connection="BENZENE_SERVICEBUS_CONNECTION"
    )
    def payments_service_bus(message: func.ServiceBusMessage) -> None:
        _service_bus(message)

elif _SERVICE_NAME == "shipping":

    @app.function_name("shipping-sb")
    @app.service_bus_queue_trigger(
        arg_name="message", queue_name="shipping", connection="BENZENE_SERVICEBUS_CONNECTION"
    )
    def shipping_service_bus(message: func.ServiceBusMessage) -> None:
        _service_bus(message)


# --- Event Hub: order:placed fan-out (orders publishes; inventory + notifications each read their
# own consumer group so both see every event) -----------------------------------------------------
if _SERVICE_NAME in ("inventory", "notifications"):

    @app.function_name(f"{_SERVICE_NAME}-eh")
    @app.event_hub_message_trigger(
        arg_name="events",
        event_hub_name="order-placed",
        connection="BENZENE_EVENTHUB_CONNECTION",
        consumer_group=_SERVICE_NAME,
        cardinality="many",
    )
    def order_placed_event_hub(events: list[func.EventHubEvent]) -> None:
        _event_hub(events)


# --- Event Grid: routed integration events (payments publishes payment:captured, shipping publishes
# shipment:dispatched -> filtered by event type to inventory/notifications/analytics) ---------------
if _SERVICE_NAME in ("inventory", "notifications", "analytics"):

    @app.function_name(f"{_SERVICE_NAME}-eg")
    @app.event_grid_trigger(arg_name="event")
    def integration_event_grid(event: func.EventGridEvent) -> None:
        # The event type (native eventType / CloudEvents type) carries the Benzene topic; the entry
        # point decodes either schema, so notifications (which subscribes to both payment:captured and
        # shipment:dispatched) needs only this one trigger.
        _event_grid(event)
