"""Azure Functions entry point (the app the Azure Functions host loads).

Uses the v2 Python programming model: one ``func.FunctionApp`` with an HTTP trigger, a Service Bus
trigger, and an Event Hub trigger. Each trigger just hands its native input to the Benzene entry
points built by :mod:`benzene.azure` — the same handlers back all three. Configure the
``BENZENE_SERVICEBUS_*`` settings (used for egress) in the Function App.

Local run:  ``func start``  (Azure Functions Core Tools)
"""

from __future__ import annotations

import azure.functions as func

from benzene.azure import event_hub_function, http_function, service_bus_function

from .host import build_azure_orders_app

_app = build_azure_orders_app()

_http = http_function(_app)
_service_bus = service_bus_function(_app)
_event_hub = event_hub_function(_app)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="orders/{id?}")
def orders_http(req: func.HttpRequest) -> func.HttpResponse:
    return _http(req)


@app.service_bus_queue_trigger(arg_name="message", queue_name="orders", connection="BENZENE_SERVICEBUS")
def orders_service_bus(message: func.ServiceBusMessage) -> None:
    _service_bus(message)


@app.event_hub_message_trigger(arg_name="events", event_hub_name="orders", connection="BENZENE_EVENTHUB", cardinality="many")
def orders_event_hub(events: list[func.EventHubEvent]) -> None:
    _event_hub(events)
