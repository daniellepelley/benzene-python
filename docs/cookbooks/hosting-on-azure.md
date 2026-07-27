# Hosting on Azure Functions (HTTP + Service Bus + Event Hub)

Host one set of Benzene handlers on Azure Functions across **three triggers** — HTTP, Service Bus,
and Event Hub — and publish events back out over Service Bus. The handlers never change between
transports.

## Prerequisites

- Python 3.10+
- `pip install benzene-azure` (add `[servicebus]` for the real outbound client)
- Runnable reference: [`examples/azure_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders)

## Build the host

The domain wiring is shared with every other cloud; only the host is Azure-specific:

```python
from benzene.azure import AzureFunctionsApp, ServiceBusMessageSender
from orders_domain import OrderService, build_orders

wiring = build_orders(OrderService(), ServiceBusMessageSender(connection_string=..., entity_name="orders"))
app = AzureFunctionsApp(http_router=wiring.router, registry=wiring.registry)
```

## Wire the Azure triggers (v2 programming model)

The `benzene.azure` entry-point helpers adapt the `azure.functions` types for you, so `function_app.py`
is thin:

```python
import azure.functions as func
from benzene.azure import event_hub_function, http_function, service_bus_function

_http, _sb, _eh = http_function(app), service_bus_function(app), event_hub_function(app)

app_fn = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app_fn.route(route="orders/{id?}")
def orders_http(req: func.HttpRequest) -> func.HttpResponse:
    return _http(req)

@app_fn.service_bus_queue_trigger(arg_name="message", queue_name="orders", connection="BENZENE_SERVICEBUS")
def orders_service_bus(message: func.ServiceBusMessage) -> None:
    _sb(message)                             # topic from application_properties

@app_fn.event_hub_message_trigger(arg_name="events", event_hub_name="orders", connection="BENZENE_EVENTHUB", cardinality="many")
def orders_event_hub(events: list[func.EventHubEvent]) -> None:
    _eh(events)                              # one scope per event
```

Service Bus / Event Hub read the topic from the message's `topic` application property; a failure
raises so the platform retries / dead-letters.

## Test every trigger in memory (dogfooded)

```python
from benzene.azure.testing import AzureFunctionsTestHost, event_hub_event
from benzene.testing import FakeMessageSender

sender = FakeMessageSender()
host = AzureFunctionsTestHost(build_app(sender=sender))

resp = host.send_http("POST", "/orders", body={"sku": "ABC"})
assert resp.status_code == 201 and sender.last_topic == "orders.created"   # ingress->handler->egress

host.send_service_bus("orders.created", {"id": "1", "sku": "A"})
host.send_event_hub_batch([
    event_hub_event("orders.created", {"id": "2", "sku": "B"}),
    event_hub_event("orders.created", {"id": "3", "sku": "C"}),
])
```

## See also

- [`benzene.azure` reference](../reference/azure.md), [`benzene.testing` reference](../reference/testing.md)
