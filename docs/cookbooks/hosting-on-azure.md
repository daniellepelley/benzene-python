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

## Adapt the Azure triggers

In an isolated-worker Function App, each trigger function adapts the `azure.functions` input to the
app:

```python
import azure.functions as func

def orders_http(req: func.HttpRequest) -> func.HttpResponse:
    r = app.handle_http_request(
        method=req.method, path=req.url.split("?")[0].split("/", 3)[-1],
        headers=dict(req.headers), body=req.get_body().decode() or "",
    )
    return func.HttpResponse(r.body, status_code=r.status_code, headers=r.headers)

def orders_service_bus(message: func.ServiceBusMessage) -> None:
    app.handle_service_bus(message)          # topic from application_properties

def orders_event_hub(events: list[func.EventHubEvent]) -> None:
    app.handle_event_hub(events)             # one scope per event
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
