# Hosting on Azure Functions (HTTP + Service Bus + Event Hub)

Host one set of Benzene handlers on Azure Functions across **three triggers** — HTTP, Service Bus,
and Event Hub — and publish events back out over Service Bus. The handlers never change between
transports.

## Prerequisites

- Python 3.10+
- `pip install benzene-azure` (add `[servicebus]` for the real outbound client)
- Runnable reference: [`examples/azure_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders)

## Build the host

The domain is the shared `OrdersStartUp`; only the host is Azure-specific. It reads the Service Bus
connection from the environment, registers the real `ServiceBusMessageSender` for the one outbound
edge, and specializes the composition root to Azure Functions with `AzureFunctionsApp.from_definition`:

```python
import os

from benzene.azure import AzureFunctionsApp, ServiceBusMessageSender
from benzene.core import Container, MessageSender, build_application
from orders_domain import OrdersStartUp


def build_azure_orders_app() -> AzureFunctionsApp:
    connection = os.environ["BENZENE_SERVICEBUS_CONNECTION"]
    entity = os.environ["BENZENE_SERVICEBUS_ENTITY"]

    def use_service_bus(services: Container) -> None:
        services.add_instance(
            MessageSender,
            ServiceBusMessageSender(connection_string=connection, entity_name=entity),
        )

    definition, _ = build_application(OrdersStartUp, overrides=[use_service_bus])
    return AzureFunctionsApp.from_definition(definition)
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
from benzene.azure.testing import event_hub_event
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host

sender = FakeMessageSender()
# Boot the real composition root, fake only the outbound edge, specialize to Azure Functions.
host = (
    create_test_host(OrdersStartUp)
    .with_services(lambda services: services.add_instance(MessageSender, sender))
    .build_azure()
)

resp = host.send_http("POST", "/orders", body={"sku": "ABC"})
assert resp.status_code == 201 and sender.last_topic == "orders:created"   # ingress->handler->egress

host.send_service_bus("orders:created", {"id": "1", "sku": "A"})
host.send_event_hub_batch([
    event_hub_event("orders:created", {"id": "2", "sku": "B"}),
    event_hub_event("orders:created", {"id": "3", "sku": "C"}),
])
```

## See also

- [`benzene.azure` reference](../reference/azure.md), [`benzene.testing` reference](../reference/testing.md)
