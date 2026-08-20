# Getting started: Benzene on Azure Functions

Take a Benzene service you already have and host it on **Azure Functions** — over HTTP, Service
Bus, and Event Hub — without touching a single handler. This guide starts from the transport-neutral
handler you built in **[Getting started](getting-started.md)** (its prerequisite) and ends with a
Function App that answers all three triggers, tested end-to-end in memory and sketched out to a real
deploy.

The whole point of the ports-and-adapters design carries straight over: the handlers, topics, and
domain wiring are the *same* objects on Azure as on your laptop; only the host that mounts them onto
Azure Functions' triggers is Azure-specific. This is the same layered adoption story as
[Getting started](getting-started.md), one level up — you add a host, not a rewrite.

## What you'll build

One Function App hosting one set of order handlers, behind three triggers:

- **HTTP** — `POST /orders` places an order.
- **Service Bus and Event Hub** — one `orders:created` subscriber answers the event from either
  trigger.
- **Egress** — placing an order publishes `orders:created` back out over Service Bus.

All of it runs in memory first — local runs and the deploy come at the end.

## Prerequisites

- **[Getting started](getting-started.md)** — you should already understand a handler, `@message` /
  `@http_endpoint`, and the `Result`/status model before hosting on a cloud.
- **Python 3.10+**, `pip`, and a virtual environment.
- **[Azure Functions Core Tools v4](https://learn.microsoft.com/azure/azure-functions/functions-run-local)**
  — the `func` CLI, to run the host locally (`func start`) and publish it.
- An Azure subscription and the **[Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli)**,
  only if you want to deploy.
- Runnable reference throughout: [`examples/azure_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders),
  built on the shared [`examples/orders_domain`](https://github.com/daniellepelley/benzene-python/tree/main/examples/orders_domain).

## 1. Install

One distribution carries the Azure Functions host and all three trigger bindings:

```bash
pip install benzene-azure            # add [servicebus] for the real outbound client
```

`benzene-azure` depends on `benzene-core` and `benzene-http`, so installing it pulls in everything
the inbound bindings and the in-memory test host need. The Azure SDK is **not** a hard dependency:

- `azure-servicebus` is an optional extra, needed only for the *outbound* `ServiceBusMessageSender`.
  Install it with `pip install "benzene-azure[servicebus]"`.
- `azure-functions` (the worker SDK) is needed only to actually run on the Functions host — the
  entry-point helpers import it lazily, and the test host never touches it. Add it to your Function
  App's `requirements.txt` for deployment:

  ```text
  azure-functions
  benzene-azure[servicebus]
  ```

## 2. The shape: one domain, one Azure host

A Benzene service on Azure has two parts, and only the second is Azure-specific:

1. **The domain** — your handlers, topics, and HTTP routes, wired onto a
   [`BenzeneStartUp`](reference/core.md) composition root (the
   [composition-root path](getting-started.md#two-ways-to-wire-a-service)). In the example this is
   `OrdersStartUp`, shared verbatim with the AWS and GCP hosts.
2. **The host** — an `AzureFunctionsApp` that mounts that domain's registry and HTTP router onto
   Azure Functions' triggers.

`AzureFunctionsApp` is one host with one inner binding per trigger (transport-bindings §1):

| Trigger | Topic comes from | Cardinality | On failure |
| --- | --- | --- | --- |
| **HTTP** | the route (via `benzene.http`) | one request | maps status → HTTP code, never crashes |
| **Service Bus** | the `topic` application property | one message, one scope | raises → retry / dead-letter |
| **Event Hub** | the `topic` property | a batch, **one scope per event**, in order | raises → stops at first failure |

These are the three triggers this guide wires. `benzene.azure` binds more — Queue Storage, Blob
Storage, Cosmos DB change feed, Timer, and Event Grid — through the same host; see the
[`benzene.azure` reference](reference/azure.md) for the full set.

## 3. Build the host

Only this file is Azure-specific. It boots the *same* `OrdersStartUp` every other host and test
boots from, overrides just the outbound `MessageSender` with the real Service Bus client, and hands
the built registry + router to `AzureFunctionsApp`
([`examples/azure_orders/host.py`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders/host.py)):

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

`build_application(OrdersStartUp, overrides=[...])` runs the startup's `configure_services` /
`configure` and returns an `AppDefinition` (its `router`, `registry`, and `standard_paths`) plus the
resolved root scope; `AzureFunctionsApp.from_definition(definition)` turns it into the host whose
single `BenzeneMessageApplication` pipeline **all three** triggers share. The `overrides` hook is the
only seam that differs between deployment and tests — here it registers the real
`ServiceBusMessageSender` for egress; a test registers a `FakeMessageSender` instead (see
[section 6](#6-test-every-trigger-in-memory)).

`AzureFunctionsApp.from_definition(...)` is the one-liner; you can still construct
`AzureFunctionsApp(http_router=..., registry=...)` directly if you're wiring a registry by hand. See
the [`benzene.azure` reference](reference/azure.md) for the full constructor.

## 4. Wire the Azure triggers (v2 programming model)

The Functions host loads a `function_app.py` that declares the triggers. Benzene's entry-point
helpers — `http_function`, `service_bus_function`, `event_hub_function` — wrap the app as the plain
callables each trigger invokes, adapting the `azure.functions` request/message types for you, so this
file stays thin
([`examples/azure_orders/function_app.py`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders/function_app.py)):

```python
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


@app.service_bus_queue_trigger(
    arg_name="message", queue_name="orders", connection="BENZENE_SERVICEBUS"
)
def orders_service_bus(message: func.ServiceBusMessage) -> None:
    _service_bus(message)                      # topic from application_properties


@app.event_hub_message_trigger(
    arg_name="events", event_hub_name="orders", connection="BENZENE_EVENTHUB", cardinality="many"
)
def orders_event_hub(events: list[func.EventHubEvent]) -> None:
    _event_hub(events)                         # one scope per event
```

Each trigger declaration is ordinary Azure Functions v2 wiring — you own `route`, `queue_name`,
`event_hub_name`, and the `connection` app-setting names. The Benzene helpers are the one-liner
bodies: `http_function(app)` decomposes `func.HttpRequest` and calls `app.handle_http(...)`, returning
a `func.HttpResponse`; `service_bus_function(app)` and `event_hub_function(app)` forward the native
message/batch into `app.handle_service_bus(...)` / `app.handle_event_hub(...)`. The same three
handlers back all three triggers — the `orders:created` subscriber runs whether the event arrives over
Service Bus or Event Hub.

## 5. The supported triggers, in detail

### HTTP

The HTTP trigger routes through the `benzene.http` binding you already know: the route resolves to a
topic, the handler runs, and its Benzene status maps to an HTTP status code
([wire-contracts §4.1](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/wire-contracts.md)).
`Result.created(order)` becomes `201`, `Result.not_found(...)` becomes `404`, and an uncaught error
becomes `503` — the binding never crashes on bad input. `handle_http(method, path, query_string,
headers, body)` returns an `AzureHttpResponse` (`status_code`, `headers`, `body`), which
`http_function` converts to a `func.HttpResponse`.

### Service Bus

One message per invocation, one Benzene scope. The topic is read from the message's
`application_properties` under the `topic` key (`TOPIC_PROPERTY`); the remaining properties become
headers, and the message body (decoded to text) is the JSON body. If the handler returns a
non-successful status — or no handler is registered for the topic — `handle_service_bus` raises a
`MessageHandlingError`, so the Functions host retries the message and eventually dead-letters it per
your queue's delivery settings.

### Event Hub

A batch of events, delivered by the `cardinality="many"` trigger. Benzene processes them **one scope
per event, in order**, and — like Service Bus — the topic comes from each event's `topic` property.
A failure raises and stops at the first failing event, matching Event Hub checkpoint semantics (the
batch is redelivered from that point, so design subscribers to be idempotent). `handle_event_hub`
also accepts a single event for a `cardinality="one"` trigger.

> `benzene.azure` also binds the Queue Storage, Blob Storage, Cosmos DB change feed, Timer, and
> Event Grid triggers, each with the same entry-point-helper shape (`queue_storage_function`,
> `blob_function`, `cosmos_function`, `timer_function`, `event_grid_function`). This guide sticks to
> the three above; the [`benzene.azure` reference](reference/azure.md#overview) covers the rest.

## 6. Test every trigger in memory

The strongest reason to keep the domain and the host separate: you can drive the *real* Azure
bindings in memory, faking only the outbound edge, with no `func start` and no cloud. The example's
suite dogfoods `benzene.testing` + `benzene.azure.testing`
([`examples/azure_orders/tests/test_azure_orders.py`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders/tests/test_azure_orders.py)):

```python
import json

import pytest
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, OrderEventLog, OrderService, OrdersStartUp
from benzene.azure.testing import event_hub_event


def make_host():
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)
        services.add_instance(MessageSender, sender)   # only the external edge is faked
        services.add_instance(OrderEventLog, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_azure()
    return host, service, sender, seen
```

`create_test_host(OrdersStartUp).with_services(overrides).build_azure()` boots the *same* startup the
deployment uses and specializes it to an `AzureFunctionsTestHost` — the only line that differs from
the AWS or GCP suites is `.build_azure()`. Then exercise each trigger through its own front door:

```python
def test_http_place_order_creates_and_publishes():
    host, service, sender, _ = make_host()

    response = host.send_http("POST", "/orders", body={"sku": "ABC", "quantity": 2})

    assert response.status_code == 201
    order = json.loads(response.body)
    assert sender.last_topic == ORDER_CREATED_TOPIC     # ingress -> handler -> egress
    assert order["id"] in service.orders


def test_service_bus_order_created_is_handled():
    host, _, _, seen = make_host()
    host.send_service_bus(ORDER_CREATED_TOPIC, {"id": "ord-sb", "sku": "ABC"})
    assert seen == ["ord-sb"]


def test_event_hub_batch_is_handled_per_event():
    host, _, _, seen = make_host()
    host.send_event_hub_batch([
        event_hub_event(ORDER_CREATED_TOPIC, {"id": "e1", "sku": "A"}),
        event_hub_event(ORDER_CREATED_TOPIC, {"id": "e2", "sku": "B"}),
    ])
    assert seen == ["e1", "e2"]


def test_service_bus_unroutable_topic_raises_for_retry():
    host, _, _, _ = make_host()
    with pytest.raises(RuntimeError):        # MessageHandlingError -> platform retries/dead-letters
        host.send_service_bus("orders:unknown", {})
```

`AzureFunctionsTestHost` gives you `send_http`, `send_service_bus`, `send_event_hub`, and
`send_event_hub_batch`, plus the `service_bus_message` / `event_hub_event` builders for hand-crafting
native message shapes. The last test pins the failure rule from
[section 5](#service-bus): an unroutable topic raises, which is exactly what tells the platform to
retry. Run the whole suite with:

```bash
pytest examples/azure_orders
```

## 7. Run it locally

The Functions Core Tools run the host on your machine:

```bash
func start
```

HTTP works with an empty storage connection, but Service Bus and Event Hub triggers need real
connections. Provide them (and the egress settings) in `local.settings.json` — machine-local,
secret-bearing, and never committed:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "BENZENE_SERVICEBUS": "<service-bus-connection-string>",
    "BENZENE_EVENTHUB": "<event-hub-connection-string>",
    "BENZENE_SERVICEBUS_CONNECTION": "<service-bus-connection-string>",
    "BENZENE_SERVICEBUS_ENTITY": "orders"
  }
}
```

`BENZENE_SERVICEBUS` / `BENZENE_EVENTHUB` are the trigger `connection` names from
[section 4](#4-wire-the-azure-triggers-v2-programming-model); `BENZENE_SERVICEBUS_CONNECTION` /
`BENZENE_SERVICEBUS_ENTITY` are what `build_azure_orders_app` reads for the outbound
`ServiceBusMessageSender`. Then `POST http://localhost:7071/api/orders` to place an order (Azure
Functions prefixes HTTP routes with `/api` by default).

## 8. Deploy

Create the Function App and its dependencies, then publish. A Consumption-plan sketch — adjust
SKU/plan, and provision the Service Bus queue and Event Hub your triggers read:

```bash
az group create --name benzene-orders-rg --location eastus
az storage account create --name benzeneorders --resource-group benzene-orders-rg \
  --location eastus --sku Standard_LRS
az functionapp create --resource-group benzene-orders-rg --consumption-plan-location eastus \
  --runtime python --runtime-version 3.11 --functions-version 4 \
  --name benzene-orders --storage-account benzeneorders

# The trigger connections + the egress settings the host reads:
az functionapp config appsettings set --name benzene-orders --resource-group benzene-orders-rg \
  --settings BENZENE_SERVICEBUS="<sb-conn>" BENZENE_EVENTHUB="<eh-conn>" \
             BENZENE_SERVICEBUS_CONNECTION="<sb-conn>" BENZENE_SERVICEBUS_ENTITY="orders"

func azure functionapp publish benzene-orders
```

Every setting your host reads at startup (`BENZENE_SERVICEBUS_CONNECTION` / `_ENTITY`) and every
trigger `connection` name (`BENZENE_SERVICEBUS`, `BENZENE_EVENTHUB`) must exist as an Application
Setting. See [Hosting on Azure Functions](cookbooks/hosting-on-azure.md) for the condensed
deploy recipe.

## 9. Publishing events out (egress)

Placing an order publishes `orders:created` back out over Service Bus.
`ServiceBusMessageSender` implements the `benzene.core.MessageSender` port over `azure-servicebus`
(the optional `[servicebus]` extra, imported lazily), carrying the Benzene topic and headers in the
message's `application_properties` — so a *subscriber* reads them straight back through the Service
Bus or Event Hub binding, closing the loop:

```python
from benzene.azure import ServiceBusMessageSender

sender = ServiceBusMessageSender(connection_string=conn, entity_name="orders")
```

Your handlers depend only on the `MessageSender` interface, never on this class — which is why the
same handler publishes over Service Bus in production and a `FakeMessageSender` in the tests above.
`benzene.azure` also ships `EventHubMessageSender`, `QueueStorageMessageSender`, and
`EventGridMessageSender`, each behind the same port — see
[Outbound clients](reference/azure.md#outbound-clients).

## Troubleshooting

- **`ModuleNotFoundError: No module named 'azure.functions'`** — `azure-functions` isn't installed.
  It's needed only to run on the Functions host; add it to the Function App's `requirements.txt`. The
  in-memory test host (`build_azure()`) never imports it.
- **`ModuleNotFoundError: No module named 'azure.servicebus'`** — you're using the real outbound
  client without the extra. `pip install "benzene-azure[servicebus]"`. Tests that fake the sender
  don't need it.
- **`build_azure() requires the 'benzene-azure' package to be installed`** — the test harness raises
  this if `benzene-azure` isn't on the path; install it (it's a `benzene-core`-only dependency
  otherwise, so the harness imports the cloud package lazily).
- **HTTP `501 not-implemented`** — the app was built without an `http_router`, so there's no HTTP
  binding. Pass `http_router=` (or a full `application=` built from a definition with routes) to
  `AzureFunctionsApp`.
- **Service Bus / Event Hub message keeps retrying then dead-letters** — a `MessageHandlingError`
  is being raised because no handler matches the message's `topic` property, or the handler returned
  a non-successful status. Confirm the producer sets the `topic` application property to a registered
  topic (see [section 5](#service-bus)).
- **`404` on every route locally** — Azure Functions prefixes HTTP routes with `/api` by default, so
  request `/api/orders`, or clear the prefix via `extensions.http.routePrefix` in `host.json`.
- **A non-HTTP trigger never fires locally** — Service Bus and Event Hub triggers need real
  `connection` settings *and* a working `AzureWebJobsStorage`; the empty-string storage that suffices
  for HTTP isn't enough (run [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
  with `"AzureWebJobsStorage": "UseDevelopmentStorage=true"`).
- **`RuntimeError: Set BENZENE_SERVICEBUS_CONNECTION and BENZENE_SERVICEBUS_ENTITY ...`** — the host
  couldn't build the real outbound client because those settings are missing. Set them (deploy /
  local); tests don't run the host — they register a `FakeMessageSender` via
  `create_test_host(OrdersStartUp).with_services(...)` instead.

## See also

- [`benzene.azure` reference](reference/azure.md) — the full host, binding, and testing API.
- [Hosting on Azure Functions](cookbooks/hosting-on-azure.md) — the condensed cookbook version of
  this guide.
- [Getting started](getting-started.md) — the transport-neutral handler this guide hosts (prerequisite).
- [`benzene.testing` reference](reference/testing.md) — `create_test_host`, `FakeMessageSender`.
- [transport-bindings specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)
  — the language-neutral host/binding contract (§1) every port implements.
- [wire-contracts specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/wire-contracts.md)
  — the status → HTTP mapping (§4.1) and the envelope shape.
