# Getting started: Benzene on Google Cloud Functions

Take a Benzene service from an empty folder to a **Google Cloud Functions (Gen2)** deployment —
reachable over **HTTP** *and* **Pub/Sub**, publishing events back out over Pub/Sub — without the
handlers ever knowing which transport they're behind. The domain code you write here is byte-for-byte
the same code you'd deploy to AWS or Azure; only the host wiring and the deploy command are
Google-specific.

This guide assumes you've read **[Getting started](getting-started.md)** — the `@message` /
`Result` / `HttpRouter` basics it covers are used here without re-explaining. It follows the runnable
[`examples/gcp_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/gcp_orders),
which hosts the shared
[`orders_domain`](https://github.com/daniellepelley/benzene-python/tree/main/examples/orders_domain)
on Cloud Functions.

## What you'll build

A single set of order handlers, hosted on Cloud Functions behind two triggers:

- **HTTP** — `POST /orders` places an order, `GET /orders/{id}` fetches one.
- **Pub/Sub** — a subscriber on the `orders:created` topic.
- **Egress** — placing an order publishes `orders:created` through a Pub/Sub outbound client.

The `orders:created` event the HTTP handler emits is the very message the Pub/Sub subscriber consumes
— ingress → handler → egress, all through Benzene, all testable in memory.

## Prerequisites

- **Python 3.10+** and a virtual environment (see [Getting started §1](getting-started.md#1-set-up-a-project)).
- Comfort with the core handler model from **[Getting started](getting-started.md)**.
- For local runs and deploys: the [`functions-framework`](https://github.com/GoogleCloudPlatform/functions-framework-python)
  and the [`gcloud` CLI](https://cloud.google.com/sdk/docs/install), authenticated (`gcloud auth login`)
  with a project set (`gcloud config set project <id>`).

## 1. Install the package

The Google Cloud host ships as one distribution, **`benzene-gcp`**. It depends on `benzene-core` and
`benzene-http`, so a single install pulls in the whole HTTP + message pipeline:

```bash
pip install benzene-gcp
```

> **Not on PyPI yet.** Until the first release these names don't resolve — install the
> `benzene-*` layers from a local checkout of this repo instead, then carry on with the guide
> unchanged:
> `git clone https://github.com/daniellepelley/benzene-python && cd benzene-python && pip install -e packages/benzene-results -e packages/benzene-core -e packages/benzene-http -e 'packages/benzene-gcp[pubsub]'`

The real Pub/Sub outbound client needs the `google-cloud-pubsub` SDK, which is an optional extra —
add it only when you'll actually publish:

```bash
pip install "benzene-gcp[pubsub]"      # adds google-cloud-pubsub for PubSubMessageSender
```

The inbound HTTP and Pub/Sub bindings, and the in-memory test host, need **no** cloud SDK. To run or
deploy the function you also need the Functions Framework loader:

```bash
pip install functions-framework
```

## 2. Write the handlers (transport-agnostic)

A handler is a plain `async` function returning a `Result`; nothing in it mentions Google Cloud.
Handlers that need a collaborator — the store, the outbound client — are produced by a `make_*`
factory that closes over their dependencies (the Pythonic take on constructor injection). From
`orders_domain/handlers.py`:

```python
from benzene.core import Handler, MessageSender
from benzene.results import Result

from .model import ORDER_CREATED_TOPIC, OrderCreated, PlaceOrder


def make_place_order(service: OrderService, sender: MessageSender) -> Handler:
    """POST /orders → create an order and publish OrderCreated (ingress → handler → egress)."""

    async def place_order(request: PlaceOrder) -> Result:
        if not request.sku:
            return Result.bad_request("sku is required")
        order = service.place(request.sku, request.quantity)
        await sender.send_message(ORDER_CREATED_TOPIC, OrderCreated(id=order.id, sku=order.sku))
        return Result.created(order)

    return place_order


def make_on_order_created(seen: list[str]) -> Handler:
    """Pub/Sub subscriber for the OrderCreated topic → records the id it saw."""

    async def on_order_created(request: OrderCreated) -> Result:
        seen.append(request.id)
        return Result.ok()

    return on_order_created
```

`place_order` doesn't call Pub/Sub directly — it depends on the `benzene.core.MessageSender` **port**.
On Cloud Functions that port is a `PubSubMessageSender`; in a test it's a `FakeMessageSender`. The
handler can't tell the difference, and that's the point.

## 3. Compose a StartUp

Routes, topics, and service registrations live in one composition root — a `BenzeneStartUp` — that
every host and every test boots from. This is the **composition-root path** from
[Getting started](getting-started.md#two-ways-to-wire-a-service), and it earns its keep here: the same
startup drives the in-memory tests in step 5 and swaps the real Pub/Sub client for a fake through one
seam. From `orders_domain`:

```python
from benzene.core import (
    AppDefinition, BenzeneStartUp, Container, MessageSender, Registry, Scope,
)
from benzene.http import HttpRouter

from .handlers import OrderService, make_get_order, make_on_order_created, make_place_order
from .model import ORDER_CREATED_TOPIC, OrderEventLog

PLACE_ORDER_TOPIC = "orders:place"
GET_ORDER_TOPIC = "orders:get"


class OrdersStartUp(BenzeneStartUp):
    def configure_services(self, services: Container, config) -> None:
        services.try_add_singleton(OrderService)          # no factory: construct the type
        services.try_add_singleton(OrderEventLog)

    def configure(self, services: Scope, config) -> AppDefinition:
        service = services.get_service(OrderService)
        sender = services.get_service(MessageSender)       # a host or test must register this
        events = services.get_service(OrderEventLog)

        router = (
            HttpRouter()
            .register("POST", "/orders", PLACE_ORDER_TOPIC, make_place_order(service, sender))
            .register("GET", "/orders/{id}", GET_ORDER_TOPIC, make_get_order(service))
        )
        registry = Registry.from_definitions(router).register(   # the HTTP topics + the subscriber
            ORDER_CREATED_TOPIC, make_on_order_created(events)
        )
        return AppDefinition(registry=registry, router=router)
```

Two things matter for the Google host:

- The **`HttpRouter`** carries the `POST /orders` / `GET /orders/{id}` routes for the HTTP trigger.
- The **`Registry`** carries every topic — the HTTP ones *and* `orders:created` — for the Pub/Sub
  trigger. `Registry.from_definitions(router)` seeds it from the routes; the chained `.register(...)`
  adds the subscriber that has no HTTP route.

No registration passes `request_type=`: each handler's payload type is read from its first-parameter
annotation (`place_order(request: PlaceOrder)`, `on_order_created(request: OrderCreated)`). `get_order`
takes `request: dict[str, Any]`, so it stays the raw decoded body — pass `request_type=` explicitly
only to override an annotation or when the parameter isn't a concrete type.

`MessageSender` is a deliberate seam: the StartUp doesn't register one — `configure` resolves it
from the container, so each host (or test) must register the client it wants. Forget to, and
`configure` fails fast with `ServiceNotRegisteredError` naming the missing dependency.

## 4. Build the Google host and expose entry points

Only one file is Google-specific. It boots the shared `OrdersStartUp`, overrides the outbound edge
with a real `PubSubMessageSender`, and specializes the app to Cloud Functions. From
`examples/gcp_orders/host.py`:

```python
import os

from benzene.core import Container, MessageSender, build_application
from benzene.gcp import GcpFunctionsApp, PubSubMessageSender
from orders_domain import OrdersStartUp


def build_gcp_orders_app() -> GcpFunctionsApp:
    topic = os.environ.get("BENZENE_PUBSUB_TOPIC")
    if not topic:
        raise RuntimeError(
            "Set BENZENE_PUBSUB_TOPIC (projects/<project>/topics/<topic>) to run the GCP host "
            "(tests use create_test_host instead)."
        )

    def use_pubsub(services: Container) -> None:
        services.add_instance(MessageSender, PubSubMessageSender(topic))

    definition, _ = build_application(OrdersStartUp, overrides=[use_pubsub])
    return GcpFunctionsApp.from_definition(definition)
```

`GcpFunctionsApp.from_definition(definition)` builds the host from the composition root's
`AppDefinition` in one line — it wires the `HttpRouter` for the HTTP trigger and the shared
`BenzeneMessageApplication` for both triggers, so **both run one pipeline over one registry**. You
can still construct it directly — `GcpFunctionsApp(http_router=router, registry=registry)` — if you're
wiring a registry by hand; the reference covers every constructor shape
([`benzene.gcp` — `GcpFunctionsApp`](reference/gcp.md#gcpfunctionsapp)).

The Functions Framework doesn't load classes — it loads plain module-level callables. `http_function`
and `pubsub_function` wrap the host into exactly those. From `examples/gcp_orders/main.py`:

```python
from benzene.gcp import http_function, pubsub_function

from .host import build_gcp_orders_app

_app = build_gcp_orders_app()

orders_http = http_function(_app)         # entry point for the HTTP-triggered function
orders_pubsub = pubsub_function(_app)     # entry point for the Pub/Sub-triggered function
```

`http_function(app)` returns `def entry(request)` (Functions-Framework/Flask request in, `(body,
status, headers)` out); `pubsub_function(app)` returns `def entry(cloud_event)` for a Pub/Sub
CloudEvent. Deploy each as its own function pointing at the matching entry point — same source, same
handlers.

## 5. Test it in memory (dogfooded, no cloud)

Both triggers run in-memory through the *real* bindings — no emulator, no network. Boot the same
`OrdersStartUp`, fake only the outbound edge with `FakeMessageSender`, and specialize to Google with
`.build_gcp()`. From `examples/gcp_orders/tests/test_gcp_orders.py`:

```python
import json

import pytest
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, OrderEventLog, OrderService, OrdersStartUp


def make_host():
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)
        services.add_instance(MessageSender, sender)       # only the external edge is faked
        services.add_instance(OrderEventLog, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_gcp()
    return host, service, sender, seen


def test_http_place_order_creates_and_publishes():
    host, service, sender, _ = make_host()

    response = host.send_http("POST", "/orders", body={"sku": "ABC", "quantity": 2})

    assert response.status_code == 201
    order = json.loads(response.body)
    assert sender.last_topic == ORDER_CREATED_TOPIC        # ingress → handler → egress
    assert sender.last_message.id == order["id"]


def test_pubsub_order_created_is_handled():
    host, _, _, seen = make_host()

    host.send_pubsub(ORDER_CREATED_TOPIC, body={"id": "ord-1", "sku": "ABC"})

    assert seen == ["ord-1"]


def test_pubsub_unroutable_topic_raises_for_redelivery():
    host, _, _, _ = make_host()
    with pytest.raises(RuntimeError):                      # not-found → raised so Pub/Sub redelivers
        host.send_pubsub("orders:unknown", body={})
```

`create_test_host(OrdersStartUp).with_services(overrides).build_gcp()` returns a
`GcpFunctionsTestHost` ([reference](reference/gcp.md#testing)) whose `send_http(...)` and
`send_pubsub(...)` drive the actual `GcpFunctionsApp.handle_http` / `handle_pubsub` code paths behind
the scenes — the same code Cloud Functions calls in production. The last test pins the
**queue-transport failure rule**: an unroutable Pub/Sub message yields a `not-found` result, which
the binding *raises* so Pub/Sub redelivers rather than silently dropping the message.

Run it:

```bash
pytest examples/gcp_orders
```

## 6. Run locally with the Functions Framework

The Functions Framework runs either entry point on your machine. Point `BENZENE_PUBSUB_TOPIC` at a
real topic (the HTTP handler publishes on success):

```bash
pip install -r examples/gcp_orders/requirements.txt
export BENZENE_PUBSUB_TOPIC="projects/<project>/topics/orders"

functions-framework --target orders_http --debug
```

In another terminal:

```bash
curl -X POST localhost:8080/orders -d '{"sku": "ABC", "quantity": 2}'
# 201 Created — and publishes orders:created
```

## 7. Deploy

Deploy each trigger as its own Gen2 function from the same source, each pointing at its entry point in
`main.py`:

```bash
# HTTP-triggered function
gcloud functions deploy orders-http \
  --gen2 --runtime python312 --region <region> --source examples/gcp_orders \
  --entry-point orders_http --trigger-http --allow-unauthenticated \
  --set-env-vars BENZENE_PUBSUB_TOPIC=projects/<project>/topics/orders

# Pub/Sub-triggered function (same source, same handlers)
gcloud functions deploy orders-pubsub \
  --gen2 --runtime python312 --region <region> --source examples/gcp_orders \
  --entry-point orders_pubsub --trigger-topic orders
```

`--entry-point` names the callable to load; `--trigger-http` vs `--trigger-topic` selects the trigger.
The HTTP function needs `BENZENE_PUBSUB_TOPIC` so its egress client knows where to publish; the
Pub/Sub function is a pure subscriber and doesn't. When the HTTP deploy finishes:

```bash
curl -X POST "$(gcloud functions describe orders-http --gen2 --region <region> \
  --format 'value(serviceConfig.uri)')/orders" -d '{"sku":"ABC","quantity":2}'
```

For the full Pub/Sub topic/subscription setup and IAM notes, see
[Hosting on Google Cloud Functions](cookbooks/hosting-on-gcp.md).

## Supported triggers

`benzene.gcp` supports exactly the two triggers the Functions Framework offers, both routed through
the same pipeline ([transport-bindings §1](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)):

| Trigger | Entry point | How the topic is resolved | Failure behavior |
| --- | --- | --- | --- |
| **HTTP** | `http_function(app)` | The `HttpRouter` maps route → Benzene topic; the Benzene status maps to an HTTP status code | Bad input becomes a proper `4xx`/`5xx` body — the binding never crashes |
| **Pub/Sub** | `pubsub_function(app)` | The `topic` message **attribute** (`TOPIC_ATTRIBUTE`); other attributes become headers, the base64 `data` is the JSON body | A failure result is **raised** (`MessageHandlingError`) so Pub/Sub redelivers |

Outbound, `PubSubMessageSender` (the `benzene.core.MessageSender` port over `google-cloud-pubsub`)
publishes every Benzene topic to one Pub/Sub `topic_path`, carrying the Benzene topic in the `topic`
attribute and forwarding headers as attributes. Inbound decoding is `decode_pubsub_message(message)`
if you ever need the raw envelope. See [`benzene.gcp` reference](reference/gcp.md#pubsub-binding).

## Troubleshooting

- **`ModuleNotFoundError: No module named 'benzene.gcp'`** — install the host distribution:
  `pip install benzene-gcp`.
- **`ModuleNotFoundError: No module named 'google.cloud'`** (or an import error only when publishing)
  — the Pub/Sub SDK is an optional extra: `pip install "benzene-gcp[pubsub]"`. The inbound bindings
  and the test host don't need it, so this only surfaces at publish time.
- **`RuntimeError: Set BENZENE_PUBSUB_TOPIC ...`** at startup — the host needs the topic to build a
  real `PubSubMessageSender`. Export `BENZENE_PUBSUB_TOPIC=projects/<project>/topics/<topic>`; tests
  don't run the host — they register a fake via `create_test_host(OrdersStartUp).with_services(...)`
  instead.
- **HTTP function returns `501 not-implemented`** — you built a `GcpFunctionsApp` with no
  `http_router`. Pass the router (or the full `AppDefinition.router`) if you want the HTTP trigger.
- **Pub/Sub messages keep redelivering** — a handler is returning a failure result (or none is
  registered for that topic), so the binding raises and Pub/Sub retries. Confirm the message's
  `topic` attribute matches a registered topic and the handler returns a successful `Result`.
- **`404` for a route you defined** — the HTTP method must match too; a `POST` route won't answer a
  `GET`.

## See also

- [`benzene.gcp` reference](reference/gcp.md) — the full host, binding, and testing API.
- [Hosting on Google Cloud Functions](cookbooks/hosting-on-gcp.md) — the task-focused cookbook,
  including topic/subscription setup.
- [`benzene.testing`](reference/testing.md) — `create_test_host`, `.build_gcp()`, `FakeMessageSender`.
- [transport-bindings](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)
  — the language-neutral rules this host implements.
- Run the same handlers elsewhere: [Hosting on AWS](cookbooks/hosting-on-aws.md),
  [Hosting on Azure](cookbooks/hosting-on-azure.md).
