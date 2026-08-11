# Getting started

Build a small Benzene service in Python — from an empty folder to a running HTTP endpoint — and see
how the layered packages let you adopt exactly as much of Benzene as you need.

## Prerequisites

- **Python 3.10+**
- `pip` and a virtual environment

## 1. Set up a project

```bash
mkdir hello-benzene && cd hello-benzene
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
```

## 2. Write a handler (level 1 + 2)

A Benzene handler is a plain `async` function from a request to a `Result`. It never sees the
transport. You need two layers for this: `benzene-core` to run it, which pulls in `benzene-results`
for the return type.

```bash
pip install benzene-core
```

Create `app.py`:

```python
from dataclasses import dataclass

from benzene.core import BenzeneMessageApplication, Registry, message
from benzene.results import Result


@dataclass
class Greet:
    name: str = "world"


@message("say:hello")
async def hello(request: Greet) -> Result:
    return Result.ok({"greeting": f"Hello {request.name}"})


application = BenzeneMessageApplication(Registry().add(hello))
```

The `@message("say:hello")` decorator registers the handler under a topic. Benzene reads the request
type from the handler's first-parameter annotation — `request: Greet` — and builds a `Greet` from the
decoded JSON body before calling you, so you don't repeat the type. (Pass `request_type=` to override,
or when the parameter isn't annotated with a concrete type — an unannotated `request` or a
`request: dict[str, Any]` stays the raw decoded body.)

## 3. Drive it with the transport-neutral envelope

Before adding any HTTP, you can already exercise the handler through the `BenzeneMessage` envelope —
this is the same wire contract every Benzene port speaks. Add to the bottom of `app.py`:

```python
import asyncio

if __name__ == "__main__":
    response = asyncio.run(
        application.handle(
            {"topic": "say:hello", "headers": {}, "body": '{"name": "Benzene"}'}
        )
    )
    print(response)
```

```bash
python app.py
# {'statusCode': 'ok', 'headers': {'content-type': 'application/json'},
#  'body': '{"greeting": "Hello Benzene"}'}
```

Note the response is transport-neutral: a Benzene **status** (`ok`), headers, and a pre-serialized
JSON **body**.

## 4. Host it over HTTP (level 3)

Now put the same handler behind a real HTTP server. Install the HTTP binding and an ASGI server:

```bash
pip install benzene-http uvicorn
```

Add an HTTP route to the handler and expose an ASGI app. Update `app.py`:

```python
from dataclasses import dataclass

from benzene.core import message
from benzene.results import Result
from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint


@dataclass
class Greet:
    name: str = "world"


@http_endpoint("GET", "/greet/{name}")
@message("say:hello")
async def hello(request: Greet) -> Result:
    return Result.ok({"greeting": f"Hello {request.name}"})


app = BenzeneHttpApp(HttpRouter().add(hello))
```

`@http_endpoint("GET", "/greet/{name}")` says *where* the request arrives; `@message("say:hello")`
says *which handler* it resolves to. The `{name}` path parameter is merged into the handler's
request.

Run it:

```bash
uvicorn app:app
```

In another terminal:

```bash
curl -i http://127.0.0.1:8000/greet/Benzene
# HTTP/1.1 200 OK
# content-type: application/json
#
# {"greeting": "Hello Benzene"}
```

The handler's Benzene status (`ok`) was mapped to HTTP `200` by the binding
([wire-contracts §4.1](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/wire-contracts.md)).
Try a route that doesn't exist and you'll get a `404` with a `not-found` body — the binding never
crashes on bad input.

## What just happened

You adopted Benzene one layer at a time:

1. `benzene-results` — the `Result` you returned.
2. `benzene-core` — registering and running the handler through the envelope.
3. `benzene-http` — hosting it over HTTP, unchanged.

The handler code never changed as you added the transport — that is the whole point of the
ports-and-adapters design. See **[Packages & adoption levels](packages.md)** for why the layering is
split this way, and the reference docs for each package:
[`benzene.results`](reference/results.md), [`benzene.core`](reference/core.md),
[`benzene.http`](reference/http.md).

## Two ways to wire a service

Everything above used the **direct path**: decorate a handler (or register it explicitly), add it to
a `Registry`/`HttpRouter`, and hand that straight to a host. There is a second path — a **composition
root** (`BenzeneStartUp`) — that the cloud guides reach for. Both are first-class; the difference is
only *how much wiring machinery* you opt into, so it's worth knowing which one you're choosing.

### The direct path (simplest)

Decorate handlers and add them, then pass the registry/router to a host — no extra classes:

```python
app = BenzeneHttpApp(HttpRouter().add(hello))       # the hello handler from step 4
```

Or register explicitly, without decorators — just as short, and equally the direct path:

```python
router = (
    HttpRouter()
    .register("POST", "/orders", "orders:place", place_order)
    .register("GET", "/orders/{id}", "orders:get", get_order)
)
app = BenzeneHttpApp(router)
```

The same shape hosts on a cloud: `AwsLambdaApp(http_router=router)`. Reach for the direct path for a
small service, a handler with no injected dependencies, or when you construct your dependencies
yourself.

### The composition root (when you want the shared seams)

A [`BenzeneStartUp`](reference/core.md) subclass splits wiring into two methods —
`configure_services` (register services into a container) and `configure` (resolve them and wire
routes/topics into an `AppDefinition`). Here `OrderService`/`OrderEventLog` are the app's own
services, and each `make_*` is a small factory that closes over its dependencies and returns a handler
(`make_place_order(service, sender)` → `async def place_order(request: PlaceOrder)`), the shape the
cloud guides build on:

```python
from benzene.core import (
    AppDefinition, BenzeneStartUp, Container, MessageSender, Registry, Scope,
)
from benzene.http import HttpRouter


class OrdersStartUp(BenzeneStartUp):
    def configure_services(self, services: Container, config) -> None:
        services.try_add_singleton(OrderService)
        services.try_add_singleton(OrderEventLog)

    def configure(self, services: Scope, config) -> AppDefinition:
        service = services.get_service(OrderService)
        sender = services.get_service(MessageSender)     # a host/test registers this
        events = services.get_service(OrderEventLog)

        router = (
            HttpRouter()
            .register("POST", "/orders", PLACE_ORDER_TOPIC, make_place_order(service, sender))
            .register("GET", "/orders/{id}", GET_ORDER_TOPIC, make_get_order(service))
        )
        registry = Registry.from_definitions(router).register(
            ORDER_CREATED_TOPIC, make_on_order_created(events)
        )
        return AppDefinition(registry=registry, router=router)
```

That extra structure is not ceremony — it buys two things a direct-path service can't get for free:

- **One test harness for every host.** `create_test_host(OrdersStartUp).with_services(...).build_aws()`
  boots the *real* app and specializes it to any cloud in one line — swap `.build_aws()` for
  `.build_gcp()` / `.build_azure()` and the same test runs unchanged.
- **One dependency-swap seam.** `configure` resolves a `MessageSender` it never constructs, so
  deployment registers the real client (`SnsMessageSender`, `PubSubMessageSender`, …) and a test
  registers a `FakeMessageSender` — and *nothing else* about the app changes.

Reach for the composition root when you want that provider-agnostic test harness or a single
dependency-swap seam; otherwise the direct path above is enough. The cloud guides
([AWS](getting-started-aws.md), [Azure](getting-started-azure.md),
[Google](getting-started-google.md), [Kubernetes](getting-started-kubernetes.md)) all build on this
same `OrdersStartUp`.

## Why not just a minimal ASGI app?

Worth asking honestly: a bare route on FastAPI, Flask, or Starlette —
`@app.get("/greet/{name}") def greet(name: str): return {"greeting": f"Hello {name}"}` — does the
same job as steps 1–4 above in one line, no `benzene-core`/`benzene-http` install. For an HTTP-only
service that never talks to anything else, that line does the same job this guide's four steps do, and
you don't need Benzene to get it — a real ASGI framework already gives HTTP everything Benzene's
envelope gives it here.

The payoff shows up the moment this same handler needs a **second** entry point — a queue another team
publishes to, a Kafka topic, a batch job that used to call this endpoint but really just wants to drop
a message. A route decorator has no answer for that; you'd write a second, separate handler and keep
both in sync by hand. With Benzene the handler above doesn't change at all: `benzene-aws`'s
[self-hosted SQS consumer](../examples/sqs_orders) or `benzene-kafka`'s
[self-hosted consumer](../examples/kafka_orders) point a worker at the *same* `@message("say:hello")`
function, because it was never written against an ASGI request in the first place — see
[`examples/k8s_orders`](../examples/k8s_orders) for that running as one process, one Deployment,
three transports. If HTTP genuinely is and always will be the only way in, reach
for FastAPI/Flask directly instead — you'll write less code, not more.

## Troubleshooting

- **`ModuleNotFoundError: No module named 'benzene.http'`** — you installed a lower layer only.
  `pip install benzene-http` (it pulls in the rest).
- **`404` for a route you defined** — the HTTP method must match too; `@http_endpoint("GET", ...)`
  won't answer a `POST`.
- **Handler raised an exception** — Benzene turns an uncaught error into a `service-unavailable`
  result (HTTP `503`) rather than crashing the server; check the response body's `detail`.
