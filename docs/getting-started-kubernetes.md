# Getting Started: Benzene on Kubernetes

This guide takes you from an empty folder to **one Benzene composition root, reached over HTTP, SQS,
and Kafka, hosted in a single Python process**. That's deliberately more than "deploy an ASGI app to
a pod": see [Why not just a minimal ASGI app?](getting-started.md#why-not-just-a-minimal-asgi-app)
for why a single-transport example wouldn't actually show what Benzene is for here.

> **Runnable version:** this guide follows [`examples/k8s_orders`](../examples/k8s_orders) — a
> Dockerfile, a Kubernetes manifest, and a `docker-compose.yml` that runs all three legs locally
> against LocalStack + a throwaway Kafka broker, no cloud account needed.

## What you'll build

```
        HTTP        ─────────┐
        SQS queue   ─────────┼──▶  orders-app (Deployment)  ──▶  orders_domain
        Kafka topic ─────────┘                                     (OrdersStartUp)
```

One composition root (a `BenzeneStartUp`), mounted by one process that runs uvicorn, an SQS consumer
loop, and a Kafka consumer loop together on one `benzene.core.WorkerHost` — one container image, one
Kubernetes Deployment.

## Prerequisites

- Python 3.10+ and Docker.
- A cluster and `kubectl` — [kind](https://kind.sigs.k8s.io/) is the quickest for local work
  (`kind create cluster`).
- To follow along with real messages rather than just reading: an SQS queue and a Kafka topic
  somewhere reachable (LocalStack and a throwaway broker via `docker compose` cover both with no
  account at all — see the [runnable example](../examples/k8s_orders)).

## 1. The shared domain

Everything downstream depends on this one composition root — the
[composition-root path](getting-started.md#two-ways-to-wire-a-service), chosen here precisely because
three transports (HTTP, SQS, Kafka) boot the *same* `OrdersStartUp` and each swaps in only its own
`MessageSender`. A domain package that any host can mount:

```
orders_domain/
    __init__.py
    model.py       # dataclasses: PlaceOrder, Order, OrderCreated
    handlers.py     # plain async handler functions, dependencies injected via a make_* factory
    startup.py      # OrdersStartUp(BenzeneStartUp) - registers services, wires routes + topics
```

```python
# handlers.py
from benzene.core import Handler, MessageSender
from benzene.results import Result

from .model import ORDER_CREATED_TOPIC, Order, OrderCreated, PlaceOrder


class OrderService:
    def __init__(self) -> None:
        self.orders: dict[str, Order] = {}

    def place(self, sku: str, quantity: int) -> Order:
        import uuid

        order = Order(id=uuid.uuid4().hex, sku=sku, quantity=quantity)
        self.orders[order.id] = order
        return order


def make_place_order(service: OrderService, sender: MessageSender) -> Handler:
    async def place_order(request: PlaceOrder) -> Result:
        if not request.sku:
            return Result.bad_request("sku is required")
        order = service.place(request.sku, request.quantity)
        await sender.send_message(ORDER_CREATED_TOPIC, OrderCreated(id=order.id, sku=order.sku))
        return Result.created(order)

    return place_order
```

```python
# startup.py
from collections.abc import Mapping

from benzene.core import AppDefinition, BenzeneStartUp, Container, MessageSender, Registry, Scope
from benzene.http import HttpRouter

from .handlers import OrderService, make_place_order

PLACE_ORDER_TOPIC = "orders:place"


class OrdersStartUp(BenzeneStartUp):
    def configure_services(self, services: Container, config: Mapping[str, str]) -> None:
        services.try_add_singleton(OrderService)

    def configure(self, services: Scope, config: Mapping[str, str]) -> AppDefinition:
        service = services.get_service(OrderService)
        sender = services.get_service(MessageSender)  # a host must register this

        router = HttpRouter().register(
            "POST", "/orders", PLACE_ORDER_TOPIC, make_place_order(service, sender)
        )
        return AppDefinition(registry=Registry.from_definitions(router), router=router)
```

`configure` deliberately resolves a `MessageSender` it never constructs — each leg below registers
its *own* concrete sender (HTTP, SQS, Kafka) as the one thing that changes between them. Nothing here
mentions Kubernetes, SQS, Kafka, or HTTP status codes — that's the point of a message handler in
Benzene's hexagonal architecture: the domain logic sits behind a port, and a transport is just an
adapter in front of it.

## 2. Build each leg, then run them together

Each leg builds its own app from `OrdersStartUp`, differing only in which `MessageSender` it
registers — three small functions, no entry-point code of their own:

```python
# http_orders/host.py
import os

from benzene.core import MessageSender, build_application, use_instance
from benzene.http import BenzeneHttpApp, HttpMessageSender
from orders_domain import OrdersStartUp


def build_http_orders_app() -> BenzeneHttpApp:
    events_url = os.environ["BENZENE_ORDERS_EVENTS_URL"]
    definition, _ = build_application(
        OrdersStartUp, overrides=[use_instance(MessageSender, HttpMessageSender(events_url))]
    )
    return BenzeneHttpApp.from_definition(definition)
```

```python
# sqs_orders/host.py
import os

from benzene.aws import SqsConsumerApp, SqsMessageSender
from benzene.core import MessageSender, build_application, use_instance
from orders_domain import OrdersStartUp


def build_sqs_orders_app() -> SqsConsumerApp:
    events_queue_url = os.environ["BENZENE_SQS_EVENTS_QUEUE_URL"]
    definition, _ = build_application(
        OrdersStartUp, overrides=[use_instance(MessageSender, SqsMessageSender(events_queue_url))]
    )
    return SqsConsumerApp.from_definition(definition)
```

```python
# kafka_orders/host.py
import os

from benzene.core import MessageSender, build_application, use_instance
from benzene.kafka import KafkaConsumerApp, KafkaMessageSender
from orders_domain import OrdersStartUp


def build_kafka_orders_app() -> KafkaConsumerApp:
    sender = KafkaMessageSender(
        os.environ["BENZENE_KAFKA_TOPIC"], bootstrap_servers=os.environ["BENZENE_KAFKA_BOOTSTRAP"]
    )
    definition, _ = build_application(OrdersStartUp, overrides=[use_instance(MessageSender, sender)])
    return KafkaConsumerApp.from_definition(definition)
```

Now the one entry point that runs all three. Running N transports in one process with coordinated
shutdown is the framework's job, so it is a `WorkerHost` — three legs, and nothing about orders:

```python
# k8s_orders/app.py
import asyncio
import os

import boto3
from benzene.aws import sqs_consumer_worker
from benzene.core import WorkerHost
from benzene.http import uvicorn_worker
from benzene.kafka import build_kafka_consumer, kafka_consumer_worker

from http_orders.host import build_http_orders_app
from kafka_orders.host import build_kafka_orders_app
from sqs_orders.host import build_sqs_orders_app


def build_orders_worker_host() -> WorkerHost:
    return (
        WorkerHost()
        .add("http", uvicorn_worker(build_http_orders_app(), port=int(os.environ["PORT"])))
        .add("sqs", sqs_consumer_worker(
            build_sqs_orders_app(),
            boto3.client("sqs"),  # default credential chain - an IRSA role on EKS
            os.environ["BENZENE_SQS_CONSUME_QUEUE_URL"],
        ))
        .add("kafka", kafka_consumer_worker(
            build_kafka_orders_app(),
            build_kafka_consumer(
                bootstrap_servers=os.environ["BENZENE_KAFKA_BOOTSTRAP"],
                group_id=os.environ.get("BENZENE_KAFKA_GROUP", "orders"),
                topics=[os.environ["BENZENE_KAFKA_CONSUME_TOPIC"]],
            ),
        ))
    )


if __name__ == "__main__":
    asyncio.run(build_orders_worker_host().run())
```

`WorkerHost` guarantees four things, and you can watch all of them happen: every leg starts on the
one event loop; whichever leg finishes **first** — a clean SIGTERM or a crash — winds the others
down; `run()` waits for them all to actually finish before returning; and a crash is re-raised
afterwards, so the process still exits non-zero and Kubernetes restarts the pod. `WorkerHost` also
refuses to run with no legs at all, at start-up, rather than booting a process that handles nothing.

### What the host does for you, and how to take it back

Each `*_worker` is a closure over the transport's own public loop function, and `WorkerHost` is an
`asyncio.gather` with a shared stop flag. Nothing is hidden, and you can write it yourself — which is
what to do the moment you want different shutdown semantics:

```python
async def main() -> None:
    stop = asyncio.Event()
    server = uvicorn.Server(uvicorn.Config(http_app, host="0.0.0.0", port=8080))

    async def supervised(leg):
        try:
            await leg
        finally:                       # one leg going down never strands the others
            stop.set()
            server.should_exit = True

    try:
        await asyncio.gather(
            supervised(server.serve()),
            supervised(run_sqs_consumer_loop(
                sqs_app, sqs_client, os.environ["BENZENE_SQS_CONSUME_QUEUE_URL"],
                should_continue=lambda: not stop.is_set(),
            )),
            supervised(run_consumer_loop(
                kafka_app, kafka_consumer, should_continue=lambda: not stop.is_set(),
            )),
        )
    finally:
        kafka_consumer.close()
```

The rungs in between are public too: `benzene.http.asgi_server_worker(server)` takes a server object
you built yourself, and `run_sqs_consumer_loop` / `run_consumer_loop` are unchanged and still callable
directly — a queue-only service just awaits one of them and needs no host at all.

### Why one event loop is safe here — the trap this avoids

Python's asyncio has one event loop per process, and `uvicorn.Server.serve()`, `run_sqs_consumer_loop`
and `run_consumer_loop` genuinely run concurrently on it — **but only because the two consumer loops'
underlying `boto3`/`confluent-kafka` calls run through `asyncio.to_thread` internally** (inside
`benzene.aws.sqs_consumer`/`benzene.kafka.consumer`). Called directly on the loop, SQS's
`receive_message` (a long-poll, up to 20 seconds) would freeze uvicorn's HTTP handling for its whole
duration — and Kafka's `consumer.poll` on an idle topic is *worse*: a synchronous call in a tight loop
with no `await` between empty polls at all, so without `to_thread` it would starve uvicorn
**permanently**, not periodically, the first time the topic went quiet. You do not need to do anything
about this — it is handled inside the bindings — but it is worth knowing why the naive version of that
`gather` line is a real trap in most other asyncio + blocking-SDK combinations.

Signals: uvicorn owns SIGINT/SIGTERM natively when `server.serve()` runs on the main thread, and
`WorkerHost.run()` starts no threads of its own precisely so that stays true. On a signal uvicorn
returns, the host sets its stop signal, and both consumer loops observe it via `should_continue` on
their next iteration.

See [Kafka Setup examples](../examples/kafka_orders) for why the Kafka leg's topic travels as a
record **header** here (not the broker-level topic name) — a genuine, documented divergence from the
.NET/Go/TypeScript ports, where the Kafka topic *is* the literal broker topic name.

## 3. Containerise it

One process, one `Dockerfile`, one image — installing all three transports' extras together:

```dockerfile
# Dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY packages/benzene-results packages/benzene-results
COPY packages/benzene-core packages/benzene-core
COPY packages/benzene-http packages/benzene-http
COPY packages/benzene-aws packages/benzene-aws
COPY packages/benzene-kafka packages/benzene-kafka
RUN pip install --no-cache-dir \
      ./packages/benzene-results ./packages/benzene-core ./packages/benzene-http \
      "./packages/benzene-aws[boto3]" "./packages/benzene-kafka[kafka]" "uvicorn>=0.30"
COPY examples/orders_domain orders_domain
COPY examples/http_orders http_orders
COPY examples/sqs_orders sqs_orders
COPY examples/kafka_orders kafka_orders
COPY examples/k8s_orders k8s_orders
ENV PORT=8080
EXPOSE 8080
CMD ["python", "-m", "k8s_orders.app"]
```

```bash
docker build -f Dockerfile -t k8s-orders:local .
kind load docker-image k8s-orders:local
```

## 4. Deploy it

One `Deployment` + `Service` — the SQS and Kafka legs don't get their own, because nothing calls this
pod over either of them; it calls out:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-app
spec:
  replicas: 2
  selector: { matchLabels: { app: orders-app } }
  template:
    metadata: { labels: { app: orders-app } }
    spec:
      containers:
        - name: orders-app
          image: k8s-orders:local
          ports: [{ containerPort: 8080 }]
          env:
            - { name: PORT, value: "8080" }
            - { name: BENZENE_ORDERS_EVENTS_URL, value: "http://downstream" }
            - { name: BENZENE_SQS_CONSUME_QUEUE_URL, value: "https://sqs.eu-west-1.amazonaws.com/<account-id>/orders-in" }
            - { name: BENZENE_SQS_EVENTS_QUEUE_URL, value: "https://sqs.eu-west-1.amazonaws.com/<account-id>/orders-events" }
            - { name: BENZENE_KAFKA_BOOTSTRAP, value: "kafka-bootstrap.kafka.svc.cluster.local:9092" }
            - { name: BENZENE_KAFKA_CONSUME_TOPIC, value: "orders-in" }
            - { name: BENZENE_KAFKA_TOPIC, value: "orders-events" }
          readinessProbe: { tcpSocket: { port: 8080 }, initialDelaySeconds: 3 }
---
apiVersion: v1
kind: Service
metadata:
  name: orders-app
spec:
  selector: { app: orders-app }
  ports: [{ port: 80, targetPort: 8080 }]
```

```bash
kubectl apply -f k8s.yaml
kubectl get pods   # 2 pods: 2x orders-app
```

## 5. Watch the same domain run three ways

```bash
kubectl port-forward service/orders-app 8080:80 &
curl -XPOST localhost:8080/orders -H 'content-type: application/json' -d '{"sku":"espresso","quantity":2}'
```

Send a message to the SQS queue or the Kafka topic directly (see [the runnable
example](../examples/k8s_orders) for exact commands against a local LocalStack/Kafka pair) and the
**same handler** runs, for a request that never touched HTTP — `kubectl logs deploy/orders-app` shows
it. That's the proof: one domain, one container, three transports.

```bash
kubectl scale deploy/orders-app --replicas=4   # scales all three transports' consuming capacity together
```

## Why not just a minimal ASGI app?

See [Why not just a minimal ASGI app?](getting-started.md#why-not-just-a-minimal-asgi-app) for the
reasoning this guide exists to prove — the short version: HTTP alone doesn't need Benzene (FastAPI/
Flask/Starlette already give HTTP its own routing), but the moment a second entry point shows up — a
queue, a stream — Benzene is the one thing that lets the handler stay unmodified.

## One process, or one per transport?

This guide combines all three transports into a single process because Python's asyncio makes it
possible, once the consumer loops' blocking calls are correctly offloaded (see section 2). It is not
the *only* shape, though, and it is not always the right one. Splitting the transports into
**separate** processes/Deployments ([`http_orders`](../examples/http_orders),
[`sqs_orders`](../examples/sqs_orders), [`kafka_orders`](../examples/kafka_orders), each already
runnable standalone, each its own image) is a legitimate alternative: each transport then scales,
rolls back, and fails independently — a bad Kafka-consumer deploy, or the Kafka leg falling behind
under load, no longer risks the HTTP leg's availability the way it does when a crash or a
misbehaving event loop is shared between all three. The tradeoff is real too: more images to build,
more Deployments to manage. `orders_domain` doesn't change either way — only how many entry points
and Dockerfiles wrap it. Reach for separate Deployments when the transports' traffic, failure modes,
or scaling needs genuinely diverge; reach for one process when they don't and the operational
simplicity of a single image/Deployment is worth more than that independence.

## Next steps

- **More self-hosted workers** — [`sqs_orders`](../examples/sqs_orders) and
  [`kafka_orders`](../examples/kafka_orders) cover SQS and Kafka in depth; both are worth reaching
  for even as a service's *only* transport, since neither's raw SDK gives you routing or a middleware
  pipeline the way an ASGI framework gives HTTP.
- **The cloud hosts** — [AWS Lambda](getting-started-aws.md) and
  [Azure Functions](getting-started-azure.md) run the same domain behind a managed event source
  instead of a self-hosted poller.
- **A multi-service mesh, self-discovered on the cluster** — [`examples/k8s_mesh`](../examples/k8s_mesh)
  is a different point in this design space: three chained services plus a mesh service that discovers
  them by Kubernetes label, interrogates each in-cluster, and serves the live Mesh UI. It deploys the
  same way this guide does (`kind` in CI, or a real EKS cluster) but is not built from this guide's
  `orders_domain` — see its own README for the architecture.
