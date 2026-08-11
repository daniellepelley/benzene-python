# Getting Started: Benzene on Kubernetes

This guide takes you from an empty folder to **one Benzene composition root running as three
independent Kubernetes Deployments** — an HTTP API, an SQS worker, and a Kafka worker — all
dispatching into the exact same handlers. That's deliberately more than "deploy an ASGI app to a
pod": see [Why not just a minimal ASGI app?](getting-started.md#why-not-just-a-minimal-asgi-app) for
why a single-transport example wouldn't actually show what Benzene is for here.

> **Runnable version:** this guide follows [`examples/k8s_orders`](../examples/k8s_orders) —
> Dockerfiles, Kubernetes manifests, and a `docker-compose.yml` that runs all three legs locally
> against LocalStack + a throwaway Kafka broker, no cloud account needed.

## What you'll build

```
                              ┌──────────────────────────────────────┐
        HTTP  ──────────────▶│  orders-api           (Deployment)    │──┐
                              └──────────────────────────────────────┘  │
                              ┌──────────────────────────────────────┐  │   all three dispatch
        SQS queue  ─────────▶│  orders-sqs-worker    (Deployment)    │──┼──▶ orders_domain
                              └──────────────────────────────────────┘  │   (OrdersStartUp)
                              ┌──────────────────────────────────────┐  │
        Kafka topic  ───────▶│  orders-kafka-worker  (Deployment)    │──┘
                              └──────────────────────────────────────┘
```

One composition root (a `BenzeneStartUp`), mounted by three separate host scripts, each its own
container image, each its own Kubernetes Deployment, each independently replicated and scaled.

## Prerequisites

- Python 3.10+ and Docker.
- A cluster and `kubectl` — [kind](https://kind.sigs.k8s.io/) is the quickest for local work
  (`kind create cluster`).
- To follow along with real messages rather than just reading: an SQS queue and a Kafka topic
  somewhere reachable (LocalStack and a throwaway broker via `docker compose` cover both with no
  account at all — see the [runnable example](../examples/k8s_orders)).

## 1. The shared domain

Everything downstream depends on this one composition root. A domain package that any host can mount:

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
from .model import PlaceOrder

PLACE_ORDER_TOPIC = "orders:place"


class OrdersStartUp(BenzeneStartUp):
    def configure_services(self, services: Container, config: Mapping[str, str]) -> None:
        services.try_add_singleton(OrderService)

    def configure(self, services: Scope, config: Mapping[str, str]) -> AppDefinition:
        service = services.get_service(OrderService)
        sender = services.get_service(MessageSender)  # a host must register this

        router = HttpRouter()
        router.register(
            "POST", "/orders", PLACE_ORDER_TOPIC, make_place_order(service, sender),
            request_type=PlaceOrder,
        )
        return AppDefinition(registry=Registry.from_definitions(router), router=router)
```

`configure` deliberately resolves a `MessageSender` it never constructs — each host below registers
its *own* concrete sender (HTTP, SQS, Kafka) as the one thing that changes between them. Nothing here
mentions Kubernetes, SQS, Kafka, or HTTP status codes — that's the point of a message handler in
Benzene's hexagonal architecture: the domain logic sits behind a port, and a transport is just an
adapter in front of it.

## 2. Host it over HTTP

```python
# http_orders/host.py
import os

from benzene.core import Container, MessageSender, build_application
from benzene.http import BenzeneHttpApp, HttpMessageSender
from orders_domain import OrdersStartUp


def build_http_orders_app() -> BenzeneHttpApp:
    events_url = os.environ["BENZENE_ORDERS_EVENTS_URL"]

    def use_http_sender(services: Container) -> None:
        services.add_instance(MessageSender, HttpMessageSender(events_url))

    definition, _ = build_application(OrdersStartUp, overrides=[use_http_sender])
    return BenzeneHttpApp.from_definition(definition)
```

```python
# http_orders/main.py — the ASGI entry point any server hosts
from .host import build_http_orders_app

app = build_http_orders_app()
```

```bash
BENZENE_ORDERS_EVENTS_URL=http://downstream uvicorn http_orders.main:app --host 0.0.0.0 --port 8080
```

This is exactly [Getting Started](getting-started.md) — nothing here is Kubernetes-specific yet.

## 3. Host it on SQS

A second, completely independent script, sharing nothing with the HTTP host except a reference to
`orders_domain`:

```python
# sqs_orders/host.py
import os

from benzene.aws import SqsConsumerApp, SqsMessageSender, run_sqs_consumer_loop
from benzene.core import Container, MessageSender, build_application
from orders_domain import OrdersStartUp


def build_sqs_orders_app() -> SqsConsumerApp:
    events_queue_url = os.environ["BENZENE_SQS_EVENTS_QUEUE_URL"]

    def use_sqs(services: Container) -> None:
        services.add_instance(MessageSender, SqsMessageSender(events_queue_url))

    definition, _ = build_application(OrdersStartUp, overrides=[use_sqs])
    return SqsConsumerApp.from_definition(definition)


async def main() -> None:
    import boto3

    client = boto3.client("sqs")  # default credential chain - an IRSA role on EKS
    await run_sqs_consumer_loop(build_sqs_orders_app(), client, os.environ["BENZENE_SQS_CONSUME_QUEUE_URL"])


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
```

```bash
pip install "benzene-aws[boto3]"
python -m sqs_orders.host
```

`run_sqs_consumer_loop` (`benzene.aws`) is a long-running poller, not a Lambda trigger — the right
shape for a pod that stays up. It long-polls the queue, runs each message through the same handlers
the HTTP host uses, and deletes only the messages that actually succeeded (the default) — a failed or
unrouted message is left on the queue for redelivery/DLQ redrive rather than silently dropped.

## 4. Host it on Kafka

A third script, independent of the other two:

```python
# kafka_orders/host.py
import os

from benzene.core import Container, MessageSender, build_application
from benzene.kafka import KafkaConsumerApp, KafkaMessageSender, run_consumer_loop
from orders_domain import OrdersStartUp


def build_kafka_orders_app() -> KafkaConsumerApp:
    def use_kafka(services: Container) -> None:
        services.add_instance(
            MessageSender,
            KafkaMessageSender(os.environ["BENZENE_KAFKA_TOPIC"], bootstrap_servers=os.environ["BENZENE_KAFKA_BOOTSTRAP"]),
        )

    definition, _ = build_application(OrdersStartUp, overrides=[use_kafka])
    return KafkaConsumerApp.from_definition(definition)


async def main() -> None:
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": os.environ["BENZENE_KAFKA_BOOTSTRAP"],
        "group.id": os.environ.get("BENZENE_KAFKA_GROUP", "orders"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([os.environ["BENZENE_KAFKA_CONSUME_TOPIC"]])
    await run_consumer_loop(build_kafka_orders_app(), consumer)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
```

```bash
pip install "benzene-kafka[kafka]"
python -m kafka_orders.host
```

The Benzene topic travels as a Kafka **header** (not the broker-level topic name) — a producer sends
to whatever physical topic you've provisioned, with a `topic` header naming the Benzene topic
(`orders:place`), and `run_consumer_loop` reads it from there. This is a genuine divergence from the
.NET port, where the Kafka topic *is* the literal broker topic name; both are documented port
divergences, not a bug in either — see each port's own binding docs for why.

## 5. Containerise all three

Each host gets its own `Dockerfile`, built against the local `packages/` checkout (or the published
PyPI names, once these packages are published):

```dockerfile
# Api.Dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY packages/benzene-results packages/benzene-results
COPY packages/benzene-core packages/benzene-core
COPY packages/benzene-http packages/benzene-http
RUN pip install --no-cache-dir ./packages/benzene-results ./packages/benzene-core ./packages/benzene-http "uvicorn>=0.30"
COPY examples/orders_domain orders_domain
COPY examples/http_orders http_orders
ENV PORT=8080
EXPOSE 8080
CMD ["uvicorn", "http_orders.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

`SqsWorker.Dockerfile` and `KafkaWorker.Dockerfile` follow the same shape, swapping the installed
package/extra and the `CMD` for `python -m sqs_orders.host` / `python -m kafka_orders.host` — a
worker has no inbound listener, so there's no `PORT`/`EXPOSE` to set.

```bash
docker build -f Api.Dockerfile         -t orders-api:local         .
docker build -f SqsWorker.Dockerfile   -t orders-sqs-worker:local  .
docker build -f KafkaWorker.Dockerfile -t orders-kafka-worker:local .
kind load docker-image orders-api:local orders-sqs-worker:local orders-kafka-worker:local
```

## 6. Deploy all three

`orders-api` gets a `Deployment` + `Service`, same as any HTTP workload. The two workers get a
`Deployment` each and **no** `Service` — nothing calls a worker pod, it calls out:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-api
spec:
  replicas: 2
  selector: { matchLabels: { app: orders-api } }
  template:
    metadata: { labels: { app: orders-api } }
    spec:
      containers:
        - name: orders-api
          image: orders-api:local
          ports: [{ containerPort: 8080 }]
          env: [{ name: PORT, value: "8080" }, { name: BENZENE_ORDERS_EVENTS_URL, value: "http://downstream" }]
          readinessProbe: { tcpSocket: { port: 8080 }, initialDelaySeconds: 3 }
---
apiVersion: v1
kind: Service
metadata:
  name: orders-api
spec:
  selector: { app: orders-api }
  ports: [{ port: 80, targetPort: 8080 }]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-sqs-worker
spec:
  replicas: 1
  selector: { matchLabels: { app: orders-sqs-worker } }
  template:
    metadata: { labels: { app: orders-sqs-worker } }
    spec:
      containers:
        - name: orders-sqs-worker
          image: orders-sqs-worker:local
          env:
            - { name: BENZENE_SQS_CONSUME_QUEUE_URL, value: "https://sqs.eu-west-1.amazonaws.com/<account-id>/orders-in" }
            - { name: BENZENE_SQS_EVENTS_QUEUE_URL, value: "https://sqs.eu-west-1.amazonaws.com/<account-id>/orders-events" }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-kafka-worker
spec:
  replicas: 1
  selector: { matchLabels: { app: orders-kafka-worker } }
  template:
    metadata: { labels: { app: orders-kafka-worker } }
    spec:
      containers:
        - name: orders-kafka-worker
          image: orders-kafka-worker:local
          env:
            - { name: BENZENE_KAFKA_BOOTSTRAP, value: "kafka-bootstrap.kafka.svc.cluster.local:9092" }
            - { name: BENZENE_KAFKA_CONSUME_TOPIC, value: "orders-in" }
            - { name: BENZENE_KAFKA_TOPIC, value: "orders-events" }
```

```bash
kubectl apply -f k8s.yaml
kubectl get pods   # 4 pods: 2x orders-api, 1x orders-sqs-worker, 1x orders-kafka-worker
```

## 7. Watch the same domain run three ways

```bash
kubectl port-forward service/orders-api 8080:80 &
curl -XPOST localhost:8080/orders -H 'content-type: application/json' -d '{"sku":"espresso","quantity":2}'
```

Send a message to the SQS queue or the Kafka topic directly (see [the runnable
example](../examples/k8s_orders) for exact commands against a local LocalStack/Kafka pair) and the
**same handler** runs, for a request that never touched HTTP — `kubectl logs deploy/orders-sqs-worker`
shows it. That's the proof: one domain, three independently deployed, independently scaled entry
points.

```bash
kubectl scale deploy/orders-kafka-worker --replicas=3   # only the Kafka leg scales
```

## Why not just a minimal ASGI app?

See [Why not just a minimal ASGI app?](getting-started.md#why-not-just-a-minimal-asgi-app) for the
reasoning this guide exists to prove — the short version: HTTP alone doesn't need Benzene (FastAPI/
Flask/Starlette already give HTTP its own routing), but the moment a second entry point shows up — a
queue, a stream — Benzene is the one thing that lets the handler stay unmodified.

## Next steps

- **More self-hosted workers** — [`sqs_orders`](../examples/sqs_orders) and
  [`kafka_orders`](../examples/kafka_orders) cover SQS and Kafka in depth; both are worth reaching
  for even as a service's *only* transport, since neither's raw SDK gives you routing or a middleware
  pipeline the way an ASGI framework gives HTTP.
- **The cloud hosts** — [AWS Lambda](getting-started-aws.md) and
  [Azure Functions](getting-started-azure.md) run the same domain behind a managed event source
  instead of a self-hosted poller.
