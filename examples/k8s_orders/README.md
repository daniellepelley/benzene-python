# One domain, one process, three transports

The runnable version of [Getting Started: Benzene on Kubernetes](../../docs/getting-started-kubernetes.md).

The same [`orders_domain`](../orders_domain) — already shared by [`http_orders`](../http_orders),
[`sqs_orders`](../sqs_orders), and [`kafka_orders`](../kafka_orders) individually — hosted over all
three transports from **one** running process:

```
        HTTP        ─────────┐
        SQS queue   ─────────┼──▶  orders-app (Deployment)  ──▶  orders_domain
        Kafka topic ─────────┘         (app.py)                    (OrdersStartUp)
```

Nothing in `orders_domain` knows which transport called it. That's the point: `app.py` runs uvicorn,
the SQS consumer loop, and the Kafka consumer loop together on one asyncio event loop, all
dispatching into the same composition root — a plain ASGI app alone gives you the HTTP leg; Benzene
gives you all three from one composition root, one image, one Deployment.

## Files

| Path | What it is |
|---|---|
| `app.py` | the one entry point - builds all three apps (`build_http_orders_app`/`build_sqs_orders_app`/`build_kafka_orders_app`, reused unmodified from their own examples) and runs uvicorn + both consumer loops together via `asyncio.gather` |
| `Dockerfile` | one image installing `benzene-http`+`uvicorn`, `benzene-aws[boto3]`, and `benzene-kafka[kafka]` together |
| `k8s/` | one Deployment + Service + a kustomize base, pointed at a real SQS queue pair and Kafka cluster via env vars - no bundled infra |
| `compose/` | `docker-compose.yml` - LocalStack (SQS) + a throwaway Kafka broker + the one service, for a credential-free local run |

`app.py`'s own module docstring explains the one thing worth knowing before copying this pattern:
`run_sqs_consumer_loop`/`run_consumer_loop` run their `boto3`/`confluent-kafka` calls via
`asyncio.to_thread` internally, specifically so they can share an event loop with uvicorn without
starving it — see [Getting Started: Benzene on
Kubernetes](../../docs/getting-started-kubernetes.md) for why that matters.

## Run it locally (no Kubernetes, no cloud account)

```bash
docker compose -f examples/k8s_orders/compose/docker-compose.yml up --build
```

Then, in three more terminals:

```bash
# 1. HTTP
curl -XPOST localhost:8080/orders -H 'content-type: application/json' \
     -d '{"sku":"espresso","quantity":2}'
# {"id":"...", "sku":"espresso", "quantity":2}

# 2. SQS - send straight to the queue LocalStack created, no HTTP involved. `run --rm --entrypoint aws`
# starts a fresh throwaway container on the sqs-init service's image/network/credentials.
docker compose -f examples/k8s_orders/compose/docker-compose.yml run --rm --entrypoint aws sqs-init \
  --endpoint-url=http://localstack:4566 sqs send-message \
    --queue-url http://localstack:4566/000000000000/orders-in \
    --message-body '{"sku":"latte","quantity":1}' \
    --message-attributes 'topic={StringValue=orders:place,DataType=String}'

# 3. Kafka - produce straight to the topic, no HTTP involved. The Benzene topic travels as a Kafka
# HEADER (not the broker-level topic name), so this needs kcat (or an equivalent) to set headers -
# kafka-console-producer alone can't. If you don't have kcat handy, `docker compose logs -f
# orders-app` after step 2 is enough to see the same handler dispatch from a second transport.
docker exec -i $(docker compose -f examples/k8s_orders/compose/docker-compose.yml ps -q kafka) \
  kafka-console-producer --bootstrap-server localhost:29092 --topic orders-in <<< \
  '{"sku":"filter","quantity":4}'
```

Three different entry points, one container's logs — `docker compose logs -f orders-app` — proving
an order placed through any of the three reaches the same `orders_domain` handlers.

## Deploy to Kubernetes

Build and load the one image (against a [kind](https://kind.sigs.k8s.io) cluster, as in the main
guide — swap for your registry's push/pull on a real cluster):

```bash
docker build -f examples/k8s_orders/Dockerfile -t k8s-orders:local .
kind load docker-image k8s-orders:local
```

Edit the placeholder env values in `k8s/app.yaml` to point at a real queue pair and cluster (there is
deliberately no bundled SQS/Kafka in this manifest — see the file's own comment for why, and for the
IRSA note on the SQS side), then:

```bash
kubectl apply -k examples/k8s_orders/k8s/
kubectl -n k8s-orders get pods   # 2 pods: 2x orders-app
kubectl -n k8s-orders logs -f deploy/orders-app
```

There's only one Deployment to scale - scaling it scales all three transports' consuming capacity
together:

```bash
kubectl -n k8s-orders scale deploy/orders-app --replicas=4
```

## Why this, and not just a plain ASGI app

See [Why not just a minimal ASGI app?](../../docs/getting-started.md#why-not-just-a-minimal-asgi-app)
for the reasoning this example exists to prove.

## The alternative: one Deployment per transport

Combining all three transports into one process is not the only valid shape - splitting them into
**separate** processes/Deployments ([`http_orders`](../http_orders), [`sqs_orders`](../sqs_orders),
[`kafka_orders`](../kafka_orders), each already runnable standalone, each its own image) is a
legitimate pattern too, and sometimes the better one: each transport then scales, rolls back, and
fails independently of the others. The tradeoff is real: more images to build, more Deployments to
manage. Reach for that shape instead when the transports' traffic, failure modes, or scaling needs
genuinely diverge - `orders_domain` doesn't change either way, only how many entry points and
Dockerfiles wrap it.
