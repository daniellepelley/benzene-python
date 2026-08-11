# One domain, three Kubernetes Deployments

The runnable version of [Getting Started: Benzene on Kubernetes](../../docs/getting-started-kubernetes.md).

The same [`orders_domain`](../orders_domain) — already shared by [`http_orders`](../http_orders),
[`sqs_orders`](../sqs_orders), and [`kafka_orders`](../kafka_orders) individually — packaged as three
separate container images and Kubernetes Deployments:

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

Nothing in `orders_domain` knows which pod called it. That's the point: the same business logic
scales, deploys, and rolls back independently behind whichever transport actually reaches it — a
plain ASGI app alone gives you the first Deployment; Benzene gives you all three from one composition
root.

## Files

| Path | What it is |
|---|---|
| `Api.Dockerfile` | builds `orders-api` from [`http_orders`](../http_orders) (`uvicorn`) |
| `SqsWorker.Dockerfile` | builds `orders-sqs-worker` from [`sqs_orders`](../sqs_orders) (`python -m sqs_orders.host`) |
| `KafkaWorker.Dockerfile` | builds `orders-kafka-worker` from [`kafka_orders`](../kafka_orders) (`python -m kafka_orders.host`) |
| `k8s/` | three Deployments (`api.yaml` also a Service) + a kustomize base, pointed at a real SQS queue pair and Kafka cluster via env vars - no bundled infra |
| `compose/` | `docker-compose.yml` - LocalStack (SQS) + a throwaway Kafka broker + all three services, for a credential-free local run |

Each Dockerfile installs the `benzene-*` packages from the local `packages/` checkout (this monorepo's
packages aren't on PyPI yet — see [`docs/publishing.md`](../../docs/publishing.md)); once they are,
each `RUN pip install ...` line collapses to the plain published package names.

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
# orders-sqs-worker` after step 2 is enough to see the same handler dispatch from a second transport.
docker exec -i $(docker compose -f examples/k8s_orders/compose/docker-compose.yml ps -q kafka) \
  kafka-console-producer --bootstrap-server localhost:29092 --topic orders-in <<< \
  '{"sku":"filter","quantity":4}'
```

`docker compose logs -f orders-api orders-sqs-worker orders-kafka-worker` to watch all three at once —
an order placed through any of the three reaches the same `orders_domain` handlers.

## Deploy to Kubernetes

Build and load the three images (against a [kind](https://kind.sigs.k8s.io) cluster, as in the main
guide — swap for your registry's push/pull on a real cluster):

```bash
docker build -f examples/k8s_orders/Api.Dockerfile         -t orders-api:local         .
docker build -f examples/k8s_orders/SqsWorker.Dockerfile   -t orders-sqs-worker:local   .
docker build -f examples/k8s_orders/KafkaWorker.Dockerfile -t orders-kafka-worker:local .
kind load docker-image orders-api:local orders-sqs-worker:local orders-kafka-worker:local
```

Edit the placeholder env values in `k8s/sqs-worker.yaml` and `k8s/kafka-worker.yaml` to point at a
real queue pair and cluster (there is deliberately no bundled SQS/Kafka in these manifests — see each
file's own comment for why, and for the IRSA note on the SQS side), then:

```bash
kubectl apply -k examples/k8s_orders/k8s/
kubectl -n k8s-orders get pods   # 4 pods: 2x orders-api, 1x orders-sqs-worker, 1x orders-kafka-worker
kubectl -n k8s-orders logs -f deploy/orders-sqs-worker
```

Scale the transports independently, because they're independent Deployments:

```bash
kubectl -n k8s-orders scale deploy/orders-kafka-worker --replicas=3
```

## Why this, and not just a plain ASGI app

See [Why not just a minimal ASGI app?](../../docs/getting-started.md#why-not-just-a-minimal-asgi-app)
for the reasoning this example exists to prove.
