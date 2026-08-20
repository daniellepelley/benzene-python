# GCP orders example

The shared [`orders_domain`](../orders_domain) hosted on **Google Cloud Functions**, exercising
**multiple transports**:

- **HTTP** — `POST /orders` (place an order) and `GET /orders/{id}` (fetch one).
- **Pub/Sub** — a subscriber on the `orders.created` topic.
- **Egress** — placing an order publishes `orders.created` via a Pub/Sub outbound client.

Only [`host.py`](host.py) is GCP-specific; the handlers and topics live in `orders_domain` and are
reused unchanged by the AWS and Azure examples.

## Run the tests (no cloud needed)

The tests dogfood `benzene.testing` + `benzene.gcp.testing`, driving the real bindings in memory:

```bash
pytest examples/gcp_orders
```

## Run locally

```bash
pip install -r requirements.txt
export BENZENE_PUBSUB_TOPIC="projects/<project>/topics/orders"
functions-framework --target orders_http --debug
curl -X POST localhost:8080/orders -d '{"sku": "ABC", "quantity": 2}'   # -> 201 + publishes orders.created
```

> **Not on PyPI yet.** `requirements.txt` names the published `benzene-*` distributions, which
> don't resolve until the first release. Install those layers editable from the repo root first —
> `pip install -e packages/benzene-results -e packages/benzene-core -e packages/benzene-http -e 'packages/benzene-gcp[pubsub]'` — and `pip install -r requirements.txt` then finds them already
> satisfied and fetches only the third-party deps.

## Deploy

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

Entry points are in [`main.py`](main.py).
