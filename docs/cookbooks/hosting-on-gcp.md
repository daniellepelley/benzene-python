# Hosting on Google Cloud Functions (HTTP + Pub/Sub)

Host one set of Benzene handlers on Google Cloud Functions behind **two triggers** — an HTTP
function and a Pub/Sub function — and publish events back out over Pub/Sub. The handlers are written
once and never change between transports.

## Prerequisites

- Python 3.10+
- `pip install benzene-gcp` (add `[pubsub]` for the real outbound client)
- A runnable reference: [`examples/gcp_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/gcp_orders)

## 1. The handlers (transport-agnostic)

Handlers that need a collaborator are built by a factory that closes over it — this is what makes
them hostable anywhere and testable with a fake:

```python
from benzene.core import Handler, MessageSender
from benzene.results import Result

def make_place_order(service, sender: MessageSender) -> Handler:
    async def place_order(request) -> Result:
        order = service.place(request.sku, request.quantity)
        await sender.send_message("orders.created", {"id": order.id, "sku": order.sku})
        return Result.created(order)
    return place_order
```

## 2. Wire routes + topics

```python
from benzene.core import Registry
from benzene.http import HttpRouter

router = HttpRouter()
router.register("POST", "/orders", "orders.place", make_place_order(service, sender))

registry = Registry()
for d in router.definitions():
    registry.add_definition(d)
registry.register("orders.created", make_on_order_created(...))   # the Pub/Sub subscriber
```

## 3. Build the host and expose entry points

```python
# main.py
from benzene.gcp import GcpFunctionsApp, http_function, pubsub_function

app = GcpFunctionsApp(http_router=router, registry=registry)
orders_http = http_function(app)
orders_pubsub = pubsub_function(app)
```

The HTTP trigger resolves the topic from the route and maps the Benzene status to an HTTP code; the
Pub/Sub trigger reads the topic from the message's `benzene-topic` attribute and raises on failure so
Pub/Sub redelivers.

## 4. Test it in memory (dogfooded, no cloud)

```python
from benzene.gcp.testing import GcpFunctionsTestHost
from benzene.testing import FakeMessageSender

sender = FakeMessageSender()
host = GcpFunctionsTestHost(build_app(sender=sender))

response = host.send_http("POST", "/orders", body={"sku": "ABC", "quantity": 2})
assert response.status_code == 201
assert sender.last_topic == "orders.created"     # ingress -> handler -> egress

host.send_pubsub("orders.created", body={"id": "ord-1", "sku": "ABC"})   # exercise the subscriber
```

## 5. Deploy

```bash
gcloud functions deploy orders-http --gen2 --runtime python312 --source . \
  --entry-point orders_http --trigger-http \
  --set-env-vars BENZENE_PUBSUB_TOPIC=projects/<project>/topics/orders

gcloud functions deploy orders-pubsub --gen2 --runtime python312 --source . \
  --entry-point orders_pubsub --trigger-topic orders
```

## See also

- [`benzene.gcp` reference](../reference/gcp.md)
- [`benzene.testing` reference](../reference/testing.md)
