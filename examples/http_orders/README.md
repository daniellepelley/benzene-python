# HTTP orders example

The shared [`orders_domain`](../orders_domain) hosted on a **standalone HTTP server** — the plain
`benzene-http` ASGI binding, no cloud runtime. This is the Python analog of the .NET `Asp` example
and the natural sibling of [`aws_orders`](../aws_orders) / [`gcp_orders`](../gcp_orders): the *same*
order handlers, mounted directly on [`BenzeneHttpApp`](../../packages/benzene-http) instead of behind
a Lambda / Cloud Functions runtime.

- **HTTP** — `POST /orders` (place an order) and `GET /orders/{id}` (fetch one).
- **Egress** — placing an order publishes `orders:created` to a downstream Benzene service over HTTP
  via an [`HttpMessageSender`](../../packages/benzene-http/benzene/http/client.py) outbound client.

Only [`host.py`](host.py) is HTTP-specific; the handlers and topics live in `orders_domain` and are
reused unchanged by the AWS, GCP, Azure, and gRPC examples.

## Run the tests (no cloud needed)

The tests dogfood `benzene.testing` — booting the app from `OrdersStartUp` and driving it through
the HTTP front door (`create_test_host(OrdersStartUp).build_http()` + `send_http`), the identical
setup to the cloud suites:

```bash
pytest examples/http_orders
```

## Run locally

`build_http_orders_app()` returns a standard ASGI app, so any ASGI server hosts it:

```bash
pip install -r requirements.txt
export BENZENE_ORDERS_EVENTS_URL="http://localhost:9000"   # the downstream Benzene service
uvicorn http_orders.main:app --port 8080
curl -X POST localhost:8080/orders -d '{"sku": "ABC", "quantity": 2}'   # -> 201 + publishes orders:created
curl localhost:8080/orders/<id>                                          # -> 200
```

The entry point is in [`main.py`](main.py). The `orders:created` subscriber that the cloud hosts
reach over Pub/Sub / SQS / SNS is delivered over a message transport in those deployments; a
standalone HTTP host is the request/response + egress front door.
