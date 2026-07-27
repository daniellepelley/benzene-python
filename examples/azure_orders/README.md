# Azure orders example

The shared [`orders_domain`](../orders_domain) hosted on **Azure Functions**, exercising **multiple
transports**:

- **HTTP** — `POST /orders` and `GET /orders/{id}`.
- **Service Bus** — the `orders.created` subscriber.
- **Event Hub** — the same subscriber, over a batched Event Hub trigger (one scope per event).
- **Egress** — placing an order publishes `orders.created` via a Service Bus outbound client.

Only [`host.py`](host.py) is Azure-specific; the handlers and topics come from `orders_domain`.

## Run the tests (no cloud needed)

```bash
pytest examples/azure_orders
```

The tests dogfood `benzene.testing` + `benzene.azure.testing`, driving the real bindings in memory.

## Deploy (sketch)

In an isolated-worker Function App, add an HTTP trigger, a Service Bus trigger, and an Event Hub
trigger whose functions adapt the `azure.functions` inputs to `app.handle_http_request(...)`,
`app.handle_service_bus(msg)`, and `app.handle_event_hub(events)` respectively, and set the
`BENZENE_SERVICEBUS_*` settings for egress.
