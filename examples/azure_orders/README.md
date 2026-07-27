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

## Deploy

The entry point is [`function_app.py`](function_app.py) (the Azure Functions v2 programming model):
an HTTP trigger, a Service Bus trigger, and an Event Hub trigger, each delegating to the
`benzene.azure` entry-point helpers. Set `BENZENE_SERVICEBUS_CONNECTION` / `BENZENE_SERVICEBUS_ENTITY`
(egress) and the trigger connection settings, then `func azure functionapp publish <app>`. Run
locally with `func start`.
