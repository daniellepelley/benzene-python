# `benzene.azure`

Host Benzene handlers on **Azure Functions** — HTTP, Service Bus, and Event Hub triggers — plus a
Service Bus outbound client. **Distribution: `benzene-azure` (depends on `benzene-core`,
`benzene-http`).**

```bash
pip install benzene-azure          # add [servicebus] for the real outbound client
```

## Overview

One host, one binding per trigger (transport-bindings §1):

- **HTTP** — topic from the route (via `benzene.http`); returns an `AzureHttpResponse`.
- **Service Bus** — one scope per message; topic from `application_properties`; a failure raises so
  the message is retried / dead-lettered.
- **Event Hub** — a batch of events; one scope per event, in order; a failure raises.

Topic for Service Bus/Event Hub comes from the `benzene-topic` application property.

## `AzureFunctionsApp`

```python
from benzene.azure import AzureFunctionsApp

app = AzureFunctionsApp(http_router=router, registry=registry)   # shares one pipeline
```

- `handle_http_request(method, path, query_string="", headers=None, body="")` → `AzureHttpResponse`.
- `handle_service_bus(message)` → `None` (duck-typed `get_body()` + `application_properties`).
- `handle_event_hub(events)` → `None` (a single event or a list).

The entry-point helpers `http_function(app)` / `service_bus_function(app)` / `event_hub_function(app)`
wrap an app as the plain callables an Azure Functions trigger invokes (adapting the `azure.functions`
types lazily) — mirroring `benzene.gcp.http_function` and `benzene.aws.to_lambda_handler`. The
package and its tests need no Azure SDK; see the example's
[`function_app.py`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders/function_app.py)
for the v2-model wiring.

## Outbound client

`ServiceBusMessageSender(connection_string=..., entity_name=...)` implements
`benzene.core.MessageSender` over `azure-servicebus` (a lazy, optional import), carrying the Benzene
topic + headers in the message's `application_properties`.

## Testing

`benzene.azure.testing` provides `AzureFunctionsTestHost` (`send_http`, `send_service_bus`,
`send_event_hub`, `send_event_hub_batch`) and the `service_bus_message` / `event_hub_event`
builders. See [Hosting on Azure Functions](../cookbooks/hosting-on-azure.md) and the runnable
[`examples/azure_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders).

## See also

- [`benzene.http`](http.md), [`benzene.core`](core.md), [`benzene.testing`](testing.md).
