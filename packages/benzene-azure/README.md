# benzene-azure

Host [Benzene Python](https://github.com/daniellepelley/benzene-python) handlers on **Azure
Functions** — HTTP, Service Bus, and Event Hub triggers — plus a Service Bus outbound client. The
same handlers, no rewrite. Depends on `benzene-core` and `benzene-http`.

```bash
pip install benzene-azure          # add [servicebus] for the real outbound client
```

```python
from benzene.azure import AzureFunctionsApp

app = AzureFunctionsApp(http_router=router, registry=registry)
# main.py adapts azure.functions request/response types to app.handle_http_request(...),
# app.handle_service_bus(msg), app.handle_event_hub(events).
```

- **HTTP** — via the `benzene.http` binding (route → topic, status mapping).
- **Service Bus** — one scope per message; topic from `application_properties`; failure raises so
  the message is retried / dead-lettered.
- **Event Hub** — a batch of events; one scope per event, in order; failure raises.
- **Outbound** — `ServiceBusMessageSender` implements `benzene.core.MessageSender` over
  `azure-servicebus` (optional extra), forwarding topic + headers as application properties.

Test every trigger in memory with `benzene.azure.testing` (no Azure SDK) — see the runnable
[`examples/azure_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders).
Mirrors .NET's `Benzene.Azure.Function.*`, and contributes the `benzene.azure` subpackage to the
shared `benzene` namespace.
