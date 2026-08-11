# benzene-azure

Host [Benzene Python](https://github.com/daniellepelley/benzene-python) handlers on **Azure
Functions** — HTTP, Service Bus, Event Hub, Queue Storage, Blob Storage, Cosmos DB change feed,
Timer, and Event Grid triggers — plus Service Bus, Queue Storage, and Event Grid outbound clients.
The same handlers, no rewrite. Depends on `benzene-core` and `benzene-http`.

```bash
pip install benzene-azure          # add [servicebus,storage,eventgrid] for the real outbound clients
```

```python
from benzene.azure import AzureFunctionsApp

app = AzureFunctionsApp(http_router=router, registry=registry)
# main.py adapts azure.functions request/response types to app.handle_http(...),
# app.handle_service_bus(msg), app.handle_event_hub(events), app.handle_queue_storage(msg),
# app.handle_blob(blob), app.handle_cosmos(docs), app.handle_timer(t), app.handle_event_grid(ev).
```

- **HTTP** — via the `benzene.http` binding (route → topic, status mapping).
- **Service Bus** — one scope per message; topic from `application_properties`; failure raises so
  the message is retried / dead-lettered.
- **Event Hub** — a batch of events; one scope per event, in order; failure raises.
- **Queue Storage** — one scope per message; a Storage Queue has no attribute channel, so the topic
  is lifted from a Benzene envelope embedded in the payload, or falls back to a configurable
  `default_topic` (the "one queue → one topic" convention). Base64 payloads are auto-detected.
- **Blob Storage** — a blob-created notification; `default_topic` (`blob:created`) + the blob
  descriptor (`name` / `uri` / `metadata`) as the body.
- **Cosmos DB change feed** — a batch of changed documents; one scope per document, in order (Event
  Hub semantics); `default_topic` is `cosmos:change`, the document is the body.
- **Timer** — one tick, one invocation; `default_topic` is `timer:tick`, the schedule info is the body.
- **Event Grid** — a single event or a batch; one scope per event; **native schema and CloudEvents
  1.0** are both accepted (told apart by `specversion`), topic from `eventType` / `type`.
- **Outbound** — `ServiceBusMessageSender`, `QueueStorageMessageSender` (embeds topic + headers as a
  Benzene envelope), and `EventGridMessageSender` (native schema or CloudEvents 1.0, topic in
  `eventType` / `type`) all implement `benzene.core.MessageSender` over the optional Azure SDKs.

Test every trigger in memory with `benzene.azure.testing` (no Azure SDK) — see the runnable
[`examples/azure_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders).
Mirrors .NET's `Benzene.Azure.Function.*` and `Benzene.Clients.Azure.*`, and contributes the
`benzene.azure` subpackage to the shared `benzene` namespace.
