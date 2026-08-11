# `benzene.azure`

Host Benzene handlers on **Azure Functions** — HTTP, Service Bus, Event Hub, Queue Storage, Blob
Storage, Cosmos DB change feed, Timer, and Event Grid triggers — plus Service Bus, Queue Storage, and
Event Grid outbound clients. **Distribution: `benzene-azure` (depends on `benzene-core`,
`benzene-http`).**

```bash
pip install benzene-azure          # add [servicebus,storage,eventgrid] for the real outbound clients
```

## Overview

One host, one binding per trigger (transport-bindings §1):

- **HTTP** — topic from the route (via `benzene.http`); returns an `AzureHttpResponse`.
- **Service Bus** — one scope per message; topic from `application_properties`; a failure raises so
  the message is retried / dead-lettered.
- **Event Hub** — a batch of events; one scope per event, in order; a failure raises.
- **Queue Storage** — one scope per message; topic lifted from an embedded Benzene envelope (what
  `QueueStorageMessageSender` writes), else an injectable `default_topic`; base64 payloads are
  auto-detected.
- **Blob Storage** — one scope per blob-created notification; no topic of its own, so an injectable
  `default_topic` (`blob:created`); the blob descriptor is the body.
- **Cosmos DB change feed** — a batch of changed documents; one scope per document, in order (as Event
  Hub does per event).
- **Timer** — one scope per tick; injectable `default_topic` (`timer:tick`); the schedule info is the
  body.
- **Event Grid** — a single event or a batch; one scope per event; **native schema or CloudEvents 1.0**
  (told apart by the CloudEvents `specversion` attribute).

Topic for the metadata-carrying transports (Service Bus, Event Hub) comes from the `topic` application
property. The envelope-less / event-shaped triggers (Queue Storage, Blob, Cosmos, Timer, Event Grid)
have no native string→string header channel, so their topic comes from an injectable default — or, for
Queue Storage and Event Grid, is lifted from the envelope the matching outbound client embedded.

## `AzureFunctionsApp`

```python
from benzene.azure import AzureFunctionsApp

app = AzureFunctionsApp(http_router=router, registry=registry)   # shares one pipeline
```

- `handle_http(method, path, query_string="", headers=None, body="")` → `AzureHttpResponse`
  (parallel to GCP's `handle_http`; the signature carries Azure Functions' decomposed request).
- `handle_service_bus(message)` → `None` (duck-typed `get_body()` + `application_properties`).
- `handle_event_hub(events)` → `None` (a single event or a list).
- `handle_queue_storage(message, *, default_topic="")` → `None`.
- `handle_blob(blob, *, default_topic="blob:created")` → `None`.
- `handle_cosmos(documents, *, default_topic="cosmos:change")` → `None` (a batch of documents).
- `handle_timer(timer, *, default_topic="timer:tick")` → `None`.
- `handle_event_grid(event, *, default_topic="")` → `None` (a single event or a batch, either schema).

An entry-point helper wraps an app as the plain callable each trigger invokes (adapting the
`azure.functions` types lazily) — `http_function`, `service_bus_function`, `event_hub_function`,
`queue_storage_function`, `blob_function`, `cosmos_function`, `timer_function`, `event_grid_function`
— mirroring `benzene.gcp.http_function` and `benzene.aws.to_lambda_handler`. The `default_topic`-taking
triggers accept it on the helper too (e.g. `queue_storage_function(app, default_topic="orders")`). The
package and its tests need no Azure SDK; see the example's
[`function_app.py`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders/function_app.py)
for the v2-model wiring.

`benzene.azure` also exports the per-trigger decoders for custom wiring — `decode_service_bus`,
`decode_event_hub_event`, `decode_queue_storage`, `decode_blob_created`, `decode_cosmos_document`,
`decode_timer`, `decode_event_grid` — plus `is_cloud_event` (the CloudEvents-vs-native discriminator).

## Outbound clients

- `ServiceBusMessageSender(connection_string=..., entity_name=...)` — carries the Benzene topic +
  headers in the message's `application_properties` (a native channel), over `azure-servicebus`
  (`[servicebus]`).
- `QueueStorageMessageSender(queue_url=..., *, queue_name=..., connection_string=...,
  base64_encode=False)` — a Storage Queue has *no* attribute channel, so the sender serializes a
  Benzene envelope `{topic, headers, body}` (the shape `decode_queue_storage` lifts straight back).
  `base64_encode` matches the classic Storage Queue convention; the decoder auto-detects it. Over
  `azure-storage-queue` (`[storage]`).
- `EventGridMessageSender(topic_endpoint=..., *, key=..., cloud_events=False)` — publishes an Event
  Grid event (native schema by default; `cloud_events=True` switches to CloudEvents 1.0), the Benzene
  topic in `eventType` / `type`. Native schema carries the headers in a `headers` field; CloudEvents
  carries them as *extension attributes*. Over `azure-eventgrid` (`[eventgrid]`).

All three implement `benzene.core.MessageSender`, import their SDK lazily, and map a send failure to
`service-unavailable` (never raising for a domain outcome).

## Testing

`benzene.azure.testing` provides `AzureFunctionsTestHost` with one `send_*` per trigger — `send_http`,
`send_service_bus`, `send_event_hub` / `send_event_hub_batch`, `send_queue_storage` / `send_queue_text`,
`send_blob`, `send_cosmos`, `send_timer`, `send_event_grid` — and the native-input fakes behind them
(`FakeServiceBusMessage`, `FakeEventHubEvent`, `FakeQueueMessage`, `FakeBlobTrigger`,
`FakeTimerRequest`). `send_queue_text` drives the "plain text under `default_topic`" path and
`send_queue_storage` the embedded-envelope path; `send_event_grid` accepts either a native or a
CloudEvents 1.0 event. Specialize the shared harness with `create_test_host(StartUp).build_azure()`.
See [Hosting on Azure Functions](../cookbooks/hosting-on-azure.md) and the runnable
[`examples/azure_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/azure_orders).

## See also

- [`benzene.http`](http.md), [`benzene.core`](core.md), [`benzene.testing`](testing.md).
