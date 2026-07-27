# `benzene.gcp`

Host Benzene handlers on **Google Cloud Functions** — HTTP + Pub/Sub triggers and a Pub/Sub outbound
client. **Distribution: `benzene-gcp` (depends on `benzene-core`, `benzene-http`).**

```bash
pip install benzene-gcp            # add [pubsub] for the real outbound client
```

## Overview

One host, two triggers, the same handlers behind both (transport-bindings §1):

- **HTTP** — delegates to the `benzene.http` binding (route → topic, status mapping).
- **Pub/Sub** — topic from the `benzene-topic` message attribute; one scope per message; a failure result
  is raised so Pub/Sub redelivers (the queue-transport failure rule).

## `GcpFunctionsApp`

```python
from benzene.gcp import GcpFunctionsApp, http_function, pubsub_function

app = GcpFunctionsApp(http_router=router, registry=registry)   # shares one pipeline across triggers

orders_http = http_function(app)         # entry point for @functions_framework.http
orders_pubsub = pubsub_function(app)     # entry point for @functions_framework.cloud_event
```

- `GcpFunctionsApp(http_router=None, registry=None, application=None)` — provide a `benzene.http`
  `HttpRouter` for the HTTP trigger and/or a `benzene.core` `Registry` for the topics; both triggers
  share one `BenzeneMessageApplication`.
- `handle_http(request)` → `(body, status, headers)` (Functions-Framework/Flask shape).
- `handle_pubsub(cloud_event)` → `None`; raises on a failure result.
- `http_function(app)` / `pubsub_function(app)` return the plain callables the Functions Framework
  loads.

## Pub/Sub binding

- `decode_pubsub_message(message)` → a `{topic, headers, body}` envelope. The Benzene topic comes
  from the `benzene-topic` message attribute (`TOPIC_ATTRIBUTE`); other attributes become headers; the
  base64 `data` is the JSON body.
- `PubSubMessageSender(topic_path, publisher=None)` — the outbound client (`benzene.core.MessageSender`)
  over `google-cloud-pubsub`, publishing to `topic_path` with the Benzene topic as an attribute and
  headers forwarded as attributes. The SDK is a lazy, optional import.

## Testing

`benzene.gcp.testing` provides `GcpFunctionsTestHost` (`send_http(...)`, `send_pubsub(...)`) plus
`HttpRequestBuilder` / `PubSubEventBuilder` to drive both triggers in memory. See
[Hosting on Google Cloud Functions](../cookbooks/hosting-on-gcp.md) and the runnable
[`examples/gcp_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/gcp_orders).

## See also

- [`benzene.http`](http.md) and [`benzene.core`](core.md) — the layers this host builds on.
- [`benzene.testing`](testing.md) — the in-memory test host and fakes.
