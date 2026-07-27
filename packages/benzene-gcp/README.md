# benzene-gcp

Host [Benzene Python](https://github.com/daniellepelley/benzene-python) handlers on **Google Cloud
Functions** — behind an HTTP trigger and a Pub/Sub trigger, with a Pub/Sub outbound client — the
same handlers, no rewrite. Depends on `benzene-core` and `benzene-http`.

```bash
pip install benzene-gcp            # add [pubsub] for the real outbound client
```

```python
from benzene.gcp import GcpFunctionsApp, http_function, pubsub_function

app = GcpFunctionsApp(http_router=router, registry=registry)

# Functions Framework entry points:
orders_http = http_function(app)         # @functions_framework.http
orders_pubsub = pubsub_function(app)     # @functions_framework.cloud_event
```

- **HTTP trigger** — delegates to the `benzene.http` binding (route → topic, status mapping).
- **Pub/Sub trigger** — topic from the `topic` message attribute; one scope per message; a failure
  result is raised so Pub/Sub redelivers.
- **Outbound** — `PubSubMessageSender` implements the `benzene.core.MessageSender` port over
  `google-cloud-pubsub` (optional extra), forwarding headers as message attributes.

Test both triggers in memory with `benzene.gcp.testing` (native event builders + a test host) —
see the runnable `examples/gcp_orders/`. Mirrors .NET's `Benzene.GoogleCloud.Functions.*`, and
contributes the `benzene.gcp` subpackage to the shared `benzene` namespace.
