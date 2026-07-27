# `benzene.testing`

In-memory test host and test doubles for testing Benzene apps without a cloud. **Distribution:
`benzene-testing` (depends on `benzene-core`; a dev/test dependency).**

```bash
pip install benzene-testing
```

## Overview

Drive your handlers through the **real** pipeline in memory, faking only the external edges. This is
the transport-neutral core of the port's testing story (mirroring .NET's `Benzene.Testing`); each
transport package adds native event builders on top (`benzene.gcp.testing`, `benzene.aws.testing`,
`benzene.azure.testing`).

## End-to-end harness: `create_test_host(StartUp)`

Test a **real service, booted from its own `BenzeneStartUp`**, pushing a message in through the
front door and asserting on the response and on what the service published — swapping any dependency
for a fake. The setup is identical across clouds; only the `build_<cloud>()` call and the `send_*`
shape change:

```python
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host

fake = FakeMessageSender()

def overrides(services):
    services.add_instance(OrderService, store)     # override ANY registration...
    services.add_instance(MessageSender, fake)      # ...only the external edge is faked

host = create_test_host(OrdersStartUp).with_services(overrides).build_aws()   # or .build_gcp() / .build_azure()

response = host.send_sqs("orders:created", order)   # native event in the front door
assert response == {"batchItemFailures": []}         # assert on the transport response
assert fake.last_topic == "orders:created"           # ...and on the client's egress
```

- `create_test_host(StartUp)` → a `TestHostBuilder`; `.with_services(fn)` overrides registrations
  (last wins — the seam for fakes), `.with_config(dict)` layers configuration.
- `.build_gcp()` / `.build_aws()` / `.build_azure()` specialize to that cloud's test host (a lazy
  import of the cloud package). Everything before the `build_*` call is transport- and cloud-neutral.

The composition root itself is core (`benzene.core`): implement `BenzeneStartUp.configure_services`
(register services) and `configure` (resolve them and wire routes/topics into an `AppDefinition`);
`build_application(StartUp, overrides=..., config=...)` boots it. Hosts and tests share the same
startup, so a test exercises exactly what deploys.

## `InMemoryBenzeneHost`

```python
from benzene.testing import InMemoryBenzeneHost

host = InMemoryBenzeneHost(registry)              # or an existing BenzeneMessageApplication
response = await host.send_message("order:create", {"sku": "ABC"}, headers={"benzene-version": "2"})
assert response["statusCode"] == "created"
```

- `send_message(topic, body=None, headers=None)` — build and send a message; returns the response
  envelope.
- `send(envelope)` — send a raw `{topic, headers, body}` envelope.

## `MessageBuilder`

```python
from benzene.testing import MessageBuilder

envelope = MessageBuilder("order:create").with_header("benzene-version", "2").with_body({"sku": "ABC"}).build()
```

A dataclass body is JSON-serialized for you.

## `FakeMessageSender`

A `benzene.core.MessageSender` that records outbound publishes instead of sending them — so a test
can prove ingress → handler → egress carried the payload:

```python
from benzene.testing import FakeMessageSender

sender = FakeMessageSender()                      # inject where a real client would go
...
assert sender.last_topic == "orders.created"
assert sender.last_message.id == created_id
assert len(sender.sent) == 1                      # each SentMessage has .topic/.message/.headers
```

## See also

- [Packages & adoption levels](../packages.md) — where this sits in the stack.
- The runnable [`examples/`](https://github.com/daniellepelley/benzene-python/tree/main/examples)
  dogfood these helpers.
