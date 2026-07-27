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
