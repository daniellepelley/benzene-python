# Calling other services (outbound clients + cross-cutting decorators)

A handler often needs to call *another* service. In Benzene that goes through one port — a
`MessageSender` (`async send_message(topic, message, headers) -> Result`) — so a handler never couples
to a transport, and cross-cutting concerns (retry, correlation ids, trace propagation) are **decorators
over that one interface**. The same wrapping works whether the underlying client is HTTP, gRPC, or a
cloud queue.

## 1. Pick a transport client

Every transport ships a `MessageSender`. Over HTTP:

```python
from benzene.http import HttpMessageSender

client = HttpMessageSender("https://orders.svc")          # topic -> {base}/{topic}
```

Over gRPC (`pip install 'benzene-grpc[transport]'`):

```python
import grpc
from benzene.grpc import GrpcMessageSender

client = GrpcMessageSender(grpc.insecure_channel("orders.svc:50051"))
```

…or a cloud client (`SnsMessageSender`, `PubSubMessageSender`, `ServiceBusMessageSender`). The handler
code below does not change when you swap one for another.

## 2. Add cross-cutting behaviour by wrapping

The decorators live in `benzene.core` (retry, correlation id) and `benzene.mesh` (trace propagation).
Each wraps a `MessageSender` and *is* one, so they compose — **order matters**: put
`with_correlation_id` *outside* `with_retry` so a retried request keeps one correlation id across all
its attempts.

```python
from benzene.core import with_retry, with_correlation_id
from benzene.mesh import with_trace_propagation

outbound = with_correlation_id(              # inject x-correlation-id ONCE (stable across retries)
    with_retry(                              # re-send transient failures (service-unavailable, …)
        with_trace_propagation(client),      # forward the current traceparent to the callee
        attempts=5,
    )
)
```

- **`with_correlation_id`** lets a request be followed across services without each call site
  remembering. Outermost, so every retry attempt shares the one id (an id the caller already set is
  left untouched).
- **`with_retry`** returns at once on success or a *real* failure (a `not-found` won't get better by
  retrying); pass a `backoff` hook to sleep between attempts.
- **`with_trace_propagation`** forwards the trace of the *current* invocation (set by
  `trace_middleware`), so the callee joins the same trace and the mesh collector can attribute
  invocation/error stats to the provider edge this service already declares via
  `ServiceDescriptor`'s `produces` (mesh.md §2.3/§4) — the trace never creates the edge itself.

## 3. Inject it through the composition root

Register the composed client as the `MessageSender` in your `BenzeneStartUp`, so handlers receive it and
a test can swap it for a fake — the [testing](../reference/testing.md) seam:

```python
from benzene.core import MessageSender

class OrdersStartUp(BenzeneStartUp):
    def configure_services(self, services, config):
        services.try_add_singleton(MessageSender, lambda scope: outbound)
```

A handler then just publishes — no transport, no retry logic, no header plumbing in sight:

```python
async def place_order(request: PlaceOrder) -> Result:
    order = store.place(request.sku, request.quantity)
    await sender.send_message("orders:created", OrderCreated(id=order.id, sku=order.sku))
    return Result.created(order)
```

## 4. Test it with a fake

The `FakeMessageSender` records what a handler published — assert on the egress, no network:

```python
from benzene.testing import FakeMessageSender

fake = FakeMessageSender()
host = create_test_host(OrdersStartUp).with_services(
    lambda s: s.add_instance(MessageSender, fake)
).build_aws()

host.send_http("POST", "/orders", body={"sku": "ABC"})
assert fake.last_topic == "orders:created"
```

See [transport-bindings §"Outbound clients"](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)
for the reverse-direction contract.
