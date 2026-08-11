# Getting started: Benzene over gRPC

Take a set of transport-neutral Benzene handlers and serve them over a **gRPC server** — every
Benzene topic exposed as one generic unary method — and call other Benzene services with the matching
gRPC **client** binding. The handlers are unchanged: the same domain that runs on AWS Lambda, GCP, or
a standalone HTTP server is served here by a single `add_benzene_handler` call, with no route table to
mirror.

This guide goes from `pip install benzene-grpc[transport]` to a running gRPC server that answers
`orders:place` and `orders:get`, publishes the `orders:created` event over an outbound client, and a
client call that dials it — all exercised in-memory with no socket, plus one real-channel round trip.
It builds on the base tutorial: read [Getting started](getting-started.md) first for the handler /
`Result` / `@message` fundamentals; here we only add the gRPC binding.

> **Runnable version:** [`examples/grpc_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/grpc_orders)
> is exactly this guide — the shared
> [`orders_domain`](https://github.com/daniellepelley/benzene-python/tree/main/examples/orders_domain)
> served over gRPC, with dogfooded tests that drive the real binding in memory and one test that
> pushes a message through a live `grpc.Server` + channel. Read it alongside this page.

## Prerequisites

- **Python 3.10+**, `pip`, and a virtual environment (see [Getting started](getting-started.md)).
- Familiarity with gRPC in Python (`grpcio`, `grpc.server`, channels) — this guide focuses on where
  Benzene slots in, not on gRPC basics.

## 1. Install the package

```bash
pip install 'benzene-grpc[transport]'   # the status mapping + the gRPC server/client transport
```

The distribution is **`benzene-grpc`**. It depends on `benzene-core` (the pipeline and message
handlers), so a single install pulls in everything the mapping needs. `grpcio` is only required by the
actual server/client *transport* and is an optional extra — install the bare package if you only need
the Benzene↔gRPC status mapping:

```bash
pip install benzene-grpc                 # the status mapping only (no grpcio)
```

Without the `[transport]` extra, `add_benzene_handler` / `GrpcMessageSender` raise a clear
`ImportError` telling you to add it. The transport is importable from one module:

```python
from benzene.grpc import add_benzene_handler, GrpcMessageSender, BenzeneGrpcHandler
```

## 2. Write handlers (transport-neutral)

Business logic lives in plain `async` handlers that never see gRPC — the same handlers you'd host over
a standalone HTTP server, GCP, or AWS. In the example they live in the shared
[`orders_domain`](https://github.com/daniellepelley/benzene-python/tree/main/examples/orders_domain)
package. The shape is the one from [Getting started](getting-started.md): a factory closes over the
handler's collaborators (an order store, an outbound `MessageSender`) and returns the `async`
function.

```python
# orders_domain/handlers.py  (excerpt)
from benzene.core import Handler, MessageSender
from benzene.results import Result

from .model import ORDER_CREATED_TOPIC, Order, OrderCreated, PlaceOrder


def make_place_order(service: OrderService, sender: MessageSender) -> Handler:
    async def place_order(request: PlaceOrder) -> Result:
        if not request.sku:
            return Result.bad_request("sku is required")
        order = service.place(request.sku, request.quantity)
        await sender.send_message(ORDER_CREATED_TOPIC, OrderCreated(id=order.id, sku=order.sku))
        return Result.created(order)          # ingress -> handler -> egress

    return place_order
```

These handlers are wired onto a `Registry` (all topics) inside a single composition root — a
[`BenzeneStartUp`](reference/core.md) subclass, `OrdersStartUp`, that both deployment and tests boot
from (the [composition-root path](getting-started.md#two-ways-to-wire-a-service)). Because the gRPC binding serves every topic as a unary method (**method = topic**), the domain's
HTTP routes `POST /orders` / `GET /orders/{id}` are reached over gRPC as the topics `orders:place` /
`orders:get` — no per-route registration. Nothing in `orders_domain` mentions gRPC.

## 3. Build the gRPC server host

Only one file is gRPC-specific. It boots the shared `OrdersStartUp`, and registers the built
application on a `grpc.Server` with `add_benzene_handler`:

```python
# grpc_orders/host.py
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from benzene.core import Container, MessageSender, application_from, build_application
from benzene.grpc import add_benzene_handler
from orders_domain import ORDER_EVENTS_KEY, OrderService, OrdersStartUp


def build_grpc_orders_server(
    bind: str = "[::]:0",
    *,
    service: OrderService | None = None,
    sender: MessageSender | None = None,
    seen: list[str] | None = None,
    max_workers: int = 4,
) -> tuple[Any, int]:
    import grpc

    def overrides(services: Container) -> None:
        if service is not None:
            services.add_instance(OrderService, service)
        if seen is not None:
            services.add_instance(ORDER_EVENTS_KEY, seen)
        if sender is not None:
            services.add_instance(MessageSender, sender)

    definition, _ = build_application(OrdersStartUp, overrides=[overrides])
    server = grpc.server(ThreadPoolExecutor(max_workers=max_workers))
    add_benzene_handler(server, application_from(definition))
    port = server.add_insecure_port(bind)
    return server, port
```

Two things to notice:

- **`add_benzene_handler(server, application)`** registers a `BenzeneGrpcHandler` — a
  `grpc.GenericRpcHandler` — on the `grpc.Server`. It dispatches by method name, and the method name
  *is* the Benzene topic, so the whole registry is served by this one call. Each unary call carries
  the message body as its bytes and the Benzene headers as request metadata; the response carries the
  mapped `StatusCode` and, on success and failure alike, a `benzene-status` trailer with the raw
  status verbatim (so a status like `created`, which maps to gRPC `OK`, survives the round trip).
- The **outbound `MessageSender`** is the only edge that differs between deployment (a real
  `GrpcMessageSender` to the next hop) and tests (a fake). That single seam is what makes the tests in
  step 6 possible.

## 4. Run the server

`build_grpc_orders_server` returns `(server, port)` — the caller starts and stops it:

```python
from grpc_orders import build_grpc_orders_server

server, port = build_grpc_orders_server("[::]:50051")   # register a real GrpcMessageSender for egress
server.start()
server.wait_for_termination()
```

`bind` defaults to an ephemeral port (`[::]:0`); the resolved port is returned so a test can dial it.

## 5. Call another service with the client binding

The reverse direction is `GrpcMessageSender` — a `benzene.core.MessageSender` over a `grpc.Channel`.
It calls `/benzene.Benzene/<topic>`, forwards the Benzene headers as request metadata, and maps the
outcome back to a `Result` (the `benzene-status` trailer wins verbatim, otherwise the gRPC
`StatusCode` is mapped):

```python
import grpc
from benzene.grpc import GrpcMessageSender
from orders_domain import PlaceOrder

client = GrpcMessageSender(grpc.insecure_channel("localhost:50051"))
result = await client.send_message("orders:place", PlaceOrder(sku="ABC", quantity=2))
assert result.status == "created"
assert result.payload["sku"] == "ABC"
```

The blocking gRPC call runs on a worker thread, so `send_message` never blocks the event loop. Because
`GrpcMessageSender` is an ordinary `MessageSender`, it is exactly what you register for egress in
step 3 to make a Benzene-to-Benzene gRPC hop — two Benzene Python services interoperate out of the
box.

## 6. Test in memory (dogfooded)

Before opening a socket, drive the real binding in-memory with `create_test_host(...).build_grpc()`.
It boots your actual `OrdersStartUp` — the same construction the server performs — and returns a host
you push native unary calls into (method = topic, metadata = headers, bytes body) through a fake
`grpc.ServicerContext`, no socket. Fake **only the external edge** (the outbound client); everything
else is the real pipeline, routing, and handlers.

```python
# grpc_orders/tests/test_grpc_orders.py
import pytest

pytest.importorskip("grpc")  # the transport needs grpcio (the benzene-grpc[transport] extra)

from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, ORDER_EVENTS_KEY, OrderService, OrdersStartUp
from orders_domain.model import OrderCreated


def make_host():
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)
        services.add_instance(MessageSender, sender)      # only the external edge is faked
        services.add_instance(ORDER_EVENTS_KEY, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_grpc()
    return host, service, sender, seen
```

`.build_grpc()` is the only gRPC-specific line — swap it for `.build_aws()` / `.build_gcp()` /
`.build_azure()` and the same test runs against another host. `send_grpc` returns a
`GrpcResponse(status, payload, code, details)`: `status` is the raw Benzene status from the
`benzene-status` trailer, `payload` is the parsed body, and `code` / `details` are the
`grpc.StatusCode` and detail set on a failure (`None` on success).

**Ingress → handler → egress:**

```python
def test_place_order_creates_and_publishes():
    host, service, sender, _ = make_host()

    reply = host.send_grpc("orders:place", body={"sku": "ABC", "quantity": 2})

    assert reply.status == "created"                       # the benzene-status trailer, verbatim
    assert reply.payload["sku"] == "ABC"
    assert sender.last_topic == ORDER_CREATED_TOPIC        # the handler published on the way out
    assert isinstance(sender.last_message, OrderCreated)
    assert sender.last_message.id == reply.payload["id"]
```

**Status mapping** — a failure keeps its exact Benzene status via the trailer *and* sets the wire code:

```python
def test_get_unknown_order_maps_not_found():
    host, _, _, _ = make_host()
    reply = host.send_grpc("orders:get", body={"id": "does-not-exist"})
    assert reply.status == "not-found"                     # survives verbatim via the trailer
    assert reply.code is not None                          # the mapped grpc.StatusCode is set too
```

One test at the bottom of the suite also opens a **real** `grpc.Server` + channel to prove the
`GrpcMessageSender` client binding works over a live socket — the transport half the in-memory harness
deliberately doesn't touch:

```python
def test_real_channel_round_trip():
    import grpc
    from benzene.grpc import GrpcMessageSender
    from orders_domain import PlaceOrder
    from grpc_orders import build_grpc_orders_server

    sender = FakeMessageSender()
    server, port = build_grpc_orders_server("localhost:0", sender=sender)
    server.start()
    channel = grpc.insecure_channel(f"localhost:{port}")
    try:
        client = GrpcMessageSender(channel)
        result = asyncio.run(client.send_message("orders:place", PlaceOrder(sku="ABC", quantity=2)))
        assert result.status == "created"
        assert sender.last_topic == ORDER_CREATED_TOPIC   # egress crossed the real hop
    finally:
        channel.close()
        server.stop(grace=None)
```

Run the suite (grpcio installed via the extra):

```bash
pip install 'benzene-grpc[transport]'
pytest examples/grpc_orders
```

See the [testing reference](reference/testing.md) and the
[`benzene.grpc` reference](reference/grpc.md) for the full surface.

## 7. Status mapping and the `benzene-status` trailer

The binding maps a Benzene status to a gRPC `StatusCode` on the way out and back on the way in. Because
several Benzene statuses collapse to one gRPC code (**every success status → `OK`**), the server always
attaches a `benzene-status` **trailer** carrying the raw status verbatim, and a client uses that
trailer in preference to re-deriving from the code.

| `Result` status | gRPC `StatusCode` |
| --- | --- |
| `ok`, `created`, `accepted`, `updated`, `deleted`, `ignored` (all success) | `OK` |
| `bad-request`, `validation-error` | `InvalidArgument` |
| `unauthorized` | `Unauthenticated` |
| `forbidden` | `PermissionDenied` |
| `not-found` | `NotFound` |
| `conflict` | `AlreadyExists` |
| `not-implemented` | `Unimplemented` |
| `service-unavailable` | `Unavailable` |
| `too-many-requests` | `ResourceExhausted` |
| `timeout` | `DeadlineExceeded` |
| `unexpected-error` / anything unrecognized | `Internal` |

You can use the mapping directly — `to_grpc(status)` (server side) and `from_grpc(code, trailer=...)`
(client side) — with gRPC codes as their canonical **names** (`"OK"`, `"InvalidArgument"`, …), no
`grpcio` needed. `BENZENE_STATUS_TRAILER` is the trailer key (`"benzene-status"`).

> **Interop note (a documented bend).** The method-path scheme `/benzene.Benzene/<topic>` is this
> port's convention, not a wire contract: the Python port serves every topic through one generic
> handler (the idiomatic gRPC-Python shape), whereas .NET registers explicit *(route → topic)*
> methods. Two Benzene *Python* services interoperate over gRPC out of the box; talking to a binding
> that uses a different method path means agreeing the path on both sides. The envelope, headers,
> status vocabulary, and `benzene-status` trailer are unaffected. `method_for(topic)` /
> `topic_for(method)` expose the convention if you need it directly.

## 8. Troubleshooting

- **`ImportError: The Benzene gRPC transport requires grpcio ...`** — you installed the bare package.
  `pip install 'benzene-grpc[transport]'`.
- **A message never routes to a handler** — the topic is the gRPC method name; confirm a handler is
  registered for that topic. An unknown topic yields `not-found` (gRPC `NotFound`), with `not-found`
  preserved verbatim in the `benzene-status` trailer.
- **A client can't tell `created` from `ok`** — both map to `StatusCode.OK`; read the
  `benzene-status` trailer instead. `GrpcMessageSender` already prefers it, so `result.status` is the
  exact status.
- **Handler raised an exception** — Benzene turns an uncaught error into a `service-unavailable`
  result (gRPC `Unavailable`) rather than crashing the server; check the response body's `detail`.

## See also

- [`benzene.grpc` reference](reference/grpc.md) — the full API: the status mapping, the trailer rule,
  `add_benzene_handler`, `GrpcMessageSender`, and the test host.
- [Getting started](getting-started.md) — the handler / `Result` / routing fundamentals this guide
  builds on.
- [`benzene.testing` reference](reference/testing.md) and [`benzene.core` reference](reference/core.md).
- Specification: [transport-bindings](https://benzene.app/docs/specification/transport-bindings),
  [wire-contracts](https://benzene.app/docs/specification/wire-contracts).
</content>
</invoke>
