# gRPC orders example

The shared [`orders_domain`](../orders_domain) served over **gRPC** — the "host anywhere" proof
beyond the clouds. Every Benzene topic is one generic gRPC unary method (**method = topic**), so the
whole domain registry is served by a single `add_benzene_handler` call:

- `orders:place` — place an order (publishes `orders:created` via the outbound client).
- `orders:get` — fetch one.
- `orders:created` — the subscriber side (invoke it as a topic like any other).

The domain's `POST /orders` / `GET /orders/{id}` HTTP routes map to the `orders:place` / `orders:get`
topics here — no route table, one handler. Only [`host.py`](host.py) is gRPC-specific; the handlers
and topics live in `orders_domain` and are reused unchanged by the cloud examples.

## Run the tests (no cloud, but a real gRPC hop)

The test boots the app from `OrdersStartUp`, serves it on an in-process `grpc.Server` (ephemeral
port), and dials it with the actual `GrpcMessageSender` — a genuine gRPC round trip, faking only the
outbound edge:

```bash
pip install 'benzene-grpc[transport]'   # grpcio
pytest examples/grpc_orders
```

## Serve it

```python
from grpc_orders import build_grpc_orders_server

server, port = build_grpc_orders_server("[::]:50051")   # register a real GrpcMessageSender for egress
server.start()
server.wait_for_termination()
```

A client is then just a `MessageSender` over a channel:

```python
import grpc
from benzene.grpc import GrpcMessageSender
from orders_domain import PlaceOrder

client = GrpcMessageSender(grpc.insecure_channel("localhost:50051"))
result = await client.send_message("orders:place", PlaceOrder(sku="ABC", quantity=2))
assert result.status == "created"
```

> **Interop note.** The method path `/benzene.Benzene/<topic>` is this port's convention (see the
> [`benzene.grpc` reference](../../docs/reference/grpc.md)). Two Benzene Python services interoperate
> over gRPC out of the box; talking to a binding that uses a different method path means agreeing the
> path on both sides. The envelope, headers, status vocabulary, and `benzene-status` trailer are
> unaffected.
