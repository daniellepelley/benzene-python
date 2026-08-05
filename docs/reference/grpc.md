# `benzene.grpc`

The gRPC edge of the Benzene wire contract: the **Benzene ↔ gRPC status mapping** (wire-contracts.md
§4.2), the `benzene-status` trailer rule, and the **server/client transport** over `grpcio`.
**Distribution: `benzene-grpc` (depends on `benzene-core`; the transport adds the `[transport]`
extra).**

```bash
pip install benzene-grpc              # the status mapping (no grpcio)
pip install 'benzene-grpc[transport]' # + the gRPC server/client transport
```

## Status mapping

```python
from benzene.grpc import to_grpc, from_grpc, BENZENE_STATUS_TRAILER

to_grpc("not-found")          # -> "NotFound"
to_grpc("created")            # -> "OK"          (every success status collapses to OK)
to_grpc("some-extension")     # -> "Internal"    (unknown/missing failure)

from_grpc("InvalidArgument")  # -> "bad-request"
from_grpc("SomeFutureCode")   # -> "unexpected-error"
```

- **Forward** (`to_grpc`, server side): a Benzene status → a gRPC `StatusCode` name. All success
  statuses map to `OK`; each failure status maps per §4.2; an unknown/missing status maps to
  `Internal`.
- **Reverse** (`from_grpc`, client side): a gRPC `StatusCode` name → a Benzene status; an
  unrecognised code falls back to `unexpected-error`.

gRPC codes are their canonical **names** as strings (`"OK"`, `"InvalidArgument"`, …), so the mapping
needs no `grpcio` dependency — a transport binding translates them to `grpc.StatusCode` members at its
edge.

## The `benzene-status` trailer

Because several Benzene statuses collapse to one gRPC code (all success → `OK`), a gRPC server MUST
attach a `benzene-status` trailer carrying the raw status verbatim. A client, seeing the trailer, uses
it in preference to re-deriving from the code:

```python
from_grpc("OK", trailer="created")   # -> "created"   (the trailer wins verbatim)
```

`BENZENE_STATUS_TRAILER` is the trailer key (`"benzene-status"`). Pinned by
`grpc-status-mapping.json`.

## The transport (`[transport]` extra)

A Benzene gRPC service is a **generic** gRPC handler: the method name *is* the topic (the `grpc` topic
source is "method"), so one handler serves every topic. Each unary call carries the message body as its
bytes and the Benzene headers as request metadata.

```python
from concurrent.futures import ThreadPoolExecutor
import grpc
from benzene.core import BenzeneMessageApplication
from benzene.grpc import add_benzene_handler, GrpcMessageSender

# Server: serve every topic as a unary method
server = grpc.server(ThreadPoolExecutor(max_workers=8))
add_benzene_handler(server, BenzeneMessageApplication(registry))
server.add_insecure_port("[::]:50051"); server.start()

# Client: a MessageSender over a channel
sender = GrpcMessageSender(grpc.insecure_channel("localhost:50051"))
result = await sender.send_message("orders:place", {"sku": "A"}, headers={"x-correlation-id": "c1"})
```

- `add_benzene_handler(server, application)` registers a `BenzeneGrpcHandler` on a `grpc.Server`. The
  response carries the mapped `StatusCode` and — on success and failure alike — a `benzene-status`
  trailer with the raw status, so a status like `created` (which maps to gRPC `OK`) survives the round
  trip exactly.
- `GrpcMessageSender(channel)` is a `MessageSender`: it calls `/benzene.Benzene/<topic>`, forwards the
  headers as metadata, and maps the outcome back (the trailer wins verbatim, else the code is mapped).
  The blocking gRPC call runs on a worker thread, so it never blocks the event loop.
- `method_for(topic)` / `topic_for(method)` are the method-path convention if you need them directly.

> **Spec note (documented bend).** The method-path scheme `/benzene.Benzene/<topic>` is this port's
> convention, not a wire contract. The gRPC binding catalog in transport-bindings §2 is *informative*
> and describes .NET's explicit *(route → topic)* registrations; the Python port instead serves every
> topic through one generic handler, which is the idiomatic gRPC-Python shape. Two Benzene *Python*
> services interoperate over gRPC out of the box; talking gRPC to a binding that uses a different
> method path (e.g. a .NET service's `/package.Service/Method`) means agreeing the path on both sides.
> This is a binding (tier-D) detail — the envelope, headers, status vocabulary, and `benzene-status`
> trailer are unaffected.

## Exports

`to_grpc`, `from_grpc`, `BENZENE_STATUS_TRAILER`, `add_benzene_handler`, `BenzeneGrpcHandler`,
`GrpcMessageSender`, `method_for`, `topic_for`.

## See also

- [`benzene.http`](http.md) — the analogous Benzene ↔ HTTP status mapping and the ASGI binding.
- [`benzene.results`](results.md) — the status vocabulary.
