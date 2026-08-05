# benzene-grpc

The **gRPC edge** of the [Benzene Python port](https://github.com/daniellepelley/benzene-python)'s
wire contract. Ships the Benzene ↔ gRPC **status mapping** (wire-contracts.md §4.2) — the foundation
of a gRPC transport binding.

Depends on [`benzene-core`](https://pypi.org/project/benzene-core/).

```bash
pip install benzene-grpc
```

```python
from benzene.grpc import to_grpc, from_grpc, BENZENE_STATUS_TRAILER

to_grpc("not-found")            # -> "NotFound"
to_grpc("created")              # -> "OK"   (all success statuses collapse to OK)
from_grpc("InvalidArgument")    # -> "bad-request"
```

Because several Benzene statuses collapse to one gRPC code, a gRPC server MUST attach a
`benzene-status` **trailer** carrying the raw status verbatim; a client, seeing it, uses it in
preference to re-deriving from the code:

```python
from_grpc("OK", trailer="created")   # -> "created"  (the trailer wins verbatim)
```

The mapping needs no `grpcio` dependency (codes are canonical name strings). The **server/client
transport** builds on it and adds the `[transport]` extra:

```bash
pip install 'benzene-grpc[transport]'
```

```python
from concurrent.futures import ThreadPoolExecutor
import grpc
from benzene.core import BenzeneMessageApplication
from benzene.grpc import add_benzene_handler, GrpcMessageSender

server = grpc.server(ThreadPoolExecutor(max_workers=8))
add_benzene_handler(server, BenzeneMessageApplication(registry))   # every topic -> a unary method
server.add_insecure_port("[::]:50051"); server.start()

sender = GrpcMessageSender(grpc.insecure_channel("localhost:50051"))   # a MessageSender
result = await sender.send_message("orders:place", {"sku": "A"})
```

The method name *is* the topic; the `benzene-status` trailer preserves the exact status across the
codes that collapse to one gRPC code. Mirrors .NET's `Benzene.Grpc`, and contributes the `benzene.grpc`
subpackage to the shared `benzene` namespace.
