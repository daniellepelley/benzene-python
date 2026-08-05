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

gRPC status codes are represented by their canonical **names** as strings (`"OK"`,
`"InvalidArgument"`, …), so the mapping needs no `grpcio` dependency — a transport binding translates
these to `grpc.StatusCode` members at its edge. That server/client transport is the next step; this
package is the conformance-pinned mapping it will build on (`grpc-status-mapping.json`).

Mirrors .NET's `Benzene.Grpc`, and contributes the `benzene.grpc` subpackage to the shared `benzene`
namespace.
