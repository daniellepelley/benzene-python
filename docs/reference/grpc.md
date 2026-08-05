# `benzene.grpc`

The gRPC edge of the Benzene wire contract: the **Benzene ↔ gRPC status mapping** (wire-contracts.md
§4.2) and the `benzene-status` trailer rule. **Distribution: `benzene-grpc` (depends on
`benzene-core`).** This is the foundation of a gRPC transport binding; the server/client transport
over `grpcio` builds on top and is the next step.

```bash
pip install benzene-grpc
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

## Exports

`to_grpc`, `from_grpc`, `BENZENE_STATUS_TRAILER`.

## See also

- [`benzene.http`](http.md) — the analogous Benzene ↔ HTTP status mapping and the ASGI binding.
- [`benzene.results`](results.md) — the status vocabulary.
