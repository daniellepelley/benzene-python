# `benzene.openapi`

An **OpenAPI 3.1** document derived from a Benzene handler registry — a sibling projection to the JSON
Schema and Cloud Service Profile the port already derives from that same registry. **Distribution:
`benzene-openapi` (depends on `benzene-core` and `benzene-http`).**

```bash
pip install benzene-openapi
```

## Overview

Benzene is message-topic-based, not REST, so there is nothing to hand a Swagger UI or a client
generator out of the box. `openapi_document` closes that gap: it projects the *same* registry the
`/benzene/spec` document and the mesh `ServiceDescriptor` read into a standard OpenAPI document,
reusing `benzene.core.json_schema` for every payload schema — never re-deriving one.

Like `ServiceSpec` it is a **pure, deterministic** registry projection with no third-party runtime
dependencies: one `POST` operation per registered `(topic, version)` under the profile's
`/benzene/invoke` base, with every request and response schema reused verbatim from `json_schema` and
every failure status mapped to its HTTP code through the port's own `benzene.http.to_http` table.
Mirrors .NET's `Benzene.Schema.OpenApi`.

## Basic usage

```python
from dataclasses import dataclass

from benzene.core import Registry
from benzene.openapi import openapi_document


@dataclass
class PlaceOrder:
    sku: str
    quantity: int


@dataclass
class OrderPlaced:
    order_id: str


registry = Registry().register(
    "orders:place", handler, request_type=PlaceOrder, response_type=OrderPlaced
)

document = openapi_document(registry, title="Orders", version="2.1.0")
```

`document` is a plain `dict[str, Any]` — serialize it with `json.dumps`, serve it, or feed it to any
OpenAPI tool.

## `openapi_document`

```python
openapi_document(
    registry: Registry,
    *,
    title: str = "Benzene service",
    version: str = "1.0.0",
    server_paths: StandardPaths | None = None,
) -> dict[str, Any]
```

- `registry` — the handler registry to project. Each definition's declared `request_type` /
  `response_type` supply the schemas.
- `title` / `version` — the OpenAPI `info` block.
- `server_paths` — a `benzene.http.StandardPaths` (default `StandardPaths()`, i.e. the `/benzene`
  prefix). Its `invoke_path` is the base every operation hangs under; pass
  `StandardPaths(prefix="/api")` to relocate it to `/api/invoke`.

`OPENAPI_VERSION` is the emitted spec version, `"3.1.0"` — chosen over 3.0.x because 3.1 adopts the JSON
Schema 2020-12 dialect that `json_schema` already derives, so the embedded schemas drop in unchanged
rather than being down-converted.

### The mapping

Benzene has no natural resource hierarchy to project, so the faithful anchor is the profile's
well-known invoke endpoint (`POST /benzene/invoke`), through which every transport invokes a topic
uniformly. Because an OpenAPI path holds at most one `POST` operation, **each `(topic, version)` becomes
its own sub-path** of that base:

- `POST {prefix}/invoke/{topic}` for an unversioned handler, and
- `POST {prefix}/invoke/{topic}/{version}` for a versioned one (mirroring the `{version}` route segment
  the HTTP binding already understands).

So the document reads as one browsable operation per topic, while the operations all live under the
single invoke base the wire binding actually multiplexes them through. The invoke base itself comes
from `StandardPaths` (`benzene.http`).

### Schemas are reused, never re-derived

Every operation's `requestBody` and success response `$ref` a component in `components.schemas` whose
value is exactly `benzene.core.json_schema` of the topic's request/response type — the same 2020-12
subset the spec and descriptor embed, with wire-naming (`orderId`) property names and `required`
tracking the caller's obligation. Failure responses map the Benzene failure statuses to HTTP codes
through `benzene.http.to_http` and share one `BenzeneProblem` component — the RFC 9457 problem
document this port actually emits (`benzene.core.error_payload` plus the two additions §4.1 requires
of an HTTP failure: the integer `status` equal to the code being sent, and the
`application/problem+json` media type) — ordered by HTTP code. Its `errors` array `$ref`s a
`BenzeneError` component, one structured error each (`message`, and `field`/`code` when the producer
knew them).

Note that `status` there is RFC 9457's **integer HTTP code**. The Benzene status string travels as
`benzeneStatus`: wire-contracts.md §1.3 withdrew the earlier `{status: string, detail: string}` shape
precisely because that member name collided with RFC 9457's own.

### Deterministic output

As with `ServiceSpec`, the document is built in a stable order — topics sorted by `(id, version)`,
paths and component-schema keys sorted lexicographically, responses ordered by HTTP code — so the same
registry always yields an identical, diff-friendly dict.

### Emitted document

```jsonc
{
  "openapi": "3.1.0",
  "info": { "title": "Orders", "version": "2.1.0" },
  "paths": {
    "/benzene/invoke/orders:place": {
      "post": {
        "operationId": "ordersPlace",
        "summary": "Invoke topic orders:place",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": { "$ref": "#/components/schemas/OrdersPlaceRequest" }
            }
          }
        },
        "responses": {
          "200": { "description": "ok", "content": { "application/json": {
            "schema": { "$ref": "#/components/schemas/OrdersPlaceResponse" } } } },
          "400": { "description": "bad-request", "content": { "application/problem+json": {
            "schema": { "$ref": "#/components/schemas/BenzeneProblem" } } } }
          // ... one entry per failure status, ordered by HTTP code
        }
      }
    }
  },
  "components": {
    "schemas": {
      // The RFC 9457 problem document a failure carries. `status` is RFC 9457's integer HTTP
      // code; the Benzene status travels as `benzeneStatus`.
      "BenzeneProblem": { "type": "object",
        "properties": {
          "type": { "type": "string", "format": "uri" }, "title": { "type": "string" },
          "status": { "type": "integer" }, "detail": { "type": "string" },
          "instance": { "type": "string" }, "benzeneStatus": { "type": "string" },
          "errors": { "type": "array",
            "items": { "$ref": "#/components/schemas/BenzeneError" } } },
        "required": ["benzeneStatus", "status"] },
      "BenzeneError": { "type": "object",
        "properties": { "message": { "type": "string" }, "field": { "type": "string" },
          "code": { "type": "string" } },
        "required": ["message"] },
      "OrdersPlaceRequest": { "type": "object", "properties": {
        "sku": { "type": "string" }, "quantity": { "type": "integer" } },
        "required": ["sku", "quantity"] },
      "OrdersPlaceResponse": { "type": "object", "properties": {
        "orderId": { "type": "string" } }, "required": ["orderId"] }
    }
  }
}
```

## `operation_id`

The canonical `operationId` for a `(topic, version)` — the camelCased topic, with the version suffixed:

```python
from benzene.openapi import operation_id

operation_id("orders:place")          # "ordersPlace"
operation_id("orders:place", "v2")    # "ordersPlace_v2"
```

```python
operation_id(topic: str, version: str = "") -> str
```

It is deterministic per pair. `openapi_document` uses it internally, and exposes it so a caller can
correlate a topic to the operation it will emit.

### Separator-collision disambiguation

Two topics that differ only by separator — `orders:place` versus `orders-place` — camel/PascalCase to
the *same* base name, which would silently overwrite a component schema and emit a duplicate
`operationId` (OpenAPI requires uniqueness). The raw-topic **path** still distinguishes them, so
`openapi_document` disambiguates the derived *names* against those already emitted: because definitions
are walked in sorted `(id, version)` order, the first claimant keeps the clean name (`ordersPlace`,
`OrdersPlaceRequest`) and later ones gain a `_2` / `_3` suffix. The output therefore stays valid and
byte-stable regardless of separator collisions.

## Exports

`openapi_document`, `operation_id`, `OPENAPI_VERSION`.

## See also

- [`benzene.core`](core.md) — the `Registry` and `json_schema` this projects; the derived `ServiceSpec`
  it is a sibling of.
- [`benzene.http`](http.md) — `StandardPaths` (the invoke base) and `to_http` (the failure-status → HTTP
  code mapping) this reuses.
- [`benzene.mesh`](mesh.md) — the mesh `ServiceDescriptor`, the third projection of the same registry.
