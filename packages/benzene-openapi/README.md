# benzene-openapi

An **OpenAPI 3.1** document derived from a [Benzene Python](https://github.com/daniellepelley/benzene-python)
handler registry — a sibling projection to the JSON Schema this port already derives and to the Cloud
Service Profile's `ServiceSpec`. Depends on `benzene-core` (schemas + registry) and `benzene-http` (the
`/benzene/invoke` base and the status mapping).

```bash
pip install benzene-openapi
```

Benzene is message-topic-based, not REST, so there is nothing to hand a Swagger UI or a client
generator out of the box. `openapi_document` closes that gap: it projects the *same* registry the spec
and mesh descriptor read into a standard OpenAPI document, reusing `benzene.core.json_schema` for every
payload schema — never re-deriving one.

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

Each registered `(topic, version)` becomes one `POST` operation under the profile's well-known invoke
base (`/benzene/invoke/{topic}`, with a `/{version}` segment for a versioned handler), and every
Benzene failure status is mapped to its HTTP code through `benzene.http.to_http`:

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
          "400": { "description": "bad-request", "content": { "application/json": {
            "schema": { "$ref": "#/components/schemas/BenzeneError" } } } }
          // ... one entry per failure status, ordered by HTTP code
        }
      }
    }
  },
  "components": {
    "schemas": {
      "BenzeneError": { "type": "object",
        "properties": { "status": { "type": "string" }, "detail": { "type": "string" } },
        "required": ["status", "detail"] },
      "OrdersPlaceRequest": { "type": "object", "properties": {
        "sku": { "type": "string" }, "quantity": { "type": "integer" } },
        "required": ["sku", "quantity"] },
      "OrdersPlaceResponse": { "type": "object", "properties": {
        "orderId": { "type": "string" } }, "required": ["orderId"] }
    }
  }
}
```

The payload schemas are exactly what `benzene.core.json_schema` emits — 2020-12 subset, wire-naming
(`orderId`) property names, `required` tracking the caller's obligation — and OpenAPI 3.1 adopts that
same dialect, so they drop in unchanged. The invoke base comes from `benzene.http.StandardPaths`; pass
`server_paths=StandardPaths(prefix="/api")` to relocate it.

Output is **deterministic**: topics are sorted by `(id, version)`, paths and component-schema keys are
sorted lexicographically, and responses are ordered by HTTP code, so the same registry always yields an
identical, diff-friendly document. Pure — no broker, no third-party package. Mirrors .NET's
`Benzene.Schema.OpenApi`, and contributes the `benzene.openapi` subpackage to the shared `benzene`
namespace.
