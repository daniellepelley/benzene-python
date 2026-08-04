# `benzene.http`

The inbound **HTTP (ASGI)** transport binding: host the handlers you wrote against `benzene.core`
behind a real HTTP server. **Distribution: `benzene-http` (depends on `benzene-core`).**

```bash
pip install benzene-http
```

## Overview

`BenzeneHttpApp` is a standard ASGI application implementing the HTTP binding from the specification
(transport-bindings §2):

- **Topic** — resolved from route/method conventions via an `HttpRouter`.
- **Headers** — HTTP headers flow in and out, both directions.
- **Status** — the handler's Benzene status maps to an HTTP code via wire-contracts §4.1.
- **Scope** — one DI scope and exactly one pipeline invocation per request.
- **Failure** — unmatched route → `404`, invalid JSON body → `400`, uncaught handler error → `503`.
  The host is never crashed by request content.

## Routing

Pair `@http_endpoint(method, path)` with `@message(topic)`. The HTTP decorator says *where* a
request arrives; `@message` says *which handler* it resolves to. Stack `@http_endpoint` to give one
handler several routes.

```python
from benzene.core import message
from benzene.results import Result
from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint

@http_endpoint("GET", "/orders/{id}")
@message("order:get")
async def get_order(request: dict) -> Result:
    return Result.ok({"id": request["id"]})

app = BenzeneHttpApp(HttpRouter().add(get_order))   # run: uvicorn module:app
```

- `{name}` placeholders match a single path segment and are surfaced as request fields.
- Routes match in registration order, first match wins; the method must also match.
- Explicit registration without decorators: `HttpRouter().register("GET", "/orders/{id}", "order:get", get_order)`.

### How the request is assembled

The handler's request is the JSON body object merged with the query string and then the captured
path parameters — **path wins, then query, then body**. So `/orders/{id}` delivers `id` as a request
field even if the body also has one.

### Versioned routes

A `{version}` path segment is treated specially (versioning.md §2): it drives **handler selection**
rather than becoming a request field. Register one route with the segment and the versioned handlers
in the message registry, and `/v1/orders` and `/v2/orders` reach the `v1` and `v2` handlers:

```python
router.register("GET", "/{version}/orders/{id}", "order:get", get_order)
```

The route segment is authoritative over both the route's static version and a caller's version
header. Without a `{version}` segment, a caller's version header (any of the fallback names —
`benzene-version`, `version`, `x-version`) overrides the route's static version.

`version` is a **reserved path-parameter name**: a `{version}` segment is consumed for handler
selection and is never delivered to the handler as a request field, so don't use `{version}` for a
genuine domain field (name it e.g. `{revision}` instead). See
[versioning in `benzene.core`](core.md#versioning).

## Status mapping

`to_http(status)` and `from_http(code)` implement the wire-contracts §4.1 table:

```python
from benzene.http import to_http, from_http

to_http("ok")          # 200
to_http("created")     # 201
to_http("not-found")   # 404
from_http(422)         # "validation-error"
```

## `BenzeneHttpApp`

```python
BenzeneHttpApp(router, application=None, pipeline=None, container=None)
```

By default it builds a `BenzeneMessageApplication` from the router's handler definitions. Pass your
own `application` (or a `pipeline` / `container`) to add middleware or DI registrations.

Two ways to invoke it:

- `await app.handle(method, path, query_string="", headers=None, body="")` → an `HttpResponse`
  (`status_code`, `headers`, `body`) — convenient in tests.
- `await app(scope, receive, send)` — the raw ASGI entry point for uvicorn/hypercorn.

## Exports

`BenzeneHttpApp`, `HttpResponse`, `HttpRouter`, `HttpEndpoint`, `http_endpoint`, `routes_of`,
`to_http`, `from_http`.

## See also

- [`benzene.core`](core.md) — the handlers and pipeline this binding hosts.
- [Getting started](../getting-started.md) — a full HTTP service from scratch.
- [transport-bindings §2](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)
  — the binding contract.
