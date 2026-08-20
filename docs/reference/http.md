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
BenzeneHttpApp(router, application=None, pipeline=None, container=None, *, standard_paths=None)
```

By default it builds a `BenzeneMessageApplication` from the router's handler definitions. Pass your
own `application` (or a `pipeline` / `container`) to add middleware or DI registrations.

Two ways to invoke it:

- `await app.handle(method, path, query_string="", headers=None, body="")` → an `HttpResponse`
  (`status_code`, `headers`, `body`) — convenient in tests.
- `await app(scope, receive, send)` — the raw ASGI entry point for uvicorn/hypercorn.

## Well-known surfaces (the Cloud Service Profile)

Pass `standard_paths=StandardPaths(...)` to expose the profile's well-known operational surfaces under
a configurable `/benzene/` prefix (design-principles §5.2; profile R3/R4/R5/R7). They are served ahead
of ordinary routing and never shadow your routes.

```python
from benzene.core import HealthChecks, ServiceSpec
from benzene.http import BenzeneHttpApp, StandardPaths

app = BenzeneHttpApp(
    router,
    application=application,
    standard_paths=StandardPaths(
        health=health_checks,                              # enables GET /benzene/health
        spec=ServiceSpec.derive(registry, service="orders"),  # enables GET /benzene/spec
        # invoke is on by default -> POST /benzene/invoke
    ),
)
```

- **`POST /benzene/invoke`** (R4) — the **wire-envelope endpoint**. The request body *is* a message
  envelope (`{topic, headers, body}`); the service returns the response envelope. HTTP `200` means the
  envelope was processed — the domain outcome is the envelope's `statusCode` — and a malformed envelope
  is a transport-level `400`. This makes a service invokable uniformly across transports.
- **`GET /benzene/health`** (R3) — the `{isHealthy, healthChecks}` aggregate (wire-contracts §5), `200`
  when healthy and `503` when not. The full aggregate is returned either way (it is run directly, so an
  unhealthy report survives — the envelope drops a failure payload). Enabled by passing `health`.
  Note the two health faces differ on failure: this HTTP surface returns the full aggregate on `503`,
  while the transport-neutral `benzene:healthcheck` envelope reply carries the standard failure body —
  the RFC 9457 problem document (`{type, title, detail, benzeneStatus, errors[]}`, wire-contracts
  §1.3) — because an unhealthy `Result` is `service-unavailable` and the envelope drops its payload.
  Both name the failing checks; the HTTP surface additionally keeps the per-check breakdown.
- **`GET /benzene/spec`** (R5) — the derived [`ServiceSpec`](core.md#service-spec): `{service, topics}`
  with each topic's request/response JSON schema, projected from the registry (never hand-written).
  Enabled by passing `spec` (a `ServiceSpec` or a callable returning one).
- **Prefix** (R7) — `prefix` defaults to `/benzene` and is configurable; relocating it moves every
  surface together (the prefix is the steer, not a cage), so tell your clients the new base.

The reserved topic **`benzene:spec`** is answered on *any* transport by `spec_interception` (the same
pattern as health and mesh interception); the HTTP `/benzene/spec` surface is its HTTP face.

The three cloud hosts drive their HTTP trigger through this same `BenzeneHttpApp`, so passing
`standard_paths=` to `GcpFunctionsApp` / `AwsLambdaApp` / `AzureFunctionsApp` exposes the identical
surfaces on a Lambda, Cloud Function, or Azure Function.

## Outbound — `HttpMessageSender`

The reverse direction: a `MessageSender` that publishes a message to another Benzene service over HTTP
POST and maps the response back via `from_http` (transport-bindings §2). It forwards the Benzene
headers as HTTP headers (plus the reserved `topic`), so correlation ids and trace context propagate.

```python
from benzene.http import HttpMessageSender

sender = HttpMessageSender("https://orders.svc")          # topic -> {base}/{topic}
result = await sender.send_message("orders:place", {"sku": "A"}, headers={"x-correlation-id": "c1"})
# a 201 -> result.status == "created"; the response body becomes result.payload
```

- `HttpMessageSender(url_for, *, transport=None, topic_header="topic")` — `url_for` resolves a topic to
  a URL: a **base URL** (topic appended as a path segment), a **`{topic: url}` map**, or a **`topic ->
  url` callable**.
- `transport` is the injectable HTTP call — `async (url, headers, body) -> HttpReply` — so a test drives
  it with a fake and no network. The default (`stdlib_transport()`) uses `urllib` on a worker thread, so
  the sender needs **no extra dependency**; inject an `httpx`-backed transport for pooling in production.

## Serving it alongside another transport

`BenzeneHttpApp` is a plain ASGI app, so the ordinary way to run it is `uvicorn my_service:app` — that
stays the right answer for an HTTP-only service. For a process that serves HTTP **and** polls a queue,
the server has to run as one leg of a [`benzene.core.WorkerHost`](core.md#workerhost--running-n-transports-in-one-process):

```python
from benzene.http import asgi_server_worker, uvicorn_worker

WorkerHost().add("http", uvicorn_worker(app, port=8080, access_log=False))

# one level down: build the server yourself, then adapt it
server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8080))
WorkerHost().add("http", asgi_server_worker(server))
```

- `uvicorn_worker(app, *, host="0.0.0.0", port=8080, **uvicorn_config)` builds
  `uvicorn.Server(uvicorn.Config(...))` — `**uvicorn_config` is forwarded verbatim — and hands it to
  `asgi_server_worker`. That is all it does. Needs the optional extra:
  `pip install "benzene-http[uvicorn]"`; the error names the extra and the rung below if it is absent,
  and it is raised when the worker is built, not on the first request.
- `asgi_server_worker(server)` supervises any server exposing `await serve()` and a settable
  `should_exit` (`SupportsAsgiServing`) — uvicorn's own shutdown flag, so a sibling leg stopping ends
  `serve()`, and a signal ending `serve()` winds the siblings down.

## Exports

`BenzeneHttpApp`, `HttpResponse`, `HttpRouter`, `HttpEndpoint`, `http_endpoint`, `routes_of`,
`to_http`, `from_http`, `HttpMessageSender`, `HttpReply`, `HttpTransport`, `stdlib_transport`,
`StandardPaths`, `DEFAULT_PREFIX`, `uvicorn_worker`, `asgi_server_worker`, `SupportsAsgiServing`.

## See also

- [`benzene.core`](core.md) — the handlers and pipeline this binding hosts.
- [Getting started](../getting-started.md) — a full HTTP service from scratch.
- [transport-bindings §2](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)
  — the binding contract.
