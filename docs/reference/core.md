# `benzene.core`

The transport-neutral message-handling engine: handlers and the `@message` decorator, the registry,
the middleware pipeline, the per-invocation DI container, and the `BenzeneMessage` envelope entry
point. **Distribution: `benzene-core` (depends on `benzene-results`).**

```bash
pip install benzene-core
```

## Handlers and `@message`

A handler is `async def handle(request) -> Result`. The `@message` decorator tags it with a topic:

```python
from benzene.core import message
from benzene.results import Result

@message("order:create", request_type=OrderRequest, response_type=OrderCreated)
async def create_order(request: OrderRequest) -> Result:
    ...
```

| Parameter | Meaning |
|---|---|
| `topic` | the topic id the handler serves |
| `version` | payload/handler version (default `""`, the unversioned handler) |
| `request_type` | dataclass/type to build from the decoded body before calling (optional) |
| `response_type` | declared response type, for descriptors/tooling (optional) |

The decorator leaves the function an ordinary callable; registration is a separate, explicit step.

## `Registry`

Maps a `(topic, version)` pair to at most one handler. Registering the same pair twice is a
**startup** error (`DuplicateHandlerError`), not a runtime ambiguity (core-concepts §2, §9).

```python
from benzene.core import Registry

registry = (
    Registry()
    .add(create_order)                          # a @message-tagged function
    .register("order:get", get_order)           # explicit, no decorator
    .register("order:get", get_order_v2, version="2")
)

registry.find("order:get")           # unversioned handler
registry.find("order:get", "2")      # exact version match
registry.find("order:get", "9")      # None — no fuzzy fallback
```

`Registry.from_definitions(*sources)` builds a registry from anything exposing `definitions()` — an
`HttpRouter`, another `Registry`, or any `SupportsDefinitions` — collapsing the `for d in
router.definitions(): registry.add_definition(d)` bridge into one call. Chain `.register(...)` to add
topics a router doesn't carry (a queue-only subscriber):

```python
registry = Registry.from_definitions(router).register("orders:created", on_created)
```

## Middleware pipeline

Middleware is `async def mw(context, next) -> None`, run in registration order (first registered is
outermost). A middleware that does not `await next()` **short-circuits** the pipeline. The pipeline
runs exactly once per invocation; the message router (topic → handler) is ordinary middleware,
registered last (core-concepts §4).

```python
from benzene.core import Context, MiddlewarePipeline, message_router

async def timing(context: Context, next) -> None:
    # ... before ...
    await next()
    # ... after ...

pipeline = MiddlewarePipeline([timing]).use(message_router(registry))
await pipeline.handle(Context("order:create", {"sku": "ABC"}))
```

### `Context`

Carries the resolved `topic`, `version`, the native `request`, lower-cased `headers`, the
per-invocation `scope`, and a `result` slot the router fills in. Transport adapters may subclass it
to add invocation-scoped facts.

## Dependency injection

A minimal container with per-invocation scoping and **overridable defaults**: the framework
registers its defaults with `try_add*`, and an application's own `add*` wins (core-concepts §8).

```python
from benzene.core import Container, Lifetime

container = Container()
container.add_singleton(Clock, lambda scope: SystemClock())
container.try_add_scoped(UnitOfWork, lambda scope: UnitOfWork())   # only if absent

scope = container.create_scope()          # one per invocation
scope.get_service(Clock)
```

Lifetimes: `Lifetime.SINGLETON`, `SCOPED`, `TRANSIENT`. Keys are arbitrary tokens (typically a
`type` or a `str`).

> The DI container mirrors .NET's `Benzene.Dependencies`; it is folded into `benzene.core` rather
> than shipped separately (the C# split existed for assembly isolation, which Python does not need).

## The `BenzeneMessage` envelope

`BenzeneMessageApplication` is the transport-neutral entry point. It decodes a request envelope
`{topic, headers, body}`, runs the pipeline with the router last, and encodes a response envelope
`{statusCode, headers, body}`. `body` is always a pre-serialized JSON string (wire-contracts §1).

```python
from benzene.core import BenzeneMessageApplication

app = BenzeneMessageApplication(registry)     # optional: pipeline=, container=
response = await app.handle(
    {"topic": "order:create", "headers": {}, "body": '{"sku": "ABC"}'}
)
# {"statusCode": "created", "headers": {"content-type": "application/json"}, "body": "..."}
```

The message version is read inbound from the first present header in `VERSION_HEADER_NAMES` —
`benzene-version` (the canonical `VERSION_HEADER`, written outbound), then `version`, then `x-version`
(versioning.md §2) — via `resolve_version(headers)`. Helpers `encode_response(result)` and
`error_payload(result)` produce the response envelope and the problem-details error body
(`{"status", "detail"}`) respectively.

## Composition root

A `BenzeneStartUp` is the single place an app declares *what it is*, independent of where it is hosted.
Subclass it and override `configure_services(container, config)` (register the app's services) and
`configure(scope, config)` (resolve services, wire routes/topics, and return an `AppDefinition`). Every
host — and the test harness — boots from the *same* startup, so a test exercises exactly what deploys.

`AppDefinition` carries the `registry`, an optional HTTP `router`, and any `middleware` the startup
wants installed ahead of the message router — so cross-cutting concerns (mesh interception, tracing,
auth) are part of the composition root, booted identically in deployment and in tests, not wired
per-host.

```python
from benzene.core import AppDefinition, BenzeneStartUp, application_from, build_application

class OrdersStartUp(BenzeneStartUp):
    def configure_services(self, services, config):
        services.try_add_singleton(OrderService, lambda scope: OrderService())

    def configure(self, services, config) -> AppDefinition:
        registry = build_orders(services.get_service(OrderService)).registry
        return AppDefinition(registry=registry, middleware=[my_middleware])

definition, scope = build_application(OrdersStartUp)          # overrides=, config= optional
app = application_from(definition)                            # registry + middleware -> runnable app
```

`build_application(startup, *, overrides=(), config=None)` registers the startup's services, applies
any `overrides` (last-registration-wins — the seam a test uses to substitute a fake), configures the
app, and returns `(AppDefinition, Scope)`. `application_from(definition)` builds the runnable
`BenzeneMessageApplication` with the startup's middleware installed ahead of the router.

## Versioning

A message carries its version in a header (versioning.md §2). Inbound, `resolve_version` reads the
first present of `VERSION_HEADER_NAMES` — `benzene-version` (canonical), then `version`, then
`x-version` — so a peer in any language reaches the right handler; outbound, write the canonical
`benzene-version`. Over HTTP a `{version}` route segment (e.g. `/v{version}/orders/{id}`) is
authoritative when present, falling back to the headers otherwise (see [`benzene.http`](http.md)).
When a request declared a version, the response echoes it in the canonical `benzene-version` header
(wire-contracts §2.1; versioning.md §4.2, "respond in the same version"), so a consumer knows which
schema the body is — a downcast reply says so. An unversioned request gets no such header, leaving
that traffic untouched.

**Selection.** By default handlers are selected by **exact `(topic, version)`** (`exact_version`): a
message with no version signal is served by the unversioned handler (version `""`), and an unknown
version is a `not-found`. Pass a `version_selector` to opt into a different policy —
`highest_version` returns the exact match if present, else the natural-highest registered version
(`v2` before `v10`); a selector is just a `VersionSelector` callable, so you can supply your own.
(The spec's built-in .NET selector falls back to the highest version; this port defaults to
exact-only so an unknown version fails loudly rather than silently routing to a newer handler —
opt into `highest_version` for the .NET-style fallback. The same keyword works on
`message_router(registry, version_selector=...)`.)

```python
from benzene.core import BenzeneMessageApplication, highest_version

app = BenzeneMessageApplication(registry, version_selector=highest_version)   # latest-wins fallback
```

To serve several payload versions of one topic, use the **casting-handler pattern** (versioning.md
§3.1) — no framework code, just an extra registration per retired version. Keep one shared latest
implementation and register a thin forwarding handler for each old version that upcasts the request:

```python
registry.register("orders:place", place_v2, version="v2", request_type=PlaceOrderV2)

def make_place_v1(latest):
    async def place_v1(request: PlaceOrderV1) -> Result:      # v1's payload shape
        return await latest(PlaceOrderV2(sku=request.sku, quantity=request.count))  # upcast v1 -> v2
    return place_v1

registry.register("orders:place", make_place_v1(place_v2), version="v1", request_type=PlaceOrderV1)
```

A v1 client (`version: v1`, the old `count` field) and a v2 client (`benzene-version: v2`, `quantity`)
now both reach the one shared `place_v2` implementation.

**Transparent casting** (versioning.md §4) removes the per-version forwarder. Register the one-step
casts *between types* on a `SchemaCasters` — shared across topics — and `casting_handler` builds the
forwarder, upcasting the request in and downcasting the response out. Casts compose: register a step
from each version to the next and a longer hop is found by breadth-first search (a direct cast always
wins over a chain), so a new version only needs a cast from the one before it:

```python
from benzene.core import SchemaCasters, casting_handler

casters = (
    SchemaCasters()
    .cast_between(PlaceOrderV1, PlaceOrderV2, lambda v1: PlaceOrderV2(v1.sku, v1.count))   # request up
    .cast_between(OrderPlacedV2, OrderPlacedV1, lambda v2: OrderPlacedV1(v2.id))           # response down
)

registry.register("orders:place", place_v2, version="v2",
                  request_type=PlaceOrderV2, response_type=OrderPlacedV2)
registry.register("orders:place",                                    # v1 served transparently
                  casting_handler(place_v2, casters, to=PlaceOrderV2, response_to=OrderPlacedV1),
                  version="v1", request_type=PlaceOrderV1, response_type=OrderPlacedV1)
```

`place_v2` only ever sees the canonical type. A failure result (no payload) passes straight through,
and an unregistered cast raises `NoCastPathError` at call time — a loud configuration error, not a
silent mis-route.

> **Spec note (documented bend).** The .NET reference's Mechanism B (§4) serves *any* incoming version
> off a **single** registration: a request-mapper decorator reads the version and upcasts before the
> handler. That model presumes non-exact routing — an unrecognised version is accepted and cast rather
> than rejected — which is incompatible with this port's fail-loud default (an unknown version is a
> `not-found`, versioning.md §3). So the port delivers §4's substance — a shared `SchemaCasters`
> registry, shortest-path chaining, and a handler that only ever sees the canonical type — through
> **one explicit registration per served version** instead of a global mapper. Each served version
> stays a deliberate, discoverable line; `casting_handler` just collapses it to one. Casters are keyed
> by payload *type* (not by `(version, topic)` as the spec's `ISchemaCasters` is), so one registry is
> shared across every topic. The wire contract is unaffected: a v1 caller still gets a v1-shaped body.

## Health checks

A service answers the reserved `benzene:healthcheck` topic by running its registered checks (core-
concepts §). Register named checks on a `HealthChecks` and install `health_interception` before the
router; it short-circuits the reserved topic (version ignored, like the mesh endpoint):

```python
from benzene.core import HealthChecks, HealthCheckResult, MiddlewarePipeline, health_interception

checks = (
    HealthChecks()
    .add("db", lambda: HealthCheckResult.healthy())          # a bool works too
    .add("queue", check_queue)                                # sync or async callable
)
pipeline = MiddlewarePipeline().use(health_interception(checks))
```

A check returns a `HealthCheckResult` (or a bool), sync or async; a check that raises counts as
unhealthy rather than crashing the endpoint. Registering two checks under the same name is a startup
error (`DuplicateHealthCheckError`), mirroring the registry — a health endpoint must not silently drop
a check. All healthy → status `ok` with the aggregate; any unhealthy → `service-unavailable` naming
the failed checks. The aggregate

```json
{"isHealthy": true, "healthChecks": {"db": {"isHealthy": true}, "queue": {"isHealthy": true}}}
```

is exactly the shape the mesh [`Heartbeat`](mesh.md) reports, so a service runs its checks once and
feeds both the health endpoint and the heartbeat: `report = await checks.run()`.

## Service spec

A `ServiceSpec` is a service's **derived** specification — `{service, topics}` with each topic's
version and request/response JSON schema — projected from the registry, so it is always the truth of
what the service serves (never hand-maintained). It is the transport-neutral core of the profile's
`/benzene/spec` surface (Cloud Service Profile R5).

```python
from benzene.core import ServiceSpec, spec_interception, SPEC_TOPIC, MiddlewarePipeline

spec = ServiceSpec.derive(registry, service="orders")
spec.to_payload()   # {"service": "orders", "topics": [{"id": ..., "requestSchema": {...}, ...}]}

# Answer the reserved benzene:spec topic on any transport (same pattern as health/mesh interception):
pipeline = MiddlewarePipeline().use(spec_interception(spec))
```

`spec_interception(spec)` short-circuits the reserved `benzene:spec` topic (version ignored), so a
service serves its spec over gRPC or a cloud queue too; over HTTP the [`/benzene/spec`](http.md) surface
is its face. Pass a callable to `spec_interception` / `StandardPaths(spec=...)` to re-derive per request
(e.g. to reflect a degraded subsystem). The mesh [`ServiceDescriptor`](mesh.md) is a richer projection
of the same registry (adding identity, placement, and a contract hash); `ServiceSpec` is the minimal
profile document and needs only `benzene.core`. Both share one schema derivation, `json_schema`.

## Transport metadata

Every message transport that carries Benzene metadata natively (SQS/SNS attributes, Pub/Sub
attributes, Service Bus / Event Hub application properties, …) exposes a string→string channel. Each
binding turns that native channel into a plain `dict` and calls `read_message_metadata`, which resolves
the reserved **topic** key out of it and returns the rest as headers (wire-contracts §2):

```python
from benzene.core import read_message_metadata, MetadataKeys

topic, headers = read_message_metadata({"topic": "orders:place", "x-correlation-id": "c1"})
# topic == "orders:place"; headers == {"x-correlation-id": "c1"}
```

The reserved names are **a single injectable value** (`MetadataKeys`, defaults `topic` /
`benzene-version`), not a literal each binding hard-codes — the defaults carry interop, and an override
applies to inbound bindings and outbound clients alike. Keys are matched case-insensitively and
returned lower-cased; a non-reserved key never routes; `benzene-version` stays among the headers for
`resolve_version` to read. The three cloud hosts share this one resolver.

## Outbound clients

An outbound client is a `MessageSender` — `async send_message(topic, message, headers) -> Result` — the
port a handler depends on to publish, implemented per transport (`benzene.http`'s `HttpMessageSender`,
`benzene.grpc`'s `GrpcMessageSender`, the cloud packages' SNS/Pub/Sub/Service Bus clients). Cross-cutting
client behaviours are **decorators over that one interface**, so they are transport-agnostic and compose:

```python
from benzene.core import with_retry, with_correlation_id

client = with_retry(with_correlation_id(sender), attempts=5)   # wraps any MessageSender
```

- `with_retry(sender, *, attempts=3, retry_on=DEFAULT_RETRYABLE, backoff=None)` — re-sends while the
  result is a *transient* failure (`service-unavailable` / `timeout` / `too-many-requests` by default); a
  success or a real failure (a `not-found` won't get better by retrying) returns at once. `backoff` is an
  optional `async (attempt) -> None` hook.
- `with_correlation_id(sender, *, header="x-correlation-id", new_id=None)` — injects a correlation-id
  header when the caller didn't set one, so every outbound message is followable across services.

(`RetryingMessageSender` / `CorrelationIdMessageSender` are the classes the sugar returns.)

## Exports

`BenzeneMessageApplication`, `Container`, `Context`, `DuplicateHandlerError`, `Handler`,
`HandlerDefinition`, `Lifetime`, `Middleware`, `MiddlewarePipeline`, `Next`, `Registry`,
`SupportsDefinitions`, `Scope`,
`ServiceNotRegisteredError`, `AppDefinition`, `BenzeneStartUp`, `HealthChecks`, `HealthCheck`,
`HealthCheckResult`, `HealthReport`, `DuplicateHealthCheckError`, `HEALTH_TOPIC`,
`health_interception`, `VERSION_HEADER`,
`VERSION_HEADER_NAMES`, `VersionSelector`, `application_from`, `build_application`, `definition_of`,
`encode_response`, `error_payload`, `exact_version`, `highest_version`, `message`, `message_router`,
`resolve_version`, `read_message_metadata`, `MetadataKeys`, `DEFAULT_METADATA_KEYS`,
`DEFAULT_TOPIC_KEY`, `DEFAULT_VERSION_KEY`, `MessageSender`, `with_retry`, `with_correlation_id`,
`RetryingMessageSender`, `CorrelationIdMessageSender`, `DEFAULT_RETRYABLE`, `SchemaCasters`,
`casting_handler`, `Cast`, `NoCastPathError`, `ServiceSpec`, `TopicSpec`, `spec_interception`,
`SPEC_TOPIC`, `json_schema`, `Schema`, `to_jsonable`, `to_request`.

## See also

- [`benzene.results`](results.md) — the `Result` handlers return.
- [`benzene.http`](http.md) — hosting these handlers over HTTP.
