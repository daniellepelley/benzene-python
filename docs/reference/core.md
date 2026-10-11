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

@message("order:create", response_type=OrderCreated)
async def create_order(request: OrderRequest) -> Result:
    ...
```

| Parameter | Meaning |
|---|---|
| `topic` | the topic id the handler serves |
| `version` | payload/handler version (default `""`, the unversioned handler) |
| `default` | keyword-only; `True` declares a versioned handler its topic's default — where a message with no version goes when the topic has no unversioned handler |
| `request_type` | dataclass/type to build from the decoded body before calling — **inferred** from the handler's first-parameter annotation (`request: OrderRequest`), so you rarely pass it; give it explicitly to override, or when the parameter is unannotated or a subscripted generic (`dict[str, Any]`) |
| `response_type` | declared response type, for descriptors/tooling (optional) |

`request_type` inference is `infer_request_type`, shared by `@message`, `Registry.register`, and
`HttpRouter.register`: it reads a **concrete** first-parameter annotation and yields `None` for a
missing one, a subscripted generic, or a union (the body then passes through as the raw decoded
value). The decorator leaves the function an ordinary callable; registration is a separate, explicit
step.

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
registry.default_for("order:get")    # where a message with no version goes
```

`register(..., default=True)` (and `@message(..., default=True)`) declares a versioned handler its
topic's default. A second declared default on one topic is a `DuplicateHandlerError`; beside an
unversioned handler it has no effect, because the unversioned handler is always the default.

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
container.add_singleton(OrderService)                             # no factory: construct the type key
container.add_singleton(Clock, SystemClock)                       # a zero-arg () -> T callable
container.try_add_scoped(UnitOfWork, lambda scope: UnitOfWork())  # the full (scope) -> T form (only if absent)

scope = container.create_scope()          # one per invocation
scope.get_service(Clock)
```

A registration factory takes three forms (core-concepts §8): **omit it** to construct the `type` key
directly, pass a **zero-arg `() -> T`** callable, or pass the full **`(scope) -> T`** form when the
service needs the scope. The `try_add_*` variants accept all three the same way.

Lifetimes: `Lifetime.SINGLETON`, `SCOPED`, `TRANSIENT`. Keys are arbitrary tokens (typically a
`type` or a `str`).

`use_instance(key, instance)` is the shorthand for the commonest override a host writes — swap in one
ready-made service before the app is wired:

```python
from benzene.core import MessageSender, build_application, use_instance

definition, _ = build_application(
    OrdersStartUp, overrides=[use_instance(MessageSender, SqsMessageSender(queue_url))]
)
```

It returns exactly the closure it replaces (`lambda services: services.add_instance(key, instance)`),
so write that closure directly the moment a host needs to register more than one service. See
[Composition root](#composition-root) for how `overrides` are applied.

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

The message version is read inbound from the one `benzene-version` header (`VERSION_HEADER`,
wire-contracts §2) via `resolve_version(headers)`, and echoed outbound under the same name;
`BenzeneMessageApplication(registry, version_header="...")` renames it in one place for both
directions and every transport. Helpers `encode_response(result)` and
`error_payload(result)` produce the response envelope and the RFC 9457 problem document that is a
failure's body (`{type, title, detail, benzeneStatus, errors[]}` — see
[`benzene.results`](results.md#problem-details-on-the-wire-wire-contracts-13); RFC 9457's integer
`status` is added only by an HTTP binding). `decode_response(response)` is the inverse — a response
envelope back into a `Result` — for any transport whose reply *is* the Benzene envelope verbatim
rather than a translated status code (an in-process dispatch, a direct AWS Lambda invoke of another
Benzene function, or a bespoke caller speaking the wire envelope directly).

```python
from benzene.core import decode_response, encode_response
from benzene.results import Result

envelope = encode_response(Result.bad_request("sku is required"))
# {"statusCode": "bad-request", "isSuccessful": False, "headers": {...}, "body": "..."}
result = decode_response(envelope)
# Result(status="bad-request", payload=None, errors=(BenzeneError("sku is required"),))
```

### `isSuccessful` is the receiver's signal, not the status text

The response envelope carries `isSuccessful`, and wire-contracts §1.2 makes it **authoritative**: a
receiver MUST prefer it over anything it derives from `statusCode`. `successful_from(envelope)` is
that rule — it reads the stated member, and falls back to the status class only for a sender that
predates it (an older peer, or a port that has not adopted it).

This matters for an application-defined status, which is outside the shared vocabulary and so means
nothing to a receiver classifying by string alone. Every transport binding here reads it, which is
what makes `Result.set("cache-warm", payload, successful=True)` survive the round trip instead of
being nacked by SQS, redelivered by Pub/Sub, or answered as gRPC `Internal`. (For a status in the
vocabulary the two always agree: `Result.set` refuses to state a contradiction.)

```python
from benzene.core import successful_from

successful_from({"statusCode": "cache-warm", "isSuccessful": True})   # True  — stated
successful_from({"statusCode": "created"})                            # True  — derived (older peer)
```

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
from benzene.core import (
    AppDefinition, BenzeneStartUp, Registry, application_from, build_application,
)

class OrdersStartUp(BenzeneStartUp):
    def configure_services(self, services, config):
        services.try_add_singleton(OrderService)          # no factory: construct the type

    def configure(self, services, config) -> AppDefinition:
        registry = Registry()
        registry.register("orders:place", make_place_order(services.get_service(OrderService)))
        return AppDefinition(registry=registry, middleware=[my_middleware])

definition, scope = build_application(OrdersStartUp)          # overrides=, config= optional
app = application_from(definition)                            # registry + middleware -> runnable app
```

`build_application(startup, *, overrides=(), config=None)` registers the startup's services, applies
any `overrides` (last-registration-wins — the seam a test uses to substitute a fake), configures the
app, and returns `(AppDefinition, Scope)`. `application_from(definition)` builds the runnable
`BenzeneMessageApplication` with the startup's middleware installed ahead of the router.

## Versioning

A message carries its version in **one** header, `benzene-version` (wire-contracts §2, versioning.md
§2) — there is no fallback list, so a caller's own `version`/`x-version` header is an ordinary header
and selects nothing. Over HTTP a `{version}` route segment (e.g. `/v{version}/orders/{id}`) supplies the
version exactly as if the header had been sent, and wins over it (see [`benzene.http`](http.md)).
When a request declared a version, the response echoes it in the canonical `benzene-version` header
(wire-contracts §2.1; versioning.md §4.2, "respond in the same version"), so a consumer knows which
schema the body is — a downcast reply says so. An unversioned request gets no such header, leaving
that traffic untouched.

**Selection** (core-concepts §2). By default handlers are selected by **exact `(topic, version)`**
(`exact_version`): an unknown version is a `not-found` naming the topic and the version, never a
fallback to another version's handler. A message with no version goes to the topic's **default
handler** — the unversioned one, else the one declared `default=True`. A topic whose handlers are all
versioned with none declared default is valid but fails every unversioned message, so the router
warns about it when the application is built (`NoDefaultHandlerWarning`, a `UserWarning` you can
filter). Pass a `version_selector` to opt into a different policy — `highest_version` returns the
exact (or default) match if present, else the natural-highest registered version (`v2` before
`v10`); a selector is just a `VersionSelector` callable, so you can supply your own. The same
keyword works on `message_router(registry, version_selector=...)`. Step 2 of the spec's rule — a
handler the version can be *cast* to — has no candidates here, because this port's casting is
registration-level: `casting_handler` registers each version it serves, so the exact match finds it.

```python
from benzene.core import BenzeneMessageApplication, highest_version

app = BenzeneMessageApplication(registry, version_selector=highest_version)   # latest-wins fallback
```

To serve several payload versions of one topic, use the **casting-handler pattern** (versioning.md
§3.1) — no framework code, just an extra registration per retired version. Keep one shared latest
implementation and register a thin forwarding handler for each old version that upcasts the request:

```python
registry.register("orders:place", place_v2, version="v2")       # request_type inferred: PlaceOrderV2

def make_place_v1(latest):
    async def place_v1(request: PlaceOrderV1) -> Result:      # v1's payload shape
        return await latest(PlaceOrderV2(sku=request.sku, quantity=request.count))  # upcast v1 -> v2
    return place_v1

registry.register("orders:place", make_place_v1(place_v2), version="v1",  # inferred: PlaceOrderV1
                  default=True)                     # a message with no version gets v1
```

Each registration's `request_type` is inferred from the handler's first-parameter annotation
(`place_v2(request: PlaceOrderV2)`, `place_v1(request: PlaceOrderV1)`), so the router builds the body
into the right per-version type without repeating it. A v1 client (the old `count`
field — `benzene-version: v1`, or no version at all) and a v2 client (`benzene-version: v2`, `quantity`) now both reach the one shared `place_v2`
implementation.

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

registry.register("orders:place", place_v2, version="v2",           # request_type inferred
                  response_type=OrderPlacedV2)
registry.register("orders:place",                                    # v1 served transparently
                  casting_handler(place_v2, casters, to=PlaceOrderV2, response_to=OrderPlacedV1),
                  version="v1", request_type=PlaceOrderV1, response_type=OrderPlacedV1,
                  default=True)
```

`place_v2` only ever sees the canonical type. The transparent (`casting_handler`) registration **must**
still pass `request_type=` explicitly — the wrapper's parameter isn't annotated with the older type, so
there is nothing to infer, and the router needs to build the body into `PlaceOrderV1` *before* the
wrapper upcasts it. A failure result (no payload) passes straight through, and an unregistered cast
raises `NoCastPathError` at call time — a loud configuration error, not a silent mis-route.

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

A service answers the reserved `benzene:healthcheck` topic by running its registered checks
(core-concepts §10; wire-contracts §5). Register named checks on a `HealthChecks` and install `health_interception` before the
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
failed rather than crashing the endpoint. Each check reports a `HealthStatus` — `ok`, `warning`
(degraded but serving: reported, does not flip `isHealthy`) or `failed` — built with
`HealthCheckResult.healthy()`, `.warning()` or `.unhealthy()`, each taking an optional `detail`, and
keyword-only `type` (the kind of dependency) and `data` (a diagnostic bag written verbatim); a bool
is `ok`/`failed`. Registering two checks under the same name is a startup error
(`DuplicateHealthCheckError`), mirroring the registry — a health endpoint must not silently drop a
check. The topic always answers status `ok` once the checks ran — an unhealthy service is still a
successful health query — and the verdict is the report's `isHealthy` (wire-contracts §5); the HTTP
`/benzene/health` route maps it to `503`. The report

```json
{"isHealthy": true, "healthChecks": {"db": {"status": "ok"}, "disk": {"status": "warning", "detail": "85% full"}}}
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
service serves its spec over gRPC or a cloud queue too. The topic's request is the optional
`{"type", "format"}` of wire-contracts §7: an absent, empty or `null` body, or `{}`, answers the
**Contract Document** below (projected from `spec`, or exactly the `contract=` document you pass — give
it the same source as `StandardPaths(contract=...)` and the topic and the HTTP route answer the
identical document); `{"type": "native"}` answers this `ServiceSpec` payload, a port-extension type;
and any other type, or a format other than `json`, answers `not-implemented` — never a different
document. `select_spec_document(type, format)` is that decision, shared with the HTTP
[`/benzene/spec`](http.md) route. Pass a callable to `spec_interception` / `StandardPaths(spec=...)` to
re-derive per request (e.g. to reflect a degraded subsystem).

The reserved reachability probe **`benzene:ping`** (`PING_TOPIC`) needs no wiring: the message router
answers it `ok` with no payload on every transport, so a queue or function health check that sends it
is never a `not-found` the transport redelivers. The mesh [`ServiceDescriptor`](mesh.md) is a richer projection of the same registry
(adding identity, placement, and a contract hash); `ServiceSpec` is the minimal profile document and
needs only `benzene.core`. Both share one schema derivation, `json_schema`.

## Contract document

A `ContractDocument` is the **cross-language** projection of the same registry — the shape
`contract-document.md` specifies, which the Cloud Service Profile's R5 requires a service to serve at
`/benzene/spec` and which every language's client generator parses:

```json
{"openapi": "3.0.1", "info": {"title": "orders", "description": "", "version": "1.0.0"},
 "messageEndpoint": "/benzene/invoke", "transports": ["http", "sqs"],
 "requests": [{"topic": "orders:place", "httpMappings": [{"method": "POST", "path": "/orders"}],
               "request": {"$ref": "#/components/schemas/PlaceOrder"}, "response": {"$ref": "..."}}],
 "events": [{"topic": "order:placed", "message": {"$ref": "..."}}],
 "components": {"schemas": {"PlaceOrder": {"type": "object", "properties": {"sku": {"type": "string"}}}}}}
```

```python
from benzene.core import ContractDocument, HttpMapping

document = ContractDocument.derive(
    registry,
    service="orders",
    version="1.0.0",
    produces=outbound,                       # -> events[], same three forms ServiceSpec.derive takes
    message_endpoint="/benzene/invoke",
    transports=("http", "sqs"),
    http_mappings={("orders:place", ""): [HttpMapping("POST", "/orders")]},
)
document.to_payload()
```

- **`derive(registry, ...)`** sees the handlers' declared Python types, so each dataclass payload is
  named once in `components.schemas` and referenced by `$ref` — that is what lets a generator emit one
  named type per payload instead of one anonymous type per topic. An untyped (`dict`) handler has no
  name worth publishing, so its schema is written inline, which §2 allows.
- **`from_spec(spec, ...)`** projects an already-derived `ServiceSpec`. The types are gone by then, so
  every schema is inline and the catalogue is empty. This is what `/benzene/spec` serves when you wired
  only `spec=`; it costs a generator the payload *names*, not any of the contract.

Emission follows §1's presence column rather than whatever is convenient: `transports` is **omitted**
when empty (never `[]`), `info` writes empty strings rather than going missing, `messageEndpoint` is
absent when the service exposes none (consumers feature-detect send capability on it), and
`requests`/`events`/`components` are always present even when empty. A `version` is omitted rather than
written as `""`, and `reserved` is written only when true.

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

## Settlement on one-way bindings

A one-way binding — a queue, a topic, a stream, an event trigger — has no caller to answer, so the
result **settles** the message (transport-bindings §1, obligation 8): acknowledged (deleted, acked,
committed, the trigger returns) when the invocation ended `isSuccessful` — `ignored` and an
application-defined success included — and otherwise declined, so the transport redelivers it. A
failure, a **missing** result (a middleware that ended the pipeline without setting one), a topic no
handler serves (a `not-found`) and an uncaught error are all declined. Delivery is therefore
at-least-once, so make handlers on one-way bindings idempotent; dead-lettering is the transport's own
redrive configuration.

Every one-way host takes the same two knobs, both off by default:

```python
from benzene.core import Settlement

Settlement(at_most_once=True)            # acknowledge whatever the outcome (failures are logged)
Settlement(ignore_unmatched_topics=True) # a topic no handler serves is acknowledged as ignored
```

passed as `settlement=` to `AwsLambdaApp`, `AzureFunctionsApp`, `GcpFunctionsApp`, `SqsConsumerApp`,
`KafkaConsumerApp` and `RabbitMqConsumerApp` (and their `from_definition`). Each host records its
shape and defaults as `SETTLEMENT_DEFAULTS` — `SettlementDefaults(binding, shape, on_failure,
unmatched_topic)`, with `QUEUE` / `ONE_SHOT` / `STREAM` / `REQUEST_RESPONSE` and `REDELIVER` /
`SKIP_AND_CONTINUE` / `NOT_APPLICABLE` — which the conformance runner checks against the fixture. A
custom one-way loop gets the same rule from `dispatch_one_way(application, settlement, decode, *args)`,
which decodes, runs, and returns a `OneWayOutcome(topic, result, response, acknowledge)`.

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

## In-process transport

`benzene.core.inprocess` is a `MessageSender` that dispatches straight to a handler pipeline built
in the same runtime — no wire hop (not even loopback). It's for the modular-monolith shape: a
module that used to be a separate service, reached over SQS/SNS/HTTP, has moved into the caller's
own process, and the topic that used to leave the process now has no reason to.

```python
from benzene.core import BenzeneMessageApplication, InProcessMessageSender, Pipelines, Registry

billing = BenzeneMessageApplication(Registry().add(charge_card))
pipelines = Pipelines().add("billing", billing)

sender = InProcessMessageSender(pipelines, "billing")   # a MessageSender, usable anywhere one is
result = await sender.send_message("billing:charge", {"orderId": "o1"})
```

`Pipelines` accumulates one named pipeline per module — `.add(name, application)` raises
`DuplicatePipelineError` for a repeated name. `InProcessMessageSender(pipelines, name)` resolves
`name` **eagerly, at construction** (not deferred to first send), raising `PipelineNotFoundError`
for a typo'd or forgotten name — the natural place to catch it in this port's explicit-wiring
style, with no separate boot-time-check mechanism needed on top.

`InProcessFanOutSender(pipelines, *names)` dispatches one send to several named pipelines
concurrently — the in-monolith equivalent of one SNS topic fanning out to several subscribers.
Each target's failure (a raised exception or an unsuccessful result) is isolated: logged via
`logging.getLogger("benzene.core.inprocess")`, but it doesn't affect the other targets or the
fan-out's own always-successful return, matching what a real SNS publish returns (accepted once
published, no visibility into subscriber outcomes). Unlike the .NET and TypeScript ports, no
per-target topic is required — each named pipeline owns its own independent `Registry` (not a
process-wide singleton), so two targets can legitimately both handle the literal same topic with
zero collision. See `benzene.core.inprocess`'s module docstring for the full port-divergence
rationale.

## `WorkerHost` — running N transports in one process

A service that speaks more than one transport has to start several things that never return on their
own, make whichever one finishes first wind the others down, and still exit non-zero if one crashed.
That is framework work, so `WorkerHost` does it:

```python
from benzene.core import WorkerHost, background_worker
from benzene.aws import sqs_consumer_worker
from benzene.http import uvicorn_worker
from benzene.kafka import kafka_consumer_worker

await (
    WorkerHost()
    .add("http", uvicorn_worker(http_app, port=8080))
    .add("sqs", sqs_consumer_worker(sqs_app, sqs_client, queue_url))
    .add("kafka", kafka_consumer_worker(kafka_app, consumer))
    .run()
)
```

- `WorkerHost(*, shutdown_timeout=30.0)` — `add(name, worker)` registers one leg (returns `self`, so
  it chains); a repeated name raises `DuplicateWorkerError` there and then. `run()` starts every leg
  concurrently with a shared `StopSignal`, and returns only once **all** of them have finished; a leg
  that has not noticed the stop signal within `shutdown_timeout` is cancelled rather than left to
  hang. If any leg raised, the first such exception is re-raised after the orderly shutdown, so the
  process exits non-zero for an orchestrator to restart. `run()` with no legs raises `NoWorkersError`
  at start-up — never a process that boots healthy and handles nothing. `host.stop` is the shared
  signal, so anything can wind the whole host down.
- A **`Worker`** is just `async def worker(stop: StopSignal) -> None` — no base class. `StopSignal`
  wraps an `asyncio.Event` and adds `should_continue()`, which drops straight into the consumer
  loops' `should_continue=` parameter.
- `background_worker(start)` adapts the other shape of long-lived leg: a `while True:` loop whose
  shutdown is *cancellation* (a poller, a reporter, a refresh timer). `start` is a callable returning
  the coroutine, so nothing is scheduled until the host runs.

`WorkerHost` is a shorthand for an `asyncio.gather` over the transports' own loop functions with a
shared `asyncio.Event` threaded through their `should_continue` parameters — that explicit form is
written out in full in `benzene/core/worker.py`'s module docstring, and remains the thing to write
when you want different shutdown semantics. The loop functions themselves
(`benzene.aws.run_sqs_consumer_loop`, `benzene.kafka.run_consumer_loop`) are unchanged and still
callable directly: a queue-only service needs no host at all.

`run()` starts no threads and installs no signal handlers, deliberately — `uvicorn.Server.serve()`
owns SIGINT/SIGTERM on the main thread, and that only works if nothing takes it away. It also cannot
make a blocking SDK call safe: sharing one event loop works because the consumer loops route their
`boto3`/`confluent-kafka` calls through `asyncio.to_thread` themselves.

## Exports

`BenzeneMessageApplication`, `Container`, `Context`, `DuplicateHandlerError`, `Handler`,
`HandlerDefinition`, `Lifetime`, `Middleware`, `MiddlewarePipeline`, `Next`, `Registry`,
`SupportsDefinitions`, `Scope`,
`ServiceNotRegisteredError`, `AppDefinition`, `BenzeneStartUp`, `HealthChecks`, `HealthCheck`,
`HealthCheckResult`, `HealthReport`, `HealthStatus`, `DuplicateHealthCheckError`, `HEALTH_TOPIC`,
`health_interception`, `VERSION_HEADER`, `NoDefaultHandlerWarning`, `VersionSelector`, `application_from`, `build_application`, `definition_of`,
`decode_response`, `encode_response`, `error_payload`, `exact_version`, `highest_version`, `message`, `message_router`,
`resolve_version`, `read_message_metadata`, `MetadataKeys`, `DEFAULT_METADATA_KEYS`,
`DEFAULT_TOPIC_KEY`, `DEFAULT_VERSION_KEY`, `MessageSender`, `with_retry`, `with_correlation_id`,
`RetryingMessageSender`, `CorrelationIdMessageSender`, `DEFAULT_RETRYABLE`, `SchemaCasters`,
`casting_handler`, `Cast`, `NoCastPathError`, `ServiceSpec`, `TopicSpec`, `spec_interception`,
`select_spec_document`, `resolve_spec`, `SPEC_TYPE_BENZENE`, `SPEC_TYPE_NATIVE`, `SPEC_FORMAT_JSON`,
`PING_TOPIC`, `Settlement`, `SettlementDefaults`, `OneWayOutcome`, `dispatch_one_way`, `redelivers`,
`AT_LEAST_ONCE`, `QUEUE`, `ONE_SHOT`, `STREAM`, `REQUEST_RESPONSE`, `REDELIVER`, `SKIP_AND_CONTINUE`,
`NOT_APPLICABLE`, `DEFAULT_HEADERS_KEY`,
`ContractDocument`, `ContractRequest`, `ContractEvent`, `ContractSource`, `HttpMapping`,
`CONTRACT_OPENAPI`, `is_reserved_topic`, `resolve_contract`,
`SPEC_TOPIC`, `json_schema`, `Schema`, `to_jsonable`, `to_request`, `Pipelines`,
`InProcessMessageSender`, `InProcessFanOutSender`, `DuplicatePipelineError`, `PipelineNotFoundError`,
`use_instance`, `WorkerHost`, `StopSignal`, `Worker`, `background_worker`, `NoWorkersError`,
`DuplicateWorkerError`.

## See also

- [`benzene.results`](results.md) — the `Result` handlers return.
- [`benzene.http`](http.md) — hosting these handlers over HTTP.
