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

## Handler discovery

Listing every handler by hand is a line you can forget, and forgetting it fails at *runtime* as a
`not-found` that looks like a routing problem. Naming the package instead removes the step:

```python
from benzene.core import BenzeneMessageApplication, Registry

app = BenzeneMessageApplication(Registry.from_package("myapp.handlers"))
```

Every `@message` handler anywhere under `myapp.handlers` is registered, including in modules nothing
else imports. Re-exporting a handler (`from .orders import create_order` in a package `__init__`) is
safe — a scan de-duplicates by function identity rather than registering it twice.

**Why you must name the package.** .NET enumerates the types in a compiled assembly without running
your code; Python can only find a decorated function by *importing the module that defines it*. The
package walk imports each submodule, and that import **is** the discovery. So the rule is: a handler
is found if it lives under a package you named, and is invisible otherwise.

### Sources

Discovery is a seam, not a fixed behaviour — the counterpart of .NET's `IMessageHandlersFinder`.
Anything with a `definitions() -> list[HandlerDefinition]` method satisfies `HandlerSource`:

| Source | Produces |
|---|---|
| `PackageHandlers(*packages)` | every tagged handler under each package tree |
| `ModuleHandlers(*modules)` | tagged handlers in those modules only, no submodule walk |
| `ExplicitHandlers(*handlers)` | the functions you pass — the explicit path, as a source |
| `CompositeHandlers(*sources)` | each source in order, skipping a handler an earlier one supplied |
| `Registry` | itself — registries compose as sources |

```python
registry = Registry().add_source(
    CompositeHandlers(
        PackageHandlers("myapp.handlers"),
        ExplicitHandlers(handler_from_a_plugin),
    )
)
```

Scanning stays *sugar*: a source yields the same records you could have passed to `register()`
yourself, so explicit registration remains first-class (core-concepts §9 requires this).

### Catching a handler you forgot

If you register explicitly and want a missed one surfaced, scan the package and compare:

```python
from benzene.core import warn_unregistered_handlers

warn_unregistered_handlers(registry, "myapp.handlers")
```

It emits an `UnregisteredHandlerWarning` per tagged handler the registry will never route to, naming
the module, the function and the topic. Because it imports the package tree, it sees handlers in
modules nothing else imported — the blind spot of checking only what is already loaded.

It **warns rather than raises** deliberately: one codebase often builds several deployables, and an
orders host legitimately does not mount the payments handlers. `build_application(...,
check_packages=["myapp.handlers"])` runs the same check at the composition root.

### When a topic does not resolve

The `not-found` result says which of three unrelated bugs you have, rather than only that dispatch
failed:

| Situation | Message |
|---|---|
| Nothing registered | `the handler registry is empty … check the application's startup wiring` |
| Topic known, version not | `the topic is registered, but only for 'unversioned', '2'` |
| Topic unknown | `12 handler(s) are registered. Did you mean 'order:create'?` |

The message travels to the caller on the wire, so it gives a count and at most one close match —
not the service's full topic inventory.

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
response = await app.handle_async(
    {"topic": "order:create", "headers": {}, "body": '{"sku": "ABC"}'}
)
# {"statusCode": "created", "headers": {"content-type": "application/json"}, "body": "..."}
```

The version header is `benzene-version` (`VERSION_HEADER`). Helpers `encode_response(result)` and
`error_payload(result)` produce the response envelope and the problem-details error body
(`{"status", "detail"}`) respectively.

## Exports

`BenzeneMessageApplication`, `Container`, `Context`, `DuplicateHandlerError`, `Handler`,
`HandlerDefinition`, `Lifetime`, `Middleware`, `MiddlewarePipeline`, `Next`, `Registry`, `Scope`,
`VERSION_HEADER`, `definition_of`, `encode_response`, `error_payload`, `message`, `message_router`,
`to_jsonable`, `to_request`.

## See also

- [`benzene.results`](results.md) — the `Result` handlers return.
- [`benzene.http`](http.md) — hosting these handlers over HTTP.
