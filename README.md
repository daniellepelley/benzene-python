# benzene-python

A **Python port of [Benzene](https://github.com/daniellepelley/Benzene)** — the middleware-based,
hexagonal (ports-and-adapters) message-handler framework whose promise is *write your message
handlers once, host them anywhere*.

This port is **spec-first**: it implements the language-neutral Benzene specification
([`core-concepts`](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/core-concepts.md),
[`wire-contracts`](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/wire-contracts.md),
[`transport-bindings`](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md))
**idiomatically in Python** — it does not translate the C# API. Cross-language interop is the point:
a Python Benzene service and a .NET/Go/TypeScript one speak the same wire contract and show up in the
same mesh.

> **Status: Core + inbound HTTP binding + three cloud hosts.** Shipped as layered packages
> (`benzene-results`, `benzene-core`, `benzene-http`, the `benzene-gcp` / `benzene-aws` /
> `benzene-azure` hosts, and `benzene-testing`) that pass the language-neutral **conformance
> fixtures** (status vocabulary, HTTP status mapping, and the end-to-end envelope cases). The mesh
> module and payload versioning are not built yet.

## Layered packages — install only what you use

Benzene is delivered as a **stack of small PyPI packages**, mirroring the .NET package layering.
This is deliberate: each package is one **adoption level**, so you take on only as much of Benzene
as you need and your deployment never ships code (or transitive dependencies) it doesn't use. Every
package contributes a subpackage to the shared `benzene` [namespace](https://peps.python.org/pep-0420/),
so `pip install benzene-http` gives you `benzene.http` alongside the `benzene.core` and
`benzene.results` it pulls in.

| Install | Import | You get | Depends on | .NET analog |
|---|---|---|---|---|
| `benzene-results` | `benzene.results` | `Result` + status vocabulary — the return type of a handler | — (zero deps) | `Benzene.Results` |
| `benzene-core` | `benzene.core` | pipeline, registry, `@message`, DI, versioning, health checks, the `BenzeneMessage` envelope | `benzene-results` | `Benzene.Core*` + `Benzene.Dependencies` |
| `benzene-http` | `benzene.http` | inbound HTTP (ASGI) binding + status mapping | `benzene-core` | `Benzene.Http` |
| `benzene-grpc` | `benzene.grpc` | Benzene↔gRPC status mapping + `benzene-status` trailer rule | `benzene-core` | `Benzene.Grpc` |
| `benzene-gcp` | `benzene.gcp` | Google Cloud Functions host (HTTP + Pub/Sub + egress) | `benzene-core`, `benzene-http` | `Benzene.GoogleCloud.Functions.*` |
| `benzene-aws` | `benzene.aws` | AWS Lambda host (API Gateway + SQS + SNS + egress) | `benzene-core`, `benzene-http` | `Benzene.Aws.Lambda.*` |
| `benzene-azure` | `benzene.azure` | Azure Functions host (HTTP + Service Bus + Event Hub + egress) | `benzene-core`, `benzene-http` | `Benzene.Azure.Function.*` |
| `benzene-mesh` | `benzene.mesh` | ServiceDescriptor + `benzene:mesh` endpoint + tracing + collector feeds + a `MeshCollector` | `benzene-core` | `Benzene.Mesh.Wire` + `Benzene.Mesh.Collector` |
| `benzene-pydantic` | `benzene.pydantic` | validate handler requests with pydantic models | `benzene-core`, `pydantic` | `Benzene.FluentValidation` |
| `benzene-testing` | `benzene.testing` | in-memory test host + fakes (dev/test) | `benzene-core` | `Benzene.Testing` |

Adoption levels, bottom to top:

1. **Just results** — `pip install benzene-results`. Type a domain service's outcomes with
   `Result.ok(...)` / `Result.not_found(...)`; no pipeline, no transport, no dependencies.
2. **Run handlers** — `pip install benzene-core`. Register handlers and drive them through the
   transport-neutral `BenzeneMessage` envelope. Everything except a concrete transport.
3. **Host over HTTP** — `pip install benzene-http`. The same handlers behind a real ASGI server.

The three cloud hosts (`benzene-gcp`, `benzene-aws`, `benzene-azure`) and cross-cutting concerns like
the mesh (`benzene-mesh`) each sit on top of `benzene-core`, so the stack grows outward without ever
forcing an all-or-nothing install. See [`docs/packages.md`](docs/packages.md) for the full rationale.

## The core idea in 60 seconds

A handler is just `async def handle(request) -> Result` — it never sees the transport:

```python
from benzene.core import BenzeneMessageApplication, Registry, message
from benzene.results import Result

@message("say:hello")
async def hello(request: dict) -> Result:
    return Result.ok({"greeting": f"Hello {request['name']}"})

app = BenzeneMessageApplication(Registry().add(hello))

# Drive it with a Benzene message envelope (the transport-neutral entry point):
response = await app.handle(
    {"topic": "say:hello", "headers": {}, "body": '{"name":"world"}'}
)
# -> {"statusCode": "ok", "headers": {"content-type": "application/json"},
#     "body": '{"greeting": "Hello world"}'}
```

Add `benzene-http` and the same handler hosts behind an ASGI server, topic resolved from the route
and the Benzene status mapped to an HTTP code:

```python
from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint

@http_endpoint("GET", "/greet/{name}")
@message("say:hello")
async def hello(request: dict) -> Result:
    return Result.ok({"greeting": f"Hello {request['name']}"})

app = BenzeneHttpApp(HttpRouter().add(hello))   # run: uvicorn module:app
```

## Repository layout

A monorepo of independently-publishable distributions:

```
packages/
  benzene-results/   benzene/results/   (Result, Status)
  benzene-core/      benzene/core/      (pipeline, registry, DI, envelope)
  benzene-http/      benzene/http/      (ASGI binding, status mapping)
  benzene-grpc/      benzene/grpc/      (Benzene<->gRPC status mapping)
  benzene-gcp/       benzene/gcp/       (Cloud Functions host: HTTP + Pub/Sub)
  benzene-aws/       benzene/aws/       (Lambda host: API Gateway + SQS + SNS)
  benzene-azure/     benzene/azure/     (Functions host: HTTP + Service Bus + Event Hub)
  benzene-mesh/      benzene/mesh/      (ServiceDescriptor, benzene:mesh, tracing, feeds)
  benzene-pydantic/  benzene/pydantic/  (pydantic request validation)
  benzene-testing/   benzene/testing/   (in-memory test host + fakes)
conformance/         language-neutral spec fixtures (shared)
examples/            runnable multi-transport cloud examples, each dogfood-tested
tests/               cross-package tests + the dependency-free conformance runner
docs/                guides, reference, and the package rationale
```

## Developing

The packages use [PEP 420 namespace packages](https://peps.python.org/pep-0420/), so for local dev
nothing needs building — the layers resolve straight off `sys.path`:

```bash
# run the tests (pytest picks up the packages/ paths from pyproject.toml)
pytest

# run the conformance runner without pytest
PYTHONPATH=packages/benzene-results:packages/benzene-core:packages/benzene-http:packages/benzene-mesh \
  python -m tests.conformance_runner
```

Or install the layers editable (what CI does — this also verifies the packaging):

```bash
pip install -e packages/benzene-results -e packages/benzene-core -e packages/benzene-http \
             -e packages/benzene-mesh -e packages/benzene-testing
pip install pytest && pytest -q
```

## Notes for readers of the .NET original

The port is spec-first, not a transliteration, so a few deliberate idiom choices differ from the C#
API. None touch the wire envelope, status vocabulary, or HTTP mapping (the interop contract):

- **No `Async` suffixes.** Python signals async with `async def`, so the entry point is
  `await app.handle(envelope)`, not `HandleAsync` — matching `MiddlewarePipeline.handle`,
  `BenzeneHttpApp.handle`, and every other method in the port.
- **Statuses are strings, not an enum.** The wire vocabulary is surfaced as `Status.OK` constants
  and `Result.ok()` / `Result.not_found()` factories (spec §3), so an application can extend the
  vocabulary — an unknown status is a failure, exactly as the spec says.
- **Explicit registration over reflection.** Handlers are registered with the `@message` decorator
  or `Registry.register(...)`; there is no assembly scanning.
- **Middleware is a declarative field, not a `Configure(app)`/`app.Use()` call.** A composition
  root's cross-cutting middleware is a `middleware: list[Middleware]` field on the `AppDefinition`
  that `BenzeneStartUp.configure()` returns; `application_from(definition)` installs it ahead of the
  message router, so a host and a test boot the same pipeline.
- **Layered PyPI packages, not one-per-C#-project.** The distributions follow the meaningful
  adoption seams (see [`docs/packages.md`](docs/packages.md)); the assembly-only C# splits are folded
  in (e.g. `Benzene.Dependencies` → `benzene.core.dependencies`).

## Conformance

The language-neutral fixtures from the spec live in [`conformance/`](conformance/) and run two ways
— the dependency-free `python -m tests.conformance_runner`, and granular pytest cases. **Every
language-neutral fixture is green**: status vocabulary, HTTP + gRPC status mappings, the envelope,
transport metadata, and all four mesh fixtures (descriptor, trace, collector, issues). Passing these plus the live cross-language
interop checks (send/receive the envelope against a
running .NET Benzene service) is what "conformant" means — see the spec's
[porting guide §3](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/porting-guide.md).

## Roadmap

1. **(done)** Wire contracts + core model + `BenzeneMessage` envelope, conformance-green.
2. **(done)** An HTTP inbound binding end-to-end (ASGI), including the status-code mapping.
3. **(done)** Layered, install-what-you-use packages (`benzene-results` / `-core` / `-http`).
4. **(done)** Cloud hosts — the "host anywhere" proof on Python, each a multi-transport,
   dogfood-tested example per the [Port Quality Standards](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/port-quality-standards.md):
   **GCP** (`benzene-gcp`, HTTP + Pub/Sub), **AWS** (`benzene-aws`, API Gateway + SQS + SNS), and
   **Azure** (`benzene-azure`, HTTP + Service Bus + Event Hub) — all done, each with egress.
5. **(done)** The mesh module (`benzene-mesh`) — the `ServiceDescriptor` (derived from the registry,
   with per-topic schemas + a contract hash), the reserved `benzene:mesh` endpoint, per-invocation
   tracing, and the collector feeds — so Python services describe themselves and appear in the mesh.
   Conformance-green against `mesh-descriptor-cases` and `mesh-trace-cases`.
6. **(done)** Payload/handler versioning — the inbound version-header fallback list
   (`benzene-version` → `version` → `x-version`), the HTTP `/v{version}/` route segment, an opt-in
   `highest_version` selector (exact-match stays the default), and the casting-handler pattern (serve
   multiple payload versions with no framework code) — all done and documented. Transparent-casting
   decorators are the one remaining, optional piece.
7. **(done)** Operational surfaces — health checks (the reserved `benzene:healthcheck` endpoint,
   whose `{isHealthy, healthChecks}` aggregate also feeds the mesh heartbeat) and the
   `benzene-pydantic` request-validation adapter. All the cross-cutting middleware (validation,
   tracing, health, mesh) installs from one composition root and is testable through the harness.
8. **(done)** The mesh **collector** — `MeshCollector`, an ordinary Benzene service that ingests the
   register/heartbeat/traces/issues feeds and answers `benzene:mesh:query:*` (fleet/service/topic/trace),
   deriving providers, consumer edges from trace parentage, per-instance health + hash-drift, and
   `missingFeeds`, plus the optional issue feed (delta-merge by fingerprint). Conformance-green
   against `mesh-collector-cases` **and** `mesh-issue-cases`.
9. **(in progress)** gRPC — the Benzene↔gRPC status mapping (`benzene-grpc`, wire-contracts §4.2) and
   the `benzene-status` trailer rule are done and conformance-green against `grpc-status-mapping`; the
   server/client transport over `grpcio` is the next step.
10. Later: the gRPC transport binding, and transparent-casting decorators for versioning.

## Documentation

Start at [`docs/index.md`](docs/index.md).
