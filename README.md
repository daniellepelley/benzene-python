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

> **Status: feature-complete against the spec, with the .NET parity roadmap now closed.** Shipped as
> eighteen layered packages — the foundations (`benzene-results`, `benzene-core`, `benzene-http`,
> `benzene-grpc`), the transport hosts (`benzene-gcp` / `benzene-aws` / `benzene-azure`, plus
> `benzene-kafka` and `benzene-rabbitmq`), the cross-cutting middleware (`benzene-resilience`,
> `benzene-auth`, `benzene-cache`, `benzene-otel`, `benzene-openapi`), the mesh (`benzene-mesh` /
> `benzene-mesh-fleet`), and the adapters (`benzene-pydantic`, `benzene-testing`) — that pass **every
> language-neutral conformance fixture**. Core, the inbound + outbound HTTP and gRPC bindings, the three
> cloud hosts (each multi-transport with egress, AWS now spanning eight Lambda event sources plus a
> self-hosted SQS consumer), the Kafka and RabbitMQ transports, the mesh module (descriptor, tracing,
> collector) and its fleet discovery/trace-mappers, resilience policies (circuit breaker, bulkhead, rate
> limiting, idempotency, sagas), authentication (Basic / JWT / OAuth2 bearer), cache-aside caching,
> OpenTelemetry trace export, OpenAPI 3.1 generation, payload versioning (headers, route segment,
> selectors, casting-handler, transparent casting), health checks, and the Cloud Service Profile's
> well-known HTTP surfaces (`/benzene/invoke`, `/benzene/health`, `/benzene/spec`) are all implemented.
> Each package
> builds a `twine`-clean sdist + wheel and a trusted-publishing [`release`](.github/workflows/release.yml)
> workflow is in place; the first PyPI publish awaits the one-time trusted-publisher setup and a version
> tag ([`docs/publishing.md`](docs/publishing.md)). See the [roadmap](#roadmap).

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
| `benzene-core` | `benzene.core` | pipeline, registry, `@message`, DI, versioning, health checks, the `BenzeneMessage` envelope, in-process transport | `benzene-results` | `Benzene.Core*` + `Benzene.Dependencies` |
| `benzene-http` | `benzene.http` | inbound HTTP (ASGI) binding + status mapping | `benzene-core` | `Benzene.Http` |
| `benzene-grpc` | `benzene.grpc` | Benzene↔gRPC status mapping + trailer rule + server/client transport (`[transport]`) | `benzene-core` (+ `grpcio`) | `Benzene.Grpc` |
| `benzene-gcp` | `benzene.gcp` | Google Cloud Functions host (HTTP + Pub/Sub + egress) | `benzene-core`, `benzene-http` | `Benzene.GoogleCloud.Functions.*` |
| `benzene-aws` | `benzene.aws` | AWS Lambda host (API Gateway + SQS + SNS + S3 + EventBridge + DynamoDB Streams + Kinesis + MSK/Kafka + direct invoke + egress) + a self-hosted SQS consumer | `benzene-core`, `benzene-http` | `Benzene.Aws.Lambda.*` + `Benzene.Clients.Aws.*` + `Benzene.Aws.Sqs` |
| `benzene-azure` | `benzene.azure` | Azure Functions host (HTTP + Service Bus + Event Hub + Queue/Blob Storage + Cosmos Change Feed + Timer + Event Grid + egress) | `benzene-core`, `benzene-http` | `Benzene.Azure.Function.*` + `Benzene.Clients.Azure.*` |
| `benzene-kafka` | `benzene.kafka` | Apache Kafka host — self-hosted consumer + Kafka-produce egress (`[kafka]`) | `benzene-core` (+ `confluent-kafka`) | `Benzene.Kafka.Core` |
| `benzene-rabbitmq` | `benzene.rabbitmq` | RabbitMQ host — self-hosted consumer + AMQP-publish egress (`[rabbitmq]`) | `benzene-core` (+ `pika`) | `Benzene.RabbitMq` |
| `benzene-resilience` | `benzene.resilience` | circuit breaker, bulkhead, rate limiting, idempotency, in-process saga | `benzene-core` | `Benzene.Resilience.Polly` + `Benzene.RateLimiting` + `Benzene.Idempotency` + `Benzene.Saga` |
| `benzene-auth` | `benzene.auth` | Basic + JWT/OAuth2 bearer middleware + API Gateway custom authorizer (`[jwt]`) | `benzene-core` (+ `PyJWT`) | `Benzene.Auth.{Basic,OAuth2}` |
| `benzene-cache` | `benzene.cache` | cache-aside abstraction + in-memory and Redis backends (`[redis]`) | `benzene-core` (+ `redis`) | `Benzene.Cache.Core` / `Benzene.Cache.Redis` |
| `benzene-openapi` | `benzene.openapi` | OpenAPI 3.1 document generated from the registry | `benzene-core`, `benzene-http` | `Benzene.Schema.OpenApi` |
| `benzene-otel` | `benzene.otel` | export the port's spans to a real OpenTelemetry SDK + response-as-event (`[otel]`) | `benzene-core`, `benzene-mesh` (+ `opentelemetry-sdk`) | `Benzene.ResponseEvents` |
| `benzene-mesh` | `benzene.mesh` | ServiceDescriptor + `benzene:mesh` endpoint + tracing + collector feeds + a `MeshCollector` | `benzene-core` | `Benzene.Mesh.Wire` + `Benzene.Mesh.Collector` |
| `benzene-mesh-fleet` | `benzene.mesh_fleet` | service-discovery adapters (AWS/Azure/K8s) + Jaeger/Tempo/X-Ray trace mappers | `benzene-core`, `benzene-mesh` | `Benzene.Mesh.Discovery.*` + `Benzene.Mesh.Fleet.*` |
| `benzene-pydantic` | `benzene.pydantic` | validate handler requests with pydantic models | `benzene-core`, `pydantic` | `Benzene.FluentValidation` |
| `benzene-testing` | `benzene.testing` | in-memory test host + fakes (dev/test) | `benzene-core` | `Benzene.Testing` |
| `benzene-codegen-client` | `benzene.codegen_client` | the `benzene-codegen` CLI — generates a typed client from any service's Contract Document (build-time) | `benzene-core` | `Benzene.CodeGen.Client` |

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

## Scaffold a new service

The **`dotnet new` equivalent** for the Python port: generate a runnable Benzene service — a
`BenzeneStartUp` composition root, a demo handler with one injected `Greeter`, the transport host and
entry point, a `pyproject.toml`, and an optional pytest component test — with a single command. The
starters live in [`templates/`](templates) and are driven by [**Copier**](https://copier.readthedocs.io/):

```bash
pip install copier

# From GitHub (the templates live in this subdirectory of the repo):
copier copy gh:daniellepelley/benzene-python/templates my-service

# ...or from a local checkout of this repo:
copier copy templates my-service
```

Copier then asks a few questions — the service name, its Python package slug, which **transport** to
wire, and whether to include tests:

| `transport` | Host | Demo handler wired as |
|---|---|---|
| `aws-apigateway` | AWS Lambda behind API Gateway (HTTP) | `GET /hello/{name}` route |
| `aws-sqs` | AWS Lambda triggered by SQS (fire-and-forget) | `hello:world` topic |
| `grpc` | a gRPC server (method = topic, host anywhere) | `hello:world` topic |

The `include_tests` toggle (default `true`) adds a pytest component test that boots the **same** app
`StartUp` configures for deployment, swaps the `Greeter` for a spy, and pushes a message through the
whole pipeline via the transport's own front door. We use Copier rather than Cookiecutter for one
reason — `copier update`: a generated project records its answers in `.copier-answers.yml`, so when
the templates improve you re-run `copier update` inside your project and pull the changes in
(three-way-merged against your edits).

> **Heads up:** a generated `pyproject.toml` depends on the real `benzene-*` package names, which are
> **not published to PyPI yet**. Until they are, install the `benzene-*` deps from a local checkout of
> this repo first (editable) and then install the generated project with `--no-deps` — the generated
> `README.md` and [`templates/README.md`](templates/README.md) spell out the exact recipe.

See [`templates/README.md`](templates/README.md) for the full question list, the transport details,
and the local-install recipe.

## Generate a typed client from a Contract Document

Add `benzene-codegen-client` to get a **typed client for someone else's service** — .NET, Go,
TypeScript, or Python — from its committed Contract Document, no hand-written DTOs:

```bash
pip install benzene-codegen-client
benzene-codegen topic --spec payments.spec.json --topic payments:capture --out payments_client.py
```

See [`docs/codegen-client.md`](docs/codegen-client.md) for the full guide.

## Repository layout

A monorepo of independently-publishable distributions:

```
packages/
  benzene-results/         benzene/results/         (Result, Status)
  benzene-core/            benzene/core/            (pipeline, registry, DI, envelope)
  benzene-http/            benzene/http/            (ASGI binding, status mapping)
  benzene-grpc/            benzene/grpc/            (Benzene<->gRPC status mapping)
  benzene-gcp/             benzene/gcp/             (Cloud Functions host: HTTP + Pub/Sub)
  benzene-aws/             benzene/aws/             (Lambda host: API Gateway + SQS + SNS)
  benzene-azure/           benzene/azure/           (Functions host: HTTP + Service Bus + Event Hub)
  benzene-mesh/            benzene/mesh/            (ServiceDescriptor, benzene:mesh, tracing, feeds)
  benzene-pydantic/        benzene/pydantic/        (pydantic request validation)
  benzene-testing/         benzene/testing/         (in-memory test host + fakes)
  benzene-codegen-client/  benzene/codegen_client/  (Contract Document -> typed client, benzene-codegen CLI)
conformance/         language-neutral spec fixtures (shared)
examples/            runnable multi-transport cloud examples, each dogfood-tested
tests/               cross-package tests + the dependency-free conformance runner
docs/                guides, reference, and the package rationale
examples/            runnable demos, including the codegen-client dogfood example
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
  message router, so a host and a test boot the same pipeline. The Cloud Service Profile's well-known
  surfaces are declared the same way — a sibling `standard_paths` field (a `benzene.http.StandardPaths`,
  typed `Any` like `router` so `benzene.core` stays HTTP-free) — so every HTTP host and the test harness
  expose `/benzene/*` off one declaration.
- **Layered PyPI packages, not one-per-C#-project.** The distributions follow the meaningful
  adoption seams (see [`docs/packages.md`](docs/packages.md)); the assembly-only C# splits are folded
  in (e.g. `Benzene.Dependencies` → `benzene.core.dependencies`).
- **The test builders serialize like the wire.** Every in-memory test-host builder
  (`MessageBuilder`, and the AWS/GCP/Azure native-event builders) encodes a dataclass body through the
  same `encode_body` policy the live transports use (camelCase fields — `orderId`, not `order_id`), so
  a passing in-memory test reflects real cross-language traffic instead of masking an interop gap.

## Conformance

The language-neutral fixtures from the spec live in [`conformance/`](conformance/) and run two ways
— the dependency-free `python -m tests.conformance_runner`, and granular pytest cases. **Every
language-neutral fixture is green**: status vocabulary, HTTP + gRPC status mappings, the envelope,
transport metadata, and all four mesh fixtures (descriptor, trace, collector, issues). Passing these plus the live cross-language
interop checks (send/receive the envelope against a
running .NET Benzene service) is what "conformant" means — see the spec's
[porting guide §3](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/porting-guide.md).

## Roadmap

> **Every language-neutral conformance fixture is green** — status vocabulary, HTTP + gRPC status
> mappings, the envelope, transport metadata, and all four mesh fixtures (descriptor, trace, collector,
> issues) — and a first transport binding has shipped for HTTP, all three clouds, gRPC, and Kafka,
> inbound and outbound. Conformance-green is the whole story for items 1–18 below; it is **not** the
> same claim as transport-surface or feature parity with the other three ports. `benzene-dotnet` binds
> roughly three times as many transports and ships circuit breaker, bulkhead, auth, and caching that
> this port doesn't yet — see "Closing the .NET parity gap" below, which exists specifically to track
> that difference and is scoped directly off a cross-language capability audit, not off this port's own
> (already-complete) original plan.

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
   `highest_version` selector (exact-match stays the default), the casting-handler pattern (serve
   multiple payload versions with no framework code), and **transparent casting** — a `SchemaCasters`
   registry (one-step casts between payload types, chained by BFS) with `casting_handler` upcasting the
   request and downcasting the response — all done and documented.
7. **(done)** Operational surfaces — health checks (the reserved `benzene:healthcheck` endpoint,
   whose `{isHealthy, healthChecks}` aggregate also feeds the mesh heartbeat) and the
   `benzene-pydantic` request-validation adapter. All the cross-cutting middleware (validation,
   tracing, health, mesh) installs from one composition root and is testable through the harness.
8. **(done)** The mesh **collector** — `MeshCollector`, an ordinary Benzene service that ingests the
   register/heartbeat/traces/issues feeds and answers `benzene:mesh:query:*` (fleet/service/topic/trace),
   deriving providers, consumer edges from trace parentage, per-instance health + hash-drift, and
   `missingFeeds`, plus the optional issue feed (delta-merge by fingerprint). Conformance-green
   against `mesh-collector-cases` **and** `mesh-issue-cases`.
9. **(done)** The single injectable **transport-metadata** resolver (`read_message_metadata` /
   `MetadataKeys`, wire-contracts §2) — reserved-topic + header resolution shared by all three cloud
   hosts. Conformance-green against `transport-metadata-cases`.
10. **(done)** gRPC (`benzene-grpc`) — the Benzene↔gRPC status mapping + `benzene-status` trailer
    (conformance-green against `grpc-status-mapping`), **and** the server/client transport over `grpcio`
    (the `[transport]` extra): a generic handler serving every topic as a unary method (method = topic),
    and a `GrpcMessageSender`, proven with a real in-process server round-trip.
11. **(done)** Outbound HTTP — `HttpMessageSender` publishes a message to another Benzene service over
    HTTP and maps the response back (the reverse direction of the HTTP binding).
12. **(done)** Transparent-casting decorators for versioning — `SchemaCasters` (one-step casts between
    payload types, composed by breadth-first search, direct preferred) + `casting_handler`, which
    upcasts an older request to the canonical type and downcasts the response back, so a handler serves
    a retired version in one registration instead of a hand-written forwarder.
13. **(done)** The Cloud Service Profile's well-known HTTP surfaces (design-principles §5.2; profile
    R3/R4/R5/R7) — `BenzeneHttpApp(standard_paths=StandardPaths(...))` exposes `/benzene/invoke` (the
    wire-envelope endpoint), `/benzene/health` (the `{isHealthy, healthChecks}` aggregate, 200/503), and
    `/benzene/spec` (the registry-derived `ServiceSpec`) under a configurable `/benzene/` prefix. The
    schema derivation moved into `benzene.core` (shared by the spec doc and the mesh descriptor), and
    the reserved `benzene:spec` topic is answered on any transport by `spec_interception`.
14. **(done)** PyPI packaging — every package carries complete metadata (PEP 639 `license = "MIT"` with
    a bundled `LICENSE`, classifiers, pinned inter-package deps) and builds a clean sdist + wheel that
    passes `twine check`. A [`release`](.github/workflows/release.yml) workflow builds all ten and
    publishes them via **trusted publishing** (OIDC, no stored tokens) on a `vX.Y.Z` tag; see
    [`docs/publishing.md`](docs/publishing.md). The first publish awaits the one-time PyPI
    trusted-publisher setup and a version tag.
15. **(done)** The mesh on real infrastructure — a Fargate **Mesh Host** (poller + collector + durable
    EFS-backed store) and a demo fleet on Lambda, stood up by one dispatchable `terraform apply`
    ([`deploy/mesh`](deploy/mesh)). It projects the catalog into the cross-language **mesh-ui** read-model
    artifacts (`benzene.mesh.build_artifacts`/`write_artifacts` — manifest, topics with schemas +
    version + `schemaMismatch` + `changes[]` + `removedTopics`, topology, usage, an AsyncAPI export,
    annotations, and per-service spec + per-check health) and serves the canonical `mesh-ui.html` at
    `/mesh-ui/`. Verified live end to end (estate, functional map, topology, usage) then torn down; the
    artifact field set is pinned against the contract by `tests/test_mesh_artifact_contract.py`.
16. **(done)** The Cloud Service Profile **self-check** — `benzene.mesh.evaluate_cloud_service_profile`
    grades a composition root's `AppDefinition` against the profile's R1–R8 at wiring time and returns a
    `CloudServiceProfileReport` whose `to_profile()` rides on the descriptor's optional `profile` field
    (`{name, missing}`, `missing` omitted when conformant, excluded from the `descriptorHash`), so any
    tool that can reach `benzene:mesh` can ask a running service which requirements its own wiring
    knows it is missing. R1–R5/R7 are read off the definition (registry + `StandardPaths`); R6 (mesh
    feeds) and R8 (outbound `traceparent`) — the pair the spec calls structurally unobservable — are
    declared explicitly. Dogfooded by [`examples/mesh_dashboard/profile.py`](examples/mesh_dashboard/profile.py).
17. **(done)** The Cloud Service Profile **live-probe checker** — `benzene.mesh.probe_cloud_service`
    (and the `python -m benzene.mesh.probe <url>` CLI) audits a *deployed* service against R1–R8 over
    plain HTTP, speaking only the language-neutral `/benzene/*` surfaces so it grades any port's service
    the same way. Each requirement gets a tri-state verdict (satisfied / not-satisfied / inconclusive)
    with a reason; the three the spec calls structurally unobservable from outside (R6's collector-
    delivery half, R8, and R7 under a non-default prefix) stay inconclusive by design rather than being
    guessed. The outside-in counterpart to item 16's self-check.
18. **(done)** Kafka transport (`benzene-kafka`) — a **self-hosted consumer** (Benzene topic from the
    record's `topic` header, headers UTF-8, one record per pipeline invocation/scope, no response
    channel → acknowledge/log; `run_consumer_loop` commits offsets on success, at-least-once) **and** a
    Kafka-produce outbound client (`KafkaMessageSender`, headers forwarded onto Kafka headers). The
    binding is duck-typed against `confluent-kafka` (an optional `[kafka]` extra), so decode, dispatch,
    and send run in memory with no broker; tested through the shared harness (`build_kafka()` +
    `send_kafka`) and dogfooded by [`examples/kafka_orders/`](examples/kafka_orders). Mirrors
    `Benzene.Kafka.Core`.
19. **(done)** A **self-hosted SQS consumer** (`benzene.aws.run_sqs_consumer_loop`/`SqsConsumerApp`) —
    distinct from the Lambda SQS trigger `benzene-aws` already had: this one polls a queue itself
    (`receive_message`/`delete_message`, an optional `[boto3]` extra), the shape a long-running worker
    or a Kubernetes Deployment needs. Benzene topic from the `topic` message attribute, one message per
    invocation/scope, deletes only on success (at-least-once, the same default as the Kafka consumer
    above). Tested through the shared harness (`build_sqs_consumer()` + `send_sqs_consumer`) and
    dogfooded by [`examples/sqs_orders/`](examples/sqs_orders) and, alongside the HTTP and Kafka hosts,
    [`examples/k8s_orders/`](examples/k8s_orders) (see
    [Getting Started: Kubernetes](docs/getting-started-kubernetes.md)). Mirrors `Benzene.Aws.Sqs`.

### Closing the .NET parity gap

`benzene-dotnet` is the reference port and, transport-for-transport and feature-for-feature, the
widest of the four. The items above took this port from nothing to fully conformant; the items below
take it from "conformant" to "as capable as .NET" — each is scoped directly against a concrete
`benzene-dotnet` package, so "done" has an unambiguous target rather than a vague aspiration.

20. **(done)** AWS transport parity — `benzene-aws` now binds **S3** (object-created notifications),
    **EventBridge**, **DynamoDB Streams**, **Kinesis Data Streams**, and **MSK/Kafka-via-Lambda**
    inbound, plus **EventBridge** and **Kinesis** outbound clients, alongside the existing API Gateway +
    SQS + SNS. Channel-less sources take their topic from an injectable convention on the host;
    DynamoDB/Kinesis report failures via partial-batch `batchItemFailures`. Mirrors
    `Benzene.Aws.Lambda.{S3,EventBridge,DynamoDb,Kinesis,Kafka}` and `Benzene.Clients.Aws.*`.
21. **(done)** Azure transport parity — `benzene-azure` now binds **Queue Storage**, **Blob Storage**,
    **Cosmos DB Change Feed**, a **Timer** trigger, and **Event Grid** (native schema **and**
    CloudEvents 1.0, distinguished by `specversion`) inbound, plus **Queue Storage** and **Event Grid**
    outbound clients, alongside the existing HTTP + Service Bus + Event Hub. Mirrors
    `Benzene.Azure.Function.{QueueStorage,BlobStorage,CosmosDb,Timer,EventGrid}` and
    `Benzene.Clients.Azure.*`.
22. **(done)** RabbitMQ — [`benzene-rabbitmq`](packages/benzene-rabbitmq): a self-hosted consumer plus
    an outbound `RabbitMqMessageSender`, shaped like the Kafka/SQS bindings — duck-typed against an
    optional `[rabbitmq]` extra (`pika`), one delivery per invocation/scope, at-least-once `basic_ack`
    on success and `basic_nack` on failure. Mirrors `Benzene.RabbitMq`.
23. **(done)** Resilience beyond retry — a circuit breaker, a bulkhead, rate limiting wired to the
    `too-many-requests` status, idempotency (dedupe redelivered messages), and an in-process saga
    (compensation/rollback, not durable), shipped as
    [`benzene-resilience`](packages/benzene-resilience). Each gating policy exposes one `execute(run)`
    seam and ships in two shapes off it — an inbound `*_interception` middleware and an outbound
    `MessageSender` decorator — so resilience composes with the core's existing `with_retry`. Mirrors
    `Benzene.Resilience.Polly`, `Benzene.RateLimiting`, `Benzene.Idempotency`, and `Benzene.Saga`.
24. **(done)** Auth — [`benzene-auth`](packages/benzene-auth): Basic auth and JWT/OAuth2 bearer-token
    interception middleware (JWT via an optional `[jwt]` extra, `PyJWT`), plus an API Gateway custom
    authorizer adapter that emits the IAM policy document. Verify/validate callables may be sync or
    async, and attach a `Principal` to the context. Mirrors `Benzene.Auth.{Basic,OAuth2}` and
    `Benzene.Aws.Lambda.ApiGateway.ApiGatewayCustomAuthorizer`.
25. **(done)** Caching — [`benzene-cache`](packages/benzene-cache): a cache-aside abstraction (`Cache`
    protocol + `get_or_load`) with an in-memory backend (TTL, injectable clock) and a Redis backend
    (optional `[redis]` extra). Mirrors `Benzene.Cache.Core` / `Benzene.Cache.Redis`.
26. **(done)** OpenTelemetry — [`benzene-otel`](packages/benzene-otel): `OtelTraceExporter` implements
    the port's existing `benzene.mesh` `TraceExporter` seam, mapping each `TraceEvent` onto a real OTel
    span (optional `[otel]` extra), so tracing exports through the OTel SDK rather than staying
    Benzene-internal; plus a response-as-event middleware mirroring `Benzene.ResponseEvents`.
27. **(done)** OpenAPI generation — [`benzene-openapi`](packages/benzene-openapi): an OpenAPI 3.1
    document projected from the registry, reusing the JSON Schema this port already derives
    (`components.schemas` `$ref`-d from one POST operation per topic). Mirrors `Benzene.Schema.OpenApi`.
28. **(done)** Mesh discovery + fleet — [`benzene-mesh-fleet`](packages/benzene-mesh-fleet): service
    discovery adapters (AWS Cloud Map / Azure / Kubernetes, each behind an optional extra) and fleet
    trace-mappers (Jaeger / Tempo-OTLP / X-Ray) that project the `benzene.mesh` `TraceEvent` model into
    each backend's shape. Mirrors `Benzene.Mesh.Discovery.*` and `Benzene.Mesh.Fleet.*`.
29. **(done)** Direct Lambda-to-Lambda invoke — `benzene-aws` now recognizes a bare
    `{topic, headers, body}` Payload (what `lambda.invoke()` sends) as a synchronous `"invoke"`
    source, answered like API Gateway with the response envelope returned verbatim; the new
    `LambdaMessageSender` outbound client calls a target function's `Invoke` API directly and decodes
    its response back into a `Result` — no broker, AWS's own request/response primitive. Any existing
    `AwsLambdaApp` answers it automatically, with zero extra host wiring. The response-envelope decode
    this needed (`benzene.core.decode_response`, the inverse of `encode_response`) is now a public,
    reusable primitive — it also replaces the equivalent inline logic the in-process sender
    (`benzene.core.inprocess`) already had. Azure Functions and Kubernetes services have no equivalent
    native primitive; they reach the same synchronous-call outcome over HTTP/gRPC instead
    (`HttpMessageSender` / `GrpcMessageSender`), which is why this item is AWS-only.

Every roadmap item (1–30) is now implemented. Sequencing followed the plan: the self-hosted SQS
consumer (19) landed alongside the Kubernetes multi-transport story it exists for (see [Getting
Started: Kubernetes](docs/getting-started-kubernetes.md)); transports (20–22) next, since they were
the largest, most visible gap and the pattern was already proven by the Kafka/SQS bindings;
resilience/auth/caching (23–25) closed the sharpest cross-language outlier; then OpenTelemetry,
OpenAPI, and mesh discovery/fleet (26–28). Each landed as its own independently-installable
distribution, so a service still pulls in only the layers it uses. Direct Lambda invoke (29) landed
next, prompted by a direct comparison against AWS's own Lambda-to-Lambda invoke capability. The
cross-language client generator (30) landed last, giving a Node/Python/Go/.NET consumer of any
Benzene service a typed client from its committed Contract Document.
30. **(done)** `benzene-codegen-client` — generates a typed, topic-scoped client from any Benzene
    service's Contract Document (`{Service}.spec.json`), conformance-green against
    `contract-document-cases.json`/`contract-hash-cases.json`. See
    [`docs/codegen-client.md`](docs/codegen-client.md).

## Documentation

Start at [`docs/index.md`](docs/index.md).
