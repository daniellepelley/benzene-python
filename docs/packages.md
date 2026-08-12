# Packages & adoption levels

Benzene for Python is not a single package. It is a **stack of small packages on PyPI**, each one an
**adoption level**. You install only the layers you use, so your application never ships Benzene code
— or transitive dependencies — that it doesn't need, and the deployed footprint stays small. This
mirrors the way .NET Benzene is split into separate NuGet packages, translated to the idiomatic
Python equivalent.

## The layers

| Install | Import | What it gives you | Depends on |
|---|---|---|---|
| [`benzene-results`](https://pypi.org/project/benzene-results/) | `benzene.results` | `Result` and the status vocabulary — the return type of every handler | nothing |
| [`benzene-core`](https://pypi.org/project/benzene-core/) | `benzene.core` | handler registry + `@message`, the middleware pipeline, per-invocation DI, and the transport-neutral `BenzeneMessage` envelope | `benzene-results` |
| [`benzene-http`](https://pypi.org/project/benzene-http/) | `benzene.http` | the inbound HTTP (ASGI) binding and the Benzene↔HTTP status mapping | `benzene-core` |
| [`benzene-grpc`](https://pypi.org/project/benzene-grpc/) | `benzene.grpc` | the Benzene↔gRPC status mapping + `benzene-status` trailer rule (transport pending) | `benzene-core` |
| [`benzene-gcp`](https://pypi.org/project/benzene-gcp/) | `benzene.gcp` | the Google Cloud Functions host (HTTP + Pub/Sub bindings, Pub/Sub outbound client) | `benzene-core`, `benzene-http` |
| [`benzene-aws`](https://pypi.org/project/benzene-aws/) | `benzene.aws` | the AWS Lambda host (API Gateway + SQS + SNS + S3 + EventBridge + DynamoDB Streams + Kinesis + Kafka/MSK + direct-invoke inbound, SNS/SQS/EventBridge/Kinesis/Lambda outbound clients), plus a self-hosted SQS consumer | `benzene-core`, `benzene-http` |
| [`benzene-azure`](https://pypi.org/project/benzene-azure/) | `benzene.azure` | the Azure Functions host (HTTP + Service Bus + Event Hub + Queue Storage + Blob Storage + Cosmos DB change feed + Timer + Event Grid inbound, Service Bus/Queue Storage/Event Grid outbound clients) | `benzene-core`, `benzene-http` |
| [`benzene-kafka`](https://pypi.org/project/benzene-kafka/) | `benzene.kafka` | the Apache Kafka host (self-hosted consumer + Kafka-produce outbound client; SDK optional) | `benzene-core` |
| [`benzene-rabbitmq`](https://pypi.org/project/benzene-rabbitmq/) | `benzene.rabbitmq` | the RabbitMQ transport (self-hosted consumer + RabbitMQ-publish outbound client; SDK optional) | `benzene-core` |
| [`benzene-resilience`](https://pypi.org/project/benzene-resilience/) | `benzene.resilience` | resilience policies as middleware — circuit breaker, bulkhead, rate limiting, idempotency, and in-process sagas | `benzene-core` |
| [`benzene-auth`](https://pypi.org/project/benzene-auth/) | `benzene.auth` | authentication middleware — Basic auth, JWT/OAuth2 bearer tokens, and an API Gateway custom-authorizer adapter | `benzene-core` |
| [`benzene-cache`](https://pypi.org/project/benzene-cache/) | `benzene.cache` | cache-aside over a narrow async `Cache` port, with in-memory and Redis backends | `benzene-core` |
| [`benzene-openapi`](https://pypi.org/project/benzene-openapi/) | `benzene.openapi` | derive an OpenAPI 3.1 document from the handler registry | `benzene-core`, `benzene-http` |
| [`benzene-otel`](https://pypi.org/project/benzene-otel/) | `benzene.otel` | export the port's existing mesh traces through the OpenTelemetry SDK, plus a response-as-event pattern (SDK optional) | `benzene-core`, `benzene-mesh` |
| [`benzene-mesh`](https://pypi.org/project/benzene-mesh/) | `benzene.mesh` | mesh self-description (`ServiceDescriptor`), the reserved `benzene:mesh` endpoint, tracing, and collector feeds | `benzene-core` |
| [`benzene-mesh-fleet`](https://pypi.org/project/benzene-mesh-fleet/) | `benzene.mesh_fleet` | cloud service-discovery adapters (AWS/Azure/Kubernetes) and trace-mappers (Jaeger/Tempo/X-Ray) for a fleet | `benzene-core`, `benzene-mesh` |
| [`benzene-pydantic`](https://pypi.org/project/benzene-pydantic/) | `benzene.pydantic` | validate handler requests with pydantic models (an optional adapter) | `benzene-core`, `pydantic` |
| [`benzene-testing`](https://pypi.org/project/benzene-testing/) | `benzene.testing` | in-memory test host + test doubles (a dev/test dependency) | `benzene-core` |

Because Benzene uses a [PEP 420 namespace package](https://peps.python.org/pep-0420/), all of these
contribute submodules to one shared `benzene` namespace. Installing a higher layer pulls in the
lower ones automatically:

```bash
pip install benzene-http      # also installs benzene-core and benzene-results
```

## Which one do I need?

**1 — Just the result model.** You want to describe a domain operation's outcome with Benzene's
vocabulary (say, to type a service-layer function) but you are not wiring up a pipeline or a
transport yet.

```bash
pip install benzene-results
```
```python
from benzene.results import Result

def reserve_seat(seat_id: str) -> Result:
    if seat_taken(seat_id):
        return Result.failure("conflict", "seat already taken")
    return Result.ok({"seat": seat_id})
```

**2 — Run handlers, transport-neutral.** You want to register handlers by topic and drive them
through the `BenzeneMessage` envelope — in tests, from a custom host, or from another transport you
are building.

```bash
pip install benzene-core
```
```python
from benzene.core import BenzeneMessageApplication, Registry, message
from benzene.results import Result

@message("order:create")
async def create_order(request: dict) -> Result:
    return Result.created({"id": "ord_1", "sku": request["sku"]})

app = BenzeneMessageApplication(Registry().add(create_order))
response = await app.handle(
    {"topic": "order:create", "headers": {}, "body": '{"sku": "ABC"}'}
)
```

**3 — Host over HTTP.** You want those same handlers reachable behind a real HTTP server.

```bash
pip install benzene-http
```
```python
from benzene.core import message
from benzene.results import Result
from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint

@http_endpoint("POST", "/orders")
@message("order:create")
async def create_order(request: dict) -> Result:
    return Result.created({"id": "ord_1", "sku": request["sku"]})

app = BenzeneHttpApp(HttpRouter().add(create_order))   # run: uvicorn module:app
```

## Why split it this way?

- **Small footprint.** A service that only needs the result model doesn't install a pipeline; a
  service that runs handlers over its own transport doesn't install the HTTP binding. You pay for
  what you use.
- **Clear dependency direction.** The arrows only point down: `http → core → results`. A lower layer
  never imports a higher one, which keeps the core transport-agnostic (a spec requirement — the core
  never references a vendor or a transport).
- **Independent evolution.** Each package versions and releases on its own cadence. A new transport
  (Pub/Sub, SQS/SNS) ships as a new package on top of `benzene-core` without touching what you
  already installed.
- **Faithful to Benzene.** The .NET original is a family of NuGet packages layered the same way; the
  Python port keeps the layering and the names close so the two are recognisable side by side.

## A note on granularity (a deliberate divergence from .NET/TypeScript)

The .NET and TypeScript ports split the bottom of the stack much more finely —
`Benzene.Abstractions`, `Benzene.Abstractions.Messages`, `Benzene.Core.Messages`,
`Benzene.Core.Middleware`, `Benzene.Dependencies`, and so on. Those splits exist to satisfy **C#
assembly / dependency isolation**, which has no equivalent cost in Python. Reproducing ten
distributions where three carry the same meaning would hurt, not help, a Python developer.

So the Python port keeps the **meaningful adoption seams** as separate packages (`results`, `core`,
`http`) and folds the assembly-only splits into them — for example, the DI container that .NET ships
as `Benzene.Dependencies` lives in `benzene.core.dependencies`. If and when a genuinely optional
concern appears (e.g. a bring-your-own-container adapter, or a validation adapter over `pydantic`),
*that* earns its own package, because it is a real adoption choice rather than an assembly boundary.
