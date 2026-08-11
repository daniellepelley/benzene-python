# Benzene Python examples

Runnable sample apps that prove the framework's promise — *write your handlers once, host them
anywhere* — and that are held to the [Port Quality Standards](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/port-quality-standards.md):
each cloud example exercises **multiple transports**, ships its **own tests that dogfood the port's
own test helpers**, and runs as a **required CI gate** (in-memory, no cloud credentials).

> **These are demos, not starting points.** An example is contrived to show off a technique, so it
> carries boilerplate you'd delete when adopting it. To *start* a new service, scaffold a vanilla
> starter from [`../templates/`](../templates) (`copier copy`) and write your handlers into it.
> Templates are where you start; examples are where you learn a technique.

## The shared domain: `orders_domain`

The transport-agnostic business logic — order handlers and models — reused by every host. A host
example is just a `host.py` that mounts this domain onto one cloud's transports. When adding a demo
capability, put the handler in `orders_domain` and wire it from the hosts rather than duplicating.

## Per-cloud hosts

| Example | Host | Transports | Package |
|---|---|---|---|
| [`gcp_orders/`](gcp_orders) | Google Cloud Functions | HTTP + Pub/Sub + Pub/Sub egress | `benzene-gcp` |
| [`aws_orders/`](aws_orders) | AWS Lambda | API Gateway (HTTP) + SQS + SNS + EventBridge + SNS egress | `benzene-aws` |
| [`azure_orders/`](azure_orders) | Azure Functions | HTTP + Service Bus + Event Hub + Event Grid + Service Bus egress | `benzene-azure` |

## Non-cloud hosts

| Example | Host | Transports | Package |
|---|---|---|---|
| [`http_orders/`](http_orders) | standalone HTTP server | HTTP (ASGI) + HTTP egress | `benzene-http` |
| [`grpc_orders/`](grpc_orders) | gRPC server | gRPC unary (method = topic) + faked egress | `benzene-grpc[transport]` |
| [`kafka_orders/`](kafka_orders) | self-hosted Kafka consumer | Kafka consume (topic = header) + Kafka produce egress | `benzene-kafka` |
| [`sqs_orders/`](sqs_orders) | self-hosted SQS consumer | SQS poll (topic = message attribute) + SQS produce egress | `benzene-aws` |
| [`rabbitmq_orders/`](rabbitmq_orders) | self-hosted RabbitMQ consumer | AMQP consume (topic = header) + AMQP publish egress | `benzene-rabbitmq` |

The [`rabbitmq_orders`](rabbitmq_orders) example mounts the same `orders_domain` on the **RabbitMQ**
binding: a self-hosted consumer that turns each delivery into one pipeline invocation (at-least-once —
ack on success, nack on failure) and a `RabbitMqMessageSender` that publishes `orders:created`. Like
Kafka it owns its own loop (no HTTP surface) and is tested through the shared harness
(`create_test_host(...).build_rabbitmq()` + `send_rabbitmq`) — only that one call differs from the
Kafka suite.

The [`sqs_orders`](sqs_orders) example mounts the same `orders_domain` on the **self-hosted SQS
consumer** binding — distinct from `aws_orders`, which reaches SQS via a *Lambda event source*. This
one polls the queue itself (at-least-once — deletes only on success) and is tested through the shared
harness (`create_test_host(...).build_sqs_consumer()` + `send_sqs_consumer`), the same
one-specialization-step setup as every other suite here.

The [`http_orders`](http_orders) example is the Python analog of the .NET `Asp` example: it mounts the
same `orders_domain` directly on `benzene-http`'s ASGI binding (no cloud runtime) and tests it through
the shared harness (`create_test_host(...).build_http()` + `send_http`) — the same one-specialization-step
setup as the cloud suites. `POST /orders` / `GET /orders/{id}` are served over plain HTTP, and placing an
order publishes `orders:created` to a downstream service via an `HttpMessageSender`.

The gRPC example mounts the *same* `orders_domain` on the gRPC binding and tests it through the shared
harness like every cloud (`create_test_host(...).build_grpc()` + `send_grpc`), plus one real-socket
test that proves the `GrpcMessageSender` client over a live channel. Because the binding serves every
topic as one generic method, the domain's `POST /orders` / `GET /orders/{id}` routes are reached as the
`orders:place` / `orders:get` topics.

## On Kubernetes: the same domain, three independent Deployments

[`k8s_orders/`](k8s_orders) doesn't add a fourth transport — it packages three of the hosts above
(`http_orders`, `sqs_orders`, `kafka_orders`) as three separate container images and Kubernetes
Deployments, all mounting the *same* `orders_domain`. See
[docs/getting-started-kubernetes.md](../docs/getting-started-kubernetes.md) for why that's worth
doing (a queue or stream is where Benzene earns its keep even as the *only* transport a service has;
HTTP alone doesn't need it).

## Pattern examples

Beyond the host examples, these demonstrate a cross-cutting Benzene *pattern* rather than a transport:

| Example | Pattern | Package |
|---|---|---|
| [`versioning/`](versioning) | handler-version dispatch by the `benzene-version` metadata header (versioning.md §3, "Mechanism A") | `benzene-core` |
| [`mesh_fleet/`](mesh_fleet) | a multi-service mesh: descriptors, tracing, and a collector fleet view | `benzene-mesh` |
| [`mesh_dashboard/`](mesh_dashboard) | the observer side: project a mesh into the full mesh-ui artifact set (schemas, health, topology, usage) | `benzene-mesh` |

## Candidate pattern examples

The .NET repo ships more pattern examples than this port has yet written up. The adapter packages
they need have **now all shipped** (see the [roadmap](../README.md#roadmap)), so these are unblocked —
a natural next addition rather than a limitation:

| Candidate example | Package (now shipped) | Notes |
|---|---|---|
| Saga | `benzene-resilience` (`Saga`) | in-process compensating rollback — see the package's tests for the shape |
| OpenTelemetry | `benzene-otel` (`OtelTraceExporter`) | export the port's existing spans through a real OTel SDK |
| Auth | `benzene-auth` | Basic/JWT interception + an API Gateway authorizer |
| Caching | `benzene-cache` | cache-aside over an in-memory or Redis backend |
| OpenAPI | `benzene-openapi` | project the registry into an OpenAPI 3.1 document |

Payload **casting** with caster chaining (the .NET `Versioning` example's "Mechanism B") is a natural
extension of the [`versioning`](versioning) example once a dedicated casting example is warranted — the
core primitives (`SchemaCasters` + `casting_handler`) already exist in `benzene-core`.

## Running the tests

All example tests run as part of the normal suite (they're on `testpaths`):

```bash
pytest examples            # just the examples
pytest                     # library + examples
```

They are in-memory and credential-free — the same tests the CI gate runs. Each example's `README.md`
covers running and deploying it against the real cloud.
