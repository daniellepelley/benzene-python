# Benzene for Python — Documentation

A Python port of [Benzene](https://github.com/daniellepelley/Benzene), the middleware-based
framework for hexagonal (ports-and-adapters) architecture: **write your message handlers once, host
them anywhere.** This port is spec-first — it implements the language-neutral Benzene specification
idiomatically in Python and interoperates on the wire with the .NET, Go, and TypeScript ports.

## Start here

- **[Getting started](getting-started.md)** — from an empty folder to a running HTTP service in a
  few minutes.
  - [AWS Lambda](getting-started-aws.md) — one function over API Gateway, SQS, and SNS
  - [Azure Functions](getting-started-azure.md) — HTTP, Service Bus, and Event Hub triggers
  - [Google Cloud Functions](getting-started-google.md) — HTTP + Pub/Sub
  - [gRPC](getting-started-grpc.md) — a unary gRPC server (and the client binding)
  - [Kubernetes](getting-started-kubernetes.md) — one domain hosted over HTTP, SQS, and Kafka from a
    single process and Deployment
- **[Packages & adoption levels](packages.md)** — how Benzene is split into layered PyPI packages,
  why, and which ones to install.
- **[Capability matrix](capability-matrix.md)** — for each production concern, what this port
  provides (with the code that provides it), what it deliberately does not do, and how to fill the
  gap.

## Reference

- **[`benzene.results`](reference/results.md)** — the `Result` type and the status vocabulary.
- **[`benzene.core`](reference/core.md)** — handlers, the `@message` decorator, the registry, the
  middleware pipeline, dependency injection, the outbound client port, and the `BenzeneMessage`
  envelope.
- **[`benzene.http`](reference/http.md)** — the inbound HTTP (ASGI) transport binding.
- **[`benzene.grpc`](reference/grpc.md)** — the Benzene↔gRPC status mapping and trailer rule.
- **[`benzene.gcp`](reference/gcp.md)** — the Google Cloud Functions host (HTTP + Pub/Sub).
- **[`benzene.aws`](reference/aws.md)** — the AWS Lambda host (API Gateway + SQS + SNS + S3 + EventBridge + DynamoDB Streams + Kinesis + Kafka/MSK + direct invoke inbound, SNS/SQS/EventBridge/Kinesis/Lambda egress) plus a self-hosted SQS consumer.
- **[`benzene.azure`](reference/azure.md)** — the Azure Functions host (HTTP + Service Bus + Event Hub + Queue Storage + Blob Storage + Cosmos DB change feed + Timer + Event Grid inbound, Service Bus/Event Hub/Queue Storage/Event Grid egress).
- **[`benzene.kafka`](reference/kafka.md)** — the Apache Kafka host (self-hosted consumer + produce client).
- **[`benzene.rabbitmq`](reference/rabbitmq.md)** — the RabbitMQ transport (self-hosted consumer + publish client).
- **[`benzene.resilience`](reference/resilience.md)** — circuit breaker, bulkhead, rate limiting, idempotency, and in-process sagas.
- **[`benzene.auth`](reference/auth.md)** — authentication middleware: Basic auth, JWT/OAuth2 bearer, and an API Gateway custom-authorizer adapter.
- **[`benzene.cache`](reference/cache.md)** — cache-aside over a narrow async `Cache` port, with in-memory and Redis backends.
- **[`benzene.openapi`](reference/openapi.md)** — derive an OpenAPI 3.1 document from the handler registry.
- **[`benzene.otel`](reference/otel.md)** — export the port's mesh traces through the OpenTelemetry SDK, plus a response-as-event pattern.
- **[`benzene.mesh`](reference/mesh.md)** — self-description, tracing, and collector feeds for the mesh.
- **[`benzene.mesh_fleet`](reference/mesh-fleet.md)** — find the services in your fleet automatically (AWS/Azure/Kubernetes registries) and send mesh traces to Jaeger, Tempo, or X-Ray.
- **[`benzene.pydantic`](reference/pydantic.md)** — validate handler requests with pydantic models.
- **[`benzene.testing`](reference/testing.md)** — the in-memory test host and test doubles.

## Tools

- **[`benzene-codegen-client`](codegen-client.md)** — generates a typed, topic-scoped Python client
  from any Benzene service's Contract Document (`{Service}.spec.json`) — .NET, Go, TypeScript, or
  Python. A build-time/CLI tool, not a runtime dependency.

## Guides & cookbooks

- **[Hosting on Google Cloud Functions](cookbooks/hosting-on-gcp.md)** — one set of handlers behind
  HTTP + Pub/Sub triggers, with Pub/Sub egress.
- **[Hosting on AWS Lambda](cookbooks/hosting-on-aws.md)** — one function across API Gateway + SQS +
  SNS, with SNS egress.
- **[Hosting on Azure Functions](cookbooks/hosting-on-azure.md)** — HTTP + Service Bus + Event Hub
  triggers, with Service Bus egress.
- **[Calling other services](cookbooks/calling-other-services.md)** — outbound `MessageSender` clients
  and cross-cutting decorators (retry, correlation id, trace propagation) that compose over one port.
- **[Joining the mesh](cookbooks/joining-the-mesh.md)** — add self-description, tracing, and collector
  feeds to a service without touching its handlers.
- **[Observing the mesh](cookbooks/observing-the-mesh.md)** — stand up the collector, publish the
  mesh-ui artifacts, and serve the Mesh UI dashboard for your fleet.
- **[Evolving a handler's payload](cookbooks/evolving-payloads.md)** — carry a version across services,
  register versioned handlers, and serve every version off one implementation with transparent casting.
- **[Examples](https://github.com/daniellepelley/benzene-python/tree/main/examples)** — runnable,
  multi-transport cloud examples, each with dogfooded tests.

## Concepts & the spec

- **[Cloud Service Profile conformance](cloud-service-profile.md)** — how this port satisfies the
  profile's R1–R8, mapped to the API and the test that proves each.

Benzene Python is faithful to the language-neutral specification. The authoritative documents live
in the main Benzene repository:

- [core-concepts](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/core-concepts.md)
  — Result, Topic, the middleware pipeline, DI, the lifecycle.
- [wire-contracts](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/wire-contracts.md)
  — the message envelope, the status vocabulary, and the HTTP status mapping.
- [transport-bindings](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)
  — what a transport binding must satisfy. Nine bindings here satisfy it: HTTP, gRPC, the three
  cloud hosts, Kafka, RabbitMQ, the self-hosted SQS consumer, and in-process dispatch.

## Status

All nineteen packages are published to PyPI as **pre-release betas** (`0.1.0b…` — for early testing,
not production; `pip install benzene-core benzene-http` works today, see
[Publishing](publishing.md)). The core, the inbound + outbound HTTP and gRPC bindings, the three
cloud hosts (GCP, AWS, Azure — each multi-transport with egress), the Kafka and RabbitMQ transports,
the mesh module (self-description, tracing, and collector feeds), the cross-cutting middleware
(resilience, auth, caching, OpenTelemetry, OpenAPI), payload/handler versioning, and the Cloud
Service Profile's well-known HTTP surfaces (`/benzene/invoke`, `/benzene/health`, `/benzene/spec`)
are implemented and conformance-green. Every conformance fixture that applies to this port passes —
two of the fourteen are conditional on mesh capabilities this port does not implement, recorded with
their reasons in `UNRUN_FIXTURES` — see the [roadmap](../README.md#roadmap).

## Project internals (maintainers)

- **[Publishing](publishing.md)** — how the nineteen packages are released to PyPI (lockstep
  versioning, trusted publishing).
- **[Mesh on AWS — plan](mesh-aws-plan.md)** — an internal planning document: the sequenced plan for
  a multi-service mesh deployed to AWS (thin poller, Fargate collector, reused mesh-ui, Terraform),
  built and verified live; only its CI automation is still open.
