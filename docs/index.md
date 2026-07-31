# Benzene for Python — Documentation

A Python port of [Benzene](https://github.com/daniellepelley/Benzene), the middleware-based
framework for hexagonal (ports-and-adapters) architecture: **write your message handlers once, host
them anywhere.** This port is spec-first — it implements the language-neutral Benzene specification
idiomatically in Python and interoperates on the wire with the .NET, Go, and TypeScript ports.

## Start here

- **[Getting started](getting-started.md)** — from an empty folder to a running HTTP service in a
  few minutes.
- **[Packages & adoption levels](packages.md)** — how Benzene is split into layered PyPI packages,
  why, and which ones to install.

## Reference

- **[`benzene.results`](reference/results.md)** — the `Result` type and the status vocabulary.
- **[`benzene.core`](reference/core.md)** — handlers, the `@message` decorator, the registry, the
  middleware pipeline, dependency injection, the outbound client port, and the `BenzeneMessage`
  envelope.
- **[`benzene.http`](reference/http.md)** — the inbound HTTP (ASGI) transport binding.
- **[`benzene.gcp`](reference/gcp.md)** — the Google Cloud Functions host (HTTP + Pub/Sub).
- **[`benzene.aws`](reference/aws.md)** — the AWS Lambda host (API Gateway + SQS + SNS).
- **[`benzene.azure`](reference/azure.md)** — the Azure Functions host (HTTP + Service Bus + Event Hub).
- **[`benzene.testing`](reference/testing.md)** — the in-memory test host and test doubles.

## Guides & cookbooks

- **[Hosting on Google Cloud Functions](cookbooks/hosting-on-gcp.md)** — one set of handlers behind
  HTTP + Pub/Sub triggers, with Pub/Sub egress.
- **[Hosting on AWS Lambda](cookbooks/hosting-on-aws.md)** — one function across API Gateway + SQS +
  SNS, with SNS egress.
- **[Hosting on Azure Functions](cookbooks/hosting-on-azure.md)** — HTTP + Service Bus + Event Hub
  triggers, with Service Bus egress.
- **[Examples](https://github.com/daniellepelley/benzene-python/tree/main/examples)** — runnable,
  multi-transport cloud examples, each with dogfooded tests.

## Concepts & the spec

Benzene Python is faithful to the language-neutral specification. The authoritative documents live
in the main Benzene repository:

- [core-concepts](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/core-concepts.md)
  — Result, Topic, the middleware pipeline, DI, the lifecycle.
- [wire-contracts](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/wire-contracts.md)
  — the message envelope, the status vocabulary, and the HTTP status mapping.
- [transport-bindings](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)
  — what a transport binding must satisfy (the HTTP binding is the first one ported here).

## Status

The core, the inbound HTTP binding, and the three cloud hosts (GCP, AWS, Azure — each multi-transport
with egress) are implemented and conformance-green. The mesh module and payload versioning are on the
[roadmap](../README.md#roadmap).
