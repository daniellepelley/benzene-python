# Capability Matrix — what this port does, deliberately doesn't, and how to fill the gap

Benzene is honest about its boundaries. This page is the single place that states, for each
common production concern, **what the Python port provides** (with the code that provides it),
**what it deliberately does not do (and why)**, and **how to solve the rest outside Benzene**.
Package layering and install guidance live in [Packages & adoption levels](packages.md); the
operational-requirements mapping (R1–R8, with the test that proves each) lives in
[Cloud Service Profile conformance](cloud-service-profile.md). This page consolidates both,
capability-first, and adds the rows they don't state: what is **absent**.

## The one idea behind every row

Benzene abstracts at the **business-logic boundary** — you write a message handler once and host
it anywhere — and **never at the transport or storage boundary**. Wrapping SQS or Service Bus
behind a generic "queue" interface would hide the transport-specific capabilities that were the
reason to choose it. So Benzene's answer to "does it abstract X?" is usually a deliberate **no**:
every binding hands you the native event/SDK object, and rolling your own middleware is a
first-class, supported path, not an escape hatch.

**"Deliberately not" and "not yet" are different things.** A row below that gives a reason is a
design decision; a row that says *not implemented* is simply unbuilt (usually something a sibling
port ships). Conflating the two is how a document like this loses your trust, so it doesn't.

## The matrix

| Capability | What this port provides | What it deliberately does NOT do (and why) | How to solve the rest |
|---|---|---|---|
| **Transport features** | Full, direct access to the native event/SDK object on every binding (the Lambda event dict, the `confluent_kafka` message, the ASGI scope) | Hide transport capabilities behind a generic queue/transport interface — the anti-pattern above | Use the raw SDK/event directly; every decode function and host exposes it |
| **Core pipeline & routing** | Handler registry + `@message`, middleware pipeline, per-invocation DI, transport-neutral envelope — `benzene-core` (`registry.py`, `pipeline.py`, `router.py`, `envelope.py`, `dependencies.py`) over the `benzene-results` status vocabulary. See [core](reference/core.md), [results](reference/results.md). | Impose the Benzene envelope on transports that already carry routing (Kafka topic, message attributes) — routing stays native where the transport has it | For envelope-less triggers (S3, Blob, Timer) the binding supplies a preset/default topic you can override |
| **Message versioning** | `(topic, version)` registration; the version is the one `benzene-version` header (no `version`/`x-version` fallback; renamed in one place with `BenzeneMessageApplication(version_header=...)`), or an HTTP `/v{version}` route segment that supplies it exactly as the header would. Selection is the spec's (core-concepts §2): exact match, else `not-found` naming topic and version; a message with no version goes to the unversioned handler, else the one declared `default=True` (`@message` / `Registry.register` / `HttpRouter.register`); a topic left with only versioned handlers and no default warns at start-up (`NoDefaultHandlerWarning`). `highest_version` is an explicit opt-in selector, never the default — `benzene-core` (`registry.py`, `router.py`, `envelope.py`). Cast reachability (selection step 2) has no candidates here: payload casting is registration-level (`casting_handler` registers each version it serves), so the exact match finds it | Content-negotiation or semver-range resolution — version selection is a small injectable function, not a policy engine | Supply your own `VersionSelector` to `message_router` |
| **HTTP (ASGI)** | Inbound ASGI binding, Benzene↔HTTP status mapping, outbound HTTP client — `benzene-http` (`app.py`, `status.py`, `client.py`). See [http](reference/http.md). | Be a web framework (templating, sessions, static files) — it is one ASGI app your server of choice hosts | Run under uvicorn/hypercorn, or mount alongside a framework in your ASGI stack |
| **gRPC** | Benzene↔gRPC status mapping with the `benzene-status` trailer rule, structured errors as `google.rpc.BadRequest` on `grpc-status-details-bin`, server + client transport — `benzene-grpc` (`status.py`, `details.py`, `server.py`, `client.py`). See [grpc](reference/grpc.md). | Bundle `grpcio` unconditionally — it is the `[transport]` extra, so the mapping is usable without the SDK | `pip install "benzene-grpc[transport]"` for the real transport |
| **Kafka (self-hosted)** | Consumer loop + produce-side client, duck-typed against `confluent-kafka` (SDK optional) — `benzene-kafka` (`consumer.py`, `producer.py`). See [kafka](reference/kafka.md). | Ship a schema-registry codec (see the Schema registry row) or hide `confluent-kafka`'s configuration surface | Configure the `Consumer`/`Producer` yourself and hand them in |
| **RabbitMQ (self-hosted)** | Consumer + publish client, SDK optional — `benzene-rabbitmq` (`consumer.py`, `producer.py`). See [rabbitmq](reference/rabbitmq.md). | Manage exchanges/queues/topology for you — that is infrastructure, declared with your own `pika`/management tooling | Declare topology in your bootstrap or IaC; pass the channel in |
| **AWS** | Lambda host with API Gateway, SQS, SNS, S3, EventBridge, DynamoDB Streams, Kinesis, Kafka/MSK, and direct-invoke inbound (`app.py`, `events.py`); SNS/SQS/EventBridge/Kinesis/Lambda outbound senders (`clients.py`); a self-hosted SQS consumer (`sqs_consumer.py`); test builders/hosts (`testing.py`) — `benzene-aws`. See [aws](reference/aws.md). | Wrap `boto3` — outbound senders take your configured client; DynamoDB/S3 as *storage* is your handler's own code (a database is not a transport) | Use `boto3` directly in handlers for anything beyond message delivery |
| **Azure** | Functions host with HTTP, Service Bus, Event Hub, Queue Storage, **Blob Storage**, Cosmos DB change feed, Timer, and Event Grid inbound; Service Bus/Event Hub/Queue Storage/Event Grid outbound clients — `benzene-azure` (`app.py`, `events.py`, `clients.py`). (Blob inbound is a binding some sibling ports defer; this port ships it — `decode_blob_created` / `handle_blob`.) See [azure](reference/azure.md). | Carry an application payload for notification-shaped triggers (Blob, Timer) — they name a thing rather than carry a message, so the descriptor is the body under a default topic | Override `default_topic` and fetch the blob/document with the Azure SDK in the handler |
| **Google Cloud** | Cloud Functions host (HTTP + Pub/Sub inbound), Pub/Sub outbound client — `benzene-gcp` (`functions.py`, `pubsub.py`). See [gcp](reference/gcp.md). | Cover the rest of GCP's eventing surface (Eventarc, Cloud Tasks, …) — those bindings are not implemented, plainly | Decode the trigger payload into an envelope yourself and call `app.handle` |
| **Health checks (R3)** | `HealthChecks` + reserved `benzene:healthcheck` topic, answering `ok` whenever the checks ran with the verdict in `isHealthy` (wire-contracts §5); per-check `status` `ok`/`warning`/`failed` (a warning does not flip `isHealthy`), optional `type`/`data`; HTTP `GET /benzene/health` maps the verdict to 200/503 with the report as the body either way — `benzene-core` (`health.py`), `benzene-http` (`app.py`, `standard.py`). No liveness/readiness topics or routes | Probe infrastructure for you — a check is your async function | Write checks against your own dependencies; see [cloud-service-profile.md](cloud-service-profile.md) |
| **Spec endpoint (R5)** | Registry-derived `ContractDocument.derive` (contract-document.md, the format every port's client generator parses) served at `GET /benzene/spec` and by the reserved `benzene:spec` topic (`spec_interception`) for an absent/empty request or `type=benzene`; this port's native `ServiceSpec` for the extension type `native`; any other `type`, or a `format` other than `json`, answers `not-implemented` (HTTP 501) — `benzene-core` (`contract.py`, `spec.py`), `benzene-http` (`app.py`, `standard.py`). The HTTP route reads the `ServiceSpec`/`ContractDocument` directly rather than dispatching the topic through the pipeline (Phase 3a's Q6) | Hand-authored spec documents — the spec derives from the one registry so it cannot drift from what is served; OpenAPI/AsyncAPI from this topic (use `benzene-openapi`) | N/A — it is derived |
| **Envelope endpoint (R4)** | `POST /benzene/invoke`: HTTP 200 for every envelope it processed whatever its status (`deleted`, `not-found`, an `unauthorized` from the pipeline's own auth included), the response envelope as the body and any inner problem document without `status`; 400 only for a body that is not an envelope. Runs the same pipeline as every other transport; `StandardPaths(invoke_topics=...)` restricts reachable topics (any `in`-supporting container), a restricted topic answering `not-found` — `benzene-http` (`app.py`, `standard.py`) | Host-enforced carrier auth before the envelope is read (the 401/403 the spec allows) — there is no such layer in `BenzeneHttpApp`; put auth middleware in the pipeline, or in front of the ASGI app | An ASGI middleware ahead of `BenzeneHttpApp` for carrier-level auth |
| **Reserved topics** | `benzene:healthcheck` (`health_interception`), `benzene:spec` (`spec_interception`), `benzene:mesh` (`benzene-mesh`), the collector's `benzene:mesh:*` topics, and `benzene:ping` — answered `ok`, no payload, by the message router on every transport with no wiring, so a queue health probe is never redelivered — `benzene-core` (`router.py`) | Reject an application topic under `benzene:` (a port MAY; this one does not) | — |
| **Mesh (service-side, R6/R8)** | `ServiceDescriptor` (reserved `benzene:mesh`), `MeshFeedSender` (register/heartbeat/traces), `trace_middleware` (one `TraceEvent` per invocation, joins inbound W3C `traceparent`), `with_trace_propagation` outbound — `benzene-mesh` (`descriptor.py`, `feeds.py`, `trace.py`). See [mesh](reference/mesh.md). | Block or fail an invocation on trace/feed export — observers are lossy by contract | Point the feeds at a collector; see [joining the mesh](cookbooks/joining-the-mesh.md) |
| **Mesh collector** | `MeshCollector` ingest/query surface, `MeshPoller` (spec + health sweeps), catalog store, issue detection, inbox — `benzene-mesh` (`collector.py`, `poller.py`, `store.py`, `issues.py`, `inbox.py`); containerised host + Terraform in [`deploy/mesh/`](../deploy/mesh/README.md) | Persist the catalog durably out of the box — the shipped store is in-memory, rebuilt by polling | Run the poller on a timer (the deploy does); back the store yourself if you need survival across restarts |
| **Mesh UI hosting** | The collector serves the canonical cross-language `mesh-ui.html` at `/benzene/mesh-ui/` (design-principles §5.2; `mount=` to move it), projecting its catalog into the mesh-ui artifacts — `benzene-mesh` (`artifacts.py`, `s3_artifacts.py`, `blob_artifacts.py`), wired in [`deploy/mesh/`](../deploy/mesh/README.md). See [observing the mesh](cookbooks/observing-the-mesh.md). | Fork or restyle the UI per-port — the page is vendored from the main repo's one canonical UI | Publish the artifacts to static hosting (S3/Blob writers ship) if you don't run the collector's server |
| **Mesh fleet** | Service-discovery adapters (AWS Cloud Map/Lambda tags, Azure resource group, Kubernetes namespace) and trace mappers (Jaeger/Tempo/X-Ray), all SDK-lazy/injectable — `benzene-mesh-fleet` (`discovery_adapters.py`, `mappers.py`). See [mesh-fleet](reference/mesh-fleet.md). | Ship or run a tracing backend — the mappers are pure field/units transforms | Wire the mapped documents into your backend's ingest API yourself |
| **Codegen / typed clients** | The `benzene-codegen` CLI: typed client from any service's Contract Document, with schema closure, RFC 8785 contract hashing — `benzene-codegen-client`. See [codegen-client](codegen-client.md). | Runtime client generation or remote-registry lookups — it is a build-time tool | Run it in CI against the target's `/benzene/spec`-derived contract |
| **OpenAPI** | OpenAPI 3.1 document derived from the handler registry — `benzene-openapi` (`generator.py`). See [openapi](reference/openapi.md). | Serve Swagger UI — document generation only | Host the JSON wherever your docs tooling wants it |
| **Observability (OTel)** | Export the port's existing mesh traces through the OpenTelemetry SDK (SDK optional), plus response-as-event middleware with a pluggable `ResponseEventSink` — `benzene-otel` (`exporter.py`, `response_events.py`). See [otel](reference/otel.md). | Sampling/backend configuration (Jaeger, X-Ray, …) — Benzene exports via the standard OTel API, it doesn't ship a backend | Configure the OTel SDK/exporter in your composition root |
| **Caching** | Cache-aside (`CacheAside`) over a narrow async `Cache` port, with in-memory and Redis backends — `benzene-cache` (`aside.py`, `memory.py`, `redis.py`). See [cache](reference/cache.md). | Invalidation strategies, distributed cache coherence — TTL + explicit eviction is the whole model | Implement `Cache` over your own store for anything richer |
| **Validation** | Pydantic request validation as an optional adapter (`validated(Model)` decorator → `validation-error` with one structured error per bad field, its `field` an RFC 6901 JSON Pointer into the request as serialized — `/items/0/sku`, using the model's aliases; omitted for a `@model_validator` rule) — `benzene-pydantic` (`validation.py`). gRPC carries the pointer as `FieldViolation`'s dotted path (`items[0].sku`) and reads it back as a pointer — `benzene-grpc` (`details.py`). See [pydantic](reference/pydantic.md). | Make pydantic a core dependency — validation is an adoption choice, so the core never imports it | Or validate by hand in the handler; a plain dict request is always allowed |
| **Resilience** | Outbound: `with_retry` (status-aware, pluggable backoff) + correlation/trace decorators over the one `MessageSender` seam — `benzene-core` (`outbound.py`). Inbound: circuit breaker, bulkhead, rate limiting as middleware — `benzene-resilience`. See [resilience](reference/resilience.md). | Re-implement a policy toolkit (the Polly analogue) or hide one behind its own abstraction — policies are small composable decorators/middleware you can read in one screen | Compose the decorators (`with_retry(with_circuit_breaker(sender))`) or write your own middleware |
| **Idempotency** | Dedupe middleware keyed on an idempotency header, replaying the first result; only successes are remembered, and never a missing result (a failure the binding redelivers); pluggable `IdempotencyStore` port + in-memory store (single-process) — `benzene-resilience` (`idempotency.py`) | Cross-instance de-duplication — independent processes (separate Lambda invocations, containers) can't coordinate at runtime; shared state can relocate the race, not remove it | Implement `IdempotencyStore` over a store with an **atomic conditional write** (DynamoDB conditional put, Redis `SET NX`), and design handlers to be naturally idempotent |
| **Settlement (one-way bindings)** | transport-bindings §1 obligation 8 on every one-way binding — see the [per-binding breakdown](#settlement--the-per-binding-breakdown). A message is acknowledged only when its invocation ended `isSuccessful` (read from the envelope, never the status text); a failure, a missing result, an unmatched topic and an uncaught error are redelivered. Every one-way host takes `settlement=Settlement(at_most_once=..., ignore_unmatched_topics=...)`, both off by default — `benzene-core` (`settlement.py`), the hosts in `benzene-aws`, `benzene-azure`, `benzene-gcp`, `benzene-kafka`, `benzene-rabbitmq`. Each host declares `SETTLEMENT_DEFAULTS`, checked against the fixture by the conformance runner | A cross-transport reliability abstraction — settlement stays transport-native, and dead-lettering is each transport's own redrive configuration | Make handlers on one-way bindings idempotent (Idempotency row); configure the transport's redrive / DLQ; use `Settlement(...)` for the exceptions |
| **Sagas / workflows** | In-process compensation-based saga with reverse-order rollback, `Result`-speaking — `benzene-resilience` (`saga.py`) | Durable crash-resume — the saga is in-memory callables; nothing persists across a crash | For crash-durable or long-running workflows use a real orchestrator (Step Functions, Durable Functions, Temporal) started from a handler |
| **AuthN / AuthZ** | **Partial.** Basic auth (`basic.py`); bearer middleware + `JwtValidator` over PyJWT — verification against a **configured static key** (HMAC secret or RSA/EC public key) with algorithm/audience/issuer constraints (`bearer.py`); API Gateway custom-authorizer adapter (`authorizer.py`) — `benzene-auth`. See [auth](reference/auth.md). | **Not implemented:** JWKS fetch / OIDC discovery — there is no key-rotation-aware validation against an IdP's metadata (the .NET and Go ports ship this; not a design decision here, simply unbuilt) | `validate` is a plain callable: supply one that resolves signing keys from your IdP's JWKS (e.g. PyJWT's `PyJWKClient`) and pass it to `bearer_token_interception` |
| **Outbox (produce-side durability)** | **Not implemented.** The .NET port ships `Benzene.Outbox`; here the only trace is a seam note — `ResponseEventSink` (`benzene-otel`, `response_events.py`) can *feed* an outbox you build, but no store-and-forward, staging, or transactional write exists | — (nothing deliberate here; this is unbuilt, not declined) | Treat every send as at-least-once and pair the consumer with the Idempotency row; or implement store-and-forward as middleware over your own durable store |
| **Oversized payloads (claim check)** | **Not implemented.** The .NET port ships `Benzene.ClaimCheck` (+ S3/Blob stores); this port has no offload/hydrate middleware or `benzene-claim-check` header handling | — (unbuilt, not declined) | Keep payloads under the transport limit, or write an offload/hydrate middleware pair over S3/Blob carrying a reference header |
| **Schema registry / wire codecs** | **Not implemented.** No Confluent wire-format codec or schema-registry client (the .NET port ships both); the Kafka binding sends/receives plain envelopes | — (unbuilt, not declined) | Serialize with the `confluent-kafka` serializers yourself before/inside the producer you hand in |
| **Database / state access** | *(nothing — by design)* | Any database or state-store abstraction — a database is not a transport; wrapping one would hide its capabilities (the core anti-pattern) | Your handler uses `boto3`/`azure-sdk`/an ORM directly; Benzene delivers the event in, persistence is yours |
| **Testing** | In-memory test host + test doubles — `benzene-testing`; per-transport builders/hosts (e.g. `AwsLambdaTestHost`, `SqsConsumerTestHost` in `benzene-aws/testing.py`). See [testing](reference/testing.md). | Require cloud SDKs or emulators to test a handler — bindings are duck-typed/injectable precisely so tests run SDK-free | Use the builders; integration against real infrastructure is your CI's choice |

## Settlement — the per-binding breakdown

What each one-way binding does with a message, from source (2026-10-11, after the Phase 1 settlement
work). "Acknowledged" means the transport may forget the message; "redeliver" means Benzene declines
the acknowledgement and the transport's own retry/redrive configuration decides what happens next.

- Every binding settles through `dispatch_one_way` (`benzene-core`, `settlement.py`), which decodes the
  native message, runs `BenzeneMessageApplication.handle`, decodes the response envelope with
  `isSuccessful` authoritative (wire-contracts §1.2), and applies the binding's `Settlement`. One rule,
  so the columns below cannot drift binding by binding.
- `handle` turns **no result** into `unexpected-error` (`encode_response`) and a malformed JSON body
  into `bad-request`; the router turns an **unmatched topic** — no handler at any version — into
  `not-found` (or `ignored`, under `ignore_unmatched_topics`), a known topic at an unknown version into
  `not-found`, and a handler exception into `service-unavailable`. The **uncaught exception** column is
  an exception escaping the pipeline (a middleware's, or the binding's decode step).
- A success — `ignored` and an application-defined status marked successful included — is acknowledged
  everywhere.
- **Overrides, on every row:** `Settlement(at_most_once=True)` acknowledges whatever the outcome (a
  failure or an exception is logged at WARNING on `benzene.settlement` and dropped);
  `Settlement(ignore_unmatched_topics=True)` acknowledges a topic no handler serves as `ignored`.
  The self-hosted loops keep their manual knobs too (`delete=False` / `commit=False` / `ack=False` +
  `on_result`; RabbitMQ `requeue=False` to dead-letter), and Kafka adds `stop_on_failure`.
- On the envelope-less sources (S3, DynamoDB, Kinesis, Blob, Cosmos DB, Timer; EventBridge without a
  `detail-type`; plain-text Queue Storage) the topic is a configured convention, so with no handler for
  it *every* delivery is an unmatched topic.

| Binding | Package / entry point | Fixture id · shape | Failure / no result / unmatched topic | Uncaught exception | Unit of settlement |
|---|---|---|---|---|---|
| AWS Lambda SQS | `benzene-aws` · `AwsLambdaApp` | `sqs` · queue | redeliver (`batchItemFailures` by `messageId`) | redeliver — the whole batch (the invocation fails) | the message; every record runs; needs *ReportBatchItemFailures* |
| AWS Lambda SNS | `benzene-aws` · `AwsLambdaApp` | `sns` · one-shot | redeliver (raises¹) | redeliver (raises¹) | the whole invocation; stops at the first failing record |
| AWS Lambda S3 | `benzene-aws` · `AwsLambdaApp` | `s3` · one-shot | redeliver (raises¹) | redeliver (raises¹) | the whole invocation; stops at the first failing record |
| AWS Lambda EventBridge | `benzene-aws` · `AwsLambdaApp` | `eventbridge` · one-shot | redeliver (raises¹) | redeliver (raises¹) | the one event |
| AWS Lambda Kafka / MSK | `benzene-aws` · `AwsLambdaApp` | `kafka` · stream (redeliver) | redeliver (raises¹) | redeliver (raises¹) | the whole invocation, across every topic-partition in it; stops at the first failing record |
| AWS Lambda DynamoDB Streams | `benzene-aws` · `AwsLambdaApp` | `dynamodb-streams` · stream (redeliver) | redeliver (`batchItemFailures` by `SequenceNumber`) | redeliver — the whole batch | the record; every record runs, and Lambda resumes the shard from the lowest reported failure, so the records after it are redelivered too |
| AWS Lambda Kinesis | `benzene-aws` · `AwsLambdaApp` | `kinesis` · stream (redeliver) | redeliver (`batchItemFailures` by `sequenceNumber`) | redeliver — the whole batch | as DynamoDB Streams |
| AWS Lambda direct invoke, asynchronous | `benzene-aws` · `AwsLambdaApp(invocation_type="Event")` | `envelope-async` · one-shot | redeliver (raises¹) | redeliver (raises¹) | the one envelope. Lambda does not reveal the invocation type, so this is a configured mode; the default (`"RequestResponse"`) is the synchronous `envelope` binding |
| AWS SQS, self-hosted | `benzene-aws` · `SqsConsumerApp` + `run_sqs_consumer_loop` | `sqs` · queue | redeliver — not deleted | redeliver — the loop raises; this and the rest of the received batch stay undeleted | the message |
| Kafka, self-hosted | `benzene-kafka` · `KafkaConsumerApp` + `run_consumer_loop` | `kafka` · stream (**skip-and-continue**) | skip-and-continue — not committed, but the loop moves on and the next commit on the partition passes it (redelivered only if the consumer restarts or rebalances first). `stop_on_failure=True`: redeliver — the loop raises without committing, and a restart resumes at the record | redeliver — the loop raises; a restart resumes at the last committed offset | the record offset, cumulative per partition |
| RabbitMQ, self-hosted | `benzene-rabbitmq` · `RabbitMqConsumerApp` + `run_consumer_loop` | `rabbitmq` · queue | redeliver — `basic_nack(requeue=True)` (or `requeue=False` → dead-letter exchange) | redeliver — the loop raises; the unacked delivery is requeued when the channel closes | the delivery |
| Azure Functions Service Bus | `benzene-azure` · `AzureFunctionsApp.handle_service_bus` | `servicebus` · queue | redeliver (raises¹) | redeliver (raises¹) | the message |
| Azure Functions Queue Storage | `benzene-azure` · `handle_queue_storage` | `queue-storage` · queue | redeliver (raises¹) | redeliver (raises¹) | the message |
| Azure Functions Event Hub | `benzene-azure` · `handle_event_hub` | `eventhub` · stream (redeliver) | redeliver (raises¹) | redeliver (raises¹) | the delivered batch; stops at the first failing event, and the batch is redelivered from its checkpoint |
| Azure Functions Cosmos DB change feed | `benzene-azure` · `handle_cosmos` | `cosmosdb-changefeed` · stream (redeliver) | redeliver (raises¹) | redeliver (raises¹) | the delivered batch; stops at the first failing document |
| Azure Functions Event Grid | `benzene-azure` · `handle_event_grid` | `event-grid` · one-shot | redeliver (raises¹) | redeliver (raises¹) | the invocation's event (a batch stops at the first failing event) |
| Azure Functions Blob Storage | `benzene-azure` · `handle_blob` | `blob-storage` · one-shot | redeliver (raises¹) | redeliver (raises¹) | the notification |
| Azure Functions Timer | `benzene-azure` · `handle_timer` | `timer` · one-shot | raises¹ — the run is recorded as failed for the host's retry policy; there is no message | raises¹ | the tick |
| GCP Cloud Functions Pub/Sub | `benzene-gcp` · `GcpFunctionsApp.handle_pubsub` | `pubsub` · queue | redeliver (raises¹) | redeliver (raises¹) | the message |

Request/response bindings settle by responding (`request-response`, no default): `http` (the ASGI
binding and every cloud host's HTTP trigger), `envelope` (`POST /benzene/invoke`, and a synchronous
Lambda direct invoke), `grpc`.

¹ **raises** — the binding raises `MessageHandlingError` (`benzene-core`, `errors.py`; it names the
topic, the status and the problem document), or lets the exception propagate, out of the trigger
function. Whether and how often the platform then redelivers — Lambda's async retries and on-failure
destinations, the Functions host's retry, poison-queue and dead-letter handling, a Cloud Function's
retry-on-failure setting — is trigger configuration Benzene does not set and this audit cannot verify
from source. For the checkpointing Functions triggers (Event Hub, Cosmos DB) and Pub/Sub-triggered
functions in particular, confirm retries are enabled.


**No binding, so nothing to settle (not implemented):** a self-hosted Azure Service Bus, Event Hub or
Cosmos DB worker; an Azure Functions Kafka trigger; a self-hosted Pub/Sub subscriber.

**Pinned by tests:** `tests/test_settlement.py` runs one outcome table — success, `ignored`, an
application-defined success, a failure, a missing result, an unmatched topic, an unknown version, a
middleware exception — through every row above, by default and under both overrides, plus Kafka's
skip-and-continue and `stop_on_failure`; the per-transport tests (`tests/test_aws_host.py`,
`tests/test_aws_parity.py`, `tests/test_aws_sqs_consumer.py`, `tests/test_kafka.py`,
`tests/test_rabbitmq.py`, `examples/azure_orders/`, `examples/gcp_orders/`) keep their own cases; the
conformance runner checks each host's `SETTLEMENT_DEFAULTS` against `transport-metadata-cases.json`.

### Where this differs from the .NET reference (each allowed by the spec)

- **Stream triggers redeliver an unmatched topic and a missing result.** .NET accepts an
  unrouted/null outcome on Lambda Kafka and the Azure Event Hub trigger so it cannot replay a partition
  forever; here both are failures, as obligation 8 states, so an unhandled topic re-fails the
  invocation on every retry — set `ignore_unmatched_topics` for a shared stream.
- **Lambda Kafka / MSK fails the whole invocation** at the first failing record; .NET defaults to
  per-partition partial-batch reporting.
- **Lambda DynamoDB Streams does not stop at the first failure.** Every record runs and every failure
  is reported; .NET stops at the first failed record. Lambda resumes from the lowest reported
  sequence number either way.
- **Lambda Kinesis and the Azure Cosmos DB trigger route per record.** .NET treats both as fan-in
  streams; here each record or document is routed under a convention topic and its result settles it.
- **The self-hosted Kafka loop defaults to skip-and-continue**, as .NET's worker does; its
  at-least-once mode stops the loop (`stop_on_failure=True`) rather than .NET's `CommitOnlyOnSuccess`
  offset store, with the same effect on restart.

## Why "we don't do that" is a feature

Every deliberate "no" above buys you something: you keep the full power of the tool you chose,
you're never blocked by a leaky abstraction, and the surface you depend on stays small. The three
*not implemented* rows (outbox, claim check, schema registry) and the auth partial are stated
plainly because they are gaps, not philosophy — a port that does less, said honestly, is still
aligned with Benzene; a port that claims more than it does is not. When you need something this
port doesn't ship, a custom middleware or a `MessageSender` decorator is the supported, documented
path — see [core](reference/core.md).
