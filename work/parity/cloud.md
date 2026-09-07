# Parity gap analysis — cloud transport depth

**Domain:** AWS / Azure / GCP / Kafka / RabbitMQ / gRPC.
**Reference:** `/workspace/benzene-dotnet` (`Benzene.Aws.*`, `Benzene.Azure.*`, `Benzene.GoogleCloud.*`,
`Benzene.Clients.*`, `Benzene.Kafka.Core`, `Benzene.RabbitMq`, `Benzene.Grpc*`).
**Target:** `/home/user/benzene-python` (`packages/benzene-{aws,azure,gcp,kafka,rabbitmq,grpc}`).
**Date:** 2026-09-07. **Analysis only — no code was modified.**

Breadth is already at parity: Python binds every inbound source and most outbound transports the .NET
port does. Everything below is **depth** — durability, throughput, poison-message handling, and the
self-hosted (non-serverless) hosting story.

> **Working-tree caveat.** During this analysis the repo was briefly mid-merge with `origin/main`,
> which appeared to carry `benzene.core.worker` (`WorkerHost`/`StopSignal`/`*_worker` factories),
> `benzene.grpc.details` (rich-error trailers), and an `EventHubMessageSender`. The tree then reverted
> to `HEAD` (`ab93d72`) without them. **Before implementing gaps 8, 10 and the Event Hub half of 5,
> re-check `origin/main`** — they may already be closed there. Everything else was verified against
> `HEAD` and is genuinely absent.

> **Frozen-contract note.** Nothing in this document changes the wire envelope, the `topic`
> metadata key, the `benzene-status` trailer, the status vocabulary, or any conformance fixture.
> Two items *touch adjacent wire surface* and are flagged inline: **gap 1** (AMQP `delivery_mode`,
> a broker-side property, not Benzene metadata) and **gap 10** (`grpc-status-details-bin`, which is
> specified by `conformance/grpc-status-mapping.json` / `problem-details-cases.json` — check the
> fixtures before writing it).

---

## Priority summary

| # | Gap | Severity |
|---|-----|----------|
| 1 | RabbitMQ publishes non-persistent — silent loss on broker restart | **Critical** |
| 2 | Kafka has no dead-letter/retry path — a poison record wedges the partition forever | **Critical** |
| 3 | No batch producers anywhere (SQS/SNS/EventBridge/Kinesis/Service Bus/Event Hub/Event Grid/Pub/Sub/Kafka) | **High** |
| 4 | RabbitMQ consumer uses `basic_get` polling, not a push consumer with prefetch | **High** |
| 5 | No self-hosted Azure workers (Service Bus / Event Hub / Cosmos change feed) | **High** |
| 6 | gRPC server spins a fresh event loop per RPC (`asyncio.run`) | **High** |
| 7 | Service Bus sender has no broker-level properties (`session_id`, `message_id`, TTL, schedule) | **High** |
| 8 | SQS/SNS: no FIFO, no 10-attribute guard, no empty-attribute skip | **Medium** |
| 9 | SQS consumer: no `ApproximateReceiveCount`, no batch concurrency, no visibility heartbeat | **Medium** |
| 10 | gRPC: no deadline/timeout propagation | **Medium** |
| 11 | Kafka producer: no message key (no per-key ordering possible) | **Medium** |
| 12 | Kafka/SQS consumers are strictly sequential — no bounded concurrency | **Medium** |
| 13 | No Step Functions client | **Medium** |
| 14 | Pub/Sub: no ordering key, no pull-subscriber worker | **Low** |
| 15 | No in-Lambda X-Ray subsegment tracing | **Low** |
| 16 | gRPC: unary only (no streaming shapes) | **Low** |

---

## 1. RabbitMQ publishes non-persistent — silent message loss

**Severity: Critical** (data loss, one-line fix).

**.NET:** `Benzene.RabbitMq/CLAUDE.md` — *"Publish is **persistent by default** (delivery mode 2) so a
message on a durable queue survives a broker restart; pass `.UseRabbitMqClient(channel, persistent:
false)` for transient delivery. This is a behavioral change from earlier versions, which always
published transient."* The .NET port hit this bug and fixed it deliberately.

**Python:** `packages/benzene-rabbitmq/benzene/rabbitmq/producer.py::RabbitMqMessageSender._properties`
builds `pika.BasicProperties(headers=headers)` with no `delivery_mode`. pika's default is `1`
(transient). Every message published onto a durable queue is lost when the broker restarts, and
`send_message` still returns `Result.ok()`. Silent, and the exact failure mode .NET already fixed.

### Implementation spec

* Package: `benzene-rabbitmq`. No new dependency, no new extra.
* `RabbitMqMessageSender.__init__(..., *, persistent: bool = True)`.
* `_properties()` → `pika.BasicProperties(headers=headers, delivery_mode=2 if self._persistent else 1)`.
  Mirror the field on the `_AmqpProperties` SDK-free stand-in dataclass (`delivery_mode: int = 2`)
  so the fake asserts the same shape.
* Document the default flip in the class docstring and the package README, matching .NET's wording:
  a durable queue plus a persistent message is the only combination that survives a broker restart.
* Tests (`packages/benzene-rabbitmq/tests/`): default publish carries `delivery_mode == 2`;
  `persistent=False` carries `1`; header/topic forwarding unchanged.
* **Wire note:** `delivery_mode` is an AMQP broker property, not Benzene metadata — no conformance
  fixture covers it. Safe.

---

## 2. Kafka has no dead-letter / retry path — a poison record wedges the partition

**Severity: Critical** (availability; a single bad record halts a partition indefinitely).

**.NET:** `Benzene.Kafka.Core` ships `KafkaDeadLetterOptions<TKey,TValue>`
(`Benzene.Kafka.Core/CLAUDE.md`): retry a failing record up to `MaxAttempts`, then re-produce the
**original** record (key, value, headers) to `DeadLetterTopic` through a caller-built producer, adding
`x-dlt-reason` (exception **type name** only, never the message text), `x-dlt-original-topic`,
`x-dlt-original-partition`, `x-dlt-original-offset`; then advance past it. If the dead-letter *produce*
itself fails, the offset is **not** stored and the worker stops — trading availability for no-loss.
Enforced invariants: `EnableAutoOffsetStore=false`, `PreserveOrderPerPartition=true`.

**Python:** `packages/benzene-kafka/benzene/kafka/consumer.py::run_consumer_loop`. On a failure it
`seek`s back to the record's offset and keeps a `blocked` map so nothing commits past it. Correct and
safe — but **unbounded**: the loop re-delivers the same poison record forever, logging a warning each
time, and the partition never advances. The docstring says *"callers cap that with the `on_result` /
`should_continue` seams"* — i.e. the framework hands the hardest part back to the user.

The offset-blocking design itself is *better* than .NET's watermark handling and should be kept. What
is missing is the bounded exit.

### Implementation spec

* Package: `benzene-kafka`. No new required dependency (the DLT producer is injected).
* New frozen dataclass in `consumer.py`:

  ```python
  @dataclass(frozen=True)
  class DeadLetterOptions:
      topic: str                     # the dead-letter topic to produce to
      producer: Any                  # duck-typed: produce(topic, value=, key=, headers=) + flush(timeout)
      max_attempts: int = 3          # total deliveries of one record before dead-lettering
      flush_timeout: float = 10.0
  ```
* `run_consumer_loop(..., dead_letter: DeadLetterOptions | None = None)`.
* Behaviour, keyed on `(topic, partition, offset)`:
  * count attempts per blocked offset; while `attempts < max_attempts`, `seek` back exactly as today;
  * on the `max_attempts`-th failure, re-produce the **original bytes** — `message.key()`,
    `message.value()`, `message.headers()` verbatim (never the decoded envelope: the wire body must
    round-trip unmodified for replay) — to `dead_letter.topic`, appending
    `x-dlt-reason` (the `Result.status` string, **never** an exception message or payload),
    `x-dlt-original-topic`, `x-dlt-original-partition`, `x-dlt-original-offset`;
  * then unblock the key and `commit` past the record;
  * if the dead-letter produce or flush fails, do **not** commit, log at `error`, and stop the loop
    (return) — matching .NET's no-loss trade. Redelivery on restart is the correct outcome.
* All produce/flush calls go through `asyncio.to_thread`, matching every other call in the module.
* Tests (`packages/benzene-kafka/tests/`), all with the existing in-memory fakes, no broker:
  retries `max_attempts` times then produces to the DLT with the four headers and the original key
  and value bytes; commits past the record afterwards; a raising producer leaves the offset
  uncommitted and stops the loop; `dead_letter=None` preserves today's block-forever behaviour byte
  for byte.
* **Do not port** .NET's `CommitOnlyOnSuccess`/`PreserveOrderPerPartition` startup-validation matrix.
  Those guard a failure mode created by .NET's concurrent dispatcher and `StoreOffset` watermark;
  Python's sequential loop plus the `blocked` map has neither.

---

## 3. No batch producers anywhere

**Severity: High** (throughput and API cost; the gap .NET issue #30 names).

**.NET:** a shared seam plus six implementations —
`Benzene.Clients/IBenzeneBatchMessageClient.cs`, `BatchSendResult.cs` (`Failures` of
`FailedBatchEntry{Index, ErrorCode, ErrorMessage}` + `AllSucceeded`), `BatchSend.cs`
(`Chunk<T>(items, chunkSize)` pairing each item with its original index), and
`Benzene.Clients.Aws.Sqs/SqsBatchMessageClient.cs` (`SendMessageBatch`, ≤10),
`Benzene.Clients.Aws.Sns/SnsBatchMessageClient.cs` (`PublishBatch`, ≤10),
`Benzene.Clients.Aws.EventBridge/EventBridgeBatchMessageClient.cs` (`PutEvents`, ≤10, **positional**
failure mapping — `PutEvents` has no per-entry id),
`Benzene.Clients.Azure.ServiceBus/ServiceBusBatchMessageClient.cs` (`ServiceBusMessageBatch`),
`Benzene.Clients.Azure.EventHub/EventHubBatchMessageClient.cs` (`EventDataBatch`, grouped by
partition key because a batch's key is fixed at creation),
`Benzene.Clients.Azure.EventGrid/EventGridBatchMessageClient.cs` (`SendEventsAsync`, chunk 100).

Two failure models, documented per transport: **AWS = per-entry** (the provider names exactly which
entries failed); **Azure = per-batch/atomic** (a failed send reports every message in that batch).

**Python:** every sender in `packages/benzene-aws/benzene/aws/clients.py`,
`packages/benzene-azure/benzene/azure/clients.py`, `packages/benzene-gcp/benzene/gcp/pubsub.py`,
`packages/benzene-kafka/benzene/kafka/producer.py`, `packages/benzene-rabbitmq/.../producer.py` is
strictly one message per API call. Publishing 1,000 events costs 1,000 round trips instead of 100.

### Implementation spec

**Shared seam — `packages/benzene-core/benzene/core/clients.py`** (alongside `MessageSender`):

```python
@dataclass(frozen=True)
class FailedMessage:
    index: int                    # position in the caller's list
    status: str                   # a Benzene status (§3 vocabulary), e.g. "service-unavailable"
    detail: str | None = None     # the provider's error code/message, if any

@dataclass(frozen=True)
class BatchResult:
    failures: tuple[FailedMessage, ...] = ()
    @property
    def all_succeeded(self) -> bool: ...
    def __bool__(self) -> bool: ...          # truthy when everything sent

@runtime_checkable
class BatchMessageSender(Protocol):          # separate Protocol; MessageSender is untouched
    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult: ...

def chunked(items: Sequence[T], size: int) -> Iterator[list[tuple[int, T]]]: ...
```

`messages` is a sequence of `(topic, message)` pairs so one batch can span Benzene topics — the
transports are all header/attribute-routed, so this costs nothing. `headers` is per-call and applies
to every entry, exactly like `send_message`. A per-entry-headers overload is *not* needed for 1.0.

**Per-transport implementations** — each is an additional `async def send_batch` on the *existing*
sender class (never a second class; Python has no reason to split the interface the way C# does).
`BatchMessageSender` is structural, so a class gains it by defining the method:

| Sender (file) | Native call | Chunk | Failure model |
|---|---|---|---|
| `SqsMessageSender` (`aws/clients.py`) | `send_message_batch(QueueUrl=, Entries=[{Id, MessageBody, MessageAttributes}])` | 10 | per-entry: `Id` = `str(index)`; read `response["Failed"]` → `FailedMessage(int(Id), "service-unavailable", f"{Code}: {Message}")` |
| `SnsMessageSender` | `publish_batch(TopicArn=, PublishBatchRequestEntries=[{Id, Message, MessageAttributes}])` | 10 | per-entry, same `Id`-carries-index trick |
| `EventBridgeMessageSender` | `put_events(Entries=[...])` | 10 | **positional** — `response["Entries"][i]` pairs with request entry `i`; a failed entry carries `ErrorCode`/`ErrorMessage`. Map by position *within each chunk*, then offset by the chunk start |
| `KinesisMessageSender` | `put_records(StreamName=, Records=[{Data, PartitionKey}])` | 500 | positional, same as EventBridge; `FailedRecordCount` gates the walk |
| `ServiceBusMessageSender` (`azure/clients.py`) | `sender.create_message_batch()` + `try_add_message` until `ValueError`, then `send_messages(batch)` | size-bounded | **per-batch atomic**: a raising send fails every index in that batch. A single message that will not fit an empty batch is its own failure (`"payload-too-large"`) without aborting the rest |
| `EventGridMessageSender` | `client.send([...])` | 100 | per-chunk atomic |
| `PubSubMessageSender` (`gcp/pubsub.py`) | N `publish()` calls, then `gather` the futures | 1000 | per-future: one `FailedMessage` per raising future |
| `KafkaMessageSender` (`kafka/producer.py`) | N `produce()` calls, **one** `flush()` | — | per-delivery-callback; the callback already exists, key it by index |
| `RabbitMqMessageSender` | N `basic_publish` under the existing lock, one thread hop | — | per-publish; a raising publish is that index |

Rules that must hold for every implementation:

* Run the whole chunk inside **one** `asyncio.to_thread` hop — the point is fewer thread hops as
  well as fewer round trips.
* A chunk-level exception (throttle, expired credentials, network) fails **only that chunk's**
  indices and the loop continues, so earlier chunks' successes are not discarded and the caller
  resends exactly what failed. This is the behaviour `SqsBatchMessageClient.cs` documents explicitly
  and is the whole reason indices are threaded through.
* A per-entry serialization failure is that entry's failure (`"bad-request"`), never an abort.
* A missing SDK still raises the teaching `ImportError` and is **never** mapped to a failure entry —
  same rule as `send_message` (see the `_boto3()` docstring in `aws/clients.py`).
* Reuse the existing attribute/envelope builders verbatim (`_string_attributes`, `_embed_headers`,
  `_make_message`, `_make_event`) so batch and single send put **identical** bytes on the wire.
* No new dependencies, no new extras — every one of these calls already exists in the pinned SDKs.

**Tests:** one shared parametrised suite per package driving the existing injected fakes:
25 messages → 3 chunks for a 10-cap transport; per-entry failure maps to the right caller index; a
chunk that raises fails only its own indices; empty input → `BatchResult()` with zero API calls;
batch and single send produce byte-identical attributes for the same message.

---

## 4. RabbitMQ consumer polls with `basic_get` instead of a push consumer

**Severity: High** (throughput; roughly an order of magnitude).

**.NET:** `Benzene.RabbitMq/CLAUDE.md` — `RabbitMqWorker` sets **prefetch QoS**, consumes with an
`AsyncEventingBasicConsumer` (deliveries are *pushed*), and fans out through
`BoundedConcurrentDispatcher<T>` so up to `ConcurrentRequests` handlers run at once; prefetch bounds
unacked deliveries. `StopAsync` cancels the consumer, drains in-flight work to `DrainTimeout`, then
closes channel and connection.

**Python:** `packages/benzene-rabbitmq/benzene/rabbitmq/consumer.py::run_consumer_loop` calls
`channel.basic_get(queue)` — one blocking AMQP round-trip per message, on a fresh `to_thread` hop,
strictly one at a time, with an `idle_sleep` backoff bolted on because an empty `basic_get` returns
immediately. The docstring is honest about it (*"pika's callback model is flattened to a poll"*), but
the cost is real: throughput is bounded by round-trip latency, no prefetch is possible, and an idle
consumer adds up to `idle_sleep` of latency to the first message after a quiet period.

### Implementation spec

* Package: `benzene-rabbitmq`. Keep `run_consumer_loop` **exactly as it is** — it is the SDK-free,
  fake-driven path the tests use and the simplest thing that works. Add a second entry point.
* `async def run_push_consumer(app, channel, *, queue, prefetch=10, concurrency=5, ack=True, requeue=True, stop=None, on_result=None) -> None`
  in the same module.
* Mechanics: `channel.basic_qos(prefetch_count=prefetch)`; register a `basic_consume` callback that
  **copies the body** (pika hands back a buffer that is only valid inside the callback — .NET flags
  the same hazard) and pushes `(method, properties, body_bytes)` onto an `asyncio.Queue`, via
  `loop.call_soon_threadsafe`; run pika's `connection.process_data_events` pump on a
  `to_thread`; drain the queue with `concurrency` `asyncio.Task` workers; ack/nack from the event
  loop by scheduling onto the pika thread (`connection.add_callback_threadsafe`) — a pika channel is
  not thread-safe, and the producer's docstring already documents that constraint.
* Reuse the **existing** settlement policy verbatim — success → `basic_ack`; a retryable status
  (`DEFAULT_RETRYABLE`) → `basic_nack(requeue=True)`; any other failure → `basic_nack(requeue=False)`
  so the queue's DLX takes it. That policy is already better than .NET's `Redelivered`-boolean
  one-retry heuristic; do not replace it.
* Shutdown: on `stop`, `basic_cancel`, then await the queue drain bounded by a `drain_timeout`
  (default 30s), then let the caller close the channel.
* Tests: a fake channel that invokes the registered callback N times — ack/nack outcomes match the
  polling loop's exactly; `prefetch` is applied via `basic_qos`; `concurrency` bounds in-flight
  handlers (assert with a counting handler); cancel drains in-flight work before returning.

---

## 5. No self-hosted Azure workers (Service Bus / Event Hub / Cosmos change feed)

**Severity: High** (hosting-model hole; Azure is serverless-only in Python).

**.NET:** three standalone, non-Functions workers —
`Benzene.Azure.ServiceBus` (`BenzeneServiceBusWorker`, `ServiceBusProcessor`, `AckMode` defaulting to
`Explicit`, `MaxConcurrentCalls`, `PrefetchCount`, `MaxAutoLockRenewalDuration`, and a
`ServiceBusSettlementHolder` letting a handler request Complete/Abandon/DeadLetter/Defer),
`Benzene.Azure.EventHub` (`BenzeneEventHubWorker`, `EventProcessorClient`, blob checkpointing,
`CheckpointInterval`, `DefaultStartingPosition`, `CatchHandlerExceptions`), and
`Benzene.Azure.CosmosDb` (`BenzeneCosmosChangeFeedWorker<TDocument>`, change-feed processor with
manual batch checkpointing).

**Python:** `packages/benzene-azure/benzene/azure/` contains only `app.py` (Azure Functions decorators),
`clients.py`, `events.py`. There is **no** consumer loop. Python ships self-hosted loops for
SQS (`aws/sqs_consumer.py`), Kafka and RabbitMQ — Azure is the one cloud where a Benzene service
cannot run on AKS, ACA, or a container at all.

### Implementation spec

Ship **Service Bus first** — it is the one with real settlement semantics and the one that unblocks
gap 6 (sessions). Event Hub second. **Skip Cosmos change feed for now** (see "Not worth porting").

* Package: `benzene-azure`, new module `benzene/azure/service_bus_consumer.py`. Optional extra
  `benzene-azure[servicebus]` already exists — no new dependency.
* Follow the SQS/Kafka module shape exactly, so the three read identically:
  * `class ServiceBusConsumerApp` with `__init__(application)`, `from_definition(definition)`, and
    `async def handle_message(message) -> Result` that never raises;
  * `def decode_service_bus_message(message) -> dict` — reuse `decode_service_bus` from
    `events.py` if its shape fits the SDK's `ServiceBusReceivedMessage`, otherwise a sibling that
    reads `application_properties` for the topic (`TOPIC_PROPERTY`) and headers, and the body as
    UTF-8. **The topic/header convention is frozen — do not invent a new one.**
  * `async def run_consumer_loop(app, receiver, *, max_messages=10, max_wait_time=5.0, should_continue=..., settle=True, on_result=None) -> None`.
* Duck-type the **receiver**, not the client: `receive_messages(max_message_count=, max_wait_time=)`,
  `complete_message(m)`, `abandon_message(m)`, `dead_letter_message(m, reason=, error_description=)`.
  That is the whole surface, so the loop is testable in memory with a fake — the standing promise in
  every other Python binding.
* Settlement policy, mirroring the RabbitMQ loop's status-aware rule (which is the best one in the
  codebase) and .NET's `Explicit` default:
  * success → `complete_message`;
  * a status in `DEFAULT_RETRYABLE` → `abandon_message` (redelivered, subject to the entity's
    max-delivery-count and DLQ);
  * any other failure (`bad-request`, `not-found` — deterministic, will fail identically forever) →
    `dead_letter_message(reason=result.status)`. Never put the exception text or payload in the
    reason.
  * `settle=False` hands settlement entirely to `on_result`.
* Run every blocking SDK call through `asyncio.to_thread`, as every other loop does. Note in the
  docstring that `azure-servicebus` also ships an `aio` client, and that a future async-native path
  is the natural follow-up — but the `to_thread` version keeps the module consistent and testable now.
* Lock renewal: expose `auto_lock_renewer` as an injected object (the SDK's
  `AutoLockRenewer`), passed through untouched — Benzene must not wrap the SDK's own capability
  (design principle 1). This is the Service Bus answer to gap 9's visibility heartbeat.
* Event Hub follow-up, same module shape: `benzene/azure/event_hub_consumer.py`, duck-typing the
  `EventHubConsumerClient`'s `receive_batch` callback, checkpointing every
  `checkpoint_interval` successfully handled events, never checkpointing a failed one.
* Tests: fake receiver replaying a scripted list; complete on success, abandon on retryable,
  dead-letter on deterministic failure, `settle=False` touches nothing, and a poison message never
  escapes `handle_message`.
* Also add worker factories (`service_bus_consumer_worker`) if/when `benzene.core.worker.WorkerHost`
  lands from `origin/main` — check first.

---

## 6. gRPC server spins a fresh event loop per RPC

**Severity: High** (correctness in an async-first framework, plus per-call overhead).

**Python:** `packages/benzene-grpc/benzene/grpc/server.py::BenzeneGrpcHandler._invoke` calls
`asyncio.run(pending)` for **every RPC**, on a `grpc.server` thread-pool thread. Consequences:

* a fresh event loop is created and destroyed per call — any loop-bound resource a handler touches
  (an `aiohttp` session, an `asyncpg`/`motor` pool, an async client cached at module scope, an
  `asyncio.Lock` — which `RabbitMqMessageSender` holds) was created on a *different* loop and either
  raises or corrupts state on the second call;
* `asyncio.to_thread` inside a handler dispatches onto a per-loop executor that is torn down each
  call, so a handler that publishes downstream (the SQS/Kafka senders all do) pays thread-pool
  churn per RPC;
* it is the only transport in the Python port that does this — `AwsLambdaApp` does the same at the
  Lambda boundary, but there the process handles one invocation at a time; a gRPC server is
  concurrent by construction.

**.NET:** `Benzene.Grpc` hosts on the ambient async host with no analogue of this problem.

### Implementation spec

* Package: `benzene-grpc`, `benzene/grpc/server.py`. Add an **async-native** handler alongside the
  sync one; do not delete `BenzeneGrpcHandler` (a caller on `grpc.server` still needs it).
* `class BenzeneAioGrpcHandler(grpc.aio.ServerInterceptor | grpc.GenericRpcHandler)` — the
  `grpc.aio` generic-handler shape, whose method handler is an `async def` awaiting
  `self._application.handle(...)` directly. No `asyncio.run`, no thread hop, one loop for the
  process.
* `def add_benzene_aio_handler(server, application) -> None` mirroring `add_benzene_handler`.
* Wire behaviour must be **byte-identical** to the sync handler: same `topic_for`/`method_for`,
  same `benzene-status` trailer on success *and* failure, same status→code mapping via
  `codes.status_to_code`, same `set_details`. Reuse the shared helpers rather than duplicating them —
  extract the response→(code, detail, trailers) computation into one module-level function both
  handlers call, so the two can never drift.
* Keep `grpcio` in the existing `transport` extra — `grpc.aio` ships in the same wheel.
* Document in the module docstring that the aio handler is the recommended path for any handler that
  touches async resources, and that the sync handler is for callers already committed to
  `grpc.server`.
* Tests: reuse `packages/benzene-grpc/benzene/grpc/testing.py`'s fakes against the aio handler;
  assert the trailer/code/details match the sync handler's for the same response, and add a
  regression test that a module-scoped `asyncio.Lock` (or any loop-bound object) survives two
  consecutive calls — the exact thing `asyncio.run` breaks today.
* **Frozen-contract check:** the trailer name, the status→code table and the details behaviour are
  covered by `conformance/grpc-status-mapping.json`. The aio handler must reuse the same mapper, not
  reimplement it. Run the conformance suite.

---

## 7. Service Bus sender has no broker-level properties

**Severity: High** (blocks duplicate detection, sessions, scheduling, TTL).

**.NET:** `Benzene.Clients.Azure.ServiceBus/CLAUDE.md` — `ServiceBusSenderProperties` maps named
headers onto broker-level `ServiceBusMessage` fields: `MessageIdHeader` → `MessageId` (enables
broker-side duplicate detection under at-least-once), `SessionIdHeader` → `SessionId` (**required** to
produce to a session-enabled entity), `ScheduledEnqueueTimeHeader` → `ScheduledEnqueueTime`,
`TimeToLiveHeader` → `TimeToLive`. Opt-in; the header also remains a plain application property.

**Python:** `packages/benzene-azure/benzene/azure/clients.py::ServiceBusMessageSender._make_message`
builds `self._message_factory(body, properties)` where `properties` is `application_properties` only.
There is no way to set `session_id`, so **Python cannot publish to a session-enabled entity at all** —
the broker rejects the send. Nor can it use duplicate detection, scheduled enqueue, or per-message TTL.

This is the producer half of the "Azure Service Bus sessions" question. The consumer half is gap 5:
.NET **does** have session consumption (`BenzeneServiceBusConfig.SessionsEnabled` +
`ServiceBusSessionProcessor`, `MaxConcurrentSessions`, `MaxConcurrentCallsPerSession`) — the issue
that claims otherwise is stale. Python has neither half. Shipping gap 7 alone is cheap and closes the
producer side; the consumer side arrives with gap 5's worker, where session support is a
`sessions_enabled` flag selecting `client.get_queue_receiver(session_id=NEXT_AVAILABLE_SESSION)`.

### Implementation spec

* Package: `benzene-azure`. No new dependency.
* New frozen dataclass in `clients.py`:

  ```python
  @dataclass(frozen=True)
  class ServiceBusSenderProperties:
      message_id_header: str | None = None
      session_id_header: str | None = None
      scheduled_enqueue_time_header: str | None = None   # ISO-8601 timestamp
      time_to_live_header: str | None = None             # seconds, or an ISO-8601 duration
  ```
* `ServiceBusMessageSender.__init__(..., sender_properties: ServiceBusSenderProperties | None = None)`.
* Extend the `message_factory` signature to `(body, application_properties, broker_properties)` where
  `broker_properties` is a `dict[str, Any]` of already-resolved SDK kwargs
  (`message_id` / `session_id` / `scheduled_enqueue_time_utc` / `time_to_live`), omitting unset ones.
  Keep backwards compatibility by inspecting the factory's arity, or — cleaner — default the third
  parameter and document the new shape; the factory is a documented test seam, so a signature change
  needs a note in the changelog.
* The named header stays in `application_properties` as well (matching .NET), so an inbound Benzene
  consumer still sees it as a header.
* Parse failures (an unparseable timestamp/duration) are a **`bad-request` `Result`**, not an
  exception and not a silent drop — the send never happened.
* Tests: each header maps onto the right broker field and remains an application property; unset
  headers produce no kwarg at all; a malformed timestamp returns `bad-request` without calling the
  SDK; the default (no `sender_properties`) is byte-identical to today.

---

## 8. SQS/SNS: no FIFO, no 10-attribute guard, no empty-attribute skip

**Severity: Medium** (one silent-failure mode, one capability hole).

**.NET:** `Benzene.Clients.Aws.Sqs/CLAUDE.md` and `Benzene.Clients.Aws.Sns/CLAUDE.md` document three
guards Python lacks:

1. **10-attribute cap.** *"SQS caps a message at 10 message attributes (the routing topic attribute
   counts toward it). Both converters fail fast with a clear `InvalidOperationException` naming the
   count."* Without it the SDK throws an opaque error the send path swallows into a generic
   `ServiceUnavailable`.
2. **Empty attribute values are skipped.** SNS and SQS both reject an empty message-attribute value.
   .NET omits the `topic` attribute when the topic is empty and skips any empty-valued header.
   *"LocalStack tolerates empty values, but real SQS does not"* — so this fails only in production.
3. **FIFO + numeric filter typing** (`SnsPublishOptions`): `MessageGroupIdHeader` → `MessageGroupId`,
   `MessageDeduplicationIdHeader` → `MessageDeduplicationId` (both required by `.fifo` topics), and
   `InferNumericAttributeTypes` so numeric subscription filter policies match.

**Python:** `packages/benzene-aws/benzene/aws/clients.py::_string_attributes` unconditionally writes
`{"topic": {...StringValue: topic}}` plus one attribute per header, with no cap check, no empty-value
skip, and no FIFO fields anywhere.

Note what this means in combination with the rest of the port: W3C `traceparent` + `tracestate` +
`correlationId` + a version header + `topic` is already five. A service with a handful of business
headers silently crosses ten and every send fails as `service-unavailable`.

### Implementation spec

* Package: `benzene-aws`, `clients.py`. No new dependency.
* In `_string_attributes`: skip any header whose value is empty; skip the `topic` entry when `topic`
  is falsy; after building, if `len(attrs) > 10` raise a `ValueError` naming the count **and the
  attribute keys** so the operator can see which to drop. Because a too-many-attributes send is a
  *programming/config* error rather than a message outcome, raising (like the `ImportError` guard
  already in this module) is right — but wrap it at the `send_message` boundary into a
  `bad-request` `Result` if the project's failure taxonomy prefers that; pick one and document it.
* FIFO, opt-in, on both `SqsMessageSender` and `SnsMessageSender`:

  ```python
  @dataclass(frozen=True)
  class FifoOptions:
      message_group_id_header: str | None = None
      message_deduplication_id_header: str | None = None
  ```
  `__init__(..., fifo: FifoOptions | None = None)`; when a named header is present, add
  `MessageGroupId=` / `MessageDeduplicationId=` to the boto3 call. The header stays an attribute too.
* SNS numeric typing: `SnsMessageSender(..., infer_numeric_attributes: bool = False)` — when on and a
  header value parses as an `int`/`float`, emit `{"DataType": "Number", "StringValue": value}`.
  Default off, exactly as .NET, so attribute types never change silently.
* Also apply all of this to `send_batch` (gap 3) — the batch entries carry the same
  `MessageGroupId`/`MessageDeduplicationId` fields.
* Tests: eleven attributes raises naming the keys; ten is fine; an empty header value and an empty
  topic are both omitted; FIFO headers map to the right kwargs and remain attributes; numeric
  inference is off by default and typed when on.

---

## 9. SQS consumer: no receive count, no batch concurrency, no visibility heartbeat

**Severity: Medium.**

**.NET:** `Benzene.Aws.Sqs/CLAUDE.md` — `SqsConsumer` *"requests the `ApproximateReceiveCount` system
attribute on each receive and surfaces it as `SqsConsumerMessageContext.ApproximateReceiveCount`
(`int?`), so a handler can make poison-message decisions (e.g. dead-letter after N deliveries)"*;
`SqsConsumerOptions.MaxDegreeOfParallelism` bounds how many messages in a poll batch run at once.
`WaitTimeSeconds` defaults to 20 — **Python already matches this**.

.NET also states plainly: *"(Visibility-timeout heartbeat — extending a slow handler's message
visibility on a background timer — is a separate, larger follow-up, not yet implemented.)"* That is a
**shared gap where Python can lead.**

**Python:** `packages/benzene-aws/benzene/aws/sqs_consumer.py::run_consumer_loop` passes only
`MessageAttributeNames=["All"]` — no `AttributeNames`, so `ApproximateReceiveCount` never arrives; and
the batch is walked with a plain `for` loop, one message at a time.

### Implementation spec

Three independent, additive changes to `sqs_consumer.py`:

1. **Receive count.** Add `AttributeNames=["ApproximateReceiveCount"]` to the `receive_message` call
   and surface it. Because the envelope shape is **frozen**, do *not* add a field to it — instead pass
   the raw message to `on_result` (it already is) and add a tiny public helper
   `def approximate_receive_count(message) -> int | None` reading
   `message["Attributes"]["ApproximateReceiveCount"]`. That gives a caller the dead-letter-after-N
   decision with zero contract change.
2. **Bounded batch concurrency.** `run_consumer_loop(..., concurrency: int = 1)`. `1` (the default)
   is exactly today's sequential behaviour. Above 1, dispatch the poll batch through an
   `asyncio.Semaphore(concurrency)` + `asyncio.gather`, deleting each message independently on its own
   success. Ordering across a standard SQS queue is not guaranteed anyway, so this is safe; document
   that a FIFO queue must keep `concurrency=1`.
3. **Visibility heartbeat** (the item .NET does not have). `run_consumer_loop(..., visibility_heartbeat: float | None = None)`:
   when set, each in-flight message gets a background `asyncio.Task` that every
   `visibility_heartbeat` seconds calls
   `change_message_visibility(QueueUrl=, ReceiptHandle=, VisibilityTimeout=int(visibility_heartbeat * 3))`
   via `to_thread`, cancelled in a `finally` when the handler returns. This is what lets a handler run
   longer than the queue's visibility timeout without the message being redelivered underneath it —
   the single most-requested SQS worker feature and a genuine chance for the Python port to be ahead.
   Errors from the heartbeat are logged, never raised (the handler must not die because a keep-alive
   call failed).
* Tests: the receive call requests the attribute and the helper reads it; `concurrency=3` runs three
  handlers concurrently and deletes each on its own success while a failure is left; the heartbeat
  fires N times for a slow handler and stops on completion; `concurrency=1` and
  `visibility_heartbeat=None` are byte-identical to today.

---

## 10. gRPC: no deadline / timeout propagation

**Severity: Medium** (an unresponsive peer hangs a worker thread indefinitely).

**.NET:** `Benzene.Grpc.Client/CLAUDE.md` — *"when the send happens inside an inbound gRPC call, it
resolves `IGrpcServerCallAccessor` and forwards that call's absolute `ServerCallContext.Deadline` onto
the downstream `CallOptions.Deadline`, so the downstream call must finish by the same wall-clock
time"*. Plus the ambient cancellation token.

**Python:** `packages/benzene-grpc/benzene/grpc/client.py::GrpcMessageSender.send_message` calls
`invoke.with_call(request, metadata=metadata)` with **no `timeout` argument at all**. There is no way
to bound a call, and no propagation of an inbound deadline. Because the call runs on an
`asyncio.to_thread` worker, a hung peer permanently consumes a thread from the default executor —
enough of them and the whole process stops making progress, including the consumer loops that share
that executor.

**.NET has no rich-error gap here** — it attaches `google.rpc.Status` on `grpc-status-details-bin`
with a `BadRequest` per validation error. `origin/main` appeared to carry a `benzene/grpc/details.py`
doing exactly this; **verify before implementing**, and if it is missing, spec it against
`conformance/grpc-status-mapping.json` and `conformance/problem-details-cases.json` rather than
against the .NET source, since that surface is frozen.

### Implementation spec

* Package: `benzene-grpc`, `client.py`. No new dependency.
* `GrpcMessageSender.__init__(self, channel, *, timeout: float | None = None)` — a default deadline
  for every call, in seconds.
* `async def send_message(..., timeout: float | None = None)` — per-call override wins.
* Pass it through: `invoke.with_call(request, metadata=metadata, timeout=effective_timeout)`.
* Map `grpc.StatusCode.DEADLINE_EXCEEDED` to the `timeout` status in `codes.code_to_status` if it does
  not already — **check `conformance/grpc-status-mapping.json` first; that mapping is frozen.**
* Deadline *propagation* (the .NET behaviour): add an optional
  `deadline: float | None = None` (an absolute `time.monotonic()`-based or epoch deadline) resolved
  to a remaining-seconds timeout at call time, so a server handler can thread its inbound deadline
  through. Do **not** build an ambient/contextvar accessor for 1.0 — it is a whole mechanism, and the
  explicit parameter covers the case honestly.
* Tests: default timeout applied; per-call override wins; no timeout configured passes `timeout=None`
  (today's behaviour); a `DEADLINE_EXCEEDED` `RpcError` maps to the `timeout` status; conformance
  suite still green.

---

## 11. Kafka producer: no message key

**Severity: Medium** (per-key ordering is unreachable).

**.NET:** `Benzene.Kafka.Core/CLAUDE.md` — *"pass `.UseKafka<T>(keyHeader: "x")` and the named
header's value becomes `Message.Key` (hash(key) → partition, giving per-key ordering/affinity);
`null` (the default) sends a keyless message (round-robin, no ordering)."*

**Python:** `packages/benzene-kafka/benzene/kafka/producer.py::send_message` calls
`producer.produce(topic, value=data, headers=header_list, on_delivery=...)` — never a `key`. Every
message round-robins across partitions, so **no ordering guarantee is achievable at all**, even
though the consumer side carefully preserves per-partition offsets. The Kinesis sender already gets
this right (`partition_key_header`, defaulting to the topic) — Kafka is the outlier.

### Implementation spec

* Package: `benzene-kafka`. No new dependency.
* `KafkaMessageSender.__init__(..., key_header: str | None = None)`.
* In `send_message`, when `key_header` is set and the named header has a non-empty value, pass
  `key=value.encode("utf-8")` to `produce`. Otherwise send keyless (today's behaviour, unchanged
  default — matching .NET, so a port does not silently change partitioning).
* Mirror it in `send_batch` (gap 3).
* Document the trade in the docstring the way the Kinesis sender does: a key buys per-key ordering
  and cache affinity, at the cost of partition skew if the key space is small.
* Tests: keyless by default; `key_header` set and present → the encoded key reaches `produce`;
  set but absent/empty → keyless; the key is *not* stripped from the headers.

---

## 12. Kafka and SQS consumers are strictly sequential

**Severity: Medium** (throughput ceiling).

**.NET:** `BenzeneKafkaConfig.ConcurrentRequests` (default 5) with `PreserveOrderPerPartition`
(default `true`) routing same-partition records to the same dispatcher lane, over
`Benzene.SelfHost.BoundedConcurrentDispatcher<T>`; `SqsConsumerOptions.MaxDegreeOfParallelism`.

**Python:** both loops `await` one message to completion before polling/dispatching the next. For an
I/O-bound handler (the common case) this leaves the event loop idle almost all the time.

The SQS half is specced in gap 9. For Kafka:

### Implementation spec

* Package: `benzene-kafka`, `consumer.py`. No new dependency.
* `run_consumer_loop(..., concurrency: int = 1, preserve_order_per_partition: bool = True)`.
* `concurrency=1` is today's loop exactly — keep that the default so nothing changes silently.
* Above 1 with `preserve_order_per_partition=True`: hash `(topic, partition)` to one of `concurrency`
  lanes, each lane an `asyncio.Task` draining its own `asyncio.Queue`. Kafka only ever promises order
  within a partition, so this is the same guarantee as sequential dispatch at N× the throughput.
* `preserve_order_per_partition=False` is round-robin across lanes — offer it, but state in the
  docstring that it is unsafe with `commit=True`, and **raise at call time** if both are set, the way
  .NET's startup validation does. Out-of-order handling plus a commit watermark loses records.
* Critically: the existing `blocked` map must be updated **from the lane**, under the invariant that
  one partition only ever has one in-flight record. With per-partition lanes that holds by
  construction — which is exactly why `preserve_order_per_partition=True` is the only safe pairing.
* **Only once this lands does .NET's `DrainOnRevoke` become relevant.** Today Python is
  safe-by-construction: a record is fully handled and its offset settled before the next poll, so a
  rebalance can never revoke a partition with work in flight. Do **not** port `DrainOnRevoke` now —
  it would be dead code guarding a failure mode Python does not have. Add it in the *same* change as
  concurrency: register an `on_revoke` callback that quiesces the revoked partitions' lanes (bounded
  by a `drain_timeout`) and commits before releasing.
* Tests: `concurrency=1` unchanged; two partitions run concurrently; two records on **one** partition
  run strictly in order; `preserve_order_per_partition=False` with `commit=True` raises.

---

## 13. No Step Functions client

**Severity: Medium** (a whole AWS orchestration primitive is unreachable through the port).

**.NET:** `Benzene.Clients.Aws.StepFunctions/CLAUDE.md` — `StepFunctionsClient.StartExecutionAsync`
starts an execution with the serialized message as input. The interesting part is the **idempotency**
overload: a caller-supplied stable token (e.g. a correlation id) becomes
`StartExecutionRequest.Name`, sanitized to Step Functions' allowed charset/length, and an
`ExecutionAlreadyExistsException` is treated as an **idempotent success (`Accepted`)**, not a failure —
so a retry after a lost response does not start a duplicate execution. Scope is honestly limited:
fire-and-forget, the `ExecutionArn` is discarded, no polling, no task-token callbacks.

**Python:** nothing. No `states` client anywhere in `packages/benzene-aws/`.

### Implementation spec

* Package: `benzene-aws`, `clients.py`. Extra: the existing `benzene-aws[boto3]` — no new dependency.
* `class StepFunctionsMessageSender` implementing `MessageSender`:

  ```python
  def __init__(self, state_machine_arn: str, client=None, serializer=None, *,
               execution_name_header: str | None = None) -> None: ...
  ```
* `send_message(topic, message, headers)`:
  * `input=` is the **Benzene envelope** `{"topic": topic, "headers": headers, "body": serialized}` —
    the same shape `LambdaMessageSender` sends, so a Step Functions state machine can hand the payload
    to a Benzene Lambda unchanged. (This is a new *usage* of the frozen envelope, not a new contract.)
  * when `execution_name_header` is set and that header is present, sanitize its value to Step
    Functions' rules — max 80 chars, and none of `<space> < > { } [ ] ? * " # % \ ^ | ~ ` $ & , ; : /`
    or control characters; replace disallowed runs with `-` and truncate — and pass it as `name=`.
    Sanitization must be **deterministic**, or the idempotency guarantee is void: hash-suffix the
    truncated form (e.g. last 8 hex of a sha256 of the original) so two different long ids never
    collide onto the same name.
  * catch the client's `ExecutionAlreadyExists` error (boto3 surfaces it as
    `client.exceptions.ExecutionAlreadyExists`, or `ClientError` with
    `Error.Code == "ExecutionAlreadyExists"` — match on the code so a duck-typed fake works) and
    return `Result.accepted()` — the execution exists, which is the outcome the caller wanted.
  * everything else: `Result.accepted()` on success (fire-and-forget; the execution runs
    asynchronously and there is no synchronous output), `service-unavailable` on any other failure,
    `ImportError` re-raised for a missing boto3 — the module's standing rules.
* **Copy .NET's honesty note into the docstring**: no awaiting, no polling, no `DescribeExecution`, no
  task-token callbacks (`send_task_success`/`send_task_failure`). A caller wanting those uses boto3
  directly — Benzene never hides the SDK.
* Tests, all against an injected fake: the envelope reaches `start_execution` as `input`; a name
  header produces a sanitized deterministic `name`; an over-long/illegal id sanitizes stably and two
  distinct ids never collide; an `ExecutionAlreadyExists` error maps to `accepted`; any other error
  maps to `service-unavailable`; missing boto3 raises the teaching `ImportError`.

---

## 14. Pub/Sub: no ordering key, no pull-subscriber worker

**Severity: Low** (.NET is thin here too — this is close to parity).

**.NET:** `Benzene.Clients.GoogleCloud.PubSub` is four files (`Extensions.cs`,
`OutboundPubSubContextConverter.cs`, `PubSubClientMiddleware.cs`, `PubSubSendMessageContext.cs`) and
has no `CLAUDE.md` — the thinnest client in the reference port. Inbound is
`Benzene.GoogleCloud.Functions.PubSub` only, i.e. push/Functions-triggered, exactly like Python.

**Python:** `packages/benzene-gcp/benzene/gcp/pubsub.py` — `decode_pubsub_message` plus
`PubSubMessageSender`. Two small holes: no `ordering_key` on publish, and no self-hosted **pull**
subscriber (the GCP counterpart of the SQS/Kafka/RabbitMQ loops).

### Implementation spec

* Ordering key (small, do it with gap 3's batch work): `PubSubMessageSender(..., ordering_key_header: str | None = None)`;
  when the named header is present pass `ordering_key=` to `publish`. Note in the docstring that the
  publisher client must be constructed with `PublisherOptions(enable_message_ordering=True)` — that
  is the caller's choice to make, not Benzene's (the injected-publisher seam already allows it).
* Pull subscriber — **defer**. It would be genuine new capability (Python would lead .NET), and the
  module shape is settled by gap 5's Service Bus worker: `PubSubConsumerApp` + a
  `run_consumer_loop(app, subscriber, subscription_path, *, max_messages=10, ack=True, ...)` duck-typing
  `pull()` / `acknowledge()` / `modify_ack_deadline()`. Worth doing *after* gaps 1–7, and it pairs
  naturally with gap 9's visibility-heartbeat pattern (`modify_ack_deadline` is the same idea).

---

## 15. No in-Lambda X-Ray subsegment tracing

**Severity: Low** (observability nicety; a real dependency cost).

**.NET:** `Benzene.Aws.Lambda.XRay/CLAUDE.md` — `AddXRayTracing()` wraps every middleware in an X-Ray
**subsegment** via the AWS X-Ray SDK, annotated with `benzene_transport`/`benzene_topic`/
`benzene_version`/`benzene_handler` (underscores because X-Ray rejects dots in annotation keys). The
justification is specific and good: OTel spans carry W3C trace ids, so exported to X-Ray through a
collector they land as *separate* traces rather than nested under the `AWS::Lambda::Function` segment.
Going straight through the X-Ray SDK is what puts the middleware timeline where an X-Ray user expects
it. It no-ops cleanly off Lambda (`EntityNotAvailableException` → run untraced).

**Python:** `packages/benzene-mesh-fleet/benzene/mesh_fleet/mappers.py::XRayTraceMapper` maps *mesh*
traces to X-Ray segment documents, which is a different thing. `packages/benzene-otel/` has an OTLP
exporter. Nothing reads `_X_AMZN_TRACE_ID` or opens subsegments inside a Lambda invocation.

### Implementation spec (low priority — ship only if a user asks)

* Package: `benzene-aws`, new module `benzene/aws/xray.py`, behind a new optional extra
  `benzene-aws[xray]` → `aws-xray-sdk`. **It must not become a required dependency**, and the module
  must not be imported from `benzene/aws/__init__.py` eagerly.
* A middleware factory `xray_middleware()` returning a Benzene middleware that opens
  `xray_recorder.in_subsegment_async(name=<middleware name>)` around `await next(...)`, annotating
  `benzene_transport` (**only once a transport is resolved** — .NET skips the `<missing>` sentinel, and
  so must this), `benzene_topic`, `benzene_version`, `benzene_handler`.
* Off-Lambda no-op: `aws_xray_sdk.core.exceptions.SegmentNotFoundException` (the Python analogue of
  `EntityNotAvailableException`) → run the inner middleware untraced. Safe to wire everywhere.
* Composes with the OTel exporter — both may be active at once.
* Tests: a fake recorder records one subsegment per middleware; the transport annotation is skipped
  while unresolved; an exception is recorded as a fault and re-raised; no segment in context → no-op.
* **Judgement:** this is the lowest-value item in the list. It buys a nicer X-Ray waterfall for
  Lambda users who are *not* using OTel, at the cost of a new SDK dependency and a middleware that
  duplicates what `benzene-otel` already does for everyone else. Ship it when someone asks.

---

## 16. gRPC: unary only

**Severity: Low.**

**.NET:** `Benzene.Grpc` routes **all four** RPC shapes (`UnaryServerHandler`,
`ClientStreamingServerHandler`, `ServerStreamingServerHandler`, `DuplexStreamingServerHandler`), with
`GrpcStreamAdapter` bridging `IAsyncStreamReader`/`IServerStreamWriter` and `IAsyncEnumerable`, and
one pipeline invocation per RPC regardless of shape (per-item middleware is explicitly out of scope).

**Python:** `packages/benzene-grpc/benzene/grpc/server.py` registers only
`grpc.unary_unary_rpc_method_handler`; `client.py` only `channel.unary_unary`.

### Implementation spec (deferred)

Do this **after** gap 6 (the aio handler), because streaming over the sync `asyncio.run` handler is
not implementable sanely. Then: `unary_stream` and `stream_unary` handlers whose Benzene body is an
`AsyncIterator[bytes]`, one pipeline invocation per RPC, `benzene-status` trailer set once at the end.
Bidirectional streaming is genuinely out of scope for a message-handler framework — .NET supports it
only because its interceptor makes it nearly free.

**Judgement:** Benzene's core abstraction is one message → one handler → one result. Streaming does
not fit it, which is why .NET says *"per-item middleware is out of scope by design"*. Low value; do
not build it speculatively.

---

## Not worth porting

**.NET-ecosystem adapters with no Python analogue needed:**

* **`Benzene.Azure.Function.SourceGenerators`** — generates the C# attribute plumbing Azure Functions
  needs. Python's `@app.function_name` decorators in `packages/benzene-azure/benzene/azure/app.py`
  already do this at runtime. Nothing to port.
* **`Benzene.CodeGen.SourceGenerators`, `Benzene.CodeGen.ApiGateway`, `Benzene.CodeGen.Terraform`** —
  compile-time codegen. Out of this domain and out of Python's idiom.
* **`*.TestHelpers` and `Benzene.Aws.Lambda.TestPayloads` as separate packages** — Python already ships
  `testing.py` *inside* each transport package (`aws/testing.py` is 544 lines, `azure/testing.py` 319,
  and there are kafka/rabbitmq/grpc/gcp ones). That is strictly better packaging: one import, no
  extra distribution, and the fakes live next to the code they fake. Do not split them out.
* **`Benzene.Aws.Lambda.AspNet`, `Benzene.Aws.Lambda.HttpBridge`, `Benzene.Grpc.AspNet`** — hosting
  glue for a framework Python does not have. `benzene-http`'s ASGI app covers the same ground.
* **`Benzene.Aws.Lambda.Hosting`, `Benzene.HostedService`, `Benzene.SelfHost`** — `IHostedService`
  wiring. Python's answer is `asyncio.gather` over the loop functions (and `WorkerHost` if it lands
  from `origin/main`). Do not port `IBenzeneWorkerStartup`.

**Architecture that must not be ported:**

* **The `IBenzeneMessageClient` / `*ContextConverter` / `*ClientMiddleware` / `OutboundContext`
  pipeline-per-send machinery.** .NET needs it because outbound cross-cutting concerns (retry,
  correlation, trace context) are middleware over a typed context. Python's `MessageSender` Protocol
  plus decorator/wrapper senders is simpler, duck-typed, and already the established idiom —
  `packages/benzene-core/benzene/core/clients.py` and `outbound.py`. Adding `send_batch` (gap 3) as a
  second Protocol method is the right shape; adding a converter/middleware layer is not.
* **Per-client auto-wired dependency health checks.** Nearly every .NET client `CLAUDE.md` documents
  an auto-registered reachability check (`GetQueueAttributes`, `GetTopicAttributes`,
  `DescribeEventBus`, `GetEventHubProperties`, `QueueDeclarePassive`, `AdminClient.GetMetadata`, …)
  wired through `AddDependencyHealthCheck` with dedup keys and a `healthCheck: false` opt-out. That
  whole mechanism is a DI-container feature. If Python wants this, the idiomatic form is a
  `async def check_health(self) -> Result` method on each sender that
  `packages/benzene-core/benzene/core/health.py` can collect — **not** a registration side effect of
  constructing a client. Treat it as its own design item outside this domain; do not smuggle it in
  with the batch work.

**Shared gaps — neither port has them; do not treat as parity debt:**

* **Kafka exactly-once / transactions, and schema registry.** .NET does not have these either
  (they are open items in its own issue #29). If Python builds a schema registry it belongs with the
  serialization packages, not the transport.
* **Step Functions task-token callbacks / `.sync` integration / execution polling.** .NET explicitly
  scopes these out ("post-1.0") and tells users to reach for the raw SDK. Match that boundary.
* **RabbitMQ RPC (`reply-to`), RabbitMQ Streams, topology management (declaring exchanges/queues).**
  .NET lists all three as deliberate non-goals. Python should too: the consumer assumes the queue and
  any DLX exist.
* **Cosmos DB change-feed worker.** .NET has one, but its own docs concede the checkpoint granularity
  is coarse (batch-level, "the coarse granularity flagged in `work/azure-roadmap-1.0.md`"). The Python
  Functions-triggered Cosmos binding already covers the common case. Skip until asked; Service Bus and
  Event Hub workers (gap 5) are far higher value.
* **Claim-check hydration for the worker contexts.** Four .NET `CLAUDE.md`s carry an identical "not
  wired here yet" note (Kafka, Service Bus, RabbitMQ, SQS consumer). Not a Python gap.

---

## Suggested sequencing

1. **Durability first, cheap:** gap 1 (RabbitMQ persistent default) — hours, prevents silent loss.
2. **Poison-message safety:** gap 2 (Kafka dead-letter) — the one thing that turns a wedged partition
   into an operable system.
3. **The big additive build:** gap 3 (batch producers) across nine senders, on one shared
   `BatchResult`/`chunked` seam in `benzene-core`.
4. **Hosting hole:** gap 5 (Azure Service Bus worker), which also unlocks gap 7's session producer.
5. **gRPC correctness:** gap 6 (aio handler), then gap 10 (timeouts).
6. Then 4, 8, 9, 11, 12, 13 as capacity allows. 14–16 on request only.
