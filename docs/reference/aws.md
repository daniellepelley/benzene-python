# `benzene.aws`

Host Benzene handlers on **AWS Lambda** — API Gateway (HTTP), SQS, SNS, S3, EventBridge, DynamoDB
Streams, Kinesis, Kafka/MSK, and direct Lambda-invoke event sources in one function — plus
SNS/SQS/EventBridge/Kinesis/Lambda outbound clients and a **self-hosted** SQS consumer.
**Distribution: `benzene-aws` (depends on `benzene-core`, `benzene-http`).**

```bash
pip install benzene-aws            # add [boto3] for the real outbound clients
```

## Overview

One host, inner bindings selected by event shape (transport-bindings §1):

- **API Gateway** — topic from the route (via `benzene.http`); response mapped to an API Gateway
  proxy response.
- **SQS** — one scope per record; failures reported via the SQS partial-batch-response
  (`batchItemFailures`) so only failed records redeliver.
- **SNS** — one scope per record; a failure raises so Lambda retries.
- **S3** (object-created) — one scope per record; no partial-batch channel, so a failure raises.
- **EventBridge** — a *single* event → one scope; a failure raises so the rule retries.
- **DynamoDB Streams / Kinesis Data Streams** — one scope per record; both support partial batches,
  so failures are reported via `batchItemFailures` keyed by the record's sequence number.
- **Kafka / MSK** — records grouped by topic-partition; one scope per record; the MSK event source has
  no partial-batch channel, so a failure raises for redelivery.
- **Direct invoke** — a bare `{"topic", "headers", "body"}` Payload, what another Lambda's
  `lambda.invoke(FunctionName=..., Payload=...)` sends (see `LambdaMessageSender` below). The one
  *synchronous* source besides API Gateway: the response envelope is returned verbatim as the
  invoke's response Payload, for the caller to decode back into a `Result`
  (`benzene.core.decode_response`).

Topic for the attribute-carrying transports (SQS, SNS, Kafka) comes from the `topic` message attribute
/ header. The channel-less sources (S3, EventBridge, DynamoDB, Kinesis) have no metadata channel on the
wire, so their topic comes from an injectable convention configured on the host — or, for DynamoDB,
from the record's `eventName` (`dynamodb:insert`). A direct invoke's topic is exactly what the caller
sent, no convention involved.

## `AwsLambdaApp`

```python
from benzene.aws import AwsLambdaApp, to_lambda_handler

app = AwsLambdaApp(http_router=router, registry=registry)   # shares one pipeline across sources
handler = to_lambda_handler(app)                            # def handler(event, context)
```

- `handle(event, context=None)` — dispatches by event shape (`event_source(event)` classifies it);
  returns an API Gateway proxy response, a `{"batchItemFailures": [...]}` partial-batch response
  (SQS, DynamoDB, Kinesis), the raw Benzene response envelope (a direct invoke), or `None` (SNS, S3,
  EventBridge, Kafka).
- `to_lambda_handler(app)` — the callable AWS Lambda invokes.
- The topic conventions for the channel-less sources are constructor keyword arguments:
  `s3_topic="s3:object-created"`, `eventbridge_topic="eventbridge:event"`,
  `kinesis_topic="kinesis:record"`, and `dynamodb_topic=None` (`None` means "derive per record from its
  `eventName`", e.g. `dynamodb:modify`). A direct invoke has no convention — its topic travels
  explicitly in the Payload.

`benzene.aws` also exports the per-source decoders behind these bindings for custom wiring:
`s3_record_envelope`, `eventbridge_envelope`, `dynamodb_record_envelope`, `kinesis_record_envelope`,
`kafka_records` / `kafka_record_envelope`, `invoke_envelope`, and the `event_source` classifier.

## Outbound clients

- `SnsMessageSender(topic_arn, client=None)` — publishes to an SNS topic ARN.
- `SqsMessageSender(queue_url, client=None)` — sends to an SQS queue URL.
- `EventBridgeMessageSender(event_bus_name, source="benzene", detail_type=None, client=None)` —
  publishes to an EventBridge bus; the `DetailType` is the fixed `detail_type` when given, else the
  Benzene topic (round-tripping with the inbound "topic from `detail-type`").
- `KinesisMessageSender(stream_name, partition_key_header="partition-key", client=None)` — puts a
  record on a Kinesis Data Stream; the partition key is read from `partition_key_header` when present,
  else the topic (so a topic's records co-locate on a shard and stay ordered).
- `LambdaMessageSender(function_name, client=None, *, invocation_type="RequestResponse", qualifier=None)`
  — invokes another Lambda function directly via AWS's `Invoke` API (no broker). The invoke Payload
  *is* the Benzene envelope, so the target needs no special wiring — any `AwsLambdaApp` answers it as
  its `"invoke"` source automatically. With the default `"RequestResponse"`, the call is synchronous:
  it waits for the target and decodes its response envelope back into a `Result`
  (`benzene.core.decode_response`) — this is the "call another function and get an answer" pattern,
  AWS's own equivalent of Lambda-to-Lambda calls. `invocation_type="Event"` is fire-and-forget instead
  — it returns `Result.accepted()` as soon as AWS queues the invoke, before the target even runs. A
  `FunctionError` in the response (the target itself faulted) or a payload that isn't a Benzene
  envelope both map to `service-unavailable`, never a crash.

All five implement `benzene.core.MessageSender` and use `boto3` (a lazy, optional import). SNS/SQS have
a native attribute channel, so the Benzene topic rides in the `topic` message attribute and headers as
attributes. EventBridge/Kinesis have *no* metadata channel, so the sender embeds the whole Benzene
envelope `{topic, headers, body}` inside the payload it serializes — keeping correlation/trace
propagation intact. Lambda's invoke Payload *is* the envelope already. A send failure maps to
`service-unavailable`, never a raise.

Azure Functions and Kubernetes services have no equivalent native "invoke another function directly"
primitive — the cross-platform way to reach the same synchronous-call outcome is over HTTP or gRPC
(`benzene.http.HttpMessageSender` / `benzene.grpc.GrpcMessageSender`), which is why `LambdaMessageSender`
is AWS-only.

## Self-hosted SQS consumer

Distinct from the Lambda SQS trigger above, `benzene.aws.sqs_consumer` polls a queue *itself* — the
shape a long-running worker or a Kubernetes Deployment needs, rather than being invoked by a Lambda
event source mapping. It mirrors `benzene.kafka`'s self-hosted consumer.

```python
from benzene.aws import SqsConsumerApp, run_sqs_consumer_loop

app = SqsConsumerApp.from_definition(definition)
await app.handle_message(message)                      # one receive_message() dict -> Result
await run_sqs_consumer_loop(app, client, queue_url)    # long-poll -> dispatch -> delete on success
```

- `SqsConsumerApp(application)` / `.from_definition(definition)` — `handle_message(message)` decodes a
  `boto3` `receive_message()` message dict (topic from the `topic` message attribute, `MessageAttributes`
  using `StringValue`/`DataType` — **not** the Lambda-event `messageAttributes` shape) and returns the
  mapped `Result`. It never raises, so a poison message can't crash the loop.
- `run_sqs_consumer_loop(app, client, queue_url, *, max_number_of_messages=10, wait_time_seconds=20,
  should_continue=..., delete=True, on_result=None)` drives a duck-typed SQS client
  (`receive_message` / `delete_message`). With `delete=True` (the default, at-least-once) a message is
  deleted only after a **successful** result, so a failed message is left on the queue individually for
  redelivery/DLQ redrive. `wait_time_seconds` defaults to 20 (SQS's maximum long-poll). The synchronous
  `boto3` calls are run via `asyncio.to_thread` so a 20-second long-poll never blocks other coroutines
  on the event loop — the pattern that lets this consumer share a process with an HTTP server.
- `decode_sqs_message(message)` is the pure decode step, exposed for custom loops.

## Testing

`benzene.aws.testing` provides `AwsLambdaTestHost` with one `send_*` per source — `send_http`,
`send_sqs` / `send_sqs_event`, `send_sns`, `send_s3`, `send_eventbridge`, `send_dynamodb`,
`send_kinesis`, `send_kafka`, `send_invoke` — and a native-event builder behind each
(`ApiGatewayRequestBuilder`, `SqsEventBuilder`, `SnsEventBuilder`, `S3EventBuilder`,
`EventBridgeEventBuilder`, `DynamoDbStreamBuilder`, `KinesisEventBuilder`, `KafkaLambdaEventBuilder`;
a direct invoke needs no builder, its Payload already *is* the envelope). The partial-batch sources
(`send_sqs*`, `send_dynamodb`, `send_kinesis`) return an `SqsBatchResponse` — assert on
`response.batch_item_failures` — mirroring how `send_http` returns a response object. `send_invoke`
returns the decoded `Result` directly (`benzene.core.decode_response`) — the same thing a real
`LambdaMessageSender` would resolve to on the caller's side.

The self-hosted SQS consumer has its own harness: `SqsConsumerTestHost` (`send_sqs_consumer` → the
mapped `Result`), the `SqsMessageBuilder` (a `receive_message()`-shaped dict, distinct from the Lambda
`SqsEventBuilder`), and `RecordingSqsClient` (a replay client whose `deleted` list a test asserts the
loop's at-least-once behaviour against — a failed message is *not* deleted). Specialize the shared
harness with `create_test_host(StartUp).build_aws()` / `.build_sqs_consumer()`. See
[Hosting on AWS Lambda](../cookbooks/hosting-on-aws.md) and the runnable
[`examples/aws_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/aws_orders).

## See also

- [`benzene.http`](http.md), [`benzene.core`](core.md), [`benzene.testing`](testing.md).
