# benzene-aws

Host [Benzene Python](https://github.com/daniellepelley/benzene-python) handlers on **AWS Lambda** —
one function, several event sources (API Gateway, SQS, SNS, S3, EventBridge, DynamoDB Streams,
Kinesis Data Streams, Kafka/MSK) — plus SNS/SQS/EventBridge/Kinesis outbound clients. The same
handlers, no rewrite. Depends on `benzene-core` and `benzene-http`.

```bash
pip install benzene-aws            # add [boto3] for the real outbound clients
```

```python
from benzene.aws import AwsLambdaApp, to_lambda_handler

app = AwsLambdaApp(http_router=router, registry=registry)
handler = to_lambda_handler(app)  # your Lambda entry point: handler(event, context)
```

- **API Gateway** — HTTP-like; route → topic, Benzene status → HTTP code.
- **SQS** — one scope per record; failures reported via the SQS partial-batch-response
  (`batchItemFailures`) so only failed records redeliver.
- **SNS** — one scope per record; a failure raises so Lambda retries.
- **S3** (object-created) — one scope per record; bucket/key/`eventName` become the body; topic from
  an injectable convention (default `s3:object-created`). No partial-batch channel, so a failure raises.
- **EventBridge** — a single event; topic from `detail-type`, body is the `detail`. A failure raises.
- **DynamoDB Streams** / **Kinesis Data Streams** — one scope per record; both support partial batches,
  so failures are reported via `batchItemFailures` keyed by the record's sequence number. DynamoDB's
  topic defaults to `dynamodb:<eventname>`; Kinesis decodes the base64 payload as the body.
- **Kafka / MSK** — records grouped by topic-partition; one scope per record; topic from the `topic`
  header (else the Kafka topic), headers decoded UTF-8, base64 `value` as the body. A failure raises.
- **Outbound** — `SnsMessageSender` / `SqsMessageSender` carry the topic + headers as native message
  attributes; `EventBridgeMessageSender` / `KinesisMessageSender` embed the Benzene envelope in the
  payload (no attribute channel). All implement `benzene.core.MessageSender` over `boto3` (optional).

Topic for SQS/SNS/Kafka comes from the reserved `topic` metadata key; the channel-less sources (S3,
EventBridge, DynamoDB, Kinesis) take their topic from a convention configured on the host. Test every
event source in memory with `benzene.aws.testing` — see the runnable
[`examples/aws_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/aws_orders).
Mirrors .NET's `Benzene.Aws.Lambda.*` / `Benzene.Clients.Aws.*`, and contributes the `benzene.aws`
subpackage to the shared `benzene` namespace.
