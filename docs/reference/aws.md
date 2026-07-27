# `benzene.aws`

Host Benzene handlers on **AWS Lambda** — API Gateway (HTTP), SQS, and SNS event sources in one
function — plus SNS/SQS outbound clients. **Distribution: `benzene-aws` (depends on `benzene-core`,
`benzene-http`).**

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

Topic for SQS/SNS comes from the `topic` message attribute.

## `AwsLambdaApp`

```python
from benzene.aws import AwsLambdaApp, to_lambda_handler

app = AwsLambdaApp(http_router=router, registry=registry)   # shares one pipeline across sources
handler = to_lambda_handler(app)                            # def handler(event, context)
```

- `handle(event, context=None)` — dispatches by event shape; returns an API Gateway proxy response,
  or an SQS `{"batchItemFailures": [...]}`, or `None` (SNS).
- `to_lambda_handler(app)` — the callable AWS Lambda invokes.

## Outbound clients

- `SnsMessageSender(topic_arn, client=None)` — publishes to an SNS topic ARN.
- `SqsMessageSender(queue_url, client=None)` — sends to an SQS queue URL.

Both implement `benzene.core.MessageSender`, carry the Benzene topic in the `topic` message
attribute, forward headers as attributes, and use `boto3` (a lazy, optional import).

## Testing

`benzene.aws.testing` provides `AwsLambdaTestHost` (`send_http`, `send_sqs`, `send_sqs_event`,
`send_sns`) and `ApiGatewayRequestBuilder` / `SqsEventBuilder` / `SnsEventBuilder`. See
[Hosting on AWS Lambda](../cookbooks/hosting-on-aws.md) and the runnable
[`examples/aws_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/aws_orders).

## See also

- [`benzene.http`](http.md), [`benzene.core`](core.md), [`benzene.testing`](testing.md).
