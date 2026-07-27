# benzene-aws

Host [Benzene Python](https://github.com/daniellepelley/benzene-python) handlers on **AWS Lambda** —
one function, several event sources (API Gateway, SQS, SNS) — plus SNS/SQS outbound clients. The
same handlers, no rewrite. Depends on `benzene-core` and `benzene-http`.

```bash
pip install benzene-aws            # add [boto3] for the real outbound clients
```

```python
from benzene.aws import AwsLambdaApp, to_lambda_handler

app = AwsLambdaApp(http_router=router, registry=registry)
handler = to_lambda_handler(app)   # your Lambda entry point: handler(event, context)
```

- **API Gateway** — HTTP-like; route → topic, Benzene status → HTTP code.
- **SQS** — one scope per record; failures reported via the SQS partial-batch-response
  (`batchItemFailures`) so only failed records redeliver.
- **SNS** — one scope per record; a failure raises so Lambda retries.
- **Outbound** — `SnsMessageSender` / `SqsMessageSender` implement `benzene.core.MessageSender` over
  `boto3` (optional extra), forwarding the topic + headers as message attributes.

Topic for SQS/SNS comes from the `topic` message attribute. Test every event source in memory with
`benzene.aws.testing` — see the runnable
[`examples/aws_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/aws_orders).
Mirrors .NET's `Benzene.Aws.Lambda.*`, and contributes the `benzene.aws` subpackage to the shared
`benzene` namespace.
