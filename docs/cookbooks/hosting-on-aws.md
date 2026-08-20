# Hosting on AWS Lambda (API Gateway + SQS + SNS)

Host one set of Benzene handlers on AWS Lambda across **three event sources** — API Gateway, SQS,
and SNS — and publish events back out over SNS. One function, one pipeline, the handlers unchanged.

(Three is this recipe's scope, not the host's limit: `benzene.aws` also binds S3, EventBridge,
DynamoDB Streams, Kinesis, Kafka/MSK, and direct Lambda-to-Lambda invoke through the same function —
see [Getting Started: AWS §7](../getting-started-aws.md#7-supported-event-sources).)

## Prerequisites

- Python 3.10+
- `pip install benzene-aws` (add `[boto3]` for the real outbound clients)
- Runnable reference: [`examples/aws_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/aws_orders)

## Build the host and entry point

The domain is the shared `OrdersStartUp`; only the host is AWS-specific. It reads the SNS topic from
the environment, registers the real `SnsMessageSender` for the one outbound edge, and specializes the
composition root to Lambda with `AwsLambdaApp.from_definition`:

```python
# host.py
import os

from benzene.aws import AwsLambdaApp, SnsMessageSender
from benzene.core import Container, MessageSender, build_application
from orders_domain import OrdersStartUp


def build_aws_orders_app() -> AwsLambdaApp:
    topic_arn = os.environ["BENZENE_SNS_TOPIC_ARN"]

    def use_sns(services: Container) -> None:
        services.add_instance(MessageSender, SnsMessageSender(topic_arn))

    definition, _ = build_application(OrdersStartUp, overrides=[use_sns])
    return AwsLambdaApp.from_definition(definition)
```

`to_lambda_handler` wraps it in the `handler(event, context)` callable Lambda invokes:

```python
# main.py
from benzene.aws import to_lambda_handler

from .host import build_aws_orders_app

handler = to_lambda_handler(build_aws_orders_app())   # point your Lambda at main.handler
```

The single `handler` dispatches by event shape:

- an **API Gateway** event → route → topic → handler, Benzene status → HTTP code;
- an **SQS** event → one scope per record (topic from the `topic` message attribute); failed
  records come back as `batchItemFailures` for redelivery;
- an **SNS** event → one scope per record; a failure raises so Lambda retries.

## Test every source in memory (dogfooded)

```python
from benzene.aws.testing import SqsEventBuilder
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host

sender = FakeMessageSender()
# Boot the real composition root, fake only the outbound edge, specialize to Lambda — one call.
host = (
    create_test_host(OrdersStartUp)
    .with_services(lambda services: services.add_instance(MessageSender, sender))
    .build_aws()
)

# API Gateway ingress -> handler -> SNS egress
resp = host.send_http("POST", "/orders", body={"sku": "ABC"})
assert resp.status_code == 201 and sender.last_topic == "orders:created"

# SQS + SNS ingress
assert host.send_sqs("orders:created", {"id": "1", "sku": "A"}).batch_item_failures == []
host.send_sns("orders:created", {"id": "2", "sku": "B"})

# SQS partial-batch failure: only the bad record is reported
event = (SqsEventBuilder()
         .with_message("orders:created", {"id": "3", "sku": "C"}, message_id="m1")
         .with_message("orders:unknown", {}, message_id="m2")
         .build())
assert host.send_sqs_event(event).batch_item_failures == [{"itemIdentifier": "m2"}]
```

## Deploy (sketch)

Package `examples/` with the `benzene-*` dependencies, set the handler to `main.handler` and
`BENZENE_SNS_TOPIC_ARN`, then attach the triggers: an API Gateway proxy integration, an SQS
event-source mapping, and an SNS subscription. All three flow into the same handler.

## See also

- [`benzene.aws` reference](../reference/aws.md), [`benzene.testing` reference](../reference/testing.md)
