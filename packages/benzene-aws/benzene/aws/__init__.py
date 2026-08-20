"""``benzene.aws`` — the AWS Lambda host and its transport bindings.

Distribution ``benzene-aws``: host the same Benzene handlers behind API Gateway (HTTP), SQS, SNS, S3
(object-created), EventBridge, DynamoDB Streams, Kinesis Data Streams, Kafka/MSK, and direct
Lambda-invoke Lambda event sources, plus SNS/SQS/EventBridge/Kinesis/Lambda outbound clients — the
last (``LambdaMessageSender``) calling another Lambda directly via AWS's own ``Invoke`` API, answered
automatically by any ``AwsLambdaApp`` with zero extra wiring. Also ships a **self-hosted** SQS
consumer (:mod:`benzene.aws.sqs_consumer` — ``SqsConsumerApp``/``run_consumer_loop``), distinct
from the Lambda SQS trigger above: that one is invoked by a Lambda event source mapping, this one
polls a queue itself, the shape a long-running worker or a Kubernetes Deployment needs. Depends on
``benzene-core`` and ``benzene-http``. Mirrors .NET's ``Benzene.Aws.Lambda.*`` /
``Benzene.Clients.Aws.*`` / ``Benzene.Aws.Sqs``.

    from benzene.aws import AwsLambdaApp, to_lambda_handler

``boto3`` is only needed for the real outbound clients and is an optional extra
(``pip install benzene-aws[boto3]``); the inbound bindings and the test host need no AWS SDK.
Contributes the ``benzene.aws`` subpackage to the shared ``benzene`` namespace.
"""

from __future__ import annotations

from .app import AwsLambdaApp, to_lambda_handler
from .clients import (
    EventBridgeMessageSender,
    KinesisMessageSender,
    LambdaMessageSender,
    SnsMessageSender,
    SqsMessageSender,
)
from .events import (
    DEFAULT_EVENTBRIDGE_TOPIC,
    DEFAULT_KINESIS_TOPIC,
    DEFAULT_S3_TOPIC,
    TOPIC_ATTRIBUTE,
    dynamodb_record_envelope,
    event_source,
    eventbridge_envelope,
    invoke_envelope,
    kafka_record_envelope,
    kafka_records,
    kinesis_record_envelope,
    s3_record_envelope,
)
from .sqs_consumer import (
    SqsConsumerApp,
    decode_sqs_message,
    run_consumer_loop,
    run_sqs_consumer_loop,
)

__all__ = [
    "DEFAULT_EVENTBRIDGE_TOPIC",
    "DEFAULT_KINESIS_TOPIC",
    "DEFAULT_S3_TOPIC",
    "AwsLambdaApp",
    "EventBridgeMessageSender",
    "KinesisMessageSender",
    "LambdaMessageSender",
    "SnsMessageSender",
    "SqsConsumerApp",
    "SqsMessageSender",
    "TOPIC_ATTRIBUTE",
    "decode_sqs_message",
    "dynamodb_record_envelope",
    "eventbridge_envelope",
    "event_source",
    "invoke_envelope",
    "kafka_record_envelope",
    "kafka_records",
    "kinesis_record_envelope",
    "run_consumer_loop",
    "run_sqs_consumer_loop",
    "s3_record_envelope",
    "to_lambda_handler",
]
