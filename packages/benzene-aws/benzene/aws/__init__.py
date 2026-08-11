"""``benzene.aws`` — the AWS Lambda host and its transport bindings.

Distribution ``benzene-aws``: host the same Benzene handlers behind API Gateway (HTTP), SQS, SNS, S3
(object-created), EventBridge, DynamoDB Streams, Kinesis Data Streams, and Kafka/MSK Lambda event
sources, plus SNS/SQS/EventBridge/Kinesis outbound clients. Depends on ``benzene-core`` and
``benzene-http``. Mirrors .NET's ``Benzene.Aws.Lambda.*`` / ``Benzene.Clients.Aws.*``.

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
    kafka_record_envelope,
    kafka_records,
    kinesis_record_envelope,
    s3_record_envelope,
)

__all__ = [
    "DEFAULT_EVENTBRIDGE_TOPIC",
    "DEFAULT_KINESIS_TOPIC",
    "DEFAULT_S3_TOPIC",
    "AwsLambdaApp",
    "EventBridgeMessageSender",
    "KinesisMessageSender",
    "SnsMessageSender",
    "SqsMessageSender",
    "TOPIC_ATTRIBUTE",
    "dynamodb_record_envelope",
    "eventbridge_envelope",
    "event_source",
    "kafka_record_envelope",
    "kafka_records",
    "kinesis_record_envelope",
    "s3_record_envelope",
    "to_lambda_handler",
]
