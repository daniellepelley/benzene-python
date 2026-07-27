"""``benzene.aws`` — the AWS Lambda host and its transport bindings.

Distribution ``benzene-aws``: host the same Benzene handlers behind API Gateway (HTTP), SQS, and SNS
Lambda event sources, plus SNS/SQS outbound clients. Depends on ``benzene-core`` and ``benzene-http``.
Mirrors .NET's ``Benzene.Aws.Lambda.*`` / ``Benzene.Clients.Aws.*``.

    from benzene.aws import AwsLambdaApp, to_lambda_handler

``boto3`` is only needed for the real outbound clients and is an optional extra
(``pip install benzene-aws[boto3]``); the inbound bindings and the test host need no AWS SDK.
Contributes the ``benzene.aws`` subpackage to the shared ``benzene`` namespace.
"""

from __future__ import annotations

from .app import AwsLambdaApp, to_lambda_handler
from .clients import SnsMessageSender, SqsMessageSender
from .events import TOPIC_ATTRIBUTE, event_source

__all__ = [
    "AwsLambdaApp",
    "SnsMessageSender",
    "SqsMessageSender",
    "TOPIC_ATTRIBUTE",
    "event_source",
    "to_lambda_handler",
]
