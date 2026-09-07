"""AWS host wiring: boot the shared ``OrdersStartUp`` and specialize it to Lambda.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — the real SNS client here, a fake in tests (via
``create_test_host``). Only this file is AWS-specific.
"""

from __future__ import annotations

import os

from benzene.aws import AwsLambdaApp, SnsMessageSender
from benzene.core import MessageSender, build_application, use_instance
from orders_domain import OrdersStartUp


def build_aws_orders_app() -> AwsLambdaApp:
    """Boot ``OrdersStartUp`` and specialize it to Lambda, publishing to the SNS topic in the env."""
    topic_arn = os.environ.get("BENZENE_SNS_TOPIC_ARN")
    if not topic_arn:
        raise RuntimeError(
            "Set BENZENE_SNS_TOPIC_ARN to run the AWS host (tests use create_test_host instead)."
        )

    definition, _ = build_application(
        OrdersStartUp, overrides=[use_instance(MessageSender, SnsMessageSender(topic_arn))]
    )
    return AwsLambdaApp.from_definition(definition)
