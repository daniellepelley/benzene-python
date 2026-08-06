"""AWS host wiring: boot the shared ``OrdersStartUp`` and specialize it to Lambda.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — the real SNS client here, a fake in tests. Only this file is
AWS-specific. The ``orders:created`` subscriber handles the event whether it arrives over SQS or SNS.
"""

from __future__ import annotations

from benzene.aws import AwsLambdaApp, SnsMessageSender
from benzene.core import Container, MessageSender, application_from, build_application
from orders_domain import ORDER_EVENTS_KEY, OrderService, OrdersStartUp


def build_aws_orders_app(
    service: OrderService | None = None,
    sender: MessageSender | None = None,
    seen: list[str] | None = None,
) -> AwsLambdaApp:
    """Build the AWS Lambda app for the order domain, booting from ``OrdersStartUp``.

    In production the default publishes ``orders:created`` to the SNS topic named by
    ``BENZENE_SNS_TOPIC_ARN``; tests override the sender via the test harness.
    """

    def overrides(services: Container) -> None:
        if service is not None:
            services.add_instance(OrderService, service)
        if seen is not None:
            services.add_instance(ORDER_EVENTS_KEY, seen)
        if sender is not None:
            services.add_instance(MessageSender, sender)
        else:
            import os

            topic_arn = os.environ.get("BENZENE_SNS_TOPIC_ARN")
            if not topic_arn:
                raise RuntimeError(
                    "Set BENZENE_SNS_TOPIC_ARN to run the AWS host with a real SNS client, "
                    "or pass sender=... in tests."
                )
            services.add_instance(MessageSender, SnsMessageSender(topic_arn))

    definition, _ = build_application(OrdersStartUp, overrides=[overrides])
    return AwsLambdaApp(http_router=definition.router, application=application_from(definition))
