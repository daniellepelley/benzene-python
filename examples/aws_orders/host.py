"""AWS host wiring: mount the shared order domain onto Lambda (API Gateway + SQS + SNS + egress).

Only this file differs from the GCP and Azure hosts — the domain (``orders_domain``) is identical.
The ``orders.created`` subscriber handles the event whether it arrives over SQS or SNS.
"""

from __future__ import annotations

from benzene.aws import AwsLambdaApp, SnsMessageSender
from benzene.core import MessageSender

from orders_domain import OrderService, build_orders


def build_aws_orders_app(
    service: OrderService | None = None,
    sender: MessageSender | None = None,
    seen: list[str] | None = None,
) -> AwsLambdaApp:
    """Build the AWS Lambda app for the order domain.

    Pass a store / outbound client / event log for tests; in production the default publishes
    ``orders.created`` to the SNS topic named by ``BENZENE_SNS_TOPIC_ARN``.
    """
    service = service or OrderService()
    if sender is None:
        import os

        sender = SnsMessageSender(os.environ["BENZENE_SNS_TOPIC_ARN"])
    wiring = build_orders(service, sender, seen if seen is not None else [])
    return AwsLambdaApp(http_router=wiring.router, registry=wiring.registry)
