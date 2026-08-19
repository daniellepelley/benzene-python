"""SQS host wiring: boot the shared ``OrdersStartUp`` and run it as a self-hosted consumer.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — a real ``SqsMessageSender`` in production, a fake in tests. Only
this file is SQS-specific.

Unlike the cloud hosts (triggered by a Lambda event source), an SQS service here owns its own loop: it
polls a queue directly and runs :func:`~benzene.aws.run_sqs_consumer_loop`, which turns each message
into one pipeline invocation and deletes it on success. There is no HTTP surface — the order domain is
reached over messages whose ``topic`` message attribute names the Benzene topic (``orders:place`` /
``orders:created``).
"""

from __future__ import annotations

import os

from benzene.aws import SqsConsumerApp, SqsMessageSender, run_sqs_consumer_loop
from benzene.core import MessageSender, build_application, use_instance
from orders_domain import OrdersStartUp


def _sqs_client():  # pragma: no cover - the real queue entry point
    """Build a ``boto3`` SQS client, honouring ``SQS_ENDPOINT_URL`` for a LocalStack/emulator run.

    Unset (the real-AWS/EKS case), ``boto3.client("sqs")`` falls through to the SDK's default
    credential chain, e.g. an IRSA-mapped pod service account - this host never prescribes how you
    authenticate. Shared by both the consume and produce sides so a LocalStack run (see
    ``compose/docker-compose.yml``) round-trips through the same emulator end to end.
    """
    import boto3

    endpoint_url = os.environ.get("SQS_ENDPOINT_URL")
    return boto3.client("sqs", endpoint_url=endpoint_url) if endpoint_url else boto3.client("sqs")


def build_sqs_orders_app() -> SqsConsumerApp:
    """Boot ``OrdersStartUp`` as an SQS consumer, publishing ``orders:created`` to the env's queue.

    The default publishes to ``BENZENE_SQS_EVENTS_QUEUE_URL``; tests supply a fake sender via
    ``create_test_host`` instead.
    """
    events_queue_url = os.environ.get("BENZENE_SQS_EVENTS_QUEUE_URL")
    if not events_queue_url:
        raise RuntimeError(
            "Set BENZENE_SQS_EVENTS_QUEUE_URL (where orders:created is published) to run the "
            "SQS host (tests use create_test_host instead)."
        )

    sender = SqsMessageSender(events_queue_url, client=_sqs_client())
    definition, _ = build_application(OrdersStartUp, overrides=[use_instance(MessageSender, sender)])
    return SqsConsumerApp.from_definition(definition)


async def main() -> None:  # pragma: no cover - the real queue entry point
    """Poll a real queue and run the loop (requires ``boto3`` + a reachable queue)."""
    queue_url = os.environ["BENZENE_SQS_CONSUME_QUEUE_URL"]
    await run_sqs_consumer_loop(build_sqs_orders_app(), _sqs_client(), queue_url)


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(main())
