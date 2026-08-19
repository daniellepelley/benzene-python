"""RabbitMQ host wiring: boot the shared ``OrdersStartUp`` and run it as a self-hosted consumer.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — a real ``RabbitMqMessageSender`` in production, a fake in tests.
Only this file is RabbitMQ-specific.

Unlike the cloud hosts (which are triggered by a runtime), a RabbitMQ service owns its own loop: it
pulls deliveries off a queue and runs :func:`~benzene.rabbitmq.run_consumer_loop`, which turns each
delivery into one pipeline invocation and acks it on success. There is no HTTP surface — the order
domain is reached over RabbitMQ deliveries whose ``topic`` header (carried in the AMQP
``properties.headers``) names the Benzene topic (``orders:place`` / ``orders:created``).
"""

from __future__ import annotations

import os

from benzene.core import MessageSender, build_application, use_instance
from benzene.rabbitmq import RabbitMqConsumerApp, RabbitMqMessageSender, run_consumer_loop
from orders_domain import OrdersStartUp


def build_rabbitmq_orders_app() -> RabbitMqConsumerApp:
    """Boot ``OrdersStartUp`` as a RabbitMQ consumer, publishing ``orders:created`` to the env's exchange.

    The default publishes to ``BENZENE_RABBITMQ_EXCHANGE`` (header-routed) on the broker at
    ``BENZENE_RABBITMQ_URL``, keyed by ``BENZENE_RABBITMQ_ROUTING_KEY``; tests supply a fake sender via
    ``create_test_host`` instead.
    """
    exchange = os.environ.get("BENZENE_RABBITMQ_EXCHANGE")
    if exchange is None:
        raise RuntimeError(
            "Set BENZENE_RABBITMQ_EXCHANGE (and BENZENE_RABBITMQ_URL) to run the RabbitMQ host "
            "(tests use create_test_host instead)."
        )
    routing_key = os.environ.get("BENZENE_RABBITMQ_ROUTING_KEY", "")
    host = os.environ.get("BENZENE_RABBITMQ_URL", "localhost")

    sender = RabbitMqMessageSender(exchange, routing_key, host=host)
    definition, _ = build_application(OrdersStartUp, overrides=[use_instance(MessageSender, sender)])
    return RabbitMqConsumerApp.from_definition(definition)


async def main() -> None:  # pragma: no cover - the real broker entry point
    """Open a real channel and run the loop (requires ``benzene-rabbitmq[rabbitmq]`` + a broker)."""
    import os

    import pika

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get("BENZENE_RABBITMQ_URL", "localhost"))
    )
    channel = connection.channel()
    queue = os.environ["BENZENE_RABBITMQ_QUEUE"]
    channel.queue_declare(queue=queue, durable=True)
    try:
        await run_consumer_loop(build_rabbitmq_orders_app(), channel, queue=queue)
    finally:
        connection.close()


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(main())
