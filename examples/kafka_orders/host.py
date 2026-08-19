"""Kafka host wiring: boot the shared ``OrdersStartUp`` and run it as a self-hosted consumer.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — a real ``KafkaMessageSender`` in production, a fake in tests.
Only this file is Kafka-specific.

Unlike the cloud hosts (which are triggered by a runtime), a Kafka service owns its own loop: it
subscribes a consumer and runs :func:`~benzene.kafka.run_consumer_loop`, which turns each record into
one pipeline invocation and commits the offset on success. There is no HTTP surface — the order domain
is reached over Kafka records whose ``topic`` header names the Benzene topic (``orders:place`` /
``orders:created``).
"""

from __future__ import annotations

import os

from benzene.core import MessageSender, build_application, use_instance
from benzene.kafka import (
    KafkaConsumerApp,
    KafkaMessageSender,
    build_kafka_consumer,
    run_consumer_loop,
)
from orders_domain import OrdersStartUp


def build_kafka_orders_app() -> KafkaConsumerApp:
    """Boot ``OrdersStartUp`` as a Kafka consumer, publishing ``orders:created`` to the env's topic.

    The default publishes to ``BENZENE_KAFKA_TOPIC`` on the broker at ``BENZENE_KAFKA_BOOTSTRAP``;
    tests supply a fake sender via ``create_test_host`` instead.
    """
    topic = os.environ.get("BENZENE_KAFKA_TOPIC")
    if not topic:
        raise RuntimeError(
            "Set BENZENE_KAFKA_TOPIC (and BENZENE_KAFKA_BOOTSTRAP) to run the Kafka host "
            "(tests use create_test_host instead)."
        )
    bootstrap = os.environ.get("BENZENE_KAFKA_BOOTSTRAP", "localhost:9092")

    sender = KafkaMessageSender(topic, bootstrap_servers=bootstrap)
    definition, _ = build_application(OrdersStartUp, overrides=[use_instance(MessageSender, sender)])
    return KafkaConsumerApp.from_definition(definition)


async def main() -> None:  # pragma: no cover - the real broker entry point
    """Subscribe a real consumer and run the loop (requires ``benzene-kafka[kafka]`` + a broker)."""
    consumer = build_kafka_consumer(
        bootstrap_servers=os.environ.get("BENZENE_KAFKA_BOOTSTRAP", "localhost:9092"),
        group_id=os.environ.get("BENZENE_KAFKA_GROUP", "orders"),
        topics=[os.environ["BENZENE_KAFKA_CONSUME_TOPIC"]],
    )
    try:
        await run_consumer_loop(build_kafka_orders_app(), consumer)
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(main())
