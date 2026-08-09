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

from benzene.core import Container, MessageSender, application_from, build_application
from benzene.kafka import KafkaConsumerApp, KafkaMessageSender, run_consumer_loop
from orders_domain import ORDER_EVENTS_KEY, OrderService, OrdersStartUp


def build_kafka_orders_app(
    service: OrderService | None = None,
    sender: MessageSender | None = None,
    seen: list[str] | None = None,
) -> KafkaConsumerApp:
    """Build the Kafka consumer app for the order domain, booting from ``OrdersStartUp``.

    Pass a store / outbound client / event log to override the defaults (tests do this via the test
    harness instead); in production the default publishes ``orders:created`` to the Kafka topic named
    by ``BENZENE_KAFKA_TOPIC`` on the broker at ``BENZENE_KAFKA_BOOTSTRAP``.
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

            topic = os.environ.get("BENZENE_KAFKA_TOPIC")
            if not topic:
                raise RuntimeError(
                    "Set BENZENE_KAFKA_TOPIC (and BENZENE_KAFKA_BOOTSTRAP) to run the Kafka host with "
                    "a real producer, or pass sender=... in tests."
                )
            bootstrap = os.environ.get("BENZENE_KAFKA_BOOTSTRAP", "localhost:9092")
            services.add_instance(
                MessageSender, KafkaMessageSender(topic, bootstrap_servers=bootstrap)
            )

    definition, _ = build_application(OrdersStartUp, overrides=[overrides])
    return KafkaConsumerApp(application_from(definition))


async def main() -> None:  # pragma: no cover - the real broker entry point
    """Subscribe a real consumer and run the loop (requires ``benzene-kafka[kafka]`` + a broker)."""
    import os

    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("BENZENE_KAFKA_BOOTSTRAP", "localhost:9092"),
            "group.id": os.environ.get("BENZENE_KAFKA_GROUP", "orders"),
            "enable.auto.commit": False,  # the loop commits on success (at-least-once)
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([os.environ["BENZENE_KAFKA_CONSUME_TOPIC"]])
    try:
        await run_consumer_loop(build_kafka_orders_app(), consumer)
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(main())
