"""The one entry point: runs the HTTP ASGI app (uvicorn), the SQS consumer loop, and the Kafka
consumer loop together in this one process, all three dispatching into ``orders_domain``.

Safe to run concurrently on one event loop because ``run_sqs_consumer_loop``/``run_consumer_loop``
run their underlying ``boto3``/``confluent-kafka`` calls via ``asyncio.to_thread`` internally (see
``benzene.aws.sqs_consumer``/``benzene.kafka.consumer``) rather than calling them directly on the
loop - called directly, SQS's ~20s long-poll (worse, Kafka's unbounded idle poll, which has no
``await`` point at all) would starve uvicorn's HTTP handling for as long as either consumer is
mid-poll. See ``docs/getting-started-kubernetes.md`` for the full story.

Shutdown is coordinated: whichever leg finishes first winds the others down. uvicorn owns
SIGINT/SIGTERM natively (``server.serve()`` runs on the main thread - nothing here starts another),
and on a signal it returns and sets ``stop``, which the SQS/Kafka loops' ``should_continue`` check on
their next iteration. Each leg is wrapped so that if it exits for *any* reason - a clean signal, or a
consumer loop crashing - it sets ``stop`` **and** tells uvicorn to exit, so one leg going down never
strands the others. A crash still propagates out of ``asyncio.gather`` for a loud, non-zero exit that
Kubernetes restarts.

Run with:

    PORT=8080 BENZENE_ORDERS_EVENTS_URL=http://localhost:9999 \\
    BENZENE_SQS_CONSUME_QUEUE_URL=... BENZENE_SQS_EVENTS_QUEUE_URL=... \\
    BENZENE_KAFKA_CONSUME_TOPIC=orders-in BENZENE_KAFKA_TOPIC=orders-events \\
      python -m k8s_orders.app
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable

import uvicorn
from benzene.aws import run_sqs_consumer_loop
from benzene.kafka import run_consumer_loop
from confluent_kafka import Consumer
from http_orders.host import build_http_orders_app
from kafka_orders.host import build_kafka_orders_app
from sqs_orders.host import _sqs_client, build_sqs_orders_app


async def main() -> None:
    http_app = build_http_orders_app()
    sqs_app = build_sqs_orders_app()
    kafka_app = build_kafka_orders_app()

    sqs_client = _sqs_client()
    consume_queue_url = os.environ["BENZENE_SQS_CONSUME_QUEUE_URL"]

    kafka_consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("BENZENE_KAFKA_BOOTSTRAP", "localhost:9092"),
            "group.id": os.environ.get("BENZENE_KAFKA_GROUP", "orders"),
            "enable.auto.commit": False,  # the loop commits on success (at-least-once)
            "auto.offset.reset": "earliest",
        }
    )
    kafka_consumer.subscribe([os.environ["BENZENE_KAFKA_CONSUME_TOPIC"]])

    config = uvicorn.Config(
        http_app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8080")),  # noqa: S104
    )
    server = uvicorn.Server(config)
    stop = asyncio.Event()

    def shutdown() -> None:
        # Whichever leg finishes first — a clean uvicorn signal, a consumer-loop crash, or a sibling
        # being cancelled — winds the others down: uvicorn leaves its serve loop and the consumer
        # loops stop on their next ``should_continue`` check. Idempotent.
        stop.set()
        server.should_exit = True

    async def supervised(leg: Awaitable[None]) -> None:
        try:
            await leg
        finally:
            shutdown()

    try:
        await asyncio.gather(
            supervised(server.serve()),
            supervised(
                run_sqs_consumer_loop(
                    sqs_app,
                    sqs_client,
                    consume_queue_url,
                    should_continue=lambda: not stop.is_set(),
                )
            ),
            supervised(
                run_consumer_loop(
                    kafka_app, kafka_consumer, should_continue=lambda: not stop.is_set()
                )
            ),
        )
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
