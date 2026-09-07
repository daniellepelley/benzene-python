"""The one entry point: HTTP, SQS and Kafka in this one process, all three dispatching into
``orders_domain``.

Three legs, one :class:`~benzene.core.WorkerHost`. The host owns everything that is not specific to
orders: one event loop, whichever leg finishes first winding the others down, and a crashing leg
still propagating so the process exits non-zero and Kubernetes restarts it. ``benzene.core.worker``
documents the hand-rolled ``asyncio.gather`` this is shorthand for — drop to it whenever you want
different shutdown semantics — and ``docs/getting-started-kubernetes.md`` covers why sharing one
loop with two blocking SDKs is safe.

Run with:

    PORT=8080 BENZENE_ORDERS_EVENTS_URL=http://localhost:9999 \\
    BENZENE_SQS_CONSUME_QUEUE_URL=... BENZENE_SQS_EVENTS_QUEUE_URL=... \\
    BENZENE_KAFKA_CONSUME_TOPIC=orders-in BENZENE_KAFKA_TOPIC=orders-events \\
      python -m k8s_orders.app
"""

from __future__ import annotations

import asyncio
import os

from benzene.aws import sqs_consumer_worker
from benzene.core import WorkerHost
from benzene.http import uvicorn_worker
from benzene.kafka import build_kafka_consumer, kafka_consumer_worker
from http_orders.host import build_http_orders_app
from kafka_orders.host import build_kafka_orders_app
from sqs_orders.host import _sqs_client, build_sqs_orders_app


def build_orders_worker_host() -> WorkerHost:
    """The three transports this service listens on — and nothing else."""
    return (
        WorkerHost()
        .add(
            "http",
            uvicorn_worker(build_http_orders_app(), port=int(os.environ.get("PORT", "8080"))),
        )
        .add(
            "sqs",
            sqs_consumer_worker(
                build_sqs_orders_app(),
                _sqs_client(),
                os.environ["BENZENE_SQS_CONSUME_QUEUE_URL"],
            ),
        )
        .add(
            "kafka",
            kafka_consumer_worker(
                build_kafka_orders_app(),
                build_kafka_consumer(
                    bootstrap_servers=os.environ.get("BENZENE_KAFKA_BOOTSTRAP", "localhost:9092"),
                    group_id=os.environ.get("BENZENE_KAFKA_GROUP", "orders"),
                    topics=[os.environ["BENZENE_KAFKA_CONSUME_TOPIC"]],
                ),
            ),
        )
    )


if __name__ == "__main__":  # pragma: no cover - the real container entry point
    asyncio.run(build_orders_worker_host().run())
