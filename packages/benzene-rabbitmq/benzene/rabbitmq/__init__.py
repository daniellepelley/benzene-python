"""``benzene.rabbitmq`` — the RabbitMQ transport binding (transport-bindings §"RabbitMQ").

A **self-hosted consumer** (topic from the ``topic`` header carried in AMQP ``properties.headers``,
headers UTF-8, one delivery per pipeline invocation, no response channel → acknowledge/log) and a
**RabbitMQ-publish outbound client** (:class:`RabbitMqMessageSender`). The binding logic is duck-typed
against ``pika`` and needs no broker or SDK to run or test; only the real channel/connection does.

    pip install benzene-rabbitmq             # the binding + testing helpers
    pip install "benzene-rabbitmq[rabbitmq]"  # + pika for the real clients

Mirrors .NET's ``Benzene.RabbitMq``. Contributes the ``benzene.rabbitmq`` subpackage to the shared
``benzene`` namespace; depends only on ``benzene-core``.
"""

from __future__ import annotations

from .consumer import (
    RabbitMqConsumerApp,
    decode_rabbitmq_message,
    run_consumer_loop,
)
from .producer import TOPIC_HEADER, RabbitMqMessageSender

__all__ = [
    "RabbitMqConsumerApp",
    "RabbitMqMessageSender",
    "TOPIC_HEADER",
    "decode_rabbitmq_message",
    "run_consumer_loop",
]
