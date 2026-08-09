"""``benzene.kafka`` — the Kafka transport binding (transport-bindings §"Kafka").

A **self-hosted consumer** (topic from the ``topic`` header, headers UTF-8, one record per pipeline
invocation, no response channel → acknowledge/log) and a **Kafka-produce outbound client**
(:class:`KafkaMessageSender`). The binding logic is duck-typed against ``confluent-kafka`` and needs
no broker or SDK to run or test; only the real producer/consumer clients do.

    pip install benzene-kafka          # the binding + testing helpers
    pip install "benzene-kafka[kafka]"  # + confluent-kafka for the real clients

Mirrors .NET's ``Benzene.Kafka.Core``. Contributes the ``benzene.kafka`` subpackage to the shared
``benzene`` namespace; depends only on ``benzene-core``.
"""

from __future__ import annotations

from .consumer import (
    KafkaConsumerApp,
    KafkaMessage,
    decode_kafka_message,
    run_consumer_loop,
)
from .producer import TOPIC_HEADER, KafkaMessageSender

__all__ = [
    "KafkaConsumerApp",
    "KafkaMessage",
    "KafkaMessageSender",
    "TOPIC_HEADER",
    "decode_kafka_message",
    "run_consumer_loop",
]
