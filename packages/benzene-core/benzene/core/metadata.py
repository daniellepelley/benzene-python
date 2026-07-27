"""Native transport metadata → (topic, headers) (wire-contracts.md §2).

Every transport that carries Benzene metadata natively exposes the same shape under a different
name — SQS/SNS message attributes, Pub/Sub attributes, Service Bus/Event Hub application
properties, Kafka/RabbitMQ headers — so the split belongs here once rather than in each binding.

Two rules from the spec that a per-binding reimplementation tends to get wrong, and did:

* **Read case-insensitively.** "All header keys are case-insensitive on read and SHOULD be written
  lower-case." A producer that wrote ``Benzene-Topic`` must still route.
* **Consume the routing key.** The topic key is metadata the binding used, not an application
  header, so it must not also appear in the header dictionary.
"""

from __future__ import annotations

from typing import Mapping

#: The metadata key carrying the topic on transports without the envelope (wire-contracts §2,
#: tier A). Defined once here so no binding can drift from the others — the failure mode is a
#: service that silently cannot receive messages from a sibling port.
TOPIC_KEY = "benzene-topic"


def take_topic(metadata: Mapping[str, object], topic_key: str = TOPIC_KEY) -> tuple[str, dict[str, str]]:
    """Split native metadata into ``(topic, headers)``.

    The topic key is matched case-insensitively and removed from the headers. Everything else
    becomes a header with its original spelling. Duplicate keys: last value wins (wire-contracts §2).
    """
    wanted = topic_key.lower()
    topic = ""
    headers: dict[str, str] = {}
    for key, value in metadata.items():
        if str(key).lower() == wanted:
            topic = str(value)
            continue
        headers[str(key)] = str(value)
    return topic, headers
