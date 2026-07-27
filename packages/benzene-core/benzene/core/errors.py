"""Core errors shared across transport bindings."""

from __future__ import annotations


class MessageHandlingError(RuntimeError):
    """A handler produced a failure result on a transport with no response channel.

    Queue/stream bindings (SQS, SNS, Pub/Sub, Service Bus, Event Hub) raise this so the platform
    retries / redelivers / dead-letters the message. It names the topic, the Benzene status, and the
    detail so the failure is actionable in the host's logs.
    """

    def __init__(self, topic: str, status: str, detail: str = "") -> None:
        self.topic = topic
        self.status = status
        self.detail = detail
        message = f"Handler for topic {topic!r} failed with status {status!r}"
        super().__init__(f"{message}: {detail}" if detail else message)
