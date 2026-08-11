"""Google Cloud Pub/Sub binding (inbound decode + outbound client).

Inbound: a Pub/Sub-triggered Cloud Function receives a message ``{data, attributes}`` (``data`` is
base64-encoded). Following the cross-port convention (transport-bindings §"AWS Lambda"/SQS-SNS), the
Benzene **topic** is carried in the ``topic`` message attribute; the remaining attributes are the
Benzene headers, and the decoded ``data`` is the JSON body.

Outbound: :class:`PubSubMessageSender` implements the :class:`~benzene.core.MessageSender` port over
``google-cloud-pubsub`` (an optional dependency, imported lazily), forwarding the header dictionary
onto the native message attributes so correlation/trace propagation works end to end.
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable
from typing import Any

from benzene.core import encode_body, read_message_metadata
from benzene.results import Result, Status

#: Message-attribute name carrying the Benzene topic (the cross-port convention).
TOPIC_ATTRIBUTE = "topic"


def decode_pubsub_message(message: dict[str, Any]) -> dict[str, Any]:
    """Decode a Pub/Sub ``message`` dict into a Benzene envelope ``{topic, headers, body}``."""
    topic, headers = read_message_metadata(message.get("attributes"))

    raw = message.get("data")
    if raw is None:
        body = ""
    elif isinstance(raw, (bytes, bytearray)):
        body = bytes(raw).decode("utf-8")
    else:
        # Pub/Sub delivers `data` base64-encoded; fall back to the raw string if it isn't.
        try:
            body = base64.b64decode(raw).decode("utf-8")
        except (ValueError, TypeError):
            body = str(raw)

    return {"topic": topic, "headers": headers, "body": body}


class PubSubMessageSender:
    """Publishes messages to a Pub/Sub topic, Benzene topic carried in the ``topic`` attribute.

    ``topic_path`` is the fully-qualified Pub/Sub topic (``projects/<p>/topics/<t>``) to publish to;
    all Benzene topics route to it, attribute-tagged (the single-topic, attribute-routed pattern). A
    ``publisher`` may be injected for testing; otherwise a ``google.cloud.pubsub_v1.PublisherClient``
    is created lazily on first use.
    """

    def __init__(
        self,
        topic_path: str,
        publisher: Any | None = None,
        serializer: Callable[[Any], str] | None = None,
    ) -> None:
        self._topic_path = topic_path
        self._publisher = publisher
        self._serialize = serializer or encode_body

    def _client(self) -> Any:
        if self._publisher is None:
            from google.cloud import pubsub_v1  # lazy: optional dependency

            self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        attributes = {str(k): str(v) for k, v in (headers or {}).items()}
        attributes[TOPIC_ATTRIBUTE] = topic
        data = self._serialize(message).encode("utf-8")

        def _publish() -> None:
            future = self._client().publish(self._topic_path, data, **attributes)
            # The publish future is synchronous under the SDK; result() surfaces publish errors.
            future.result()

        try:
            # Run the blocking publish (and its result() wait) on a worker thread so an
            # ``await send_message(...)`` never blocks the event loop.
            await asyncio.to_thread(_publish)
        except Exception as ex:  # a failed publish is a service-unavailable, not a crash
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()
