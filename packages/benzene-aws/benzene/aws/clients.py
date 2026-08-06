"""AWS outbound clients (SNS, SQS) implementing the ``benzene.core.MessageSender`` port.

Each forwards the Benzene topic and headers onto the native message-attribute channel (the
``topic`` attribute plus one attribute per header) so correlation/trace propagation works end to
end. ``boto3`` is an optional dependency, imported lazily.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from benzene.core import encode_body
from benzene.results import Result, Status

from .events import TOPIC_ATTRIBUTE


def _string_attributes(topic: str, headers: dict[str, str] | None) -> dict[str, dict[str, str]]:
    attrs = {TOPIC_ATTRIBUTE: {"DataType": "String", "StringValue": topic}}
    for key, value in (headers or {}).items():
        attrs[str(key)] = {"DataType": "String", "StringValue": str(value)}
    return attrs


class SnsMessageSender:
    """Publishes to an SNS topic ARN, Benzene topic carried in the ``topic`` message attribute."""

    def __init__(
        self, topic_arn: str, client: Any | None = None, serializer: Callable[[Any], str] | None = None
    ) -> None:
        self._topic_arn = topic_arn
        self._client = client
        self._serialize = serializer or encode_body

    def _sns(self) -> Any:
        if self._client is None:
            import boto3  # lazy: optional dependency

            self._client = boto3.client("sns")
        return self._client

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            self._sns().publish(
                TopicArn=self._topic_arn,
                Message=self._serialize(message),
                MessageAttributes=_string_attributes(topic, headers),
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()


class SqsMessageSender:
    """Sends to an SQS queue URL, Benzene topic carried in the ``topic`` message attribute."""

    def __init__(
        self, queue_url: str, client: Any | None = None, serializer: Callable[[Any], str] | None = None
    ) -> None:
        self._queue_url = queue_url
        self._client = client
        self._serialize = serializer or encode_body

    def _sqs(self) -> Any:
        if self._client is None:
            import boto3  # lazy: optional dependency

            self._client = boto3.client("sqs")
        return self._client

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            self._sqs().send_message(
                QueueUrl=self._queue_url,
                MessageBody=self._serialize(message),
                MessageAttributes=_string_attributes(topic, headers),
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()
