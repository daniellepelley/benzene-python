"""Test doubles for the external edges (mirrors .NET's ``FakeBenzeneMessageSender``).

The dogfooding rule (port-quality-standards §4) is that example tests fake *only* the external
edges — the outbound client here — and exercise the real pipeline, routing, and handlers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from benzene.results import Result


@dataclass
class SentMessage:
    topic: str
    message: Any
    headers: dict[str, str]


class FakeMessageSender:
    """A :class:`~benzene.core.MessageSender` that records what was published instead of sending it.

    Assert on ``last_topic`` / ``last_message`` / ``last_headers`` (or the full ``sent`` list) to
    prove ingress → handler → egress carried the payload through.
    """

    def __init__(self, result: Result | None = None) -> None:
        self._result = result if result is not None else Result.ok()
        self.sent: list[SentMessage] = []

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        self.sent.append(SentMessage(topic, message, dict(headers or {})))
        return self._result

    @property
    def last(self) -> SentMessage | None:
        return self.sent[-1] if self.sent else None

    @property
    def last_topic(self) -> str | None:
        return self.last.topic if self.last else None

    @property
    def last_message(self) -> Any:
        return self.last.message if self.last else None

    @property
    def last_headers(self) -> dict[str, str] | None:
        return self.last.headers if self.last else None
