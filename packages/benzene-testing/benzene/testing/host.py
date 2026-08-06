"""In-memory test host + message builder (mirrors .NET ``Benzene.Testing``).

Drive a Benzene application in memory — no cloud, no network — exactly as a transport would, so an
example (or an adopter) can integration-test its handlers and middleware for real while faking only
the external edges. This is the transport-neutral core; each transport package ships its own native
event builders (e.g. ``benzene.gcp.testing``) on top of it.
"""

from __future__ import annotations

import json
from typing import Any

from benzene.core import BenzeneMessageApplication, Registry


class MessageBuilder:
    """Builds a Benzene message envelope ``{topic, headers, body}`` for a test host."""

    def __init__(self, topic: str) -> None:
        self._topic = topic
        self._headers: dict[str, str] = {}
        self._body: Any = {}

    def with_header(self, key: str, value: str) -> MessageBuilder:
        self._headers[key] = value
        return self

    def with_body(self, body: Any) -> MessageBuilder:
        self._body = body
        return self

    def build(self) -> dict[str, Any]:
        return {
            "topic": self._topic,
            "headers": dict(self._headers),
            "body": self._body if isinstance(self._body, str) else json.dumps(_to_jsonable(self._body)),
        }


class InMemoryBenzeneHost:
    """Runs a :class:`~benzene.core.BenzeneMessageApplication` in memory.

    Build it from a ready application or from a registry. ``send_message`` drives one message
    through the full pipeline and returns the response envelope.
    """

    def __init__(self, app_or_registry: BenzeneMessageApplication | Registry) -> None:
        if isinstance(app_or_registry, BenzeneMessageApplication):
            self._app = app_or_registry
        else:
            self._app = BenzeneMessageApplication(app_or_registry)

    async def send(self, envelope: dict[str, Any]) -> dict[str, Any]:
        """Send a raw ``{topic, headers, body}`` envelope; returns the response envelope."""
        return await self._app.handle(envelope)

    async def send_message(
        self, topic: str, body: Any = None, headers: dict[str, str] | None = None
    ) -> dict[str, Any]:
        """Convenience: build and send a message from ``topic``/``body``/``headers``."""
        builder = MessageBuilder(topic)
        if headers:
            for key, value in headers.items():
                builder.with_header(key, value)
        if body is not None:
            builder.with_body(body)
        return await self.send(builder.build())


def _to_jsonable(value: Any) -> Any:
    from dataclasses import asdict, is_dataclass

    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    return value
