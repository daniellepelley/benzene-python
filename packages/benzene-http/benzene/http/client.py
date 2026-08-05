"""Outbound HTTP client — send a Benzene message to another service over HTTP (transport-bindings §2).

The reverse direction of the HTTP binding: an :class:`HttpMessageSender` implements the
:class:`~benzene.core.MessageSender` port by **POSTing the message body to the target service** and
mapping the HTTP response back to a :class:`~benzene.results.Result` via the reverse status table
(:func:`from_http`). Benzene headers are forwarded as HTTP headers (plus the reserved ``topic`` key),
so correlation ids and trace context propagate end-to-end.

The actual HTTP call is an **injectable transport** — an ``async (url, headers, body) -> HttpReply``
callable — so a test drives the sender with a fake and no network. The default transport uses the
standard library (``urllib`` on a worker thread), so the sender works with **zero extra dependencies**;
pass an ``httpx``-backed transport for connection pooling in production.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, TypeAlias

from benzene.core import encode_body
from benzene.results import Result, Status, is_successful

from .status import from_http


@dataclass(frozen=True)
class HttpReply:
    """A minimal HTTP response the transport hands back: the status code and the (text) body."""

    status_code: int
    body: str = ""


#: The injectable HTTP call: ``await transport(url, headers, body) -> HttpReply``.
HttpTransport = Callable[[str, dict[str, str], str], Awaitable[HttpReply]]

#: How a topic becomes a URL: a base URL (``{base}/{topic}``), a ``{topic: url}`` map, or a callable.
UrlFor: TypeAlias = "str | Mapping[str, str] | Callable[[str], str]"


class HttpMessageSender:
    """A :class:`~benzene.core.MessageSender` that publishes over HTTP POST.

    ``url_for`` resolves a topic to a URL — a base URL (the topic is appended as a path segment), a
    ``{topic: url}`` mapping, or a ``topic -> url`` callable. ``transport`` is the injectable HTTP call
    (default: the stdlib transport). The topic travels in the ``topic`` header so a peer that routes by
    metadata still resolves it.
    """

    def __init__(
        self,
        url_for: UrlFor,
        *,
        transport: HttpTransport | None = None,
        topic_header: str = "topic",
    ) -> None:
        self._url_for = url_for
        self._transport = transport or stdlib_transport()
        self._topic_header = topic_header

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        url = self._resolve_url(topic)
        out_headers = {**(headers or {}), self._topic_header: topic, "content-type": "application/json"}
        reply = await self._transport(url, out_headers, encode_body(message))
        return _reply_to_result(reply)

    def _resolve_url(self, topic: str) -> str:
        target = self._url_for
        if callable(target):
            return target(topic)
        if isinstance(target, Mapping):
            if topic not in target:
                raise KeyError(f"No URL configured for topic {topic!r} in the HttpMessageSender map")
            return target[topic]
        return f"{str(target).rstrip('/')}/{topic}"


def _reply_to_result(reply: HttpReply) -> Result:
    status = from_http(reply.status_code)
    parsed = _parse(reply.body)
    if is_successful(status):
        return Result(status, parsed)
    detail = parsed.get("detail") if isinstance(parsed, dict) else None
    return Result.failure(status, detail) if detail else Result.failure(status)


def _parse(body: str) -> Any:
    if not body:
        return None
    try:
        return json.loads(body)
    except (ValueError, TypeError):
        return body


def stdlib_transport(*, timeout: float = 30.0) -> HttpTransport:
    """A zero-dependency :data:`HttpTransport` using ``urllib`` on a worker thread (POST)."""

    async def transport(url: str, headers: dict[str, str], body: str) -> HttpReply:
        def _post() -> HttpReply:
            request = urllib.request.Request(
                url, data=body.encode("utf-8"), headers=headers, method="POST"
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    return HttpReply(response.status, response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:  # a 4xx/5xx is a mapped Result, not an exception
                return HttpReply(exc.code, exc.read().decode("utf-8"))

        return await asyncio.to_thread(_post)

    return transport
