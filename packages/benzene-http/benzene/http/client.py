"""Outbound HTTP client — send a Benzene message to another service over HTTP (transport-bindings §2).

Two shapes of outbound HTTP client live here, both implementing the
:class:`~benzene.core.MessageSender` port:

- :class:`HttpMessageSender` — the reverse direction of the *route-based* HTTP binding: it **POSTs the
  message body** to the target service and maps the HTTP status back to a
  :class:`~benzene.results.Result` via the reverse status table (:func:`from_http`). Use it against a
  service that exposes a topic as its own HTTP route.
- :class:`InvokeMessageSender` — the outbound counterpart of the Cloud Service Profile's
  ``/benzene/invoke`` surface (design-principles §5.2, R4). It POSTs the **full wire envelope**
  (``{topic, headers, body}``) to one ``/benzene/invoke`` URL and reads the **response envelope** back,
  so the domain status travels *inside* the envelope (``/benzene/invoke`` always answers HTTP 200 for a
  processed message). This is what a service uses to report mesh feeds to a collector host, or to call
  any peer uniformly regardless of that peer's routes.

Benzene headers are forwarded as HTTP headers (plus, for :class:`HttpMessageSender`, the reserved
``topic`` key), so correlation ids and trace context propagate end-to-end.

The actual HTTP call is an **injectable transport** — an ``async (url, headers, body) -> HttpReply``
callable — so a test drives the sender with a fake and no network. The default transport uses the
standard library (``urllib`` on a worker thread), so the sender works with **zero extra dependencies**;
pass an ``httpx``-backed transport for connection pooling in production. :func:`stdlib_get_transport`
is the read-only companion (an ``async (url) -> HttpReply`` GET) an aggregator uses to fetch a peer's
``/benzene/spec`` and ``/benzene/health`` documents.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, TypeAlias

from benzene.core import encode_body
from benzene.results import Result, is_successful

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


#: The injectable HTTP read: ``await get(url) -> HttpReply`` (a GET, no request body).
HttpGet = Callable[[str], Awaitable[HttpReply]]


def stdlib_get_transport(*, timeout: float = 10.0, headers: dict[str, str] | None = None) -> HttpGet:
    """A zero-dependency :data:`HttpGet` using ``urllib`` on a worker thread (GET).

    A 4xx/5xx is returned as an :class:`HttpReply` (status + body), *not* raised — an aggregator must
    tell a genuinely unreachable service (a connection error, which does raise :class:`OSError`) apart
    from one that answered ``503`` with a valid unhealthy ``/benzene/health`` body.
    """

    base_headers = {"accept": "application/json", **(headers or {})}

    async def get(url: str) -> HttpReply:
        def _get() -> HttpReply:
            request = urllib.request.Request(url, headers=base_headers, method="GET")
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    return HttpReply(response.status, response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:  # a 4xx/5xx still carries a body worth reading
                return HttpReply(exc.code, exc.read().decode("utf-8"))

        return await asyncio.to_thread(_get)

    return get


class InvokeMessageSender:
    """A :class:`~benzene.core.MessageSender` that POSTs a wire envelope to ``/benzene/invoke``.

    The outbound counterpart of the profile's ``/benzene/invoke`` surface (R4): the message is wrapped
    in the wire envelope ``{topic, headers, body}`` and POSTed to the resolved invoke URL, and the
    **response envelope** is mapped back to a :class:`~benzene.results.Result`. Because ``/benzene/invoke``
    answers HTTP 200 for any *processed* message, the domain outcome is read from the response
    envelope's ``statusCode`` — not the HTTP status — so a ``not-found``/``service-unavailable`` handler
    result round-trips faithfully.

    ``url_for`` resolves a topic to the target's invoke URL — a single URL string (every topic goes to
    the same ``/benzene/invoke``), a ``{topic: url}`` mapping, or a ``topic -> url`` callable — so one
    sender can fan a fleet's topics out to different peers. Headers (e.g. a forwarded ``traceparent``)
    ride inside the envelope, so trace context propagates across the hop.
    """

    def __init__(
        self,
        url_for: UrlFor,
        *,
        transport: HttpTransport | None = None,
    ) -> None:
        self._url_for = url_for
        self._transport = transport or stdlib_transport()

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        url = self._resolve_url(topic)
        envelope = json.dumps(
            {"topic": topic, "headers": headers or {}, "body": encode_body(message)}
        )
        reply = await self._transport(url, {"content-type": "application/json"}, envelope)
        return _envelope_to_result(reply)

    def _resolve_url(self, topic: str) -> str:
        target = self._url_for
        if callable(target):
            return target(topic)
        if isinstance(target, Mapping):
            if topic not in target:
                raise KeyError(f"No invoke URL configured for topic {topic!r} in the InvokeMessageSender map")
            return target[topic]
        return str(target)


def _envelope_to_result(reply: HttpReply) -> Result:
    """Map a ``/benzene/invoke`` HTTP reply (an envelope, or a transport-level error) to a Result."""
    if reply.status_code >= 400:
        # A transport-level failure (malformed envelope → 400, host down → mapped by the transport):
        # the message was never processed, so surface it as the reverse-mapped HTTP status.
        return Result.failure(from_http(reply.status_code))
    envelope = _parse(reply.body)
    if not isinstance(envelope, dict) or "statusCode" not in envelope:
        return Result.failure(from_http(reply.status_code))
    status = str(envelope["statusCode"])
    payload = _parse(envelope.get("body") or "")
    if is_successful(status):
        return Result(status, payload)
    detail = payload.get("detail") if isinstance(payload, dict) else None
    return Result.failure(status, detail) if detail else Result.failure(status)
