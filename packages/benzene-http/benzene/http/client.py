"""Outbound HTTP client — send a Benzene message to another service over HTTP (transport-bindings §2).

The reverse direction of the HTTP binding: an :class:`HttpMessageSender` implements the
:class:`~benzene.core.MessageSender` port by **POSTing the message body to the target service** and
mapping the HTTP response back to a :class:`~benzene.results.Result` — its status from a failure's
problem document (``benzeneStatus``) when it states one, else via the reverse status table
(:func:`from_http`). Benzene headers are forwarded as HTTP headers (plus the reserved ``topic`` key),
so correlation ids and trace context propagate end-to-end.

The actual HTTP call is an **injectable transport** — an ``async (url, headers, body) -> HttpReply``
callable — so a test drives the sender with a fake and no network. The default transport uses the
standard library (``urllib`` on a worker thread), so the sender works with **zero extra dependencies**;
pass an ``httpx``-backed transport for connection pooling in production.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, TypeAlias

from benzene.core import encode_body, failed_call_result
from benzene.results import Result, is_successful, problem_errors, problem_status

from .status import from_http


@dataclass(frozen=True)
class HttpReply:
    """A minimal HTTP response the transport hands back: the status code and the (text) body."""

    status_code: int
    body: str = ""


#: The injectable HTTP call: ``await transport(url, headers, body) -> HttpReply``. A call that gets
#: no reply (no connection, refused, reset, timed out) raises an :class:`OSError` - a
#: :class:`ConnectionError` or :class:`TimeoutError` - which the sender turns into a ``timeout`` or
#: ``service-unavailable`` Result; an SDK-backed transport translates its own errors to those
#: (httpx's ``TimeoutException`` to ``TimeoutError``, its ``TransportError`` to ``ConnectionError``).
#: Any HTTP status, 4xx/5xx included, is a reply and is returned, never raised.
HttpTransport = Callable[[str, dict[str, str], str], Awaitable[HttpReply]]

#: How a topic becomes a URL: a base URL (``{base}/{topic}``), a ``{topic: url}`` map, or a callable.
UrlFor: TypeAlias = "str | Mapping[str, str] | Callable[[str], str]"


class HttpMessageSender:
    """A :class:`~benzene.core.MessageSender` that publishes over HTTP POST.

    ``url_for`` resolves a topic to a URL — a base URL (the topic is appended as a path segment), a
    ``{topic: url}`` mapping, or a ``topic -> url`` callable. ``transport`` is the injectable HTTP call
    (default: the stdlib transport). The topic travels in the ``topic`` header so a peer that routes by
    metadata still resolves it.

    A call that fails before any reply arrives returns a Result, never raises (transport-bindings §1):
    ``timeout`` when the deadline elapsed - the transport's own, or ``timeout`` seconds for the whole
    call when given - and ``service-unavailable`` otherwise (no connection, refused, reset).
    """

    def __init__(
        self,
        url_for: UrlFor,
        *,
        transport: HttpTransport | None = None,
        topic_header: str = "topic",
        timeout: float | None = None,
    ) -> None:
        self._url_for = url_for
        self._transport = transport or stdlib_transport()
        self._topic_header = topic_header
        self._timeout = timeout

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        url = self._resolve_url(topic)
        out_headers = {**(headers or {}), self._topic_header: topic, "content-type": "application/json"}
        try:
            reply = await self._call(url, out_headers, encode_body(message))
        except (OSError, asyncio.TimeoutError) as exc:  # 3.10's asyncio.TimeoutError is no OSError
            return failed_call_result(exc, f"POST {url}")
        return _reply_to_result(reply)

    async def _call(self, url: str, headers: dict[str, str], body: str) -> HttpReply:
        if self._timeout is None:
            return await self._transport(url, headers, body)
        try:
            return await asyncio.wait_for(self._transport(url, headers, body), self._timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"no reply within {self._timeout}s") from exc

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
    if not isinstance(parsed, dict):
        return Result.failure(status)
    # The peer's problem document (section 1.3). Its benzeneStatus, when it states a failure, is the
    # result's status (wire-contracts 4.1, spec 1.1): the reverse table collapses every status it has
    # no row for - an application-defined one answers 500 and would read back as unexpected-error -
    # so the table is only the fallback, for a body that states no status. errors is authoritative
    # and ordered when present, detail stands in only when it is absent; same rules as the envelope
    # decoder, from the same functions.
    return Result.failure(problem_status(parsed) or status, *problem_errors(parsed))


def _parse(body: str) -> Any:
    if not body:
        return None
    try:
        return json.loads(body)
    except (ValueError, TypeError):
        return body


def stdlib_transport(*, timeout: float = 30.0) -> HttpTransport:
    """A zero-dependency :data:`HttpTransport` using ``urllib`` on a worker thread (POST).

    ``timeout`` is ``urllib``'s socket timeout: the deadline for connecting and for each read. A call
    with no reply raises as :data:`HttpTransport` says - ``urllib``'s ``URLError`` (an ``OSError``,
    a timeout kept as its ``reason``), a ``TimeoutError``, or a ``ConnectionError`` for a connection
    the peer dropped mid-reply - for the sender to map.
    """

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
            except http.client.HTTPException as exc:  # a garbled or truncated reply is no reply
                raise ConnectionError(f"{type(exc).__name__}: {exc}") from exc

        return await asyncio.to_thread(_post)

    return transport
