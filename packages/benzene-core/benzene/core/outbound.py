"""Cross-cutting outbound-client decorators (transport-bindings.md, "Outbound clients").

The spec makes cross-cutting client behaviours — correlation-id injection, retry, trace propagation —
**decorators over the one `MessageSender` interface**, so they are transport-agnostic: the same
retry wraps an HTTP client, a gRPC client, or an SQS client unchanged. Each decorator here wraps a
:class:`~benzene.core.MessageSender` and *is* a :class:`MessageSender`, so they compose freely
(``with_retry(with_correlation_id(sender))``).
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from benzene.results import Result, Status

from .clients import MessageSender

#: The failure statuses a retry treats as transient by default (a fresh attempt might succeed).
DEFAULT_RETRYABLE: frozenset[str] = frozenset(
    {Status.SERVICE_UNAVAILABLE, Status.TIMEOUT, Status.TOO_MANY_REQUESTS}
)


class RetryingMessageSender:
    """Wraps a :class:`MessageSender`, re-sending while the result is a *transient* failure.

    A success, or a failure outside ``retry_on`` (a real ``not-found``/``bad-request`` won't get better
    by retrying), returns immediately. ``backoff`` is an optional ``async (attempt) -> None`` hook
    (e.g. sleep) run between attempts.
    """

    def __init__(
        self,
        inner: MessageSender,
        *,
        attempts: int = 3,
        retry_on: Iterable[str] = DEFAULT_RETRYABLE,
        backoff: Callable[[int], Awaitable[None]] | None = None,
    ) -> None:
        self._inner = inner
        self._attempts = max(1, attempts)
        self._retry_on = frozenset(retry_on)
        self._backoff = backoff

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        result = await self._inner.send_message(topic, message, headers)
        for attempt in range(1, self._attempts):
            if result.is_successful or result.status not in self._retry_on:
                return result
            if self._backoff is not None:
                await self._backoff(attempt)
            result = await self._inner.send_message(topic, message, headers)
        return result


class CorrelationIdMessageSender:
    """Wraps a :class:`MessageSender`, injecting a correlation-id header when the caller set none.

    Correlation ids let a request be followed across services; this makes every outbound message carry
    one without each call site remembering to. An id the caller already supplied is left untouched.
    """

    def __init__(
        self,
        inner: MessageSender,
        *,
        header: str = "x-correlation-id",
        new_id: Callable[[], str] | None = None,
    ) -> None:
        self._inner = inner
        self._header = header
        self._new_id = new_id or (lambda: uuid.uuid4().hex)

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        out = dict(headers or {})
        if not any(key.lower() == self._header.lower() for key in out):
            out[self._header] = self._new_id()
        return await self._inner.send_message(topic, message, out)


def with_retry(inner: MessageSender, **options: Any) -> RetryingMessageSender:
    """Sugar for :class:`RetryingMessageSender` — ``with_retry(sender, attempts=5)``."""
    return RetryingMessageSender(inner, **options)


def with_correlation_id(inner: MessageSender, **options: Any) -> CorrelationIdMessageSender:
    """Sugar for :class:`CorrelationIdMessageSender`."""
    return CorrelationIdMessageSender(inner, **options)
