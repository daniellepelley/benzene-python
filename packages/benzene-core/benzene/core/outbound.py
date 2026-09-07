"""Cross-cutting outbound-client decorators (transport-bindings.md, "Outbound clients").

The spec makes cross-cutting client behaviours — correlation-id injection, retry, trace propagation —
**decorators over the one `MessageSender` interface**, so they are transport-agnostic: the same
retry wraps an HTTP client, a gRPC client, or an SQS client unchanged. Each decorator here wraps a
:class:`~benzene.core.MessageSender` and *is* a :class:`MessageSender`, so they compose freely
(``with_retry(with_correlation_id(sender))``).

Each is also a :class:`~benzene.core.BatchMessageSender`: a decorator over an interface has to carry
*both* of that interface's verbs or wrapping a sender would silently take its batch API away. Where
the wrapped sender has a native ``send_batch`` it is used as-is (the whole point — one API call for
N messages); where it has not, :func:`~benzene.core.send_batch_sequentially` falls back to N
``send_message`` calls **through this decorator's inner sender**, so the decoration still applies to
every message.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import replace
from typing import Any

from benzene.results import Result, Status

from .clients import (
    BatchResult,
    FailedMessage,
    MessageSender,
    delegate_batch,
)

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

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """Re-send **only** the entries whose failure is transient, at the caller's own indices.

        Retrying a partial failure by resending the whole batch would duplicate every message that
        already landed, so each attempt carries just the still-retryable subset and its failures are
        mapped back onto the caller's original positions. A ``bad-request`` entry is kept as it is
        and never resent — the same rule :meth:`send_message` applies, on the same ``retry_on`` set.
        """
        outstanding = list(enumerate(messages))
        failures: dict[int, FailedMessage] = {}
        for attempt in range(1, self._attempts + 1):
            result = await delegate_batch(self._inner, [pair for _, pair in outstanding], headers)
            for index, _pair in outstanding:
                failures.pop(index, None)  # this attempt is the new truth for what it carried
            for failure in result.failures:
                original = outstanding[failure.index][0]
                failures[original] = replace(failure, index=original)
            if attempt >= self._attempts:
                break
            outstanding = [
                (index, pair)
                for index, pair in outstanding
                if index in failures and failures[index].status in self._retry_on
            ]
            if not outstanding:
                break
            if self._backoff is not None:
                await self._backoff(attempt)
        return BatchResult(tuple(failures[index] for index in sorted(failures)))


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
        return await self._inner.send_message(topic, message, self._with_id(headers))

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """Inject one correlation id for the batch — a batch is one publish action by one caller.

        ``headers`` applies to every entry in the seam, so a per-message id would need a per-entry
        header channel the seam deliberately does not have; a caller that wants one id per message
        sends them one at a time, or sets the header itself.
        """
        return await delegate_batch(self._inner, messages, self._with_id(headers))

    def _with_id(self, headers: dict[str, str] | None) -> dict[str, str]:
        out = dict(headers or {})
        if not any(key.lower() == self._header.lower() for key in out):
            out[self._header] = self._new_id()
        return out


def with_retry(inner: MessageSender, **options: Any) -> RetryingMessageSender:
    """Sugar for :class:`RetryingMessageSender` — ``with_retry(sender, attempts=5)``."""
    return RetryingMessageSender(inner, **options)


def with_correlation_id(inner: MessageSender, **options: Any) -> CorrelationIdMessageSender:
    """Sugar for :class:`CorrelationIdMessageSender`."""
    return CorrelationIdMessageSender(inner, **options)
