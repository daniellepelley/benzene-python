"""Outbound client port (transport-bindings.md, "Outbound clients (the reverse direction)").

Every outbound client implements one interface — ``send_message(topic, message, headers) -> Result``
— over whatever native transport it wraps (Pub/Sub publish, SNS/SQS, Service Bus, an HTTP POST of
the wire envelope, …). Keeping the port in the core (not in a transport package) is what lets a
handler depend on "an outbound client" without coupling to a vendor, and lets tests substitute a
fake — the same shape the .NET port expresses as ``IBenzeneMessageSender``.
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol, runtime_checkable

from benzene.results import Result, Status


@runtime_checkable
class MessageSender(Protocol):
    """Sends a message on a topic through some transport, returning a Benzene :class:`Result`."""

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result: ...


#: Exception class names that mean "the caller's deadline elapsed" in SDKs this package cannot import:
#: botocore's ``ReadTimeoutError``/``ConnectTimeoutError``, httpx's ``TimeoutException`` family,
#: azure-core's ``Service*TimeoutError``, requests' ``Timeout``, google-api-core's
#: ``DeadlineExceeded`` - matched anywhere in the exception's MRO, so a subclass counts too.
_TIMEOUT_NAME_MARKERS = ("Timeout", "TimedOut", "DeadlineExceeded")


def is_timeout(error: BaseException) -> bool:
    """Whether ``error`` reports that the caller's own deadline elapsed, rather than a dead peer.

    A builtin :class:`TimeoutError` (``socket.timeout`` and, from 3.11, ``asyncio.TimeoutError``),
    3.10's distinct ``asyncio.TimeoutError``, or an SDK exception named for a timeout. A wrapper whose
    cause (``__cause__``/``__context__``) or ``reason`` - ``urllib``'s ``URLError`` keeps the socket
    timeout there - is a timeout counts as one.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    pending: list[BaseException] = []
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (TimeoutError, asyncio.TimeoutError)):
            return True
        if any(
            marker in cls.__name__
            for cls in type(current).__mro__
            for marker in _TIMEOUT_NAME_MARKERS
        ):
            return True
        for linked in (
            getattr(current, "reason", None),
            current.__cause__,
            current.__context__,
        ):
            if isinstance(linked, BaseException):
                pending.append(linked)
        current = pending.pop() if pending else None
    return False


def failed_call_result(error: BaseException, target: str = "") -> Result[Any]:
    """The :class:`Result` for an outbound call that failed before any reply arrived.

    transport-bindings.md §1 (spec 1.1): no connection, a refused or reset request, or the client's
    own deadline SHOULD yield a result rather than an exception - ``timeout`` when the deadline
    elapsed (:func:`is_timeout`), otherwise ``service-unavailable`` - so a caller handles a dead peer
    as it handles every other failure, and the retry and circuit-breaker decorators see it as the
    transient failure it is. The error's own text is the message, prefixed with ``target`` (the URL,
    queue or function the call was for) when given.
    """
    status = Status.TIMEOUT if is_timeout(error) else Status.SERVICE_UNAVAILABLE
    reason = str(error) or type(error).__name__
    return Result.failure(status, f"{target}: {reason}" if target else reason)
