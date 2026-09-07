"""Outbound client port (transport-bindings.md, "Outbound clients (the reverse direction)").

Every outbound client implements one interface — ``send_message(topic, message, headers) -> Result``
— over whatever native transport it wraps (Pub/Sub publish, SNS/SQS, Service Bus, an HTTP POST of
the wire envelope, …). Keeping the port in the core (not in a transport package) is what lets a
handler depend on "an outbound client" without coupling to a vendor, and lets tests substitute a
fake — the same shape the .NET port expresses as ``IBenzeneMessageSender``.

Beside it sits the **batch** seam — :class:`BatchMessageSender`, :class:`BatchResult`,
:class:`FailedMessage` and :func:`chunked` — for the transports whose APIs take N messages per call
(SQS ``SendMessageBatch``, SNS ``PublishBatch``, EventBridge ``PutEvents``, Kinesis ``PutRecords``,
Service Bus / Event Hub batches, Event Grid). Publishing a thousand events over ``send_message`` is
a thousand round trips; over ``send_batch`` it is a hundred or fewer. This mirrors .NET's
``IBenzeneBatchMessageClient`` / ``BatchSendResult`` / ``BatchSend.Chunk``, with one deliberate
difference: Python's protocol is *structural*, so a sender gains the capability by defining
``send_batch`` — there is no second client class to construct and wire up.

**Why a batch result is not a Result.** Every one of these APIs is explicitly partial-failure:
``SendMessageBatch`` answers with ``Successful`` *and* ``Failed`` lists, ``PutEvents`` with a
per-entry error. Collapsing that to one status would hide exactly the information the caller needs
— *which* messages to resend — so :class:`BatchResult` reports per-message outcomes and nothing
else. ``send_batch`` never raises for a message outcome, on the same rule ``send_message`` follows;
a missing optional SDK still escapes as its teaching ``ImportError``, because a forgotten extra is a
deployment error, not a message outcome.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar, runtime_checkable

from benzene.results import Result

T = TypeVar("T")


@runtime_checkable
class MessageSender(Protocol):
    """Sends a message on a topic through some transport, returning a Benzene :class:`Result`."""

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result: ...


@dataclass(frozen=True)
class FailedMessage:
    """One message of a batch that did not make it, at the caller's own index.

    ``index`` is the position in the sequence *the caller passed* — never the position within the
    chunk the transport actually sent — so ``[messages[f.index] for f in result.failures]`` is
    precisely the resend list. ``status`` is a Benzene status from the section 3 vocabulary, chosen
    so that the retryable ones (``service-unavailable``/``timeout``/``too-many-requests``, see
    :data:`~benzene.core.outbound.DEFAULT_RETRYABLE`) are the ones a fresh attempt might fix and a
    caller fault (``bad-request``) is not. ``detail`` carries the provider's own error code and
    message when it gave one, for logs and dead-letter records.
    """

    index: int
    status: str
    detail: str | None = None


@dataclass(frozen=True)
class BatchResult:
    """The per-message outcome of one :meth:`BatchMessageSender.send_batch` call.

    Success is the absence of a failure entry: a batch of 1,000 that all landed carries an empty
    ``failures`` tuple, and a partial failure carries one entry per message that did not.

    ``failures`` is always ordered by caller index, whatever order the provider reported them in
    (SQS's ``Failed`` list has no defined order, and a size-bounded transport discovers an oversized
    message before the batch around it fails), so a caller can read, log and resend deterministically.
    """

    failures: tuple[FailedMessage, ...] = ()

    def __post_init__(self) -> None:
        """Normalise to a tuple ordered by caller index — the annotation is documentation, not
        enforcement, and every implementation would otherwise have to remember to sort."""
        ordered = tuple(sorted(self.failures, key=lambda failure: failure.index))
        if not isinstance(self.failures, tuple) or ordered != self.failures:
            object.__setattr__(self, "failures", ordered)

    @property
    def all_succeeded(self) -> bool:
        """Whether every message in the batch was accepted by the transport."""
        return not self.failures

    def __bool__(self) -> bool:
        """Truthy when everything sent — ``if not await sender.send_batch(...)`` reads correctly."""
        return self.all_succeeded

    @property
    def failed_indexes(self) -> tuple[int, ...]:
        """The caller-list positions that failed, in order."""
        return tuple(failure.index for failure in self.failures)

    def failure_for(self, index: int) -> FailedMessage | None:
        """The failure recorded for a caller-list position, or ``None`` if that message sent."""
        for failure in self.failures:
            if failure.index == index:
                return failure
        return None


@runtime_checkable
class BatchMessageSender(Protocol):
    """Sends N messages through one transport call, reporting a per-message outcome.

    ``messages`` is a sequence of ``(topic, message)`` pairs, so one batch can span Benzene topics —
    every transport here routes by attribute/property, so mixing topics in one API call costs
    nothing. ``headers`` is per-call and applies to every entry, exactly like ``send_message``.

    **Ordering.** Entries keep the caller's order within each transport call, and chunks are sent in
    order, one after another — but none of these transports promises cross-call ordering, and a
    caller that resends the failed subset necessarily reorders it relative to the messages that
    landed first time. Where order matters, key the transport (a Kinesis partition key, an SQS FIFO
    message group) and treat batching as a throughput optimisation within that key, not as a
    sequencing guarantee.
    """

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult: ...


def chunked(items: Sequence[T], size: int) -> Iterator[list[tuple[int, T]]]:
    """Split ``items`` into ``size``-capped chunks, each item paired with its original index.

    The index is what lets a chunk's per-entry failures be reported at the caller's own positions
    (mirrors .NET's ``BatchSend.Chunk``). An empty sequence yields nothing at all — a caller with no
    messages must cost zero API calls.
    """
    if size < 1:
        raise ValueError(f"chunk size must be at least 1, got {size}")
    for start in range(0, len(items), size):
        window = items[start : start + size]
        yield [(start + offset, item) for offset, item in enumerate(window)]


async def send_batch_sequentially(
    sender: MessageSender,
    messages: Sequence[tuple[str, Any]],
    headers: dict[str, str] | None = None,
) -> BatchResult:
    """Send a batch one message at a time through ``sender``, still reporting per-message outcomes.

    The honest fallback for a transport with **no** batch API (Azure Storage Queues, a Lambda
    invoke, a plain HTTP client): it is N round trips and it is documented as such wherever it is
    used — it must never be presented as an atomic batch. What it does preserve is the seam's
    contract: every message is attempted, one failure aborts nothing, and each failure is reported
    at the caller's index with the status its own ``send_message`` returned.

    A raised exception is mapped to that one message's failure rather than losing the rest — except
    an ``ImportError``, which stays a deployment error and propagates, as it does from
    ``send_message``.
    """
    failures: list[FailedMessage] = []
    for index, (topic, message) in enumerate(messages):
        try:
            result = await sender.send_message(topic, message, headers)
        except ImportError:
            raise  # a missing optional SDK is a deployment error, never a per-message outcome
        except Exception as ex:  # a sender that raises is a bug, but not this batch's problem
            failures.append(FailedMessage(index, "service-unavailable", str(ex)))
            continue
        if not result.is_successful:
            failures.append(FailedMessage(index, result.status, "; ".join(result.messages) or None))
    return BatchResult(tuple(failures))


async def delegate_batch(
    inner: Any, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None
) -> BatchResult:
    """Send through ``inner``'s native ``send_batch`` when it has one, else one message at a time.

    The structural-typing pay-off: a decorator does not need to know whether the sender it wraps
    talks to SQS (native batch) or to an HTTP endpoint (no such thing), and gains batching for free
    the day that sender grows a ``send_batch``.
    """
    send_batch = getattr(inner, "send_batch", None)
    if send_batch is None:
        return await send_batch_sequentially(inner, messages, headers)
    result: BatchResult = await send_batch(messages, headers)
    return result
