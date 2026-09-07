"""The offload half (outbound): store the body, send a reference.

**Why a decorator and not a middleware.** .NET offloads with a middleware because its outbound side
*is* a middleware chain (``IMiddleware<OutboundContext>``). Python's outbound side is a decorator over
the one :class:`~benzene.core.MessageSender` seam — ``with_retry``, ``with_correlation_id``,
``with_rate_limit``, ``with_outbox`` — so the claim check belongs in that idiom: ``with_claim_check``
*is* a ``MessageSender``, the call site never changes, and the handler still writes
``await sender.send_message(topic, message)``. Same semantics, Python's shape. (The receiving half
*is* a middleware, because the receiving side is a pipeline in both ports —
:func:`~benzene.claim_check.claim_check_interception`.)

**Composition order.** Put the claim check directly around the transport, with transport-guarding
decorators *inside* it::

    sender = with_correlation_id(with_claim_check(with_retry(sqs), store))

* Anything **outside** the claim check sees the real message — which is what header-stampers
  (correlation id) and capture (the outbox) want.
* Anything **inside** it sees the placeholder — which is what retry wants: a retried send re-sends
  the same tiny reference rather than re-uploading the payload and orphaning the first copy. Putting
  retry outside would put a blob per attempt in the store.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from benzene.core import MessageSender, encode_body
from benzene.results import Result

from .store import ClaimCheckStore
from .wire import CLAIM_CHECK_HEADER, claim_check_placeholder

#: 192 KiB, matching .NET's ``ClaimCheckOptions.DefaultThresholdBytes``. Derived from the smallest
#: limit in the 256 KB family (SQS/SNS/EventBridge/Service Bus standard), leaving headroom for the
#: message attributes and envelope, which count against the same limit. A service on Azure Queue
#: Storage (64 KB) must lower it; there is no single number that fits every transport, which is why
#: it is a parameter and not a constant in the code path.
DEFAULT_THRESHOLD_BYTES = 192 * 1024


class ClaimCheckMessageSender:
    """Offloads an oversized message to a :class:`~benzene.claim_check.ClaimCheckStore`.

    Below ``threshold_bytes`` of serialized UTF-8 body the message is passed to ``inner``
    **untouched**, and the store is never contacted — most messages never offload and must not pay
    for the ones that do. At or above it (or always, with ``always_offload=True``), the serialized
    body is put into the store and ``inner`` is called with the placeholder body and the
    store-issued reference added to the headers.

    **Offload-then-send is not atomic, and this is stated rather than hidden.** The put happens
    before the send. A failed put *raises*, and the send never happens — the caller learns the
    message did not go, which is the safe direction. A successful put followed by a failed send
    leaves an orphaned object in the store until its retention rule expires it. That is the honest
    cost of the pattern; there is no two-phase commit here and pretending otherwise would be worse.

    **Serializer consistency.** ``serializer`` is used to measure *and* to store the body, and
    defaults to :func:`benzene.core.encode_body` — the single wire-encoding entry point every
    outbound transport in this port uses, so by default the bytes measured and stored are exactly
    the bytes an inline send would have produced. If you gave the transport a custom serializer
    (``SnsMessageSender(arn, serializer=my_dumps)``), pass **the same one here**; otherwise the size
    decision is made against a body the transport would never have sent. Python is safer than .NET
    on the other half of this coupling: the stored body is decoded by *this package's* hydrate
    middleware (default ``json.loads``), not by the receiving transport's deserializer, so a
    mismatch cannot silently corrupt a payload — it is a loud decode error naming the coupling. See
    :func:`~benzene.claim_check.claim_check_interception`.
    """

    def __init__(
        self,
        inner: MessageSender,
        store: ClaimCheckStore,
        *,
        threshold_bytes: int = DEFAULT_THRESHOLD_BYTES,
        always_offload: bool = False,
        header: str = CLAIM_CHECK_HEADER,
        serializer: Callable[[Any], str] = encode_body,
    ) -> None:
        self._inner = inner
        self._store = store
        self._threshold_bytes = threshold_bytes
        self._always_offload = always_offload
        # Header keys travel lower-cased (Context normalises on read); normalise on write too, so an
        # override spelled in mixed case still matches at the far end.
        self._header = header.lower()
        self._serialize = serializer

    @property
    def inner(self) -> MessageSender:
        """The transport this decorator wraps."""
        return self._inner

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        body = self._serialize(message)
        size = len(body.encode("utf-8"))
        if not self._always_offload and size < self._threshold_bytes:
            return await self._inner.send_message(topic, message, headers)

        # The put comes first and is not shielded: a store failure means no send at all.
        reference = await self._store.put(body, topic)
        stamped = {**(headers or {}), self._header: reference}
        return await self._inner.send_message(topic, claim_check_placeholder(reference), stamped)


def with_claim_check(
    inner: MessageSender, store: ClaimCheckStore, **options: Any
) -> ClaimCheckMessageSender:
    """Sugar for :class:`ClaimCheckMessageSender` — ``with_claim_check(sqs_sender, store)``."""
    return ClaimCheckMessageSender(inner, store, **options)
