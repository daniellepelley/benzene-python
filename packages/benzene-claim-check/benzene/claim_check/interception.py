"""The hydrate half (inbound): resolve the reference, put the real body back.

Install it ahead of the message router — after the observability prelude, so a store fetch shows up
in the trace, and before anything that reads ``context.request``::

    pipeline.use(tracing_interception(...))
    pipeline.use(claim_check_interception(store))
    pipeline.use(message_router(registry))

**Where Python is structurally simpler than .NET, and it is worth saying why.** .NET has to replace
the *raw transport body* before deserialization, which needs an ``IMessageBodySetter<TContext>``
implementation per transport — and Azure Service Bus hydration is blocked outright, because
``ServiceBusReceivedMessage.Body`` has no setter. Python has no such problem: every host funnels
through :class:`benzene.core.BenzeneMessageApplication`, which parses the body once and hands the
pipeline a ``context.request``. One middleware therefore hydrates every transport, Service Bus
included, and the body-setter abstraction is deliberately **not** ported.

The one behavioural difference that follows: .NET replaces the body *before* deserialization, Python
replaces the already-parsed request. Same wire outcome, same handler input — but it means the fetched
body is decoded here, by ``decoder`` (``json.loads`` by default), rather than by the transport's own
deserializer.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from benzene.core import Context, Middleware, Next

from .store import ClaimCheckNotFound, ClaimCheckStore
from .wire import CLAIM_CHECK_HEADER


def claim_check_interception(
    store: ClaimCheckStore,
    *,
    header: str = CLAIM_CHECK_HEADER,
    decoder: Callable[[str], Any] = json.loads,
) -> Middleware:
    """Middleware that replaces a claim-checked message's request with the stored payload.

    * **No header → nothing happens.** The message passes straight through and the store is never
      contacted. Most messages are not offloaded, and they must not pay for the ones that are.
    * **Header present → the store is authoritative.** The reference is resolved and
      ``context.request`` is replaced with the decoded body before the router maps it onto the
      handler's declared type. The placeholder body is never read — the header is the contract
      (``wire-contracts.md`` §2.1) and a consumer must not interpret the body of an offloaded
      message.
    * **Unresolvable → loud.** A ``None`` from the store raises
      :class:`~benzene.claim_check.ClaimCheckNotFound`; a reference outside the store's own
      configuration raises :class:`~benzene.claim_check.ClaimCheckStoreMismatch` from the store
      itself, before any fetch. Neither is caught here. The pipeline turns an escaping exception into
      an unsuccessful result, so the transport's normal semantics — nack, redelivery, eventually a
      dead-letter — apply exactly as they would for any other unprocessable message. There is no
      silent skip, and a placeholder is never handed to a handler.
    * **Nothing is ever deleted.** Fan-out siblings and redeliveries re-read the same reference; see
      :class:`~benzene.claim_check.ClaimCheckStore` for why a read-time delete is forbidden.

    ``header`` must match the sender's (overriding it is a deployment agreement that applies to both
    directions). ``decoder`` must match the sending side's serializer: both default to the wire pair
    (:func:`benzene.core.encode_body` / ``json.loads``), and a mismatch is a loud decode failure that
    names the coupling rather than a corrupted payload.
    """
    name = header.lower()

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        reference = context.headers.get(name, "").strip()
        if not reference:
            await next()
            return

        body = await store.get(reference)
        if body is None:
            raise ClaimCheckNotFound(reference)

        try:
            context.request = decoder(body)
        except ValueError as exc:
            raise ValueError(
                f"The claim-checked payload at {reference!r} could not be decoded: {exc}. The "
                "hydrate middleware's decoder must match the serializer the offloading sender was "
                "given — both default to the wire pair (benzene.core.encode_body / json.loads); if "
                "you passed a custom serializer to ClaimCheckMessageSender, pass its counterpart as "
                "claim_check_interception(store, decoder=...)."
            ) from exc

        await next()

    return middleware
