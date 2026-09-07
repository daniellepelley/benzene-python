"""``benzene.claim_check`` — send a payload the transport is too small to carry.

**The problem.** Every transport caps a message: SQS, SNS, EventBridge and Service Bus standard at
256 KB, Azure Queue Storage at 64 KB. A handler with a legitimately larger message — a document, an
image manifest, a batch of a few thousand rows — simply cannot send it. The publish is refused, and
there is nothing the application can do about it from inside Benzene.

**What this package does.** ``with_claim_check(sender, store)`` puts the serialized body into a blob
store and sends a tiny placeholder in its place, carrying the store-issued reference on the
``benzene-claim-check`` header. On the receiving side, ``claim_check_interception(store)`` reads that
header, resolves the reference, and puts the real payload back before the router maps it — so the
handler sees exactly the request that was sent, and neither side's code knows anything happened.

**What it guarantees, exactly.**

* **Offload-then-send is not atomic.** The put happens before the send. A failed put raises and the
  send never happens (the safe direction). A successful put followed by a failed send orphans the
  stored object until its retention rule expires it. There is no two-phase commit here.
* **A missing payload fails loud.** An unresolvable reference raises :class:`ClaimCheckNotFound` and
  the message fails, so the transport's own nack → redelivery → dead-letter path applies. There is
  never a silent skip, and a placeholder is never handed to a handler.
* **Nothing is ever deleted on read.** Fan-out delivers one offloaded message to several consumers
  and at-least-once transports redeliver; a read-time delete would starve the siblings and make a
  retry permanently unhydratable. Retention is store-side expiry owned by infrastructure — an S3
  lifecycle rule or a Blob lifecycle policy on the store's prefix — sized to outlive queue retention
  plus every dead-letter redrive window. Benzene does not create that rule, and a store with no rule
  grows forever.
* **A store resolves only its own references.** A reference outside a store's configured
  scheme/bucket/container/prefix raises :class:`ClaimCheckStoreMismatch` *before* any fetch. It
  arrived on a wire header, so it is attacker-controllable; that check is a security boundary, and it
  is why "not mine" is a different exception from "not found".
* **:class:`InMemoryClaimCheckStore` is single-process.** On more than one instance the receiving pod
  is usually not the sending one, so every hydration misses. Use :class:`S3ClaimCheckStore` or
  :class:`BlobClaimCheckStore` for anything real.

**The wire surface is a cross-port contract**, taken byte-for-byte from .NET's ``Benzene.ClaimCheck``
so a Python offload is hydratable by a .NET consumer and vice versa: the header
:data:`CLAIM_CHECK_HEADER` (``benzene-claim-check``, a **Tier C** add-on header in
``wire-contracts.md`` §2) and the placeholder key :data:`PLACEHOLDER_KEY` (``_benzeneClaimCheck``).
The specification covers the header and the resolution rules in §2.1 and deliberately leaves the
*placeholder body* unspecified; matching .NET's placeholder is therefore this port moving with .NET
slightly ahead of the written spec, and should be proposed upstream. See :mod:`.wire` for the quoted
.NET source and the full argument.

Mirrors .NET's ``Benzene.ClaimCheck`` in semantics, in Python's idiom: the outbound half is a
``MessageSender`` decorator rather than an outbound-route middleware (Python's outbound side is
decorators), and the inbound half needs no ``IMessageBodySetter<TContext>`` per transport, because
every Python host funnels through one parsed ``context.request`` — so one middleware hydrates every
transport, Azure Service Bus included, which .NET cannot do.
"""

from __future__ import annotations

from .blob import AZBLOB_SCHEME, BlobClaimCheckStore
from .interception import claim_check_interception
from .keys import DEFAULT_PREFIX, default_key
from .memory import DEFAULT_TTL_SECONDS, MEMORY_SCHEME, InMemoryClaimCheckStore
from .s3 import S3_SCHEME, S3ClaimCheckStore
from .sender import DEFAULT_THRESHOLD_BYTES, ClaimCheckMessageSender, with_claim_check
from .store import (
    ClaimCheckError,
    ClaimCheckNotFound,
    ClaimCheckStore,
    ClaimCheckStoreMismatch,
)
from .wire import CLAIM_CHECK_HEADER, PLACEHOLDER_KEY, claim_check_placeholder

__all__ = [
    "AZBLOB_SCHEME",
    "CLAIM_CHECK_HEADER",
    "DEFAULT_PREFIX",
    "DEFAULT_THRESHOLD_BYTES",
    "DEFAULT_TTL_SECONDS",
    "MEMORY_SCHEME",
    "PLACEHOLDER_KEY",
    "S3_SCHEME",
    "BlobClaimCheckStore",
    "ClaimCheckError",
    "ClaimCheckMessageSender",
    "ClaimCheckNotFound",
    "ClaimCheckStore",
    "ClaimCheckStoreMismatch",
    "InMemoryClaimCheckStore",
    "S3ClaimCheckStore",
    "claim_check_interception",
    "claim_check_placeholder",
    "default_key",
    "with_claim_check",
]
