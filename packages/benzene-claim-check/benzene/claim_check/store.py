"""The store port — where an offloaded body lives, and the two ways resolving one can fail.

The port is three methods and no policy. ``put`` takes the serialized wire body and answers an
opaque, store-issued reference; ``get`` resolves one back; ``delete`` removes one. Neither middleware
half ever calls ``delete`` — see :class:`ClaimCheckStore` for why that is a deliberate omission
rather than an oversight.

**The two failure modes are deliberately separate types.** A reference that is well-formed for this
store but resolves to nothing is a *miss* (``get`` answers ``None``, and the hydrate middleware turns
that into :class:`ClaimCheckNotFound`). A reference that is not this store's to resolve at all — a
foreign scheme, another service's bucket, a location outside the configured prefix — is a
:class:`ClaimCheckStoreMismatch`, raised *before* the backing client is touched. Merging them would
be a real mistake, and not a cosmetic one:

* they have different causes — an expiry or a lost blob against a wrong or hostile reference;
* they have different operator responses — widen the retention window against investigate who sent
  that reference;
* and the second is a **security boundary**. The reference arrives on a wire header, which means it
  is attacker-controllable. A store that treated "not mine" as "not found" would have to *try* the
  fetch to discover the answer, which is precisely the fetch it must never make
  (``wire-contracts.md`` §2.1: "a consumer MUST NOT fetch an attacker-supplied arbitrary location").
  Checking the reference against the store's own configuration first, and refusing loudly, is the
  whole mechanism.

.NET draws the same line with ``ClaimCheckNotFoundException`` / ``ClaimCheckStoreMismatchException``.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


class ClaimCheckError(Exception):
    """Base class for the claim check's two failure modes, for a caller that wants to catch both."""


class ClaimCheckNotFound(ClaimCheckError):
    """A reference this store could have issued resolved to nothing — expired, or never stored.

    Raised by the hydrate middleware, never swallowed: the message fails, so the transport's own
    semantics (nack → redelivery → dead-letter) apply exactly as they would to any other
    unprocessable message. There is no silent skip and the placeholder is never handed to a handler.
    """

    def __init__(self, reference: str) -> None:
        super().__init__(
            f"No claim-check payload was found for reference {reference!r}. It may have expired, or "
            "never existed. Check that the sender and receiver share the same store, and that the "
            "store's retention window outlives queue retention plus any dead-letter redrive window."
        )
        self.reference = reference


class ClaimCheckStoreMismatch(ClaimCheckError):
    """A reference that lies outside this store's own configuration — refused, not attempted.

    A security boundary, not a not-found: the reference came off a wire header, so a store must
    verify it belongs to its own scheme/bucket/container/prefix *before* issuing any fetch, and must
    never fall through to treating it as merely missing.
    """

    def __init__(self, reference: str) -> None:
        super().__init__(
            f"The claim-check reference {reference!r} does not belong to this store's own "
            "configuration (scheme/location/prefix). Refusing to resolve a reference outside the "
            "store's configuration."
        )
        self.reference = reference


@runtime_checkable
class ClaimCheckStore(Protocol):
    """Pluggable persistence for offloaded payloads.

    Implement this to put offloaded bodies somewhere else (a different bucket layout, a database, a
    filesystem) without touching either middleware half. Two rules bind every implementation:

    **1. Refuse a foreign reference.** :meth:`get` MUST raise :class:`ClaimCheckStoreMismatch` for a
    reference outside this store's own configuration, before contacting the backing service — see
    this module's docstring.

    **2. Never delete on read.** :meth:`get` MUST NOT remove what it returns, and neither middleware
    half calls :meth:`delete`. A fan-out transport (SNS, Pub/Sub) delivers one offloaded message to
    several independent consumers, and every at-least-once transport redelivers: a read-time delete
    would starve the siblings and make a retry permanently unhydratable — turning a transient failure
    into a poison message. ``wire-contracts.md`` §2.1 states this as a prohibition.

    Retention is therefore **store-side expiry owned by infrastructure** — an S3 lifecycle rule or an
    Azure Blob lifecycle-management policy on the store's prefix, the same posture the outbox takes
    to its own retention. Benzene does not create that policy; sizing it is a deployment decision, and
    the rule is: **the retention window must outlive the longest path from send to last possible
    consumption** — queue retention plus every dead-letter redrive window a consumer might use. Too
    short and a legitimate redelivery arrives to find its payload gone; too long only costs storage.

    :meth:`delete` exists for the operator and for tests — cleaning up after a load test, purging a
    payload on a data-subject request, or removing an orphan a failed send left behind. It is never a
    read path.
    """

    async def put(self, body: str, topic: str) -> str:
        """Store ``body`` verbatim and return an opaque reference of the form ``scheme://location/key``.

        ``topic`` is for key partitioning only (an S3 prefix per topic, say); it plays no part in
        resolving a reference. The body is the serialized wire body, so a store never needs to know
        what a payload means.
        """
        ...

    async def get(self, reference: str) -> str | None:
        """Resolve a reference back to its stored body, or ``None`` when it is missing or expired.

        Raises :class:`ClaimCheckStoreMismatch` — before touching the backing service — when the
        reference is not this store's to resolve.
        """
        ...

    async def delete(self, reference: str) -> None:
        """Remove a stored payload. **Never called on the read path** — see the class docstring.

        A no-op for a reference that is already gone. Raises :class:`ClaimCheckStoreMismatch` for a
        foreign reference, on the same boundary rule as :meth:`get`.
        """
        ...
