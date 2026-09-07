"""The in-process store — for tests, local development, and a genuinely single-instance service.

Mirrors .NET's ``InMemoryClaimCheckStore`` (a dictionary, a lock, and a TTL) with the same caveat,
which is worth stating plainly because getting it wrong is silent: **state lives in this process
only**. On a multi-instance deployment the instance that offloaded a payload and the instance that
receives the message are usually not the same one, so every hydration on another pod raises
:class:`~benzene.claim_check.ClaimCheckNotFound` and the message dead-letters. A real deployment needs
a shared, durable store — :class:`~benzene.claim_check.S3ClaimCheckStore` or
:class:`~benzene.claim_check.BlobClaimCheckStore`.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from urllib.parse import quote

from .store import ClaimCheckStoreMismatch

#: The reference scheme this store issues and accepts (.NET ``InMemoryClaimCheckStore.Scheme``).
MEMORY_SCHEME = "memory"

#: 24 hours, matching .NET's default. As with any store, size it to outlive queue retention plus
#: every dead-letter redrive window — in-process that is bounded by the process's own lifetime too.
DEFAULT_TTL_SECONDS = 24 * 60 * 60


class InMemoryClaimCheckStore:
    """A process-local :class:`~benzene.claim_check.ClaimCheckStore` with a per-entry TTL.

    ``ttl`` (seconds) bounds how long a payload is retained; ``None`` keeps entries for the process
    lifetime. ``clock`` is injectable so a test can expire an entry without sleeping. Entries are
    expired lazily, on the next read of their key.

    References are ``memory://{topic}/{uuid4 hex}`` with the topic percent-encoded, so a Benzene
    topic's ``:`` cannot be mistaken for structure in the reference.
    """

    def __init__(
        self,
        *,
        ttl: float | None = DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        new_key: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> None:
        self._ttl = ttl
        self._clock = clock
        self._new_key = new_key
        self._entries: dict[str, tuple[str, float | None]] = {}

    async def put(self, body: str, topic: str) -> str:
        reference = f"{MEMORY_SCHEME}://{quote(topic, safe='')}/{self._new_key()}"
        expires_at = None if self._ttl is None else self._clock() + self._ttl
        self._entries[reference] = (body, expires_at)
        return reference

    async def get(self, reference: str) -> str | None:
        self._require_own(reference)
        entry = self._entries.get(reference)
        if entry is None:
            return None
        body, expires_at = entry
        if expires_at is not None and self._clock() >= expires_at:
            del self._entries[reference]
            return None
        return body

    async def delete(self, reference: str) -> None:
        """Operator/test cleanup. Never called by the hydrate middleware — see the port's docstring."""
        self._require_own(reference)
        self._entries.pop(reference, None)

    def _require_own(self, reference: str) -> None:
        # The boundary check comes first, and is a plain prefix comparison rather than a URL parse:
        # a Benzene topic carries a ':' that urlparse would not round-trip.
        if not reference.startswith(f"{MEMORY_SCHEME}://"):
            raise ClaimCheckStoreMismatch(reference)
