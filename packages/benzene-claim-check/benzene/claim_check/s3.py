"""The S3 store (extra ``[s3]``) — the durable claim-check store for an AWS deployment.

Mirrors .NET's ``Benzene.ClaimCheck.Aws.S3``. ``boto3`` is an optional dependency, imported lazily
with the teaching ImportError every AWS binding in this port uses, and a client can be injected so
the store is fully exercisable with no AWS SDK present.

**Retention is a bucket lifecycle rule, and this package does not create one.** Nothing here deletes
on read (see :class:`~benzene.claim_check.ClaimCheckStore`), so a bucket with no expiration rule
grows forever — an unbounded cost leak. Configure an S3 Lifecycle rule on the store's prefix, sized
to outlive queue retention plus every dead-letter redrive window a consumer might use.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from .keys import DEFAULT_PREFIX, default_key
from .store import ClaimCheckStoreMismatch

#: The reference scheme this store issues and accepts.
S3_SCHEME = "s3"


def _is_missing(exc: Exception) -> bool:
    """True for boto3's ``NoSuchKey``/404 on a missing object; anything else re-raises."""
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    return str(response.get("Error", {}).get("Code", "")) in ("NoSuchKey", "404")


class S3ClaimCheckStore:
    """Stores offloaded payloads as S3 objects, issuing ``s3://{bucket}/{key}`` references.

    Keys are ``{prefix}{topic}/{key()}`` — the topic **verbatim**, because S3 keys permit the ``:``
    a Benzene topic carries, and per-topic prefixes are what let one bucket serve several topics with
    different lifecycle rules. ``client`` (a boto3 S3 client) may be injected; otherwise one is
    created lazily on first use. ``key`` is injectable for tests and for a deployment that wants a
    different layout.

    Every blocking SDK call runs through :func:`asyncio.to_thread`, so a hydration never stalls the
    event loop — the same rule the rest of this port's AWS bindings follow.
    """

    def __init__(
        self,
        bucket: str,
        *,
        client: Any | None = None,
        prefix: str = DEFAULT_PREFIX,
        key: Callable[[], str] = default_key,
        content_type: str = "application/octet-stream",
    ) -> None:
        self._bucket = bucket
        self._client = client
        self._prefix = prefix.lstrip("/")
        self._key = key
        self._content_type = content_type

    def _s3(self) -> Any:
        """The S3 client, importing the optional ``[s3]`` SDK lazily on first use.

        A missing SDK is a *deployment* error, not a message outcome, so it surfaces as an
        ImportError naming the exact extra rather than being mapped to a retryable failure.
        """
        if self._client is None:
            try:
                import boto3  # noqa: PLC0415 - lazy: optional [s3] extra
            except ImportError as exc:
                raise ImportError(
                    "S3ClaimCheckStore requires boto3 — install it with "
                    "'pip install benzene-claim-check[s3]', or pass a client you built yourself."
                ) from exc
            self._client = boto3.client("s3")
        return self._client

    @property
    def _reference_prefix(self) -> str:
        return f"{S3_SCHEME}://{self._bucket}/{self._prefix}"

    async def put(self, body: str, topic: str) -> str:
        key = f"{self._prefix}{topic}/{self._key()}"
        await asyncio.to_thread(
            self._s3().put_object,
            Bucket=self._bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType=self._content_type,
        )
        return f"{S3_SCHEME}://{self._bucket}/{key}"

    async def get(self, reference: str) -> str | None:
        key = self._require_own(reference)
        try:
            response = await asyncio.to_thread(self._s3().get_object, Bucket=self._bucket, Key=key)
        except Exception as exc:
            if _is_missing(exc):
                return None  # expired or never stored — a miss, not a mismatch
            raise
        body = await asyncio.to_thread(response["Body"].read)
        return body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)

    async def delete(self, reference: str) -> None:
        """Operator/test cleanup. Never called on the read path — see the port's docstring."""
        key = self._require_own(reference)
        await asyncio.to_thread(self._s3().delete_object, Bucket=self._bucket, Key=key)

    def _require_own(self, reference: str) -> str:
        """Validate the reference against this store's own bucket and prefix, and return its key.

        A **single prefix comparison**, deliberately — not :func:`urllib.parse.urlparse`, whose
        quoting rules would not round-trip the ``:`` in a Benzene topic. The check happens before any
        client call: the reference arrives on a wire header, so resolving one outside this store's
        configuration would mean fetching an attacker-supplied location.
        """
        if not reference.startswith(self._reference_prefix):
            raise ClaimCheckStoreMismatch(reference)
        return reference[len(f"{S3_SCHEME}://{self._bucket}/") :]
