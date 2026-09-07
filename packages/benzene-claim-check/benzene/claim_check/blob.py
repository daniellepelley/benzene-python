"""The Azure Blob Storage store (extra ``[azure]``) — the durable store for an Azure deployment.

Mirrors .NET's ``Benzene.ClaimCheck.Azure.Blob``. The Azure SDK is an optional dependency, imported
lazily with the teaching ImportError every Azure binding in this port uses, and a
``ContainerClient`` can be injected so the store is fully exercisable with no Azure SDK present.

**Retention is a lifecycle-management policy, and this package does not create one.** Nothing here
deletes on read (see :class:`~benzene.claim_check.ClaimCheckStore`), so a container with no
expiration policy grows forever. Configure Blob lifecycle management on the store's prefix, sized to
outlive queue retention plus every dead-letter redrive window.

Azure Queue Storage's 64 KB message limit is the tightest of any transport this port supports, so a
service on it should lower ``threshold_bytes`` well below the 192 KiB default (see
:data:`~benzene.claim_check.DEFAULT_THRESHOLD_BYTES`) — the default is sized for the 256 KB family.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from .keys import DEFAULT_PREFIX, default_key
from .store import ClaimCheckStoreMismatch

#: The reference scheme this store issues and accepts.
AZBLOB_SCHEME = "azblob"


def _is_missing(exc: Exception) -> bool:
    """True for the SDK's ``ResourceNotFoundError``/404; anything else re-raises.

    Matched by type name and status code rather than by ``except ResourceNotFoundError`` so the
    module stays importable — and this branch stays testable — with no Azure SDK installed.
    """
    return type(exc).__name__ == "ResourceNotFoundError" or getattr(exc, "status_code", None) == 404


def _read(stream: Any) -> bytes:
    """Drain a downloader: ``readall`` on the real SDK, ``read`` on anything simpler."""
    readall = getattr(stream, "readall", None)
    return bytes(readall() if callable(readall) else stream.read())


class BlobClaimCheckStore:
    """Stores offloaded payloads as blobs, issuing ``azblob://{container}/{key}`` references.

    Keys are ``{prefix}{topic}/{key()}`` — the same layout as
    :class:`~benzene.claim_check.S3ClaimCheckStore`, with the topic verbatim. Pass ``client`` (an
    ``azure.storage.blob.ContainerClient``) to inject one, or ``account_url`` to have one built
    lazily with ``DefaultAzureCredential``.

    Every blocking SDK call runs through :func:`asyncio.to_thread`, so a hydration never stalls the
    event loop.
    """

    def __init__(
        self,
        container: str,
        *,
        account_url: str | None = None,
        client: Any | None = None,
        prefix: str = DEFAULT_PREFIX,
        key: Callable[[], str] = default_key,
    ) -> None:
        self._container = container
        self._account_url = account_url
        self._client = client
        self._prefix = prefix.lstrip("/")
        self._key = key

    def _blobs(self) -> Any:
        """The container client, importing the optional ``[azure]`` SDK lazily on first use."""
        if self._client is None:
            try:
                from azure.identity import (  # noqa: PLC0415 - lazy: optional [azure] extra
                    DefaultAzureCredential,
                )
                from azure.storage.blob import ContainerClient  # noqa: PLC0415 - lazy
            except ImportError as exc:
                raise ImportError(
                    "BlobClaimCheckStore requires azure-storage-blob and azure-identity — install "
                    "them with 'pip install benzene-claim-check[azure]', or pass a ContainerClient "
                    "you built yourself."
                ) from exc
            self._client = ContainerClient(
                self._account_url, self._container, credential=DefaultAzureCredential()
            )
        return self._client

    @property
    def _reference_prefix(self) -> str:
        return f"{AZBLOB_SCHEME}://{self._container}/{self._prefix}"

    async def put(self, body: str, topic: str) -> str:
        name = f"{self._prefix}{topic}/{self._key()}"
        await asyncio.to_thread(
            self._blobs().upload_blob, name, body.encode("utf-8"), overwrite=True
        )
        return f"{AZBLOB_SCHEME}://{self._container}/{name}"

    async def get(self, reference: str) -> str | None:
        name = self._require_own(reference)
        try:
            stream = await asyncio.to_thread(self._blobs().download_blob, name)
            body = await asyncio.to_thread(_read, stream)
        except Exception as exc:
            if _is_missing(exc):
                return None  # expired or never stored — a miss, not a mismatch
            raise
        return body.decode("utf-8")

    async def delete(self, reference: str) -> None:
        """Operator/test cleanup. Never called on the read path — see the port's docstring."""
        name = self._require_own(reference)
        try:
            await asyncio.to_thread(self._blobs().delete_blob, name)
        except Exception as exc:
            if _is_missing(exc):
                return  # already gone: deleting what does not exist is a no-op, never an error
            raise

    def _require_own(self, reference: str) -> str:
        """Validate against this store's own container and prefix, and return the blob name.

        A single prefix comparison, before any client call — the same security boundary
        :class:`~benzene.claim_check.S3ClaimCheckStore` documents.
        """
        if not reference.startswith(self._reference_prefix):
            raise ClaimCheckStoreMismatch(reference)
        return reference[len(f"{AZBLOB_SCHEME}://{self._container}/") :]
