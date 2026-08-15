"""Publish the mesh-ui artifacts to Azure Blob Storage (the Azure analogue of :mod:`.artifacts`' local
writer and :mod:`.s3_artifacts`' S3 counterpart).

Where :func:`~benzene.mesh.artifacts.write_artifacts` lays the catalog out on a local filesystem for a
co-hosted host to serve (the Fargate/K8s collector), an Azure Functions-based mesh aggregator has no
persistent filesystem of its own to serve from — it publishes to Blob Storage instead, and something
else (a static viewer, or the container's own `$web` static-website endpoint) reads the blobs back.
:class:`BlobArtifactStore` is that seam: given a container (an ``azure.storage.blob.ContainerClient``,
or an account URL + container name to build one), it uploads one JSON blob per document.
:func:`write_artifacts_to_blob` is the Blob counterpart of ``write_artifacts``/``write_artifacts_to_s3``
— same ``(collector, sources, generated_at)`` shape, targeting a store instead of a directory/bucket —
and lays out the identical document set under the store's prefix: ``manifest.json``, ``topology.json``,
``topics.json``, ``usage.json``, ``asyncapi.json``, ``annotations.json``, and one
``services/{name}.json`` per service.

:meth:`BlobArtifactStore.write` is deliberately generic (key, document) rather than tied to a fixed
manifest, so a caller can also use it to publish an out-of-band document alongside the catalog — e.g.
the discovered registry from a :class:`~benzene.mesh_fleet.discovery.Discovery` pass, mirroring the AWS
mesh Lambda's ``store.write('registry.json', ...)`` (:mod:`.s3_artifacts`).

This module is purely additive: it does not change ``artifacts.py``, ``store.py``, or ``s3_artifacts.py``
— the local filesystem path (:class:`~benzene.mesh.store.JsonFileCollectorStore`, plain
``write_artifacts``) and the S3 path are both unaffected by its presence. The Azure Storage Blob SDK is
an optional dependency, imported lazily, matching every other Azure binding in this port
(``benzene.azure.clients``); construct with an injected ``client`` (an
``azure.storage.blob.ContainerClient``) to run with no Azure SDK present.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from .artifacts import ServiceEndpoint, build_artifacts
from .collector import MeshCollector


class BlobArtifactStore:
    """Uploads JSON documents into an Azure Blob Storage container under an optional key prefix.

    ``prefix`` is stripped of leading/trailing slashes and joined with a plain ``/`` — pass ``""``
    (the default) to publish at the container root. Each :meth:`write` is one ``upload_blob`` call
    (``overwrite=True``); there is no batching, so a partial publish (a failure partway through
    :func:`write_artifacts_to_blob`) can leave a stale sibling document, exactly as
    :class:`~benzene.mesh.s3_artifacts.S3ArtifactStore` documents for S3 — callers that need
    all-or-nothing semantics should retry the whole pass.
    """

    def __init__(
        self,
        account_url: str | None = None,
        container_name: str | None = None,
        prefix: str = "",
        *,
        client: Any | None = None,
    ) -> None:
        self._account_url = account_url
        self._container_name = container_name
        self._prefix = prefix.strip("/")
        self._client = client

    def _container(self) -> Any:
        if self._client is None:
            from azure.identity import DefaultAzureCredential  # lazy: optional [azure] dependency
            from azure.storage.blob import ContainerClient

            self._client = ContainerClient(
                self._account_url, self._container_name, credential=DefaultAzureCredential()
            )
        return self._client

    def _key(self, key: str) -> str:
        key = key.lstrip("/")
        return f"{self._prefix}/{key}" if self._prefix else key

    def write(self, key: str, document: dict[str, Any]) -> None:
        """Upload one JSON-serialized ``document`` at ``key`` (resolved against the store's prefix)."""
        from azure.storage.blob import ContentSettings  # lazy: optional [azure] dependency

        body = json.dumps(document, ensure_ascii=False, indent=2).encode("utf-8")
        self._container().upload_blob(
            self._key(key),
            body,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )


def write_artifacts_to_blob(
    store: BlobArtifactStore,
    collector: MeshCollector,
    *,
    sources: Iterable[ServiceEndpoint] = (),
    generated_at: str,
) -> dict[str, Any]:
    """Build the mesh-ui artifacts and publish them to Blob Storage via ``store``.

    The Blob counterpart of :func:`~benzene.mesh.artifacts.write_artifacts` /
    :func:`~benzene.mesh.s3_artifacts.write_artifacts_to_s3`: same signature, same document set,
    written as Blob Storage objects instead of files/S3 objects. Returns the built artifacts (as
    :func:`~benzene.mesh.artifacts.build_artifacts` does), so a caller can inspect or log what it just
    published without a round-trip read.
    """
    artifacts = build_artifacts(collector, sources=sources, generated_at=generated_at)
    store.write("manifest.json", artifacts["manifest"])
    store.write("topology.json", artifacts["topology"])
    store.write("topics.json", artifacts["topics"])
    store.write("usage.json", artifacts["usage"])
    store.write("asyncapi.json", artifacts["asyncapi"])
    store.write("annotations.json", artifacts["annotations"])
    for name, document in artifacts["services"].items():
        store.write(f"services/{name}.json", document)
    return artifacts
