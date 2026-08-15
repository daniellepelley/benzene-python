"""Publish the mesh-ui artifacts to S3 (the AWS analogue of :mod:`.artifacts`' local writer).

Where :func:`~benzene.mesh.artifacts.write_artifacts` lays the catalog out on a local filesystem for a
co-hosted host to serve (the Fargate/K8s collector), a Lambda-based mesh aggregator has no persistent
filesystem of its own to serve from — it publishes to S3 instead, and something else (a static viewer,
or the bucket's own website endpoint) reads the objects back. :class:`S3ArtifactStore` is that seam:
given a bucket (and an optional key prefix), it puts one JSON object per document.
:func:`write_artifacts_to_s3` is the S3 counterpart of ``write_artifacts`` — same
``(collector, sources, generated_at)`` shape, targeting a store instead of a directory — and lays out
the identical document set under the store's prefix: ``manifest.json``, ``topology.json``,
``topics.json``, ``usage.json``, ``asyncapi.json``, ``annotations.json``, and one
``services/{name}.json`` per service.

:meth:`S3ArtifactStore.write` is deliberately generic (key, document) rather than tied to a fixed
manifest, so a caller can also use it to publish an out-of-band document alongside the catalog — e.g.
the discovered registry from a :class:`~benzene.mesh_fleet.discovery.Discovery` pass, mirroring
TypeScript's ``store.publishAsync('registry.json', ...)`` in its Lambda mesh aggregator.

This module is purely additive: it does not change ``artifacts.py`` or ``store.py``, and the local
filesystem path (:class:`~benzene.mesh.store.JsonFileCollectorStore`, plain ``write_artifacts``) is
unaffected by its presence. ``boto3`` is an optional dependency, imported lazily, matching every other
AWS binding in this port (``benzene.aws.clients``); construct with an injected ``client`` to run with
no AWS SDK present.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from .artifacts import ServiceEndpoint, build_artifacts
from .collector import MeshCollector


class S3ArtifactStore:
    """Puts JSON documents into an S3 bucket under an optional key prefix.

    ``prefix`` is stripped of leading/trailing slashes and joined with a plain ``/`` — pass ``""``
    (the default) to publish at the bucket root. Each :meth:`write` is one ``PutObject`` call; there is
    no batching, so a partial publish (a failure partway through :func:`write_artifacts_to_s3`) can
    leave a stale sibling document, exactly as a partial local write could before the atomic-replace
    each file gets on disk — callers that need all-or-nothing semantics should retry the whole pass.
    """

    def __init__(self, bucket: str, prefix: str = "", *, client: Any | None = None) -> None:
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._client = client

    def _s3(self) -> Any:
        if self._client is None:
            import boto3  # lazy: optional [aws] dependency

            self._client = boto3.client("s3")
        return self._client

    def _key(self, key: str) -> str:
        key = key.lstrip("/")
        return f"{self._prefix}/{key}" if self._prefix else key

    def write(self, key: str, document: dict[str, Any]) -> None:
        """Put one JSON-serialized ``document`` at ``key`` (resolved against the store's prefix)."""
        body = json.dumps(document, ensure_ascii=False, indent=2).encode("utf-8")
        self._s3().put_object(
            Bucket=self._bucket,
            Key=self._key(key),
            Body=body,
            ContentType="application/json",
        )


def write_artifacts_to_s3(
    store: S3ArtifactStore,
    collector: MeshCollector,
    *,
    sources: Iterable[ServiceEndpoint] = (),
    generated_at: str,
) -> dict[str, Any]:
    """Build the mesh-ui artifacts and publish them to S3 via ``store``.

    The S3 counterpart of :func:`~benzene.mesh.artifacts.write_artifacts`: same signature, same
    document set, written as S3 objects instead of files. Returns the built artifacts (as
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
