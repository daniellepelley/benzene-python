"""The Blob artifact publisher — BlobArtifactStore + write_artifacts_to_blob (the Azure analogue of the
local filesystem write_artifacts, and the Azure counterpart of S3ArtifactStore/write_artifacts_to_s3).
Credential-free: an injected fake stands in for the ``azure.storage.blob.ContainerClient``, so the lazy
``azure-identity``/``azure-storage-blob`` import never fires for the client itself. ``write`` still
constructs a real ``ContentSettings`` object, so this file needs the optional ``azure-storage-blob`` SDK
installed (mirrors ``tests/test_azure_egress.py``'s ``ServiceBusMessage`` precedent) — it is skipped,
not failed, when that extra isn't present.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("azure.storage.blob")

from benzene.mesh import BlobArtifactStore, MeshCollector, write_artifacts_to_blob

_AT = "2026-08-15T00:00:00+00:00"


class _FakeContainerClient:
    """A stand-in for an ``azure.storage.blob.ContainerClient`` (only ``upload_blob``)."""

    def __init__(self) -> None:
        self.blobs: dict[str, dict] = {}

    def upload_blob(self, name, data, *, overwrite, content_settings):
        self.blobs[name] = {
            "document": json.loads(data.decode("utf-8")),
            "overwrite": overwrite,
            "contentType": content_settings.content_type,
        }


def _fleet() -> MeshCollector:
    c = MeshCollector()
    c.ingest_register(
        {"service": "orders", "topics": [{"id": "order:create"}], "descriptorHash": "h-ord"}
    )
    c.ingest_heartbeat(
        {
            "service": "orders",
            "instanceId": "i1",
            "descriptorHash": "h-ord",
            "health": {"isHealthy": True},
        }
    )
    return c


# --- BlobArtifactStore ---------------------------------------------------------------------------


def test_store_writes_json_body_and_content_type_at_the_container_root() -> None:
    fake = _FakeContainerClient()
    store = BlobArtifactStore(client=fake)

    store.write("manifest.json", {"a": 1})

    blob = fake.blobs["manifest.json"]
    assert blob["document"] == {"a": 1}
    assert blob["overwrite"] is True
    assert blob["contentType"] == "application/json"


def test_store_resolves_keys_under_a_prefix_regardless_of_leading_trailing_slashes() -> None:
    fake = _FakeContainerClient()
    store = BlobArtifactStore(prefix="/mesh/catalog/", client=fake)

    store.write("/services/orders.json", {})

    assert "mesh/catalog/services/orders.json" in fake.blobs


def test_store_with_no_prefix_writes_at_the_root() -> None:
    fake = _FakeContainerClient()
    store = BlobArtifactStore(client=fake)

    store.write("registry.json", {"services": []})

    assert "registry.json" in fake.blobs


# --- write_artifacts_to_blob -----------------------------------------------------------------------


def test_write_artifacts_to_blob_publishes_the_full_document_set() -> None:
    fake = _FakeContainerClient()
    store = BlobArtifactStore(prefix="mesh", client=fake)

    artifacts = write_artifacts_to_blob(store, _fleet(), generated_at=_AT)

    expected_keys = {
        "mesh/manifest.json",
        "mesh/topology.json",
        "mesh/topics.json",
        "mesh/usage.json",
        "mesh/asyncapi.json",
        "mesh/annotations.json",
        "mesh/services/orders.json",
    }
    assert expected_keys == set(fake.blobs)
    assert fake.blobs["mesh/manifest.json"]["document"] == artifacts["manifest"]
    assert fake.blobs["mesh/services/orders.json"]["document"] == artifacts["services"]["orders"]
    assert artifacts["manifest"]["services"][0]["name"] == "orders"


def test_write_artifacts_to_blob_returns_the_same_shape_as_build_artifacts() -> None:
    fake = _FakeContainerClient()
    store = BlobArtifactStore(client=fake)

    artifacts = write_artifacts_to_blob(store, _fleet(), generated_at=_AT)

    assert set(artifacts) == {
        "manifest",
        "topology",
        "topics",
        "services",
        "usage",
        "asyncapi",
        "annotations",
    }


def test_write_artifacts_to_blob_can_also_publish_an_out_of_band_document() -> None:
    # The store's write() is generic — a caller (the mesh Function) can publish the discovered
    # registry alongside the catalog, mirroring the S3 mesh Lambda's store.write('registry.json', ...).
    fake = _FakeContainerClient()
    store = BlobArtifactStore(prefix="mesh", client=fake)

    store.write("registry.json", {"services": ["orders"]})

    assert fake.blobs["mesh/registry.json"]["document"] == {"services": ["orders"]}
