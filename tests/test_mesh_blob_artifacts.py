"""The Blob artifact publisher — BlobArtifactStore + write_artifacts_to_blob (the Azure analogue of the
local filesystem write_artifacts, and the Azure counterpart of S3ArtifactStore/write_artifacts_to_s3).

Credential-free AND SDK-free: an injected fake stands in for the ``azure.storage.blob.ContainerClient``,
so neither the lazy ``azure-identity``/``azure-storage-blob`` import for the client nor the one for
``ContentSettings`` fires. That is the whole point of the injected-``client`` seam the class documents,
and this file runs everywhere as a result — it used to ``importorskip`` the SDK, which meant every
assertion below was silently skipped on any machine (and in CI) without the optional ``[azure]`` extra.
"""

from __future__ import annotations

import builtins
import json

from benzene.mesh import BlobArtifactStore, MeshCollector, write_artifacts_to_blob

_AT = "2026-08-15T00:00:00+00:00"


class _FakeContainerClient:
    """A stand-in for an ``azure.storage.blob.ContainerClient`` (only ``upload_blob``)."""

    def __init__(self) -> None:
        self.blobs: dict[str, dict] = {}

    def upload_blob(self, name, data, *, overwrite, content_settings=None):
        # content_settings is optional here for the same reason it is optional in the store: it can
        # only be built from the real SDK, and a fake container client has no reason to need one.
        self.blobs[name] = {
            "document": json.loads(data.decode("utf-8")),
            "overwrite": overwrite,
            "contentType": content_settings.content_type if content_settings is not None else None,
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
    # application/json when the SDK is installed to build a ContentSettings with; None when it is
    # not. Both are correct - what must never happen is the write failing outright, which is what
    # the SDK-free test below pins.
    assert blob["contentType"] in ("application/json", None)


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


# --- the injected-client seam works with no Azure SDK at all ---------------------------------------


def test_store_writes_with_an_injected_client_even_when_the_azure_sdk_cannot_be_imported(
    monkeypatch,
) -> None:
    """The seam the class documents: an injected client needs no Azure SDK present.

    This is pinned by blocking the import rather than by relying on the SDK being absent, so it
    fails on a developer machine that HAS the extra installed too. ``write`` previously imported
    ``ContentSettings`` unconditionally, which broke the documented promise and turned every
    example test that used a fake container client red in CI.
    """
    real_import = builtins.__import__

    def no_azure(name, *args, **kwargs):
        if name.startswith("azure"):
            raise ImportError(f"blocked for this test: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_azure)

    fake = _FakeContainerClient()
    store = BlobArtifactStore(prefix="mesh", client=fake)

    store.write("registry.json", {"services": ["orders"]})

    blob = fake.blobs["mesh/registry.json"]
    assert blob["document"] == {"services": ["orders"]}
    assert blob["overwrite"] is True
    assert blob["contentType"] is None  # the kwarg is omitted, not faked


def test_write_artifacts_to_blob_publishes_the_whole_set_with_no_azure_sdk(monkeypatch) -> None:
    real_import = builtins.__import__

    def no_azure(name, *args, **kwargs):
        if name.startswith("azure"):
            raise ImportError(f"blocked for this test: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_azure)

    fake = _FakeContainerClient()
    store = BlobArtifactStore(client=fake)

    artifacts = write_artifacts_to_blob(store, _fleet(), generated_at=_AT)

    assert "manifest.json" in fake.blobs
    assert "services/orders.json" in fake.blobs
    assert fake.blobs["manifest.json"]["document"] == artifacts["manifest"]
