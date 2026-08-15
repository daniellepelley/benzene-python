"""The S3 artifact publisher — S3ArtifactStore + write_artifacts_to_s3 (the AWS analogue of the local
filesystem write_artifacts). Credential-free: an injected fake stands in for the boto3 S3 client, so
the lazy ``boto3`` import never fires.
"""

from __future__ import annotations

import json

from benzene.mesh import MeshCollector, S3ArtifactStore, write_artifacts_to_s3

_AT = "2026-08-08T00:00:00+00:00"


class _FakeS3Client:
    """A stand-in for a boto3 ``s3`` client (only the one call the store makes: ``put_object``)."""

    def __init__(self) -> None:
        self.puts: dict[str, dict] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):  # noqa: N803 - boto3 kwarg names
        self.puts[Key] = {
            "bucket": Bucket,
            "document": json.loads(Body.decode("utf-8")),
            "contentType": ContentType,
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


# --- S3ArtifactStore -----------------------------------------------------------------------------


def test_store_writes_json_body_and_content_type_at_the_bucket_root() -> None:
    fake = _FakeS3Client()
    store = S3ArtifactStore("my-bucket", client=fake)

    store.write("manifest.json", {"a": 1})

    put = fake.puts["manifest.json"]
    assert put["bucket"] == "my-bucket"
    assert put["document"] == {"a": 1}
    assert put["contentType"] == "application/json"


def test_store_resolves_keys_under_a_prefix_regardless_of_leading_trailing_slashes() -> None:
    fake = _FakeS3Client()
    store = S3ArtifactStore("bucket", "/mesh/catalog/", client=fake)

    store.write("/services/orders.json", {})

    assert "mesh/catalog/services/orders.json" in fake.puts


def test_store_with_no_prefix_writes_at_the_root() -> None:
    fake = _FakeS3Client()
    store = S3ArtifactStore("bucket", client=fake)

    store.write("registry.json", {"services": []})

    assert "registry.json" in fake.puts


# --- write_artifacts_to_s3 -------------------------------------------------------------------------


def test_write_artifacts_to_s3_publishes_the_full_document_set() -> None:
    fake = _FakeS3Client()
    store = S3ArtifactStore("bucket", "mesh", client=fake)

    artifacts = write_artifacts_to_s3(store, _fleet(), generated_at=_AT)

    expected_keys = {
        "mesh/manifest.json",
        "mesh/topology.json",
        "mesh/topics.json",
        "mesh/usage.json",
        "mesh/asyncapi.json",
        "mesh/annotations.json",
        "mesh/services/orders.json",
    }
    assert expected_keys == set(fake.puts)
    assert fake.puts["mesh/manifest.json"]["document"] == artifacts["manifest"]
    assert fake.puts["mesh/services/orders.json"]["document"] == artifacts["services"]["orders"]
    assert artifacts["manifest"]["services"][0]["name"] == "orders"


def test_write_artifacts_to_s3_returns_the_same_shape_as_build_artifacts() -> None:
    fake = _FakeS3Client()
    store = S3ArtifactStore("bucket", client=fake)

    artifacts = write_artifacts_to_s3(store, _fleet(), generated_at=_AT)

    assert set(artifacts) == {
        "manifest",
        "topology",
        "topics",
        "services",
        "usage",
        "asyncapi",
        "annotations",
    }


def test_write_artifacts_to_s3_can_also_publish_an_out_of_band_document() -> None:
    # The store's write() is generic — a caller (the mesh Lambda) can publish the discovered registry
    # alongside the catalog, mirroring TypeScript's store.publishAsync('registry.json', ...).
    fake = _FakeS3Client()
    store = S3ArtifactStore("bucket", "mesh", client=fake)

    store.write("registry.json", {"services": ["orders"]})

    assert fake.puts["mesh/registry.json"]["document"] == {"services": ["orders"]}
