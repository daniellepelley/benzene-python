"""S3TraceInbox — the race-free trace push seam for a Lambda-based mesh (mesh.md §3, §6).
Credential-free: an injected fake stands in for the boto3 S3 client, so the lazy ``boto3`` import
never fires.
"""

from __future__ import annotations

import asyncio
import json

from benzene.mesh import S3TraceInbox
from benzene.results import Status


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3Client:
    """A stand-in for a boto3 ``s3`` client — ``put_object``/``list_objects_v2``/``get_object``/
    ``delete_objects``, enough to drive :class:`S3TraceInbox` end to end."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []

    def put_object(self, *, Bucket, Key, Body, ContentType):  # noqa: N803 - boto3 kwarg names
        self.objects[Key] = Body

    def list_objects_v2(self, *, Bucket, Prefix, ContinuationToken=None):  # noqa: N803
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def get_object(self, *, Bucket, Key):  # noqa: N803
        return {"Body": _FakeBody(self.objects[Key])}

    def delete_objects(self, *, Bucket, Delete):  # noqa: N803
        for entry in Delete["Objects"]:
            key = entry["Key"]
            self.objects.pop(key, None)
            self.deleted.append(key)


def test_send_message_writes_a_uniquely_keyed_object_under_the_prefix() -> None:
    fake = _FakeS3Client()
    inbox = S3TraceInbox("bucket", "_state/traces", client=fake)

    result = asyncio.run(inbox.send_message("benzene:mesh:traces", {"events": [{"a": 1}]}))

    assert result.is_successful
    assert len(fake.objects) == 1
    (key,) = fake.objects
    assert key.startswith("_state/traces/") and key.endswith(".json")
    assert json.loads(fake.objects[key]) == {"events": [{"a": 1}]}


def test_two_independent_pushes_both_survive_no_shared_state_to_race_over() -> None:
    # The whole point: two "concurrent" pushes (as SNS/EventBridge fan-out would trigger) write to
    # DIFFERENT keys, so neither can clobber the other the way a load-mutate-save store would.
    fake = _FakeS3Client()
    inbox = S3TraceInbox("bucket", client=fake)

    asyncio.run(inbox.send_message("t", {"events": [{"service": "inventory"}]}))
    asyncio.run(inbox.send_message("t", {"events": [{"service": "notifications"}]}))

    assert len(fake.objects) == 2
    drained = inbox.drain()
    services = {e["events"][0]["service"] for e in drained}
    assert services == {"inventory", "notifications"}


def test_drain_deletes_everything_it_read() -> None:
    fake = _FakeS3Client()
    inbox = S3TraceInbox("bucket", client=fake)
    asyncio.run(inbox.send_message("t", {"events": [{"a": 1}]}))

    first = inbox.drain()
    assert len(first) == 1
    assert fake.objects == {}

    second = inbox.drain()
    assert second == []


def test_drain_of_an_empty_prefix_is_an_empty_list_not_an_error() -> None:
    inbox = S3TraceInbox("bucket", client=_FakeS3Client())
    assert inbox.drain() == []


def test_drain_skips_a_corrupt_object_without_failing_the_whole_drain() -> None:
    fake = _FakeS3Client()
    fake.objects["_state/traces/good.json"] = json.dumps({"events": [{"a": 1}]}).encode("utf-8")
    fake.objects["_state/traces/bad.json"] = b"{ not json"
    inbox = S3TraceInbox("bucket", client=fake)

    drained = inbox.drain()

    assert drained == [{"events": [{"a": 1}]}]
    # Both objects are still consumed (removed) even though one was unparseable.
    assert fake.objects == {}


def test_send_message_failure_is_a_result_not_a_raise() -> None:
    class _BrokenClient(_FakeS3Client):
        def put_object(self, **kwargs):
            raise RuntimeError("network blip")

    inbox = S3TraceInbox("bucket", client=_BrokenClient())
    result = asyncio.run(inbox.send_message("t", {"events": []}))

    assert not result.is_successful
    assert result.status == Status.SERVICE_UNAVAILABLE
