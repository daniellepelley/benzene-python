"""A race-free inbox for trace batches pushed by concurrent, stateless writers — the seam a
Lambda-based mesh needs that a load-mutate-save :class:`~benzene.mesh.CollectorStore` cannot safely be
(mesh.md §6, §3).

One order fanning out over SNS/EventBridge invokes *multiple* service Lambdas at once, each pushing its
own trace batch independently. If every push did read-modify-write against one shared
:class:`~benzene.mesh.S3CollectorStore` snapshot (load it, merge this batch in, save it back), two
pushes landing close together race: whichever writer saves last wins, silently discarding whatever the
other wrote in between — not a rare edge case at this estate's scale, but the *normal* shape of a
single order. :class:`S3TraceInbox` sidesteps the race entirely rather than adding locking/retries:
each push is one independent ``PutObject`` to a **uniquely keyed** object, so concurrent writers never
touch the same key and there is nothing to contend over. A single reader later drains the whole
prefix — lists every object, reads it, and deletes it — in one pass; that reader is meant to be the
mesh's own scheduled aggregation pass, the sole writer of the shared collector snapshot, so *that*
merge is race-free the ordinary way (one writer at a time).

Implements :class:`~benzene.core.MessageSender` (an async ``send_message``), so
``MeshFeedSender(S3TraceInbox(bucket, prefix))`` slots in exactly where
``MeshFeedSender(HttpMessageSender(...))``/``MeshFeedSender(LambdaMessageSender(...))`` do elsewhere in
this port — a service's ``benzene:mesh:traces`` feed lands in S3 instead of being delivered live.
``boto3`` is an optional dependency, imported lazily, matching every other AWS binding in this port.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from benzene.results import Result, Status


class S3TraceInbox:
    """Push (write, race-free) and :meth:`drain` (read + delete, single-reader) trace batches in S3.

    ``prefix`` is stripped of leading/trailing slashes; each pushed object is
    ``<prefix>/<uuid4>.json``. Construct with an injected ``client`` to run with no AWS SDK present.
    """

    def __init__(self, bucket: str, prefix: str = "_state/traces", *, client: Any | None = None) -> None:
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._client = client

    def _s3(self) -> Any:
        if self._client is None:
            import boto3  # lazy: optional [aws] dependency

            self._client = boto3.client("s3")
        return self._client

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        """Write ``message`` (the ``{"events": [...]}`` batch :class:`~benzene.mesh.MeshFeedSender`
        hands ``publish_traces``) as one new, uniquely keyed object. Never contends with any other
        writer, so this never fails on a conflict — only on a genuine S3/network error."""
        key = f"{self._prefix}/{uuid.uuid4().hex}.json"
        body = json.dumps(message).encode("utf-8")
        try:
            await asyncio.to_thread(
                self._s3().put_object,
                Bucket=self._bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
        except Exception as exc:  # noqa: BLE001 - mirrors every other MessageSender's own contract
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(exc))
        return Result.ok()

    def drain(self) -> list[dict[str, Any]]:
        """List, read, and delete every object under the prefix; returns each push's body.

        Not safe to call concurrently with itself — meant for a single reader (one aggregation pass at
        a time). A push that lands *during* a drain either isn't listed yet (picked up next drain) or
        is listed and processed — never lost, since nothing is deleted until after it's been read.
        A corrupt object (a push that failed mid-write) is skipped rather than failing the whole drain.
        """
        s3 = self._s3()
        keys: list[str] = []
        bodies: list[dict[str, Any]] = []
        continuation: str | None = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": self._bucket, "Prefix": f"{self._prefix}/"}
            if continuation:
                kwargs["ContinuationToken"] = continuation
            response = s3.list_objects_v2(**kwargs)
            for entry in response.get("Contents", []):
                key = entry["Key"]
                keys.append(key)
                obj = s3.get_object(Bucket=self._bucket, Key=key)
                try:
                    parsed = json.loads(obj["Body"].read())
                except (ValueError, TypeError):
                    continue  # a corrupt/partial push: drop it, don't fail the whole drain
                if isinstance(parsed, dict):
                    bodies.append(parsed)
            if not response.get("IsTruncated"):
                break
            continuation = response.get("NextContinuationToken")
        if keys:
            s3.delete_objects(Bucket=self._bucket, Delete={"Objects": [{"Key": k} for k in keys]})
        return bodies
