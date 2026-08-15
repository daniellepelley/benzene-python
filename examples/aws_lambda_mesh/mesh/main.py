"""Lambda entry point for the mesh function — fires on the EventBridge schedule (default every 5
minutes) or an on-demand invoke, mirroring TS's ``functions/mesh.ts``: every invocation just runs
``discovery_service.run_mesh_aggregation``'s discover -> interrogate -> drain -> publish pass and
returns how many services it catalogued. This Lambda is deliberately **not** tagged for discovery, so
it never interrogates itself.

Env: ``MESH_ARTIFACT_BUCKET`` (required), ``MESH_ARTIFACT_PREFIX`` (default ``"mesh"``),
``MESH_DISCOVERY_TAG_KEY`` (default ``"benzene"``, matching Terraform's ``discovery_tag_key``),
``MESH_STATE_KEY`` (default ``"_state/collector.json"``) and ``MESH_TRACE_PREFIX`` (default
``"_state/traces"``) — both kept outside the public ``MESH_ARTIFACT_PREFIX`` the static viewer serves.

The trace inbox (:class:`~benzene.mesh.S3TraceInbox`) is what a service Lambda pushes its trace batch
into after each of its own invocations (``service/host.py``); this pass is the only thing that ever
drains it, so merging pushed traces into the durable collector snapshot (:class:`~benzene.mesh.
S3CollectorStore`) stays single-writer even though pushes themselves can happen concurrently (one SNS/
EventBridge fan-out invokes several service Lambdas at once) — see ``discovery_service.py``'s module
docstring for why that split matters.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import asdict
from typing import Any

from benzene.mesh import S3ArtifactStore, S3CollectorStore, S3TraceInbox
from benzene.mesh_fleet import AwsLambdaDiscovery

from .discovery_service import run_mesh_aggregation


def _bucket() -> str:
    bucket = os.environ.get("MESH_ARTIFACT_BUCKET")
    if not bucket:
        raise RuntimeError("Set MESH_ARTIFACT_BUCKET to run the mesh Lambda.")
    return bucket


def handler(event: Any = None, context: Any = None) -> dict[str, Any]:
    bucket = _bucket()
    discovery = AwsLambdaDiscovery(tag_key=os.environ.get("MESH_DISCOVERY_TAG_KEY", "benzene"))
    store = S3ArtifactStore(bucket, os.environ.get("MESH_ARTIFACT_PREFIX", "mesh"))
    collector_store = S3CollectorStore(bucket, os.environ.get("MESH_STATE_KEY", "_state/collector.json"))
    trace_inbox = S3TraceInbox(bucket, os.environ.get("MESH_TRACE_PREFIX", "_state/traces"))
    summary = asyncio.run(
        run_mesh_aggregation(
            discovery=discovery,
            store=store,
            collector_store=collector_store,
            trace_inbox=trace_inbox,
        )
    )
    return asdict(summary)
