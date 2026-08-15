"""Lambda entry point for the mesh function — answers two invoke shapes on the one handler.

Env: ``MESH_ARTIFACT_BUCKET`` (required), ``MESH_ARTIFACT_PREFIX`` (default ``"mesh"``),
``MESH_DISCOVERY_TAG_KEY`` (default ``"benzene"``, matching Terraform's ``discovery_tag_key``),
``MESH_STATE_KEY`` (default ``"_state/collector.json"`` — the durable collector snapshot, kept outside
the public ``MESH_ARTIFACT_PREFIX`` the static viewer serves).

1. **The EventBridge schedule's constant payload** (see ``deploy/main.tf``'s
   ``aws_cloudwatch_event_rule.aggregate``), or an on-demand invoke with no ``topic`` — runs
   ``discovery_service.run_mesh_aggregation``'s discover -> interrogate -> publish pass, mirroring
   TS's ``functions/mesh.ts``, and returns how many services it catalogued. This Lambda is
   deliberately **not** tagged for discovery, so it never interrogates itself.
2. **A direct Lambda invoke carrying a Benzene envelope** (``{"topic": "benzene:mesh:traces", ...}``,
   pushed by a service Lambda after each of its own invocations via ``MESH_FUNCTION_NAME`` — see
   ``service/host.py``) — handed straight to the collector's reserved-topic registry
   (:func:`~benzene.mesh.collector_registry`) rather than running a full aggregation pass, exactly as
   any other :class:`~benzene.aws.AwsLambdaApp` answers a direct invoke. The invoke Payload *is* the
   envelope, so no event-source classification is needed to tell the two shapes apart — only the
   aggregation trigger's payload lacks a ``topic`` key.

Both share the same durable :class:`~benzene.mesh.S3CollectorStore` snapshot, so consumer edges
derived from traces ingested between scheduled aggregation passes are already in the catalog the next
pass publishes.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import asdict
from typing import Any

from benzene.mesh import S3ArtifactStore, S3CollectorStore
from benzene.mesh_fleet import AwsLambdaDiscovery

from .discovery_service import ingest_pushed_feed, run_mesh_aggregation


def _bucket() -> str:
    bucket = os.environ.get("MESH_ARTIFACT_BUCKET")
    if not bucket:
        raise RuntimeError("Set MESH_ARTIFACT_BUCKET to run the mesh Lambda.")
    return bucket


def _collector_store() -> S3CollectorStore:
    return S3CollectorStore(_bucket(), os.environ.get("MESH_STATE_KEY", "_state/collector.json"))


def handler(event: Any = None, context: Any = None) -> dict[str, Any]:
    if isinstance(event, dict) and "topic" in event:
        return asyncio.run(ingest_pushed_feed(event, collector_store=_collector_store()))

    discovery = AwsLambdaDiscovery(tag_key=os.environ.get("MESH_DISCOVERY_TAG_KEY", "benzene"))
    store = S3ArtifactStore(_bucket(), os.environ.get("MESH_ARTIFACT_PREFIX", "mesh"))
    summary = asyncio.run(
        run_mesh_aggregation(discovery=discovery, store=store, collector_store=_collector_store())
    )
    return asdict(summary)
