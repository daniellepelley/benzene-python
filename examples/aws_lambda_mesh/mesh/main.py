"""Lambda entry point for the mesh function — the production wiring for ``discovery_service.py``'s
``run_mesh_aggregation``, driven by real AWS instead of the fakes the tests inject.

Env: ``MESH_ARTIFACT_BUCKET`` (required), ``MESH_ARTIFACT_PREFIX`` (default ``"mesh"``),
``MESH_DISCOVERY_TAG_KEY`` (default ``"benzene"``, matching Terraform's ``discovery_tag_key``).

Fires on an EventBridge schedule with a constant payload (see ``deploy/main.tf``'s
``aws_cloudwatch_event_rule.aggregate``) or an on-demand invoke — the event is intentionally unused,
mirroring TS's ``functions/mesh.ts``: every invocation just runs the same discover -> aggregate pass
and returns how many services it catalogued. This Lambda is deliberately **not** tagged for discovery,
so it never interrogates itself.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import asdict
from typing import Any

from benzene.mesh import S3ArtifactStore
from benzene.mesh_fleet import AwsLambdaDiscovery

from .discovery_service import run_mesh_aggregation


def _bucket() -> str:
    bucket = os.environ.get("MESH_ARTIFACT_BUCKET")
    if not bucket:
        raise RuntimeError("Set MESH_ARTIFACT_BUCKET to run the mesh Lambda.")
    return bucket


def handler(event: Any = None, context: Any = None) -> dict[str, Any]:
    discovery = AwsLambdaDiscovery(tag_key=os.environ.get("MESH_DISCOVERY_TAG_KEY", "benzene"))
    store = S3ArtifactStore(_bucket(), os.environ.get("MESH_ARTIFACT_PREFIX", "mesh"))
    summary = asyncio.run(run_mesh_aggregation(discovery=discovery, store=store))
    return asdict(summary)
