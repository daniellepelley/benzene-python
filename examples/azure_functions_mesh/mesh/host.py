"""Azure host wiring for the mesh Function: build the real :class:`~benzene.mesh_fleet.AzureDiscovery`
+ :class:`~benzene.mesh.BlobArtifactStore` from env vars and run the discover -> interrogate ->
publish pass. Mirrors ``examples/aws_lambda_mesh/mesh/main.py``'s env-driven production wiring, split
out from the entry point so it can be driven by a timer trigger *or* an on-demand call with the same
function.

Env: ``MESH_SUBSCRIPTION_ID`` (required — the Azure subscription :class:`AzureDiscovery` enumerates),
``MESH_DISCOVERY_TAG`` (default ``"benzene:service"``, matching ``AzureDiscovery``'s own default and
Terraform's ``var.discovery_tag``), ``MESH_BLOB_ACCOUNT_URL`` (required — the storage account's blob
endpoint), ``MESH_BLOB_CONTAINER`` (default ``"mesh"``), ``MESH_ARTIFACT_PREFIX`` (default ``""``).
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping

from benzene.mesh import BlobArtifactStore
from benzene.mesh_fleet import AzureDiscovery

from .discovery_service import MeshAggregateSummary, run_mesh_aggregation


def _require(env: Mapping[str, str], key: str) -> str:
    value = env.get(key)
    if not value:
        raise RuntimeError(
            f"Set {key} to run the mesh Function (tests call run_mesh_aggregation directly instead)."
        )
    return value


def run_aggregation_from_env(env: Mapping[str, str] | None = None) -> MeshAggregateSummary:
    """Build the real Azure discovery + Blob store from the environment and run one aggregation pass."""
    env = os.environ if env is None else env
    discovery = AzureDiscovery(
        _require(env, "MESH_SUBSCRIPTION_ID"),
        service_tag=env.get("MESH_DISCOVERY_TAG", "benzene:service"),
    )
    store = BlobArtifactStore(
        _require(env, "MESH_BLOB_ACCOUNT_URL"),
        env.get("MESH_BLOB_CONTAINER", "mesh"),
        env.get("MESH_ARTIFACT_PREFIX", ""),
    )
    return asyncio.run(run_mesh_aggregation(discovery=discovery, store=store))
