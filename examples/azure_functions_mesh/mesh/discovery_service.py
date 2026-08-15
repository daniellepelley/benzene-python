"""The mesh's discover -> interrogate -> aggregate pass — the Azure Functions counterpart of
``examples/aws_lambda_mesh/mesh/discovery_service.py`` and ``examples/k8s_mesh/mesh/discovery_service.py``.

On each run it:

1. **discovers** the ``benzene``-tagged service Function Apps —
   :class:`~benzene.mesh_fleet.AzureDiscovery` (real Azure Resource Manager ``resources.list``,
   enumerating ``Microsoft.Web/sites`` tagged ``benzene:service``, per the task's discovery contract);
2. **interrogates** each discovered Function App over **real HTTPS** —
   :class:`~benzene.mesh.HttpServiceSource` GETs ``/benzene/spec`` + ``/benzene/health`` on
   ``https://{address}`` (``AzureDiscovery`` hands back a bare hostname; **this module is the one place
   that prepends the scheme**, same as every other example's discovery-to-HTTP wiring in this repo) —
   fed into a real :class:`~benzene.mesh.MeshPoller`/:class:`~benzene.mesh.MeshCollector`, the same
   transport-agnostic core every Benzene mesh uses. Azure's HTTP-addressable endpoints need no
   AWS-Lambda-style special-casing (no direct-invoke reserved topics) — the Cloud Service Profile's own
   HTTP surface *is* the interrogation surface, exactly as ``examples/k8s_mesh`` proves on Kubernetes;
3. **publishes** the discovered registry (``registry.json``) and the full catalog (``manifest.json``,
   ``topology.json``, ``topics.json``, ``usage.json``, ``asyncapi.json``, ``annotations.json``,
   ``services/{name}.json``) to Blob Storage via :class:`~benzene.mesh.BlobArtifactStore` /
   :func:`~benzene.mesh.write_artifacts_to_blob`.

Nothing here is Azure Functions-*hosting* concerned (that's ``main.py``/``function_app.py``): this
module is plain async Python, driven directly by the tests with a fake discovery + an injected HTTP
fetch, and by the real Azure SDKs in deployment.
"""

from __future__ import annotations

import datetime
from collections.abc import Callable
from dataclasses import dataclass

from benzene.mesh import (
    BlobArtifactStore,
    HttpServiceSource,
    MeshCollector,
    MeshPoller,
    ServiceSource,
    write_artifacts_to_blob,
)
from benzene.mesh.poller import HttpGet
from benzene.mesh_fleet import Discovery, ServiceEndpoint


@dataclass(frozen=True)
class MeshAggregateSummary:
    """The aggregation run's outcome — mirrors ``examples/aws_lambda_mesh``'s ``MeshAggregateSummary``."""

    discovered: int


def azure_service_source(endpoint: ServiceEndpoint, *, fetch: HttpGet | None = None) -> ServiceSource:
    """An :class:`~benzene.mesh.HttpServiceSource` for one discovered Azure Function App.

    ``AzureDiscovery`` hands back a bare hostname (e.g. ``myapp.azurewebsites.net``) as ``address`` —
    there is no separate "scheme" field to discover, since ARM's ``defaultHostName``/``fqdn`` never
    carry one — so this is the one place ``https://`` gets prepended before polling.
    """
    return HttpServiceSource(endpoint.name, f"https://{endpoint.address}", fetch=fetch)


def _registry_document(endpoints: list[ServiceEndpoint], generated_at: str) -> dict:
    """The discovered-config document published alongside the catalog (mirrors
    ``examples/aws_lambda_mesh``'s ``registry.json`` — "discovery creates the config")."""
    return {
        "generatedAtUtc": generated_at,
        "services": [
            {"name": e.name, "address": e.address, "metadata": dict(e.metadata)} for e in endpoints
        ],
    }


async def run_mesh_aggregation(
    *,
    discovery: Discovery,
    store: BlobArtifactStore,
    source_factory: Callable[[ServiceEndpoint], ServiceSource] = azure_service_source,
    generated_at: str | None = None,
) -> MeshAggregateSummary:
    """Discover -> interrogate -> publish, once. Never raises for one down service — a failed
    interrogation is folded into the collector's catalog as an unreachable/unhealthy service, exactly
    as :class:`~benzene.mesh.MeshPoller` already handles a down :class:`~benzene.mesh.ServiceSource`.
    """
    endpoints = await discovery.discover()
    sources = [source_factory(e) for e in endpoints]

    collector = MeshCollector()
    await MeshPoller(collector, sources).poll_once()

    at = generated_at or _utc_now()
    store.write("registry.json", _registry_document(endpoints, at))
    write_artifacts_to_blob(store, collector, generated_at=at)

    return MeshAggregateSummary(discovered=len(endpoints))


def _utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
