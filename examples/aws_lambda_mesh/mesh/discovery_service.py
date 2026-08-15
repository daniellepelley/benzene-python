"""The mesh's discover -> interrogate -> aggregate pass — the AWS Lambda counterpart of
``examples/k8s_mesh/mesh/discovery_service.py``, and the Python counterpart of TS's
``functions/mesh.ts`` (``runMeshAggregation``) / .NET's ``Mesh/MeshAggregateHandler``.

On each run it:

1. **discovers** the ``benzene``-tagged service Lambdas — :class:`~benzene.mesh_fleet.AwsLambdaDiscovery`
   (real ``ListFunctions`` + ``ListTags``, filtered by tag);
2. **interrogates** each discovered function by a real, synchronous **direct Lambda Invoke** on the two
   reserved topics every service Lambda answers (``ServiceStartUp``'s ``mesh_interception`` /
   ``health_interception``) — :class:`~benzene.mesh.MeshPoller` over one
   :class:`~benzene.mesh.CallableServiceSource` per discovered function, exactly the seam
   ``benzene.mesh`` ships for a bespoke transport;
3. **drains the trace inbox** — every :class:`~benzene.mesh.S3TraceInbox` object a service Lambda
   pushed since the last pass (``service/host.py``'s :class:`~benzene.mesh.MeshFeedSender`), merging
   each batch into the same collector this pass just polled;
4. **publishes** the discovered registry and the aggregated catalog (manifest/services/topics/topology/
   usage/asyncapi/annotations) to S3 via :class:`~benzene.mesh.S3ArtifactStore`.

This pass is the collector's **sole writer** — the reason traces are pushed to an inbox rather than
ingested straight into the shared collector snapshot from each service Lambda's own invocation: two
services fanned out from one SNS/EventBridge message push independently and (nearly) concurrently, and
a direct load-mutate-save against shared state races under exactly that condition (whichever writer
saves last wins, discarding what the other wrote). Draining the inbox only here, one aggregation pass
at a time, keeps the collector's persisted snapshot single-writer.

Nothing here is AWS-Lambda-*hosting* concerned (that's ``main.py``): this module is plain async
Python, driven directly by the tests with a fake discovery + a fake Lambda client, and by ``main.py``
in deployment with the real ``boto3`` clients.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Any

from benzene.aws import LambdaMessageSender
from benzene.core import HEALTH_TOPIC
from benzene.mesh import (
    MESH_TOPIC,
    CallableServiceSource,
    CollectorStore,
    MeshCollector,
    MeshPoller,
    NullCollectorStore,
    PollError,
    S3ArtifactStore,
    S3TraceInbox,
    write_artifacts_to_s3,
)
from benzene.mesh_fleet import Discovery, ServiceEndpoint


@dataclass(frozen=True)
class MeshAggregateSummary:
    """The aggregation run's outcome — mirrors TS's/​.NET's ``MeshAggregateSummary``."""

    discovered: int


def lambda_service_source(
    function_name: str, *, client: Any | None = None
) -> CallableServiceSource:
    """A :class:`~benzene.mesh.CallableServiceSource` that interrogates one Lambda by direct invoke.

    ``spec`` invokes the reserved ``benzene:mesh`` topic (answered by ``mesh_interception``) and
    ``health`` the reserved ``benzene:healthcheck`` topic (``health_interception``) — the same two
    surfaces a Cloud Service Profile service answers over HTTP at ``/benzene/spec``/``/benzene/health``,
    reached here over AWS's own request/response primitive instead (no HTTP endpoint needed).
    """
    sender = LambdaMessageSender(function_name, client=client)

    async def fetch_spec() -> dict[str, Any]:
        result = await sender.send_message(MESH_TOPIC, None)
        if not isinstance(result.payload, dict):
            detail = "; ".join(result.errors) or result.status
            raise PollError(f"{function_name}: {MESH_TOPIC} invoke failed: {detail}")
        return result.payload

    async def fetch_health() -> dict[str, Any]:
        result = await sender.send_message(HEALTH_TOPIC, None)
        # A degraded service still answers with the health aggregate as its payload (service-
        # unavailable, not a transport failure) — only a missing/malformed payload is a poll error.
        if not isinstance(result.payload, dict):
            detail = "; ".join(result.errors) or result.status
            raise PollError(f"{function_name}: {HEALTH_TOPIC} invoke failed: {detail}")
        return result.payload

    return CallableServiceSource(name=function_name, spec=fetch_spec, health=fetch_health)


def _registry_document(endpoints: list[ServiceEndpoint], generated_at: str) -> dict[str, Any]:
    """The discovered-config document published alongside the catalog (mirrors TS's ``registry.json``
    written via ``MeshRegistryJson.serialize`` — "discovery creates the config")."""
    return {
        "generatedAtUtc": generated_at,
        "services": [
            {"name": e.name, "address": e.address, "metadata": dict(e.metadata)} for e in endpoints
        ],
    }


async def run_mesh_aggregation(
    *,
    discovery: Discovery,
    store: S3ArtifactStore,
    lambda_client: Any | None = None,
    generated_at: str | None = None,
    collector_store: CollectorStore | None = None,
    trace_inbox: S3TraceInbox | None = None,
) -> MeshAggregateSummary:
    """Discover -> interrogate -> drain -> publish, once. Never raises for one down service — a failed
    interrogation is folded into the collector's catalog as an unreachable/unhealthy service, exactly
    as :class:`~benzene.mesh.MeshPoller` already handles a down :class:`~benzene.mesh.ServiceSource`.

    ``collector_store`` (default :class:`~benzene.mesh.NullCollectorStore`, i.e. in-memory only) backs
    the collector this pass polls into and persists it after. ``trace_inbox``, when given, is drained
    into the *same* collector before publishing — every trace batch a service Lambda pushed since the
    last pass, merged in this single-writer pass so consumer edges derived from them land in the
    published catalog without racing the concurrent pushes that produced them (see the module
    docstring). Both default to doing nothing (in-memory only / no inbox), matching every existing
    caller and test that doesn't pass them.
    """
    endpoints = await discovery.discover()
    sources = [lambda_service_source(e.address, client=lambda_client) for e in endpoints]

    collector = MeshCollector(store=collector_store or NullCollectorStore())
    await MeshPoller(collector, sources).poll_once()
    if trace_inbox is not None:
        drained = trace_inbox.drain()
        if drained:
            # One merged ingest (one persisted-store write), not one per pushed batch.
            events = [event for batch in drained for event in batch.get("events") or []]
            collector.ingest_traces({"events": events})

    at = generated_at or _utc_now()
    store.write("registry.json", _registry_document(endpoints, at))
    write_artifacts_to_s3(store, collector, generated_at=at)

    return MeshAggregateSummary(discovered=len(endpoints))


def _utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
