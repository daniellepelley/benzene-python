"""``benzene.mesh.aws`` — AWS enrichment + discovery sources for the mesh (the ``[aws]`` extra).

Three adapters over AWS APIs:

- :class:`XRayTopologySource` — ``client → server`` edges with real request rate, error rate, and
  p50/p95/p99 latency from **AWS X-Ray**'s service graph (``source: "xray"``), an
  :class:`~benzene.mesh.MeshArtifactEmitter` ``topology_source``.
- :class:`CloudWatchUsageSource` — per-(topic, transport, status) counts and mean duration from
  **CloudWatch** metrics (``source: "cloudwatch"``), an :class:`~benzene.mesh.MeshArtifactEmitter`
  ``usage_source``.
- :class:`AwsLambdaDiscoveryProvider` — self-discovers a mesh's services from **tagged Lambda functions**
  into a :class:`~benzene.mesh.MeshServiceRegistry` the aggregator polls (so the aggregator finds its
  fleet instead of being hand-fed a ``mesh.json``).

Each defines its AWS dependency as a minimal :class:`~typing.Protocol` (:class:`XRayServiceGraphClient`,
:class:`CloudWatchClient`, :class:`LambdaClient`) so unit tests pass hand-written fakes, with a
``boto3``-backed adapter (:class:`Boto3XRayServiceGraphClient`, :class:`Boto3CloudWatchClient`,
:class:`Boto3LambdaClient`) that imports ``boto3`` lazily. This subpackage lives behind the
``benzene-mesh[aws]`` extra and is **not** imported from ``benzene.mesh``'s top level, so importing
``benzene.mesh`` for descriptors/tracing/collector never needs ``boto3``.

    pip install benzene-mesh[aws]

Mirrors .NET's ``Benzene.Mesh.Fleet.Aws.XRay`` + ``Benzene.Mesh.Usage.CloudWatch`` +
``Benzene.Mesh.Discovery.Aws``.
"""

from __future__ import annotations

from .cloudwatch import (
    CLOUDWATCH_SOURCE,
    Boto3CloudWatchClient,
    CloudWatchClient,
    CloudWatchUsageOptions,
    CloudWatchUsageSource,
)
from .discovery import (
    DEFAULT_MAX_CONCURRENT_TAG_READS,
    DEFAULT_TAG_KEY,
    MESH_PATH_TAG,
    MESH_URL_TAG,
    AwsLambdaDiscoveryProvider,
    Boto3LambdaClient,
    LambdaClient,
    MeshDiscoveryFilter,
)
from .xray import (
    XRAY_SOURCE,
    Boto3XRayServiceGraphClient,
    XRayServiceGraphClient,
    XRayTopologySource,
)

__all__ = [
    "CLOUDWATCH_SOURCE",
    "DEFAULT_MAX_CONCURRENT_TAG_READS",
    "DEFAULT_TAG_KEY",
    "MESH_PATH_TAG",
    "MESH_URL_TAG",
    "AwsLambdaDiscoveryProvider",
    "Boto3CloudWatchClient",
    "Boto3LambdaClient",
    "Boto3XRayServiceGraphClient",
    "CloudWatchClient",
    "CloudWatchUsageOptions",
    "CloudWatchUsageSource",
    "LambdaClient",
    "MeshDiscoveryFilter",
    "XRAY_SOURCE",
    "XRayServiceGraphClient",
    "XRayTopologySource",
]
