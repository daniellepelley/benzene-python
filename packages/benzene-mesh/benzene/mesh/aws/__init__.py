"""``benzene.mesh.aws`` — AWS enrichment sources for the mesh artifact emitter (the ``[aws]`` extra).

Two adapters that feed real observability data into :class:`~benzene.mesh.MeshArtifactEmitter`'s optional
``topology_source`` / ``usage_source`` hooks:

- :class:`XRayTopologySource` — ``client → server`` edges with real request rate, error rate, and
  p50/p95/p99 latency from **AWS X-Ray**'s service graph (``source: "xray"``).
- :class:`CloudWatchUsageSource` — per-(topic, transport, status) counts and mean duration from
  **CloudWatch** metrics (``source: "cloudwatch"``).

Each defines its AWS dependency as a minimal :class:`~typing.Protocol` (:class:`XRayServiceGraphClient`,
:class:`CloudWatchClient`) so unit tests pass hand-written fakes, with a ``boto3``-backed adapter
(:class:`Boto3XRayServiceGraphClient`, :class:`Boto3CloudWatchClient`) that imports ``boto3`` lazily. This
subpackage lives behind the ``benzene-mesh[aws]`` extra and is **not** imported from ``benzene.mesh``'s top
level, so importing ``benzene.mesh`` for descriptors/tracing/collector never needs ``boto3``.

    pip install benzene-mesh[aws]

Mirrors .NET's ``Benzene.Mesh.Fleet.Aws.XRay`` + ``Benzene.Mesh.Usage.CloudWatch``.
"""

from __future__ import annotations

from .cloudwatch import (
    CLOUDWATCH_SOURCE,
    Boto3CloudWatchClient,
    CloudWatchClient,
    CloudWatchUsageOptions,
    CloudWatchUsageSource,
)
from .xray import (
    XRAY_SOURCE,
    Boto3XRayServiceGraphClient,
    XRayServiceGraphClient,
    XRayTopologySource,
)

__all__ = [
    "CLOUDWATCH_SOURCE",
    "Boto3CloudWatchClient",
    "Boto3XRayServiceGraphClient",
    "CloudWatchClient",
    "CloudWatchUsageOptions",
    "CloudWatchUsageSource",
    "XRAY_SOURCE",
    "XRayServiceGraphClient",
    "XRayTopologySource",
]
