"""``benzene.mesh_fleet`` — cloud service discovery + fleet trace-mappers for a Benzene mesh.

Two adjacent capabilities the mesh needs once it spans a real fleet, both of which the port already
had the model for and only needed the adapters:

* **Discovery** (:class:`Discovery`) answers *which services are in the mesh* by reading a cloud
  registry — AWS Cloud Map (:class:`AwsCloudMapDiscovery`), tagged AWS Lambda functions
  (:class:`AwsLambdaDiscovery`), Azure (:class:`AzureDiscovery`), or Kubernetes
  (:class:`KubernetesDiscovery`) — into a flat list of :class:`ServiceEndpoint`\\ s, with
  :class:`StaticDiscovery` as the SDK-free default and test double. A mesh :class:`~benzene.mesh.MeshPoller`
  already knows how to *read* a service once it has the address; discovery is the seam that supplies
  the addresses instead of a hand-written list.

* **Fleet trace-mappers** (:class:`TraceMapper`) project the mesh's own trace/span model
  (:class:`~benzene.mesh.TraceEvent`) into the JSON a tracing backend ingests —
  :class:`JaegerTraceMapper`, :class:`TempoTraceMapper` (OTLP-JSON), :class:`XRayTraceMapper`. Because
  a mesh trace already *is* a cross-language span, this port ships tracing ahead of the field: the
  same telemetry lands in Jaeger, Tempo, or X-Ray by choosing a mapper, no re-instrumentation.

Every cloud SDK is an optional, lazily-imported extra (``[aws]`` / ``[azure]`` / ``[kubernetes]``) and
every client is injectable, so the whole package — and its tests — import and run with no SDK present.
Mirrors .NET's ``Benzene.Mesh.Discovery.*`` and ``Benzene.Mesh.Fleet.*``, and contributes the
``benzene.mesh_fleet`` subpackage to the shared ``benzene`` namespace.
"""

from __future__ import annotations

from .discovery import Discovery, ServiceEndpoint, StaticDiscovery
from .discovery_adapters import (
    AwsCloudMapDiscovery,
    AwsLambdaDiscovery,
    AzureDiscovery,
    KubernetesDiscovery,
)
from .mappers import (
    JaegerTraceMapper,
    TempoTraceMapper,
    TraceMapper,
    XRayTraceMapper,
)

__all__ = [
    "AwsCloudMapDiscovery",
    "AwsLambdaDiscovery",
    "AzureDiscovery",
    "Discovery",
    "JaegerTraceMapper",
    "KubernetesDiscovery",
    "ServiceEndpoint",
    "StaticDiscovery",
    "TempoTraceMapper",
    "TraceMapper",
    "XRayTraceMapper",
]
