"""``benzene.mesh`` — make a Python Benzene service show up in the mesh (mesh.md).

The mesh module turns a service into a first-class citizen of a Benzene mesh: it **describes itself**
(the :class:`ServiceDescriptor`, derived from the handler registry, with per-topic request/response
schemas and a content hash), **answers the reserved ``benzene:mesh`` topic** with that descriptor
(:func:`mesh_interception`), **traces every invocation** (:func:`trace_middleware` → one
:class:`TraceEvent` each), and **reports into a collector** over an outbound client
(:class:`MeshFeedSender`).

Everything here is optional and additive — install it to join the mesh, leave it out and the rest of
your service is unchanged. Depends only on ``benzene-core``.

    pip install benzene-mesh

Mirrors .NET's ``Benzene.Mesh``. Contributes the ``benzene.mesh`` subpackage to the shared
``benzene`` namespace.
"""

from __future__ import annotations

from .collector import (
    QUERY_FLEET_TOPIC,
    QUERY_SERVICE_TOPIC,
    QUERY_TOPIC_TOPIC,
    QUERY_TRACE_TOPIC,
    CollectorBadRequest,
    CollectorNotFound,
    MeshCollector,
    collector_registry,
)
from .feeds import (
    HEARTBEAT_TOPIC,
    ISSUES_TOPIC,
    REGISTER_TOPIC,
    TRACES_TOPIC,
    Heartbeat,
    MeshFeedSender,
)
from .descriptor import (
    MESH_TOPIC,
    ServiceDescriptor,
    ServiceInfo,
    TopicDescriptor,
)
from .interception import DescriptorSource, mesh_interception
from .issues import (
    CLASSIFICATIONS,
    Issue,
    IssueAggregator,
    IssueBatch,
    classify,
    issue_fingerprint,
)
from .schema import Schema, json_schema
from .trace import (
    InMemoryTraceExporter,
    QueueTraceExporter,
    TraceEvent,
    TraceExporter,
    new_span_id,
    new_trace_id,
    parse_traceparent,
    trace_middleware,
)

__all__ = [
    "CLASSIFICATIONS",
    "CollectorBadRequest",
    "CollectorNotFound",
    "DescriptorSource",
    "HEARTBEAT_TOPIC",
    "Heartbeat",
    "ISSUES_TOPIC",
    "InMemoryTraceExporter",
    "Issue",
    "IssueAggregator",
    "IssueBatch",
    "MESH_TOPIC",
    "MeshCollector",
    "MeshFeedSender",
    "QUERY_FLEET_TOPIC",
    "QUERY_SERVICE_TOPIC",
    "QUERY_TOPIC_TOPIC",
    "QUERY_TRACE_TOPIC",
    "QueueTraceExporter",
    "REGISTER_TOPIC",
    "collector_registry",
    "Schema",
    "ServiceDescriptor",
    "ServiceInfo",
    "TRACES_TOPIC",
    "TopicDescriptor",
    "TraceEvent",
    "TraceExporter",
    "classify",
    "issue_fingerprint",
    "json_schema",
    "mesh_interception",
    "new_span_id",
    "new_trace_id",
    "parse_traceparent",
    "trace_middleware",
]
