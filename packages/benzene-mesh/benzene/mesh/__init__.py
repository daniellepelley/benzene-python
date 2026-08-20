"""``benzene.mesh`` — make a Python Benzene service show up in the mesh (mesh.md).

The mesh module turns a service into a first-class citizen of a Benzene mesh: it **describes itself**
(the :class:`ServiceDescriptor`, derived from the handler registry, with per-topic request/response
schemas and a content hash), **answers the reserved ``benzene:mesh`` topic** with that descriptor
(:func:`mesh_interception`), **traces every invocation** (:func:`trace_interception` → one
:class:`TraceEvent` each), and **reports into a collector** over an outbound client
(:class:`MeshFeedSender`).

Everything here is optional and additive — install it to join the mesh, leave it out and the rest of
your service is unchanged. Depends only on ``benzene-core``; the S3 artifact publisher
(:class:`S3ArtifactStore`) additionally needs the optional ``[aws]`` extra (``boto3``), imported lazily.

    pip install benzene-mesh

Mirrors .NET's ``Benzene.Mesh``. Contributes the ``benzene.mesh`` subpackage to the shared
``benzene`` namespace.
"""

from __future__ import annotations

from .artifacts import build_artifacts, write_artifacts
from .collector import (
    QUERY_FLEET_TOPIC,
    QUERY_SERVICE_TOPIC,
    QUERY_TOPIC_TOPIC,
    QUERY_TRACE_TOPIC,
    CollectorBadRequest,
    CollectorError,
    CollectorNotFound,
    MeshCollector,
    collector_registry,
)
from .descriptor import (
    MESH_TOPIC,
    ServiceDescriptor,
    ServiceInfo,
    TopicDescriptor,
)
from .feeds import (
    HEARTBEAT_TOPIC,
    ISSUES_TOPIC,
    REGISTER_TOPIC,
    TRACES_TOPIC,
    Heartbeat,
    MeshFeedSender,
)
from .inbox import S3TraceInbox
from .interception import DescriptorSource, mesh_interception
from .issues import (
    CLASSIFICATIONS,
    Issue,
    IssueAggregator,
    IssueBatch,
    classify,
    issue_fingerprint,
)
from .outbound import DuplicateOutboundRegistrationError, OutboundDefinition, OutboundRegistry
from .poller import (
    CallableServiceSource,
    HttpServiceSource,
    MeshPoller,
    PollError,
    PollResult,
    ServiceSource,
)
from .probe import (
    CloudServiceProbeReport,
    RequirementProbe,
    Verdict,
    probe_cloud_service,
)
from .profile import (
    PROFILE_NAME,
    REQUIREMENT_IDS,
    CloudServiceProfileReport,
    RequirementCheck,
    evaluate_cloud_service_profile,
)
from .s3_artifacts import S3ArtifactStore, write_artifacts_to_s3
from .schema import Schema, json_schema
from .store import (
    CollectorStore,
    JsonFileCollectorStore,
    NullCollectorStore,
    S3CollectorStore,
)
from .trace import (
    InMemoryTraceExporter,
    QueueTraceExporter,
    TraceEvent,
    TraceExporter,
    TracePropagatingMessageSender,
    current_traceparent,
    new_span_id,
    new_trace_id,
    parse_traceparent,
    trace_interception,
    trace_middleware,
    with_trace_propagation,
)

__all__ = [
    "CLASSIFICATIONS",
    "CallableServiceSource",
    "HttpServiceSource",
    "MeshPoller",
    "PollError",
    "PollResult",
    "ServiceSource",
    "CollectorBadRequest",
    "CollectorError",
    "CollectorNotFound",
    "CollectorStore",
    "CloudServiceProbeReport",
    "CloudServiceProfileReport",
    "DescriptorSource",
    "DuplicateOutboundRegistrationError",
    "PROFILE_NAME",
    "REQUIREMENT_IDS",
    "RequirementCheck",
    "RequirementProbe",
    "Verdict",
    "evaluate_cloud_service_profile",
    "probe_cloud_service",
    "HEARTBEAT_TOPIC",
    "Heartbeat",
    "ISSUES_TOPIC",
    "InMemoryTraceExporter",
    "Issue",
    "IssueAggregator",
    "IssueBatch",
    "JsonFileCollectorStore",
    "MESH_TOPIC",
    "MeshCollector",
    "MeshFeedSender",
    "NullCollectorStore",
    "OutboundDefinition",
    "OutboundRegistry",
    "QUERY_FLEET_TOPIC",
    "QUERY_SERVICE_TOPIC",
    "QUERY_TOPIC_TOPIC",
    "QUERY_TRACE_TOPIC",
    "QueueTraceExporter",
    "REGISTER_TOPIC",
    "S3ArtifactStore",
    "S3CollectorStore",
    "S3TraceInbox",
    "collector_registry",
    "Schema",
    "ServiceDescriptor",
    "ServiceInfo",
    "TRACES_TOPIC",
    "TopicDescriptor",
    "TraceEvent",
    "TraceExporter",
    "TracePropagatingMessageSender",
    "build_artifacts",
    "classify",
    "current_traceparent",
    "with_trace_propagation",
    "issue_fingerprint",
    "json_schema",
    "mesh_interception",
    "new_span_id",
    "new_trace_id",
    "parse_traceparent",
    "trace_interception",
    "trace_middleware",
    "write_artifacts",
    "write_artifacts_to_s3",
]
