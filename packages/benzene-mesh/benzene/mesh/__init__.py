"""``benzene.mesh`` — make a Python Benzene service show up in the mesh (mesh.md).

The mesh module turns a service into a first-class citizen of a Benzene mesh. A service describes itself
once — the :class:`~benzene.core.ContractDocument` its composition root derives and ``benzene:spec``
serves — and **registers that same document** with a collector (:func:`registration_document`,
mesh.md §2). It **traces every invocation** (:func:`trace_middleware` → one :class:`TraceEvent` each)
and **reports into a collector** over an outbound client (:class:`MeshFeedSender`: registration,
heartbeats, traces, issues). The receiving side is :class:`MeshCollector`; the pull side —
:class:`MeshPoller` and :func:`probe_cloud_service` — reads the Contract Document a service's
``/benzene/spec`` answers with (:mod:`benzene.mesh.specdoc`), so a Python mesh grades and ingests a
.NET, Go, or TypeScript service exactly as it does one of its own.

Everything here is optional and additive — install it to join the mesh, leave it out and the rest of
your service is unchanged. Depends only on ``benzene-core``; the S3 artifact publisher
(:class:`S3ArtifactStore`) additionally needs the optional ``[aws]`` extra (``boto3``), and the Blob
artifact publisher (:class:`BlobArtifactStore`) the optional ``[azure]`` extra
(``azure-storage-blob`` + ``azure-identity``) — both imported lazily.

    pip install benzene-mesh

Mirrors .NET's ``Benzene.Mesh``. Contributes the ``benzene.mesh`` subpackage to the shared
``benzene`` namespace.
"""

from __future__ import annotations

from .artifacts import build_artifacts, write_artifacts
from .blob_artifacts import BlobArtifactStore, write_artifacts_to_blob
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
from .feeds import (
    HEARTBEAT_TOPIC,
    ISSUES_TOPIC,
    REGISTER_TOPIC,
    TRACES_TOPIC,
    Heartbeat,
    MeshFeedSender,
    registration_document,
)
from .inbox import S3TraceInbox
from .issues import (
    CLASSIFICATIONS,
    Issue,
    IssueAggregator,
    IssueBatch,
    classify,
    issue_fingerprint,
)
from .outbound import (
    DuplicateOutboundRegistrationError,
    EventDeclaration,
    OutboundRegistry,
    SendDeclaration,
)
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
    MESH_REQUIREMENT_IDS,
    PROFILE_NAME,
    REQUIREMENT_IDS,
    CloudServiceProfileReport,
    RequirementCheck,
    declare_cloud_service_profile,
    evaluate_cloud_service_profile,
)
from .s3_artifacts import S3ArtifactStore, write_artifacts_to_s3
from .specdoc import (
    is_contract_document,
    known_members,
    resolve_schema,
    spec_entries,
    spec_registration_hash,
    spec_service,
    spec_service_version,
    spec_topic_ids,
)
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
    current_tracestate,
    new_span_id,
    new_trace_id,
    parse_traceparent,
    trace_middleware,
    with_trace_propagation,
)

__all__ = [
    "CLASSIFICATIONS",
    "BlobArtifactStore",
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
    "DuplicateOutboundRegistrationError",
    "EventDeclaration",
    "SendDeclaration",
    "MESH_REQUIREMENT_IDS",
    "PROFILE_NAME",
    "REQUIREMENT_IDS",
    "RequirementCheck",
    "RequirementProbe",
    "Verdict",
    "declare_cloud_service_profile",
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
    "MeshCollector",
    "MeshFeedSender",
    "NullCollectorStore",
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
    "TRACES_TOPIC",
    "TraceEvent",
    "TraceExporter",
    "TracePropagatingMessageSender",
    "build_artifacts",
    "is_contract_document",
    "known_members",
    "registration_document",
    "resolve_schema",
    "spec_entries",
    "spec_service_version",
    "spec_topic_ids",
    "spec_registration_hash",
    "spec_service",
    "classify",
    "current_traceparent",
    "current_tracestate",
    "with_trace_propagation",
    "issue_fingerprint",
    "new_span_id",
    "new_trace_id",
    "parse_traceparent",
    "trace_middleware",
    "write_artifacts",
    "write_artifacts_to_blob",
    "write_artifacts_to_s3",
]
