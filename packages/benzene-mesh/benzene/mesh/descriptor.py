"""The ServiceDescriptor — a service's self-description, derived from its registry (mesh.md §2).

At startup a mesh-enabled service projects its handler registry into a **ServiceDescriptor**: the
service identity, its placement, and one entry per registered topic carrying the request/response
schema (:mod:`benzene.mesh.schema`). This is what the mesh UI shows and what a peer checks a
contract against. It is derived, never hand-written — :meth:`ServiceDescriptor.derive` reads the
registry, so the descriptor is always the truth of what the service serves.

``descriptorHash`` is a content hash over the *contract* (identity, placement, topics, schemas) —
it detects this service's own redeploys and is deliberately **never compared across ports**. It is
computed over a canonical JSON form (keys sorted lexicographically, no insignificant whitespace)
with ``instanceId``, ``degraded``, ``profile``, and ``descriptorHash`` itself excluded, so it is
invariant to which instance emitted it but sensitive to the service version and the topic set.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from benzene.core import Registry

from .schema import Schema, json_schema

#: The reserved topic id a mesh-enabled service intercepts to return its descriptor (mesh.md §1).
MESH_TOPIC = "benzene:mesh"

# Excluded from the hash: per-instance or additive fields that must not change the contract identity.
_HASH_EXCLUDED = frozenset({"instanceId", "degraded", "profile", "descriptorHash"})


@dataclass(frozen=True)
class ServiceInfo:
    """What the host/app knows about itself that the registry can't derive (mesh.md §2).

    Only ``service`` is required; a port emits what it knows and omits (never nulls) the rest.
    ``runtime`` defaults to ``"python"`` — the per-port identity of this implementation.
    """

    service: str
    service_version: str | None = None
    instance_id: str | None = None
    runtime: str | None = "python"
    binding: str | None = None
    placement: dict[str, str] | None = None
    degraded: list[str] | None = None
    profile: dict[str, Any] | None = None


@dataclass(frozen=True)
class TopicDescriptor:
    """One registered topic's projection: its id, optional version, and payload schemas."""

    id: str
    version: str = ""
    request_schema: Schema = field(default_factory=dict)
    response_schema: Schema = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"id": self.id}
        if self.version:  # omit an empty version rather than emit "" (mesh.md: omit, don't null)
            payload["version"] = self.version
        payload["requestSchema"] = self.request_schema
        payload["responseSchema"] = self.response_schema
        return payload


@dataclass(frozen=True)
class ServiceDescriptor:
    """A service's self-description. Build it with :meth:`derive`; read :meth:`to_payload`."""

    info: ServiceInfo
    topics: tuple[TopicDescriptor, ...]

    @classmethod
    def derive(cls, registry: Registry, info: ServiceInfo) -> "ServiceDescriptor":
        """Project a registry + :class:`ServiceInfo` into a descriptor (topics sorted by id, version)."""
        topics = tuple(
            sorted(
                (
                    TopicDescriptor(
                        id=d.topic,
                        version=d.version,
                        request_schema=json_schema(d.request_type),
                        response_schema=json_schema(d.response_type),
                    )
                    for d in registry.definitions()
                ),
                key=lambda t: (t.id, t.version),
            )
        )
        return cls(info=info, topics=topics)

    def descriptor_hash(self) -> str:
        """``"sha256:" + hex(sha256(canonical-json))`` over the contract (mesh.md §2.2)."""
        canonical = {k: v for k, v in self._base_payload().items() if k not in _HASH_EXCLUDED}
        encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def to_payload(self) -> dict[str, Any]:
        """The full ServiceDescriptor wire payload, including ``descriptorHash``."""
        payload = self._base_payload()
        payload["descriptorHash"] = self.descriptor_hash()
        return payload

    def _base_payload(self) -> dict[str, Any]:
        """The descriptor without ``descriptorHash`` — the input to the hash and to :meth:`to_payload`."""
        info = self.info
        payload: dict[str, Any] = {"service": info.service}
        if info.service_version:
            payload["serviceVersion"] = info.service_version
        if info.runtime:
            payload["runtime"] = info.runtime
        if info.binding:
            payload["binding"] = info.binding
        if info.instance_id:
            payload["instanceId"] = info.instance_id
        if info.placement:
            payload["placement"] = dict(info.placement)
        payload["topics"] = [topic.to_payload() for topic in self.topics]
        if info.degraded:
            payload["degraded"] = list(info.degraded)
        if info.profile:
            payload["profile"] = dict(info.profile)
        return payload
