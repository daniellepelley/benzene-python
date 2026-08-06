"""The derived service specification — what a service serves, projected from its registry.

The Cloud Service Profile (R5) requires a service to **derive** a spec document from its handler
registry and expose it (over HTTP at ``/benzene/spec``), so the registry is the single source of truth
for the service's contract — never a hand-maintained document. A :class:`ServiceSpec` is that
projection: the service name and one entry per registered topic carrying its version and the
request/response JSON schema (:func:`benzene.core.json_schema`).

This is the transport-neutral core of R5. The reserved topic ``benzene:spec`` is answered by
:func:`spec_interception` (the same interception pattern as health checks and the mesh endpoint), so a
service can serve its spec over *any* transport; the HTTP binding maps ``/benzene/spec`` onto it.

The mesh :class:`~benzene.mesh.ServiceDescriptor` is a richer, mesh-facing projection of the same
registry (adding identity, placement, and a contract hash); ``ServiceSpec`` is the minimal
profile document and depends only on ``benzene.core``, so a service claims R5 without pulling in the
mesh module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from benzene.results import Result

from .context import Context
from .pipeline import Middleware, Next
from .registry import Registry
from .schema import Schema, json_schema

#: The reserved topic id a service intercepts to return its derived spec document.
SPEC_TOPIC = "benzene:spec"


@dataclass(frozen=True)
class TopicSpec:
    """One registered topic's projection: its id, optional version, and payload schemas."""

    id: str
    version: str = ""
    request_schema: Schema = field(default_factory=dict)
    response_schema: Schema = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"id": self.id}
        if self.version:  # omit an empty version rather than emit "" (spec: omit, don't null)
            payload["version"] = self.version
        payload["requestSchema"] = self.request_schema
        payload["responseSchema"] = self.response_schema
        return payload


@dataclass(frozen=True)
class ServiceSpec:
    """A service's derived specification. Build it with :meth:`derive`; read :meth:`to_payload`."""

    service: str
    topics: tuple[TopicSpec, ...]

    @classmethod
    def derive(cls, registry: Registry, *, service: str) -> "ServiceSpec":
        """Project a registry into a spec document (topics sorted by id then version)."""
        topics = tuple(
            sorted(
                (
                    TopicSpec(
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
        return cls(service=service, topics=topics)

    def to_payload(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "topics": [topic.to_payload() for topic in self.topics],
        }


#: A spec, or a zero-arg callable returning one (re-derived per request if the registry can change).
SpecSource = ServiceSpec | Callable[[], ServiceSpec]


def spec_interception(spec: SpecSource, *, aliases: Iterable[str] = ()) -> Middleware:
    """Middleware that answers ``benzene:spec`` (and any ``aliases``) with the derived spec document.

    ``spec`` may be a :class:`ServiceSpec` or a callable returning one. Interception is by topic id,
    version ignored — install it before the message router, exactly like health/mesh interception.
    """
    topics = {SPEC_TOPIC, *aliases}

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        if context.topic in topics:
            resolved = spec() if callable(spec) else spec
            context.result = Result.ok(resolved.to_payload())
            return  # short-circuit: the reserved topic never reaches the router
        await next()

    return middleware
