"""The derived service specification — what a service serves, projected from its registry.

The Cloud Service Profile (R5) requires a service to **derive** a spec document from its handler
registry and expose it (over HTTP at ``/benzene/spec``), so the registry is the single source of truth
for the service's contract — never a hand-maintained document. A :class:`ServiceSpec` is that
projection: the service name and one entry per registered topic carrying its version and the
request/response JSON schema (:func:`benzene.core.json_schema`), plus — for a service that declares
one (mesh.md §2.3) — the topics it *produces*, so the document describes both sides of the service's
contract and not just what it consumes.

This is the transport-neutral core of R5. The reserved topic ``benzene:spec`` is answered by
:func:`spec_interception` (the same interception pattern as health checks and the mesh endpoint), so a
service can serve its spec over *any* transport; the HTTP binding maps ``/benzene/spec`` onto it.

The mesh :class:`~benzene.mesh.ServiceDescriptor` is a richer, mesh-facing projection of the same
registry (adding identity, placement, and a contract hash); ``ServiceSpec`` is the minimal
profile document and depends only on ``benzene.core``, so a service claims R5 without pulling in the
mesh module.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from benzene.results import Result

from .context import Context
from .pipeline import Middleware, Next
from .registry import Registry
from .schema import Schema, json_schema

#: The reserved topic id a service intercepts to return its derived spec document.
SPEC_TOPIC = "benzene:spec"


class OutboundTopic(Protocol):
    """One *declared* outbound topic — the structural shape of ``benzene.mesh``'s
    ``OutboundDefinition`` (mesh.md §2.3), stated here as a protocol so ``benzene.core`` can project
    an outbound declaration without importing (or depending on) the mesh module."""

    @property
    def topic(self) -> str: ...

    @property
    def version(self) -> str: ...

    @property
    def request_type(self) -> type | None: ...

    @property
    def response_type(self) -> type | None: ...


@runtime_checkable
class SupportsOutboundDefinitions(Protocol):
    """Anything that can enumerate declared outbound topics — e.g. ``benzene.mesh``'s
    ``OutboundRegistry``, the mesh-side counterpart of :class:`~benzene.core.Registry`."""

    def definitions(self) -> Sequence[OutboundTopic]: ...


#: What :meth:`ServiceSpec.derive` accepts as a service's produced-topic declaration: an outbound
#: registry, or any iterable of outbound definitions — or of bare topic ids, so a service that stays
#: on ``benzene.core`` (no ``benzene.mesh`` dependency, exactly what R5 is meant to be reachable
#: without) can still declare what it produces.
ProducesSource = SupportsOutboundDefinitions | Iterable[OutboundTopic | str]


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
    produces: tuple[TopicSpec, ...] = ()

    @classmethod
    def derive(
        cls, registry: Registry, *, service: str, produces: ProducesSource | None = None
    ) -> ServiceSpec:
        """Project a registry into a spec document (topics sorted by id then version).

        ``produces`` declares the topics this service *sends* (mesh.md §2.3), projected into the
        document's own ``produces`` the same way the registry projects into ``topics``: sorted by id
        then version, schemas derived identically. It mirrors
        :meth:`~benzene.mesh.ServiceDescriptor.derive`'s third argument and exists for the same
        reason — a service that registers a handler for a topic is that topic's **consumer**, so
        without a declaration a spec document can only ever describe consumers, and a mesh that
        interrogates services by *pulling* this document (rather than being pushed a descriptor)
        would see every topic with consumers and no providers.

        Accepts an outbound registry, an iterable of outbound definitions, or an iterable of bare
        topic ids (the schema-less form, for a service that declares its outbound contract without
        taking a ``benzene.mesh`` dependency). Omitted (the default) yields no ``produces`` at all —
        the field is absent from the payload rather than an empty array, matching this document's
        omit-don't-null convention; a mesh reads an absent ``produces`` as "declares no outbound
        topics", the same reading it gives an empty one.
        """
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
        produced = tuple(
            sorted(
                (_outbound_spec(item) for item in _outbound_items(produces)),
                key=lambda t: (t.id, t.version),
            )
        )
        return cls(service=service, topics=topics, produces=produced)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "service": self.service,
            "topics": [topic.to_payload() for topic in self.topics],
        }
        if self.produces:  # omit an undeclared produces rather than emit [] (spec: omit, don't null)
            payload["produces"] = [topic.to_payload() for topic in self.produces]
        return payload


def _outbound_items(produces: ProducesSource | None) -> Iterable[OutboundTopic | str]:
    """Normalize the three accepted ``produces`` forms into one iterable of declarations."""
    if produces is None:
        return ()
    if isinstance(produces, str):  # a bare str is iterable — silently declaring one topic per letter
        raise TypeError(
            f"produces={produces!r} is a single string; pass a sequence of topic ids "
            f"(e.g. [{produces!r}]), an outbound registry, or outbound definitions."
        )
    if isinstance(produces, SupportsOutboundDefinitions):
        return produces.definitions()
    return produces


def _outbound_spec(item: OutboundTopic | str) -> TopicSpec:
    """One declared outbound topic as a :class:`TopicSpec` — a bare id carries no schemas."""
    if isinstance(item, str):
        return TopicSpec(id=item)
    return TopicSpec(
        id=item.topic,
        version=item.version,
        request_schema=json_schema(item.request_type),
        response_schema=json_schema(item.response_type),
    )


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
