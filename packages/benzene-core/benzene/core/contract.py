"""The Contract Document — the registry projected into the format every client generator parses.

The Cloud Service Profile's **R5** requires a service to derive a spec document from its handler
registry and serve it at ``/benzene/spec`` (``?type=benzene&format=json``), and
``contract-document.md`` defines that document's shape:

    {"openapi": "3.0.1", "info": {...}, "messageEndpoint": "...", "transports": [...],
     "requests": [...], "events": [...], "components": {"schemas": {...}}}

It is deliberately *not* full OpenAPI — ``openapi`` is a heritage marker naming the schema dialect
(OpenAPI 3.0 Schema Objects), and there are no ``paths``: a Benzene service is addressed by topic.
``requests[]`` is one entry per request/response topic and ``events[]`` one per topic the service
*produces*, each pointing at a schema either inline or by ``$ref`` into ``components.schemas``.

This is the format a .NET, TypeScript, Go, or Python client generator reads — the reason it is
spec'd at all is that four generators must parse one file. :class:`~benzene.core.ServiceSpec`'s
``{service, topics}`` payload is this port's own older, native shape; it is still served (the
``?type=native`` switch on the HTTP surface), but it is not what R5 names and no other port's
tooling can read it.

Two entry points, because a service reaches this document from two places:

* :meth:`ContractDocument.derive` — the **registry** projection. It sees the handlers' declared
  Python types, so it names each dataclass payload in ``components.schemas`` and refers to it by
  ``$ref``, which is what lets a generator emit a named type per payload rather than an anonymous
  one per topic.
* :meth:`ContractDocument.from_spec` — the projection of an already-derived
  :class:`~benzene.core.ServiceSpec`. The types are gone by then, so every schema is written inline
  and the catalogue is empty (§2 allows either) — the point is that a host holding only a
  ``ServiceSpec`` still serves a conformant Contract Document rather than something no other port
  can parse.

Emission follows §1's presence column exactly: ``transports`` is omitted when empty rather than
written as ``[]``, ``info`` writes empty strings rather than going missing, ``messageEndpoint`` is
absent when the service exposes no such endpoint (consumers feature-detect send capability on it),
and ``requests``/``events``/``components`` are always present even when empty.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .registry import SupportsDefinitions
from .schema import Schema, json_schema
from .spec import ProducesSource, ServiceSpec, outbound_items

#: The ``openapi`` marker every Contract Document carries (contract-document.md §1). A heritage
#: marker naming the schema-object dialect, not a claim to be an OpenAPI document — §1 is explicit
#: that a consumer MUST NOT reject a document on this value.
CONTRACT_OPENAPI = "3.0.1"

#: The only ``$ref`` form a Contract Document may use (contract-document.md §4).
SCHEMA_REF_PREFIX = "#/components/schemas/"

#: The reserved-topic prefix (contract-document.md §5.1) — the half of the reserved-detection rule a
#: producer can apply without being told.
RESERVED_TOPIC_PREFIX = "benzene:"


@dataclass(frozen=True)
class HttpMapping:
    """One ``httpMappings[]`` pair — a topic's explicit HTTP exposure (contract-document.md §2)."""

    method: str
    path: str

    def to_payload(self) -> dict[str, Any]:
        return {"method": self.method, "path": self.path}


@dataclass(frozen=True)
class ContractRequest:
    """One ``requests[]`` entry: a request/response topic (contract-document.md §2)."""

    topic: str
    request: Schema = field(default_factory=dict)
    response: Schema = field(default_factory=dict)
    version: str = ""
    reserved: bool = False
    http_mappings: tuple[HttpMapping, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"topic": self.topic}
        # Absent and empty are not the same thing (§2): absence means the unversioned handler, and a
        # producer MUST NOT write "version": "" for it.
        if self.version:
            payload["version"] = self.version
        if self.reserved:  # written only when true; its absence also means not-reserved (§2)
            payload["reserved"] = True
        if self.http_mappings:  # omitted when empty — an absent array means "no HTTP exposure"
            payload["httpMappings"] = [mapping.to_payload() for mapping in self.http_mappings]
        payload["request"] = self.request
        payload["response"] = self.response
        return payload


@dataclass(frozen=True)
class ContractEvent:
    """One ``events[]`` entry: a topic the service produces fire-and-forget (§3)."""

    topic: str
    message: Schema = field(default_factory=dict)
    version: str = ""

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"topic": self.topic}
        if self.version:
            payload["version"] = self.version
        payload["message"] = self.message
        return payload


@dataclass(frozen=True)
class ContractDocument:
    """A service's Contract Document. Build it with :meth:`derive` / :meth:`from_spec`."""

    service: str
    version: str = ""
    description: str = ""
    requests: tuple[ContractRequest, ...] = ()
    events: tuple[ContractEvent, ...] = ()
    schemas: dict[str, Schema] = field(default_factory=dict)
    message_endpoint: str | None = None
    transports: tuple[str, ...] = ()

    @classmethod
    def derive(
        cls,
        registry: SupportsDefinitions,
        *,
        service: str,
        version: str = "",
        description: str = "",
        produces: ProducesSource | None = None,
        message_endpoint: str | None = None,
        transports: Sequence[str] = (),
        http_mappings: Mapping[tuple[str, str], Sequence[HttpMapping]] | None = None,
    ) -> ContractDocument:
        """Project a handler registry into a Contract Document (entries sorted by topic then version).

        ``produces`` declares the topics this service *sends* and becomes ``events[]``, the same
        three accepted forms as :meth:`~benzene.core.ServiceSpec.derive`. ``http_mappings`` is keyed
        by ``(topic, version)`` — the same key the registry and an HTTP router agree on — because a
        route resolves to one handler, not to a topic across all its versions.

        Every declared payload type that derives to a real object schema is named once in
        ``components.schemas`` and referenced by ``$ref``; anything else (an untyped ``dict``
        handler, a primitive, an unknown type) is written inline, which §2 allows and which is the
        honest rendering of "this handler declared no shape".
        """
        catalogue = _Catalogue()
        mappings = http_mappings or {}
        requests = tuple(
            sorted(
                (
                    ContractRequest(
                        topic=definition.topic,
                        request=catalogue.reference(definition.request_type),
                        response=catalogue.reference(definition.response_type),
                        version=definition.version,
                        reserved=is_reserved_topic(definition.topic),
                        http_mappings=tuple(
                            mappings.get((definition.topic, definition.version), ())
                        ),
                    )
                    for definition in registry.definitions()
                ),
                key=lambda entry: (entry.topic, entry.version),
            )
        )
        events = tuple(
            sorted(
                (_event_of(item, catalogue) for item in outbound_items(produces)),
                key=lambda entry: (entry.topic, entry.version),
            )
        )
        return cls(
            service=service,
            version=version,
            description=description,
            requests=requests,
            events=events,
            schemas=catalogue.schemas,
            message_endpoint=message_endpoint,
            transports=tuple(transports),
        )

    @classmethod
    def from_spec(
        cls,
        spec: ServiceSpec,
        *,
        version: str = "",
        description: str = "",
        message_endpoint: str | None = None,
        transports: Sequence[str] = (),
        http_mappings: Mapping[tuple[str, str], Sequence[HttpMapping]] | None = None,
    ) -> ContractDocument:
        """Project an already-derived :class:`~benzene.core.ServiceSpec` into a Contract Document.

        The ``ServiceSpec`` carries derived schemas but not the types they came from, so every
        schema is written inline and ``components.schemas`` is empty. That costs a generator its
        payload *names*, not any of the contract — which is why a host that can reach the registry
        should prefer :meth:`derive`.
        """
        mappings = http_mappings or {}
        requests = tuple(
            ContractRequest(
                topic=topic.id,
                request=dict(topic.request_schema),
                response=dict(topic.response_schema),
                version=topic.version,
                reserved=is_reserved_topic(topic.id),
                http_mappings=tuple(mappings.get((topic.id, topic.version), ())),
            )
            for topic in spec.topics
        )
        events = tuple(
            # A produced topic's request schema is the message it sends; a fire-and-forget event
            # has no response half to carry (§3).
            ContractEvent(topic=topic.id, message=dict(topic.request_schema), version=topic.version)
            for topic in spec.produces
        )
        return cls(
            service=spec.service,
            version=version,
            description=description,
            requests=requests,
            events=events,
            message_endpoint=message_endpoint,
            transports=tuple(transports),
        )

    def to_payload(self) -> dict[str, Any]:
        """This document as Contract Document JSON (contract-document.md §1)."""
        payload: dict[str, Any] = {
            "openapi": CONTRACT_OPENAPI,
            # REQUIRED, and a producer with no description/version writes empty strings rather than
            # a missing object (§1) — absence would read as "this producer has no info to give".
            "info": {
                "title": self.service,
                "description": self.description,
                "version": self.version,
            },
        }
        # OPTIONAL and absent when the service exposes no message endpoint: consumers feature-detect
        # send capability on its presence and must never assume a default path (§1).
        if self.message_endpoint is not None:
            payload["messageEndpoint"] = self.message_endpoint
        if self.transports:  # omitted when empty, never an empty array (§1)
            payload["transports"] = list(self.transports)
        # REQUIRED and always present, possibly empty — unlike the two above.
        payload["requests"] = [entry.to_payload() for entry in self.requests]
        payload["events"] = [entry.to_payload() for entry in self.events]
        payload["components"] = {"schemas": dict(self.schemas)}
        return payload


#: A Contract Document, or a zero-arg callable returning one (re-derived per request).
ContractSource = ContractDocument | Callable[[], "ContractDocument"]


def is_reserved_topic(topic: str) -> bool:
    """Whether ``topic`` is a reserved Benzene utility topic by its prefix (§5.1's second half)."""
    return topic.startswith(RESERVED_TOPIC_PREFIX)


def resolve_contract(source: ContractSource | None) -> ContractDocument | None:
    """The document a :data:`ContractSource` denotes, calling it when it is a callable."""
    if source is None:
        return None
    return source() if callable(source) else source


def _event_of(item: Any, catalogue: _Catalogue) -> ContractEvent:
    """One declared outbound topic as an ``events[]`` entry — a bare topic id carries no schema."""
    if isinstance(item, str):
        return ContractEvent(topic=item)
    return ContractEvent(
        topic=item.topic, message=catalogue.reference(item.request_type), version=item.version
    )


class _Catalogue:
    """Builds ``components.schemas`` while handing back a ``$ref`` (or an inline schema) per type.

    A type is catalogued once and referenced everywhere it appears, so two topics taking the same
    payload agree on one named schema — which is the whole reason a generator can emit one type for
    it. Two *different* types that happen to share a ``__name__`` (the same class name in two
    modules) get distinct names, since a catalogue keyed by name would otherwise silently give one
    of them the other's shape.
    """

    def __init__(self) -> None:
        self.schemas: dict[str, Schema] = {}
        self._names: dict[Any, str] = {}

    def reference(self, py_type: Any) -> Schema:
        derived = json_schema(py_type)
        if not self._is_nameable(py_type, derived):
            return derived  # inline: §2 takes a Schema Object here, $ref or not
        name = self._names.get(py_type)
        if name is None:
            name = self._unique(getattr(py_type, "__name__", "Schema"))
            self._names[py_type] = name
            self.schemas[name] = derived
        return {"$ref": SCHEMA_REF_PREFIX + name}

    @staticmethod
    def _is_nameable(py_type: Any, derived: Schema) -> bool:
        """Only a declared object payload earns a catalogue entry.

        A ``dict``-typed handler, a primitive, or an unknown type derives to a shape with no name
        worth publishing — cataloguing it would fill ``components.schemas`` with entries no
        generator can turn into a meaningful type.
        """
        return (
            isinstance(py_type, type)
            and dataclasses.is_dataclass(py_type)
            and derived.get("type") == "object"
        )

    def _unique(self, name: str) -> str:
        if name not in self.schemas:
            return name
        suffix = 2
        while f"{name}{suffix}" in self.schemas:
            suffix += 1
        return f"{name}{suffix}"
