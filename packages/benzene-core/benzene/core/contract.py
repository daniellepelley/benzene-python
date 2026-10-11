"""The Contract Document — the one self-description a service derives and serves (contract-document.md).

A service projects its handler registry and its outbound declarations into one document::

    {"schemaDialect": "openapi-3.0", "service": "orders", "serviceVersion": "1.4.2",
     "versionScheme": "semver", "description": "...", "runtime": "python",
     "placement": {"cloud": "aws"}, "messageEndpoint": "/benzene/invoke",
     "transports": ["http", "sqs"], "contractHash": "sha256:...", "registrationHash": "sha256:...",
     "requests": [...], "events": [...], "sends": [...], "components": {"schemas": {...}}}

It is what the reserved ``benzene:spec`` topic answers with (:func:`spec_interception`), what
``GET /benzene/spec`` serves (through the same pipeline), the profile's R5 document, the body a
mesh-enabled service registers with a collector (mesh.md §2), and the one input every language's
client generator reads. The deployment-side fields — ``runtime``, ``placement``, ``profile`` and
``degraded`` — sit outside both hashes, so identity and placement never change them. The schemas are OpenAPI 3.0 Schema Objects
(:mod:`benzene.core.contract_schema`); the document itself is not an OpenAPI document.

``requests[]`` is one entry per registered handler; ``events[]`` and ``sends[]`` come from an
:class:`~benzene.core.OutboundRegistry`'s event and send declarations. No ``benzene:`` topic is ever
listed — the reserved topics' shapes are defined once, by the spec. Both hashes are always present
and computed over this document (:mod:`benzene.core.contract_hash`).

Derive it **once, at the composition root**: :func:`~benzene.core.application_from` does so for an
:class:`~benzene.core.AppDefinition` that declares a :class:`ContractInfo`, which is what makes a
component-name collision fail before the first message is handled, and what makes the topic and the
HTTP route serve the identical document.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Protocol, runtime_checkable

from benzene.results import Result

from .context import Context
from .contract_hash import RESERVED_TOPIC_PREFIX, contract_hash, registration_hash
from .contract_schema import Schema, SchemaCatalogue
from .declarations import OutboundRegistry
from .pipeline import Middleware, Next
from .registry import SupportsDefinitions

#: The dialect of every schema in the document (contract-document.md §1).
SCHEMA_DIALECT = "openapi-3.0"

#: The reserved topic a service answers with its Contract Document (contract-document.md §7).
SPEC_TOPIC = "benzene:spec"

#: ``benzene:spec``'s default ``type`` and ``format``: this document, as JSON (§7).
SPEC_TYPE_BENZENE = "benzene"
SPEC_FORMAT_JSON = "json"

#: A handler declares it returns no payload with ``response_type=NoPayload``: its ``requests[]``
#: entry then has no ``response`` (§2). An undeclared ``response_type`` (``None``) means a payload of
#: unconstrained shape — ``{}``.
NoPayload: type = type(None)

#: The service-version comparison schemes (service-versions.md §2).
VERSION_SCHEMES = frozenset({"integer", "semver", "lexicographic"})

#: This port's ``runtime`` value (contract-document.md §1).
RUNTIME = "python"

#: The members a producer may name in ``degraded`` — the ones it could not determine (§1).
DEGRADABLE_MEMBERS = ("requests", "events", "sends")


@dataclass(frozen=True)
class HttpMapping:
    """One ``httpMappings[]`` pair — a topic's HTTP route (contract-document.md §2)."""

    method: str
    path: str

    def to_payload(self) -> dict[str, Any]:
        return {"method": self.method, "path": self.path}


@runtime_checkable
class SupportsHttpMappings(Protocol):
    """Anything that knows each ``(topic, version)``'s HTTP routes — e.g. ``benzene.http``'s router."""

    def http_mappings(self) -> Mapping[tuple[str, str], Sequence[HttpMapping]]: ...


@runtime_checkable
class SupportsMessageEndpoint(Protocol):
    """Anything that knows the HTTP envelope endpoint's path — e.g. ``benzene.http.StandardPaths``."""

    @property
    def message_endpoint(self) -> str | None: ...


@dataclass(frozen=True)
class ContractInfo:
    """What a service says about itself that its registrations cannot: the document's identity.

    Declared on :class:`~benzene.core.AppDefinition` (``contract=ContractInfo("orders")``), it turns
    the self-description on: the document is derived at the composition root and ``benzene:spec``
    answers with it. ``transports`` lists every transport this service receives messages over —
    declared, not detected, because no one host sees all of them.

    ``placement`` is where the service runs — ``{"cloud": "aws" | "azure" | "gcp" | "self-hosted"}``,
    plus ``region`` only where the platform documents how to know it (never guessed). ``runtime`` is
    the implementing port. ``profile`` is a conformance-profile self-check
    (``{"name": "cloud-service", "missing"?: [...]}`` — ``benzene.mesh`` evaluates one), and
    ``degraded`` names the members (``requests``, ``events``, ``sends``) this service could not
    determine. All four ride on the document outside its hashes (contract-document §1), so the
    document ``benzene:spec`` serves and the one a mesh registration carries are the same.
    """

    service: str
    service_version: str = ""
    version_scheme: str = ""
    description: str = ""
    transports: tuple[str, ...] = ()
    placement: Mapping[str, str] | None = None
    runtime: str = RUNTIME
    profile: Mapping[str, Any] | None = None
    degraded: tuple[str, ...] = ()


@dataclass(frozen=True)
class ContractRequest:
    """One ``requests[]`` entry. ``response`` is ``None`` when the handler returns no payload."""

    topic: str
    request: Schema = field(default_factory=dict)
    response: Schema | None = field(default_factory=dict)
    version: str = ""
    http_mappings: tuple[HttpMapping, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"topic": self.topic}
        # Absent and empty differ (§2): absence means the unversioned handler, and a producer MUST
        # NOT write "version": "" for it.
        if self.version:
            payload["version"] = self.version
        if self.http_mappings:  # omitted when empty
            payload["httpMappings"] = [mapping.to_payload() for mapping in self.http_mappings]
        payload["request"] = self.request
        if self.response is not None:  # absent means "no payload"; {} means "any payload"
            payload["response"] = self.response
        return payload


@dataclass(frozen=True)
class ContractEvent:
    """One ``events[]`` entry: an event this service publishes and defines (§3.1)."""

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
class ContractSend:
    """One ``sends[]`` entry: a message to a topic another party defines (§3.2).

    ``response`` is ``None`` for a one-way send.
    """

    topic: str
    request: Schema = field(default_factory=dict)
    response: Schema | None = None
    version: str = ""

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"topic": self.topic}
        if self.version:
            payload["version"] = self.version
        payload["request"] = self.request
        if self.response is not None:
            payload["response"] = self.response
        return payload


@dataclass(frozen=True)
class ContractDocument:
    """A service's Contract Document. Build it with :meth:`derive`; serve :meth:`to_payload`.

    ``runtime``, ``placement``, ``profile`` and ``degraded`` are the deployment-side fields
    (contract-document §1), declared on :class:`ContractInfo`: outside both hashes, and carried
    alike by the served document and the mesh register body (mesh.md §2).
    """

    service: str
    service_version: str = ""
    version_scheme: str = ""
    description: str = ""
    requests: tuple[ContractRequest, ...] = ()
    events: tuple[ContractEvent, ...] = ()
    sends: tuple[ContractSend, ...] = ()
    schemas: Mapping[str, Schema] = field(default_factory=dict)
    message_endpoint: str | None = None
    transports: tuple[str, ...] = ()
    runtime: str = ""
    placement: Mapping[str, str] | None = None
    profile: Mapping[str, Any] | None = None
    degraded: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        unknown = [member for member in self.degraded if member not in DEGRADABLE_MEMBERS]
        if unknown:
            raise ValueError(
                f"degraded names {unknown}; only {list(DEGRADABLE_MEMBERS)} can be undetermined "
                "(contract-document §1)"
            )

    @classmethod
    def derive(
        cls,
        registry: SupportsDefinitions,
        *,
        service: str,
        service_version: str = "",
        version_scheme: str = "",
        description: str = "",
        outbound: OutboundRegistry | None = None,
        message_endpoint: str | None = None,
        transports: Sequence[str] = (),
        http_mappings: Mapping[tuple[str, str], Sequence[HttpMapping]] | None = None,
        runtime: str = "",
        placement: Mapping[str, str] | None = None,
        profile: Mapping[str, Any] | None = None,
        degraded: Sequence[str] = (),
    ) -> ContractDocument:
        """Project a handler registry and outbound declarations into a Contract Document.

        Entries are sorted by topic then version; ``benzene:`` topics are never listed. Every
        dataclass and serializable enum reachable from an entry is named in ``components.schemas``
        (contract-document §4.1); two different types claiming one name raise
        :class:`~benzene.core.ComponentNameCollisionError` here, naming both and the remedy.
        ``http_mappings`` is keyed by ``(topic, version)`` — a route resolves to one handler.
        """
        if not service:
            raise ValueError("A Contract Document needs a service name (contract-document §1).")
        if version_scheme and version_scheme not in VERSION_SCHEMES:
            raise ValueError(
                f"version_scheme {version_scheme!r} is not one of {sorted(VERSION_SCHEMES)} "
                "(service-versions.md §2)."
            )
        catalogue = SchemaCatalogue()
        mappings = http_mappings or {}

        requests = [
            (
                definition.topic,
                definition.version,
                catalogue.schema(definition.request_type, topic=definition.topic),
                None
                if definition.response_type is NoPayload
                else catalogue.schema(definition.response_type, topic=definition.topic),
                tuple(mappings.get((definition.topic, definition.version), ())),
            )
            for definition in registry.definitions()
            if not _reserved(definition.topic)
        ]
        events = [
            (e.topic, e.version, catalogue.schema(e.message_type, topic=e.topic))
            for e in (outbound.events() if outbound is not None else ())
            if not _reserved(e.topic)
        ]
        sends = [
            (
                s.topic,
                s.version,
                catalogue.schema(s.request_type, topic=s.topic),
                None
                if s.response_type is None or s.response_type is NoPayload
                else catalogue.schema(s.response_type, topic=s.topic),
            )
            for s in (outbound.sends() if outbound is not None else ())
            if not _reserved(s.topic)
        ]
        schemas, rename = catalogue.finish()

        return cls(
            service=service,
            service_version=service_version,
            version_scheme=version_scheme,
            description=description,
            requests=tuple(
                sorted(
                    (
                        ContractRequest(
                            topic=topic,
                            version=version,
                            request=rename(request),
                            response=None if response is None else rename(response),
                            http_mappings=routes,
                        )
                        for topic, version, request, response, routes in requests
                    ),
                    key=lambda entry: (entry.topic, entry.version),
                )
            ),
            events=tuple(
                sorted(
                    (
                        ContractEvent(topic=topic, version=version, message=rename(message))
                        for topic, version, message in events
                    ),
                    key=lambda entry: (entry.topic, entry.version),
                )
            ),
            sends=tuple(
                sorted(
                    (
                        ContractSend(
                            topic=topic,
                            version=version,
                            request=rename(request),
                            response=None if response is None else rename(response),
                        )
                        for topic, version, request, response in sends
                    ),
                    key=lambda entry: (entry.topic, entry.version),
                )
            ),
            schemas=schemas,
            message_endpoint=message_endpoint,
            transports=tuple(transports),
            runtime=runtime,
            placement=dict(placement) if placement else None,
            profile=dict(profile) if profile else None,
            degraded=tuple(dict.fromkeys(degraded)),
        )

    @cached_property
    def contract_hash(self) -> str:
        """``contractHash`` (§6.1): over ``requests`` and ``events``."""
        return contract_hash(self._body())

    @cached_property
    def registration_hash(self) -> str:
        """``registrationHash`` (§6.1): over ``requests``, ``events`` and ``sends``."""
        return registration_hash(self._body())

    def to_payload(self) -> dict[str, Any]:
        """This document as Contract Document JSON (contract-document.md §1)."""
        payload: dict[str, Any] = {"schemaDialect": SCHEMA_DIALECT, "service": self.service}
        if self.service_version:
            payload["serviceVersion"] = self.service_version
        if self.version_scheme:
            payload["versionScheme"] = self.version_scheme
        if self.description:
            payload["description"] = self.description
        if self.runtime:
            payload["runtime"] = self.runtime
        if self.placement:
            payload["placement"] = dict(self.placement)
        if self.profile:
            payload["profile"] = dict(self.profile)
        if self.degraded:  # omitted when empty
            payload["degraded"] = list(self.degraded)
        # Absent when there is no envelope endpoint: a consumer feature-detects send capability on
        # its presence and never assumes a default path.
        if self.message_endpoint is not None:
            payload["messageEndpoint"] = self.message_endpoint
        if self.transports:  # omitted when empty
            payload["transports"] = list(self.transports)
        payload["contractHash"] = self.contract_hash
        payload["registrationHash"] = self.registration_hash
        payload.update(self._body())
        return payload

    def _body(self) -> dict[str, Any]:
        return {
            "requests": [entry.to_payload() for entry in self.requests],
            "events": [entry.to_payload() for entry in self.events],
            "sends": [entry.to_payload() for entry in self.sends],
            "components": {"schemas": dict(self.schemas)},
        }


def select_spec_document(type: object = None, format: object = None) -> str | None:  # noqa: A002
    """Why a ``benzene:spec`` request cannot be served, or ``None`` when it asks for this document.

    An absent or empty ``type``/``format`` means ``benzene``/``json``; values compare
    case-insensitively. Anything else is answered ``not-implemented`` — never with a different
    document (contract-document §7).
    """
    kind = str(type or "").strip().lower() or SPEC_TYPE_BENZENE
    rendering = str(format or "").strip().lower() or SPEC_FORMAT_JSON
    if kind != SPEC_TYPE_BENZENE:
        return (
            f"benzene:spec type {kind!r} is not implemented by this service; it serves "
            f"{SPEC_TYPE_BENZENE!r} (the Contract Document)"
        )
    if rendering != SPEC_FORMAT_JSON:
        return (
            f"benzene:spec format {rendering!r} is not implemented by this service; it renders "
            f"{SPEC_FORMAT_JSON!r} only"
        )
    return None


def spec_interception(document: ContractDocument, *, aliases: Sequence[str] = ()) -> Middleware:
    """Middleware that answers ``benzene:spec`` (and any ``aliases``) with ``document``.

    The request is the optional ``{"type", "format"}`` of contract-document §7: an absent body,
    ``null`` or ``{}`` means ``benzene``/``json`` — the document, as ``ok``. An unsupported type or
    format is answered ``not-implemented``. Interception is by topic id, version ignored.

    Install it **after** any authentication middleware and before the message router, so the topic
    runs the same pipeline as every other one — :func:`~benzene.core.application_from` does exactly
    that for an :class:`~benzene.core.AppDefinition` declaring a :class:`ContractInfo`.
    """
    topics = {SPEC_TOPIC, *aliases}
    payload = document.to_payload()  # derived once: the document never changes while serving

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        if context.topic not in topics:
            await next()
            return
        request = context.request
        if request is None:
            request = {}
        if not isinstance(request, dict):
            context.result = Result.bad_request(
                'benzene:spec takes an optional {"type": ..., "format": ...} object'
            )
            return
        unsupported = select_spec_document(request.get("type"), request.get("format"))
        context.result = (
            Result.not_implemented(unsupported) if unsupported is not None else Result.ok(payload)
        )
        # Terminal for the reserved topic: it never reaches the router.

    return middleware


def derive_contract(
    info: ContractInfo,
    registry: SupportsDefinitions,
    *,
    outbound: OutboundRegistry | None = None,
    router: Any = None,
    standard_paths: Any = None,
) -> ContractDocument:
    """The composition root's one derivation: ``info`` plus everything the app declared.

    ``router`` contributes each topic's ``httpMappings`` when it knows them
    (:class:`SupportsHttpMappings`), and ``standard_paths`` the ``messageEndpoint``
    (:class:`SupportsMessageEndpoint`) — both duck-typed, so this stays transport-free.
    """
    return ContractDocument.derive(
        registry,
        service=info.service,
        service_version=info.service_version,
        version_scheme=info.version_scheme,
        description=info.description,
        outbound=outbound,
        message_endpoint=(
            standard_paths.message_endpoint
            if isinstance(standard_paths, SupportsMessageEndpoint)
            else None
        ),
        transports=info.transports,
        http_mappings=router.http_mappings() if isinstance(router, SupportsHttpMappings) else None,
        runtime=info.runtime,
        placement=info.placement,
        profile=info.profile,
        degraded=info.degraded,
    )


def is_reserved_topic(topic: str) -> bool:
    """Whether ``topic`` is a reserved Benzene topic, by its ``benzene:`` prefix."""
    return topic.startswith(RESERVED_TOPIC_PREFIX)


_reserved = is_reserved_topic
