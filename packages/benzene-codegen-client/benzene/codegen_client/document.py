"""Contract Document parser (contract-document.md §§1-4).

A Contract Document (conventionally ``{service}.spec.json``) describes the topics a service serves,
the events it publishes and the messages it sends, with their payload schemas. This module parses the
raw JSON into a small, typed model; every OpenAPI 3.0 Schema Object (``request``/``response``/
``message``, and every ``components.schemas`` entry) is kept as a plain ``dict`` — schema objects are
producer-defined arbitrary JSON, and re-modelling them as a Python class hierarchy would buy nothing
that :mod:`benzene.codegen_client.schema_closure` and :mod:`benzene.codegen_client.types` don't
already do by walking the dict directly.

A document whose ``schemaDialect`` this generator does not know is rejected
(:class:`UnknownSchemaDialectError`), as §1 requires — including one with no ``schemaDialect`` at all,
the shape producers wrote before ``spec-0.3.0``. A ``benzene:`` entry is ignored, never listed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: The only $ref prefix a Contract Document may use (contract-document.md §1).
SCHEMA_REF_PREFIX = "#/components/schemas/"

#: The reserved-topic prefix: such entries are ignored (contract-document.md §1).
RESERVED_TOPIC_PREFIX = "benzene:"

#: The schema dialects this generator understands (contract-document.md §1).
KNOWN_SCHEMA_DIALECTS = frozenset({"openapi-3.0"})


class ContractDocumentError(ValueError):
    """The input is not a well-formed Contract Document."""


class UnknownSchemaDialectError(ContractDocumentError):
    """The document's ``schemaDialect`` is one this generator does not know (§1: reject it)."""

    kind = "unknown-schema-dialect"

    def __init__(self, dialect: Any) -> None:
        self.dialect = dialect
        found = "has no schemaDialect" if dialect is None else f"has schemaDialect {dialect!r}"
        super().__init__(
            f"The document {found}; this generator reads {sorted(KNOWN_SCHEMA_DIALECTS)}. "
            "A document without one predates spec-0.3.0: fetch it again from an up-to-date service."
        )


@dataclass(frozen=True)
class HttpMapping:
    method: str
    path: str


@dataclass(frozen=True)
class RequestResponse:
    """One ``requests[]`` entry (contract-document.md §2). ``response`` is ``None`` when absent —
    the handler returns no payload."""

    topic: str
    request: dict[str, Any]
    response: dict[str, Any] | None
    version: str | None = None
    http_mappings: tuple[HttpMapping, ...] = ()

    @property
    def version_present(self) -> bool:
        return self.version is not None

    @property
    def response_present(self) -> bool:
        return self.response is not None


@dataclass(frozen=True)
class EventEntry:
    """One ``events[]`` entry (contract-document.md §3.1)."""

    topic: str
    message: dict[str, Any]
    version: str | None = None

    @property
    def version_present(self) -> bool:
        return self.version is not None


@dataclass(frozen=True)
class SendEntry:
    """One ``sends[]`` entry (contract-document.md §3.2). ``response`` is ``None`` for one-way."""

    topic: str
    request: dict[str, Any]
    response: dict[str, Any] | None = None
    version: str | None = None

    @property
    def version_present(self) -> bool:
        return self.version is not None

    @property
    def response_present(self) -> bool:
        return self.response is not None


@dataclass(frozen=True)
class ContractDocument:
    """A parsed Contract Document (contract-document.md §1), or a projection of one."""

    schema_dialect: str
    service: str
    service_version: str | None = None
    version_scheme: str | None = None
    description: str | None = None
    requests: tuple[RequestResponse, ...] = ()
    events: tuple[EventEntry, ...] = ()
    sends: tuple[SendEntry, ...] = ()
    schemas: dict[str, Any] = field(default_factory=dict)
    message_endpoint: str | None = None
    transports: tuple[str, ...] = ()
    contract_hash: str | None = None
    registration_hash: str | None = None

    def topics(self) -> tuple[str, ...]:
        return tuple(r.topic for r in self.requests)

    def find_request(self, topic: str, version: str | None = None) -> RequestResponse | None:
        """The ``requests[]`` entry for ``topic`` — the first one, or the one at ``version``."""
        for r in self.requests:
            if r.topic == topic and (version is None or r.version == version):
                return r
        return None


def is_reserved_topic(topic: str) -> bool:
    return topic.startswith(RESERVED_TOPIC_PREFIX)


def parse_document(data: dict[str, Any]) -> ContractDocument:
    """Parse a raw Contract Document (already ``json.loads``-ed) into :class:`ContractDocument`."""
    if not isinstance(data, dict):
        raise ContractDocumentError("A Contract Document must be a JSON object.")

    dialect = data.get("schemaDialect")
    if dialect not in KNOWN_SCHEMA_DIALECTS:
        raise UnknownSchemaDialectError(dialect)

    components = data.get("components") or {}
    return ContractDocument(
        schema_dialect=str(dialect),
        service=str(data.get("service") or ""),
        service_version=data.get("serviceVersion"),
        version_scheme=data.get("versionScheme"),
        description=data.get("description"),
        requests=tuple(
            _parse_request(entry) for entry in _domain_entries(data, "requests")
        ),
        events=tuple(_parse_event(entry) for entry in _domain_entries(data, "events")),
        sends=tuple(_parse_send(entry) for entry in _domain_entries(data, "sends")),
        schemas=dict(components.get("schemas") or {}),
        message_endpoint=data.get("messageEndpoint"),
        transports=tuple(data.get("transports") or ()),
        contract_hash=data.get("contractHash"),
        registration_hash=data.get("registrationHash"),
    )


def _domain_entries(data: dict[str, Any], member: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for entry in data.get(member) or []:
        if not isinstance(entry, dict) or "topic" not in entry:
            raise ContractDocumentError(
                f"{member}[] entry is missing required field 'topic': {entry!r}"
            )
        if not is_reserved_topic(str(entry["topic"])):
            entries.append(entry)
    return entries


def _parse_request(entry: dict[str, Any]) -> RequestResponse:
    http_mappings = tuple(
        HttpMapping(method=m["method"], path=m["path"]) for m in entry.get("httpMappings") or []
    )
    return RequestResponse(
        topic=entry["topic"],
        request=entry.get("request") or {},
        response=entry.get("response"),
        version=entry.get("version"),
        http_mappings=http_mappings,
    )


def _parse_event(entry: dict[str, Any]) -> EventEntry:
    return EventEntry(
        topic=entry["topic"],
        message=entry.get("message") or {},
        version=entry.get("version"),
    )


def _parse_send(entry: dict[str, Any]) -> SendEntry:
    return SendEntry(
        topic=entry["topic"],
        request=entry.get("request") or {},
        response=entry.get("response"),
        version=entry.get("version"),
    )


def to_raw_document(document: ContractDocument) -> dict[str, Any]:
    """Serialize a (possibly projected) :class:`ContractDocument` back to Contract Document JSON.

    The hashes are not copied: :mod:`benzene.codegen_client.contract_hash` recomputes them over the
    projection, which is what a projected document's hashes are (contract-document §5.2-§5.3).
    """
    requests: list[dict[str, Any]] = []
    for r in document.requests:
        entry: dict[str, Any] = {"topic": r.topic}
        if r.version is not None:
            entry["version"] = r.version
        if r.http_mappings:
            entry["httpMappings"] = [{"method": m.method, "path": m.path} for m in r.http_mappings]
        entry["request"] = r.request
        if r.response is not None:
            entry["response"] = r.response
        requests.append(entry)

    events: list[dict[str, Any]] = []
    for e in document.events:
        entry = {"topic": e.topic}
        if e.version is not None:
            entry["version"] = e.version
        entry["message"] = e.message
        events.append(entry)

    sends: list[dict[str, Any]] = []
    for s in document.sends:
        entry = {"topic": s.topic}
        if s.version is not None:
            entry["version"] = s.version
        entry["request"] = s.request
        if s.response is not None:
            entry["response"] = s.response
        sends.append(entry)

    raw: dict[str, Any] = {"schemaDialect": document.schema_dialect, "service": document.service}
    if document.service_version is not None:
        raw["serviceVersion"] = document.service_version
    if document.version_scheme is not None:
        raw["versionScheme"] = document.version_scheme
    if document.description is not None:
        raw["description"] = document.description
    if document.message_endpoint is not None:
        raw["messageEndpoint"] = document.message_endpoint
    if document.transports:
        raw["transports"] = list(document.transports)
    raw["requests"] = requests
    raw["events"] = events
    raw["sends"] = sends
    raw["components"] = {"schemas": document.schemas}
    return raw
