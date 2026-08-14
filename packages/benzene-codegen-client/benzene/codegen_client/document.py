"""Contract Document parser (contract-document.md §§1-4).

A Contract Document (conventionally ``{Service}.spec.json``) describes every topic a service
serves: request/response shapes, payload schemas, and (optionally) how topics are reachable over
HTTP. This module parses the raw JSON into a small, typed model; every OpenAPI 3.0 Schema Object
(``request``/``response``/``message``, and every ``components.schemas`` entry) is kept as a plain
``dict`` — schema objects are producer-defined arbitrary JSON, and re-modelling them as a Python
class hierarchy would buy nothing that :mod:`benzene.codegen_client.schema_closure` and
:mod:`benzene.codegen_client.types` don't already do by walking the dict directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: The only $ref prefix a Contract Document may use (contract-document.md §4).
SCHEMA_REF_PREFIX = "#/components/schemas/"

#: The reserved-topic prefix (contract-document.md §5.1).
RESERVED_TOPIC_PREFIX = "benzene:"


class ContractDocumentError(ValueError):
    """The input is not a well-formed Contract Document."""


@dataclass(frozen=True)
class HttpMapping:
    method: str
    path: str


@dataclass(frozen=True)
class RequestResponse:
    """One ``requests[]`` entry (contract-document.md §2)."""

    topic: str
    request: dict[str, Any]
    response: dict[str, Any]
    version: str | None = None
    reserved: bool = False
    http_mappings: tuple[HttpMapping, ...] = ()

    @property
    def version_present(self) -> bool:
        return self.version is not None

    def is_reserved(self) -> bool:
        """contract-document.md §5.1: the flag OR the ``benzene:`` prefix — both are checked."""
        return self.reserved or is_reserved_topic(self.topic)


@dataclass(frozen=True)
class EventEntry:
    """One ``events[]`` entry (contract-document.md §3)."""

    topic: str
    message: dict[str, Any]
    version: str | None = None

    @property
    def version_present(self) -> bool:
        return self.version is not None


@dataclass(frozen=True)
class ContractDocument:
    """A parsed Contract Document (contract-document.md §1), or a projection of one."""

    openapi: str
    info: dict[str, Any]
    requests: tuple[RequestResponse, ...] = ()
    events: tuple[EventEntry, ...] = ()
    schemas: dict[str, Any] = field(default_factory=dict)
    message_endpoint: str | None = None
    transports: tuple[str, ...] = ()

    def topics(self) -> tuple[str, ...]:
        return tuple(r.topic for r in self.requests)

    def find_request(self, topic: str) -> RequestResponse | None:
        for r in self.requests:
            if r.topic == topic:
                return r
        return None


def is_reserved_topic(topic: str) -> bool:
    return topic.startswith(RESERVED_TOPIC_PREFIX)


def parse_document(data: dict[str, Any]) -> ContractDocument:
    """Parse a raw Contract Document (already ``json.loads``-ed) into :class:`ContractDocument`."""
    if not isinstance(data, dict):
        raise ContractDocumentError("A Contract Document must be a JSON object.")

    openapi = data.get("openapi", "3.0.1")
    info = data.get("info") or {}

    requests = tuple(_parse_request(entry) for entry in data.get("requests") or [])
    events = tuple(_parse_event(entry) for entry in data.get("events") or [])

    components = data.get("components") or {}
    schemas = dict(components.get("schemas") or {})

    transports = tuple(data.get("transports") or ())

    return ContractDocument(
        openapi=openapi,
        info=info,
        requests=requests,
        events=events,
        schemas=schemas,
        message_endpoint=data.get("messageEndpoint"),
        transports=transports,
    )


def _parse_request(entry: dict[str, Any]) -> RequestResponse:
    if "topic" not in entry:
        raise ContractDocumentError(f"requests[] entry is missing required field 'topic': {entry!r}")
    http_mappings = tuple(
        HttpMapping(method=m["method"], path=m["path"]) for m in entry.get("httpMappings") or []
    )
    return RequestResponse(
        topic=entry["topic"],
        request=entry.get("request") or {},
        response=entry.get("response") or {},
        version=entry.get("version"),
        reserved=bool(entry.get("reserved", False)),
        http_mappings=http_mappings,
    )


def _parse_event(entry: dict[str, Any]) -> EventEntry:
    if "topic" not in entry:
        raise ContractDocumentError(f"events[] entry is missing required field 'topic': {entry!r}")
    return EventEntry(
        topic=entry["topic"],
        message=entry.get("message") or {},
        version=entry.get("version"),
    )


def to_raw_document(document: ContractDocument) -> dict[str, Any]:
    """Serialize a (possibly projected) :class:`ContractDocument` back to Contract Document JSON shape.

    Used to feed :mod:`benzene.codegen_client.contract_hash`, which operates on the raw JSON shape
    (contract-document.md §6) rather than this parsed model, so the hash algorithm has one input
    shape whether it is fed a fixture's literal JSON or a document this generator projected.
    """
    requests: list[dict[str, Any]] = []
    for r in document.requests:
        entry: dict[str, Any] = {"topic": r.topic}
        if r.version is not None:
            entry["version"] = r.version
        if r.reserved:
            entry["reserved"] = True
        if r.http_mappings:
            entry["httpMappings"] = [{"method": m.method, "path": m.path} for m in r.http_mappings]
        entry["request"] = r.request
        entry["response"] = r.response
        requests.append(entry)

    events: list[dict[str, Any]] = []
    for e in document.events:
        entry = {"topic": e.topic}
        if e.version is not None:
            entry["version"] = e.version
        entry["message"] = e.message
        events.append(entry)

    raw: dict[str, Any] = {
        "openapi": document.openapi,
        "info": document.info,
        "requests": requests,
        "events": events,
        "components": {"schemas": document.schemas},
    }
    if document.message_endpoint is not None:
        raw["messageEndpoint"] = document.message_endpoint
    if document.transports:
        raw["transports"] = list(document.transports)
    return raw
