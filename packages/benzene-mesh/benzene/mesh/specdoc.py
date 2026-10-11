"""Reading a service's Contract Document — whatever port served it — for mesh tooling.

``/benzene/spec`` answers with the Contract Document (contract-document.md):
``{schemaDialect, service, contractHash, registrationHash, requests[], events[], sends[],
components}``. The probe and the poller both need four things out of it — the service name, the
topics it consumes, the topics it produces, and its ``registrationHash`` — so the reading lives here
once rather than being written twice and drifting.

Topics come back in the descriptor's entry shape (``{id, version?, requestSchema, responseSchema}``),
because that is what :class:`~benzene.mesh.MeshCollector` ingests until the register body becomes
the document itself. Schemas are dereferenced against ``components.schemas`` (a reference into a
catalogue the collector never sees would compare as "the shape changed" the moment a producer renamed
a class); a reference cycle is cut with ``{}``, as the hash projection cuts it (contract-document
§6.2). Produced topics are ``events`` ∪ ``sends``: a published event's publisher and a sender to
another party's topic are both providers.

``benzene:`` entries are ignored: a producer must not list them, and a consumer ignores any it meets
(contract-document §1).
"""

from __future__ import annotations

from typing import Any

#: The reserved-topic prefix (contract-document.md §1).
RESERVED_TOPIC_PREFIX = "benzene:"

#: The dialect this reader understands (contract-document.md §1).
SCHEMA_DIALECT = "openapi-3.0"

#: The only ``$ref`` form a Contract Document uses (contract-document.md §1).
_SCHEMA_REF_PREFIX = "#/components/schemas/"


def is_reserved_topic(topic: str) -> bool:
    return topic.startswith(RESERVED_TOPIC_PREFIX)


def is_contract_document(document: dict[str, Any]) -> bool:
    """Whether ``document`` is a Contract Document in a dialect this reader knows.

    A consumer MUST reject a document whose ``schemaDialect`` it does not know (§1), so a document
    without the field — the pre-``spec-0.3.0`` ``openapi``/``info`` shape — is not one.
    """
    return (
        document.get("schemaDialect") == SCHEMA_DIALECT
        and bool(document.get("service"))
        and isinstance(document.get("requests"), list)
    )


def spec_service(document: dict[str, Any], default: str = "") -> str:
    """The service name (``service``, REQUIRED by §1)."""
    service = document.get("service")
    return str(service) if service else default


def spec_registration_hash(document: dict[str, Any]) -> str | None:
    """The served ``registrationHash`` — what a collector compares heartbeats against."""
    value = document.get("registrationHash")
    return str(value) if value else None


def spec_topics(document: dict[str, Any]) -> list[dict[str, Any]]:
    """The topics the service **consumes** (``requests``), as descriptor topic entries."""
    schemas = _catalogue(document)
    return [
        _entry(
            str(request["topic"]),
            request.get("version"),
            requestSchema=_resolve(request.get("request"), schemas),
            responseSchema=_resolve(request.get("response"), schemas),
        )
        for request in _entries(document, "requests")
    ]


def spec_produces(document: dict[str, Any]) -> list[dict[str, Any]]:
    """The topics the service **produces** — ``events`` ∪ ``sends`` — as descriptor topic entries."""
    schemas = _catalogue(document)
    produced = [
        _entry(
            str(event["topic"]),
            event.get("version"),
            requestSchema=_resolve(event.get("message"), schemas),
        )
        for event in _entries(document, "events")
    ]
    produced.extend(
        _entry(
            str(send["topic"]),
            send.get("version"),
            requestSchema=_resolve(send.get("request"), schemas),
            responseSchema=_resolve(send.get("response"), schemas),
        )
        for send in _entries(document, "sends")
    )
    return produced


def _entries(document: dict[str, Any], member: str) -> list[dict[str, Any]]:
    entries = document.get(member)
    if not isinstance(entries, list):
        return []
    return [
        entry
        for entry in entries
        if isinstance(entry, dict)
        and entry.get("topic")
        and not is_reserved_topic(str(entry["topic"]))
    ]


def _catalogue(document: dict[str, Any]) -> dict[str, Any]:
    components = document.get("components")
    schemas = components.get("schemas") if isinstance(components, dict) else None
    return schemas if isinstance(schemas, dict) else {}


def _entry(topic: str, version: Any, **schemas: dict[str, Any]) -> dict[str, Any]:
    entry: dict[str, Any] = {"id": topic}
    if version:  # an unversioned entry omits version (§2)
        entry["version"] = str(version)
    for name, schema in schemas.items():
        if schema:
            entry[name] = schema
    return entry


def _resolve(schema: Any, schemas: dict[str, Any], seen: tuple[str, ...] = ()) -> dict[str, Any]:
    """A schema object with its ``$ref`` s replaced by the catalogue entries they name.

    A reference already being expanded on the current path is cut with ``{}`` — the hash
    projection's cycle cut (§6.2 step 3) — so a recursive type never leaves a dangling reference
    into a catalogue the collector does not keep.
    """
    if not isinstance(schema, dict):
        return {}
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith(_SCHEMA_REF_PREFIX):
        name = ref[len(_SCHEMA_REF_PREFIX) :]
        if name in seen or name not in schemas:
            return {}
        return _resolve(schemas[name], schemas, (*seen, name))
    return {key: _resolve_value(value, schemas, seen) for key, value in schema.items()}


def _resolve_value(value: Any, schemas: dict[str, Any], seen: tuple[str, ...]) -> Any:
    if isinstance(value, dict):
        return _resolve(value, schemas, seen)
    if isinstance(value, list):
        return [_resolve_value(item, schemas, seen) for item in value]
    return value
