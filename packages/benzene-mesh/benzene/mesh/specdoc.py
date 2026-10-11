"""Reading a Contract Document — whatever port produced it — for mesh tooling.

A service describes itself once, with the Contract Document (contract-document.md):
``{schemaDialect, service, contractHash, registrationHash, requests[], events[], sends[],
components}``. ``/benzene/spec`` and the ``benzene:spec`` topic serve it, and the
``benzene:mesh:register`` body *is* it (mesh.md §2), so the collector, the poller and the probe all
read the same shape through the functions here rather than three copies drifting apart.

A collector accepts a reduced document (mesh.md §2, §6): only ``service`` is required, and a member
that is absent — or named in ``degraded`` — is **unknown**, not empty (:func:`known_members`).

Schemas are dereferenced against ``components.schemas`` (:func:`resolve_schema`): a reference into a
catalogue the collector does not keep would compare as "the shape changed" the moment a producer
renamed a class. A reference cycle is cut with ``{}``, as the hash projection cuts it
(contract-document §6.2).

``benzene:`` entries are ignored: a producer must not list them, and a consumer ignores any it meets
(contract-document §1).
"""

from __future__ import annotations

from typing import Any

#: The reserved-topic prefix (contract-document.md §1).
RESERVED_TOPIC_PREFIX = "benzene:"

#: The dialect this reader understands (contract-document.md §1).
SCHEMA_DIALECT = "openapi-3.0"

#: The three entry members of a document (contract-document.md §1).
MEMBERS = ("requests", "events", "sends")

#: The only ``$ref`` form a Contract Document uses (contract-document.md §1).
_SCHEMA_REF_PREFIX = "#/components/schemas/"


def is_reserved_topic(topic: str) -> bool:
    return topic.startswith(RESERVED_TOPIC_PREFIX)


def is_contract_document(document: dict[str, Any]) -> bool:
    """Whether ``document`` is a complete Contract Document in a dialect this reader knows.

    A consumer MUST reject a document whose ``schemaDialect`` it does not know (§1), so a document
    without the field — the pre-``spec-0.3.0`` ``openapi``/``info`` shape — is not one. (A collector
    is more lenient with a register body: see :func:`known_members`.)
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


def spec_service_version(document: dict[str, Any]) -> str | None:
    """The ``serviceVersion``, or ``None`` when the document declares none."""
    value = document.get("serviceVersion")
    return str(value) if value else None


def spec_registration_hash(document: dict[str, Any]) -> str | None:
    """The served ``registrationHash`` — what a collector compares heartbeats against."""
    value = document.get("registrationHash")
    return str(value) if value else None


def known_members(document: dict[str, Any]) -> frozenset[str]:
    """The members of ``requests``/``events``/``sends`` the document actually asserts.

    A member is known when it is present as a list and not named in ``degraded``. An unknown member
    contributes no edges, and its emptiness asserts nothing (contract-document §1, mesh.md §2).
    """
    degraded = document.get("degraded")
    undetermined = {str(m) for m in degraded} if isinstance(degraded, list) else set()
    return frozenset(
        member
        for member in MEMBERS
        if isinstance(document.get(member), list) and member not in undetermined
    )


def spec_entries(document: dict[str, Any], member: str) -> list[dict[str, Any]]:
    """The well-formed, non-reserved entries of one member (``requests``, ``events`` or ``sends``)."""
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


def spec_topic_ids(document: dict[str, Any], *members: str) -> list[str]:
    """The topics named by ``members``' entries, first occurrence order, reserved topics dropped."""
    topics: dict[str, None] = {}
    for member in members:
        for entry in spec_entries(document, member):
            topics.setdefault(str(entry["topic"]), None)
    return list(topics)


def schema_catalogue(document: dict[str, Any]) -> dict[str, Any]:
    """The document's ``components.schemas`` (empty when absent or malformed)."""
    components = document.get("components")
    schemas = components.get("schemas") if isinstance(components, dict) else None
    return schemas if isinstance(schemas, dict) else {}


def resolve_schema(
    schema: Any, schemas: dict[str, Any], seen: tuple[str, ...] = ()
) -> dict[str, Any]:
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
        return resolve_schema(schemas[name], schemas, (*seen, name))
    return {key: _resolve_value(value, schemas, seen) for key, value in schema.items()}


def _resolve_value(value: Any, schemas: dict[str, Any], seen: tuple[str, ...]) -> Any:
    if isinstance(value, dict):
        return resolve_schema(value, schemas, seen)
    if isinstance(value, list):
        return [_resolve_value(item, schemas, seen) for item in value]
    return value
