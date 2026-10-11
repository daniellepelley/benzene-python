"""The two Contract Document hashes (contract-document.md §6).

::

    hash(P)          = "sha256:" + lowercase-hex(sha256(JCS(P)))
    contractHash     = hash({"requests": …, "events": …})
    registrationHash = hash({"requests": …, "events": …, "sends": …})

``P`` is a **projection** of the document (§6.2): only the allow-listed members and entry fields;
``benzene:`` entries dropped; every ``$ref`` dereferenced (a reference already being expanded on the
current path becomes ``{}``, as does a nullable wrapper around such a reference); documentation
keywords, ``format`` and ``discriminator.mapping`` stripped at every schema position; ``required`` sorted (dropped when empty),
an empty ``properties`` dropped; entries sorted by topic then version; ``httpMappings`` methods
upper-cased and sorted. Component names, hoisting depth, entry order and schema documentation
therefore never move either hash, so two ports describing the same wire shapes agree.

``JCS`` is RFC 8785 via the `rfc8785 <https://pypi.org/project/rfc8785/>`_ package — §6.3 asks for a
canonicaliser with no corners left for four implementations to disagree on, which is why
``benzene-core`` takes this one dependency rather than hand-writing one.

This lives in ``benzene-core``, not the code generator: a service computes both hashes for the
document it serves, a generated client embeds the one it was built against, and a drift check
compares the two — the served value, never a private recomputation over a different projection.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from typing import Any

import rfc8785

#: The only ``$ref`` form a Contract Document may use (contract-document.md §1).
SCHEMA_REF_PREFIX = "#/components/schemas/"

#: The reserved-topic prefix: entries under it are outside every projection (§6.2 step 2).
RESERVED_TOPIC_PREFIX = "benzene:"

_HASH_PREFIX = "sha256:"

#: §6.2 step 2: the members each entry keeps. Everything else (``example`` above all) is dropped.
_ENTRY_FIELDS: dict[str, tuple[str, ...]] = {
    "requests": ("topic", "version", "httpMappings", "request", "response"),
    "events": ("topic", "version", "message"),
    "sends": ("topic", "version", "request", "response"),
}

#: The entry fields that hold a Schema Object.
_SCHEMA_FIELDS = frozenset({"request", "response", "message"})

#: §6.2 step 4: documentation keywords (and ``format``) removed at every schema position.
_ANNOTATIONS = frozenset(
    {
        "title",
        "description",
        "default",
        "examples",
        "example",
        "deprecated",
        "readOnly",
        "writeOnly",
        "$comment",
        "$id",
        "$schema",
        "format",
    }
)


def contract_hash(document: Mapping[str, Any]) -> str:
    """``contractHash``: what a client of the service depends on — ``requests`` and ``events``."""
    return _hash(projection(document, ("requests", "events")))


def registration_hash(document: Mapping[str, Any]) -> str:
    """``registrationHash``: what a mesh collector's graph depends on — ``sends`` too."""
    return _hash(projection(document, ("requests", "events", "sends")))


def projection(document: Mapping[str, Any], members: Iterable[str]) -> dict[str, Any]:
    """§6.2 steps 1–5: the projection ``P`` of ``document`` over ``members``.

    Every named member is present in ``P``, empty or not, so ``registrationHash`` never coincides with
    ``contractHash`` by accident.
    """
    components = document.get("components")
    schemas = components.get("schemas") if isinstance(components, Mapping) else None
    catalogue: Mapping[str, Any] = schemas if isinstance(schemas, Mapping) else {}
    projected: dict[str, Any] = {}
    for member in members:
        fields = _ENTRY_FIELDS[member]
        entries = [
            _project_entry(entry, fields, catalogue)
            for entry in document.get(member) or ()
            if isinstance(entry, Mapping)
            and not str(entry.get("topic", "")).startswith(RESERVED_TOPIC_PREFIX)
        ]
        # Topic, then version with an absent version first; UTF-16 code-unit comparison.
        entries.sort(key=lambda e: (_utf16(e["topic"]), "version" in e, _utf16(e.get("version", ""))))
        projected[member] = entries
    return projected


def normalize_schema(schema: Any, catalogue: Mapping[str, Any]) -> Any:
    """§6.2 steps 3–4 for one schema: dereferenced, annotations stripped, ``required`` sorted.

    Also how a producer compares two candidate schemas for one component name (§4.1 rule 4).
    """
    return _normalize(schema, catalogue, frozenset())


def _project_entry(
    entry: Mapping[str, Any], fields: tuple[str, ...], catalogue: Mapping[str, Any]
) -> dict[str, Any]:
    projected: dict[str, Any] = {}
    for name in fields:
        if name not in entry:
            continue
        value = entry[name]
        if name in _SCHEMA_FIELDS:
            projected[name] = _normalize(value, catalogue, frozenset())
        elif name == "httpMappings":
            mappings = [
                {"method": str(m.get("method", "")).upper(), "path": m.get("path", "")}
                for m in value or ()
                if isinstance(m, Mapping)
            ]
            mappings.sort(key=lambda m: (_utf16(m["method"]), _utf16(m["path"])))
            projected[name] = mappings
        else:
            projected[name] = value
    return projected


def _normalize(schema: Any, catalogue: Mapping[str, Any], expanding: frozenset[str]) -> Any:
    if not isinstance(schema, Mapping):
        return schema
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith(SCHEMA_REF_PREFIX):
        name = ref[len(SCHEMA_REF_PREFIX) :]
        if name in expanding:
            return {}  # a cycle: the reference already being expanded on this path is cut
        return _normalize(catalogue.get(name, {}), catalogue, expanding | {name})

    out: dict[str, Any] = {}
    for key, value in schema.items():
        if key in _ANNOTATIONS or key.startswith("x-"):
            continue
        if key == "properties" and isinstance(value, Mapping):
            out[key] = {
                prop: _normalize(sub, catalogue, expanding) for prop, sub in value.items()
            }
        elif key in ("items", "additionalProperties", "not") and isinstance(value, Mapping):
            out[key] = _normalize(value, catalogue, expanding)
        elif key in ("allOf", "anyOf", "oneOf") and isinstance(value, list):
            out[key] = [_normalize(member, catalogue, expanding) for member in value]
        elif key == "required" and isinstance(value, list):
            if value:
                out[key] = sorted(value, key=_utf16)
        elif key == "discriminator" and isinstance(value, Mapping):
            # ``mapping`` holds component names, which are not contract (§4.1).
            out[key] = {k: v for k, v in value.items() if k != "mapping"}
        else:
            out[key] = value
    if out.get("properties") == {}:
        del out["properties"]
    if _is_cut_nullable_reference(schema, out, expanding):
        return {}
    return out


def _is_cut_nullable_reference(
    original: Mapping[str, Any], normalized: Mapping[str, Any], expanding: frozenset[str]
) -> bool:
    """A nullable-reference wrapper (§4.2) whose one reference was cut at a cycle normalizes to
    ``{}`` as a whole — so a recursive ``T | None`` field hashes like a recursive ``T`` field."""
    members = original.get("allOf")
    if not (isinstance(members, list) and len(members) == 1 and isinstance(members[0], Mapping)):
        return False
    ref = members[0].get("$ref")
    if not (isinstance(ref, str) and ref.startswith(SCHEMA_REF_PREFIX)):
        return False
    return (
        ref[len(SCHEMA_REF_PREFIX) :] in expanding
        and normalized.get("nullable") is True
        and set(normalized) <= {"type", "nullable", "allOf"}
    )


def _utf16(value: Any) -> bytes:
    """A sort key comparing strings by UTF-16 code units, as JCS sorts member names (§6.2)."""
    return str(value).encode("utf-16-be")


def _hash(projected: dict[str, Any]) -> str:
    return _HASH_PREFIX + hashlib.sha256(rfc8785.dumps(projected)).hexdigest()
