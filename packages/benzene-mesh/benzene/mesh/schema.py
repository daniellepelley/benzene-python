"""Payload-schema derivation — a Python type → the JSON Schema 2020-12 subset (mesh.md section 2).

A service's descriptor projects each handler's declared request/response type into a small,
fixed subset of JSON Schema so the mesh can show — and other services can check — the exact shape
each topic accepts and returns. The mapping is the spec's *schema derivation* table, read through
Python's type system:

======================  =========================================================
Python type             JSON Schema
======================  =========================================================
``str``                 ``{"type": "string"}``
``bool``                ``{"type": "boolean"}``      (checked before ``int``)
``int``                 ``{"type": "integer"}``
``float``               ``{"type": "number"}``
``datetime``            ``{"type": "string", "format": "date-time"}``   (RFC 3339)
``bytes``               ``{"type": "string"}``       (base64 on the wire)
``T | None``            ``T``'s schema with ``"null"`` added to its ``type``
``list[T]`` / ``tuple`` ``{"type": "array", "items": <T>}``
``dict[str, T]``        ``{"type": "object", "additionalProperties": <T>}``
a ``@dataclass``        ``{"type": "object", "properties": {...}, "required": [...]}``
anything else           ``{}``   (unknown/custom — matches everything)
======================  =========================================================

**Property names follow the wire naming policy** (``benzene.core.to_camel`` — dataclass fields are
emitted camelCase), so the schema describes what actually crosses the wire. **A field is
``required`` iff it has no default** (no ``default`` and no ``default_factory``) — the properties a
caller must supply. This is a declaration-time reading of the spec's "always emitted by the
marshaler": Python's marshaler happens to *emit* every field (defaulted or not), so ``required``
deliberately tracks the caller's obligation, not runtime emission — which is exactly what the
language-neutral conformance fixture pins (``errors`` carries a default and is *not* required).
Properties are listed in declaration order (determinism feeds the descriptor hash). Recursive types
cut the cycle with ``{}`` rather than emitting a ``$ref``.
"""

from __future__ import annotations

import dataclasses
import datetime
from typing import Any, Union, get_args, get_origin, get_type_hints

from benzene.core import to_camel

# JSON Schema is a plain ``dict[str, Any]`` here — small enough that a TypedDict would only add noise.
Schema = dict[str, Any]

_PRIMITIVES: dict[type, Schema] = {
    bool: {"type": "boolean"},  # before int — bool is a subclass of int
    int: {"type": "integer"},
    float: {"type": "number"},
    str: {"type": "string"},
    bytes: {"type": "string"},
    datetime.datetime: {"type": "string", "format": "date-time"},
}


def json_schema(py_type: Any) -> Schema:
    """Derive the JSON Schema (2020-12 subset) for a Python type. ``None`` → the open schema ``{}``."""
    return _schema(py_type, seen=())


def _schema(py_type: Any, seen: tuple[Any, ...]) -> Schema:
    if py_type is None or py_type is type(None) or py_type is Any:
        return {}

    origin = get_origin(py_type)

    # Optional / Union: fold None into a nullable type; a genuine multi-type union stays open ({}).
    if origin is Union:
        args = [a for a in get_args(py_type) if a is not type(None)]
        nullable = len(args) != len(get_args(py_type))
        if len(args) != 1:
            return {}  # unions we can't render as a single subset type
        inner = _schema(args[0], seen)
        return _as_nullable(inner) if nullable else inner

    if origin in (list, tuple, set, frozenset):
        item_args = get_args(py_type)
        item = _schema(item_args[0], seen) if item_args else {}
        return {"type": "array", "items": item}

    if origin is dict:
        value_args = get_args(py_type)
        value = _schema(value_args[1], seen) if len(value_args) == 2 else {}
        return {"type": "object", "additionalProperties": value}

    for primitive, schema in _PRIMITIVES.items():
        if py_type is primitive:
            return dict(schema)

    if dataclasses.is_dataclass(py_type) and isinstance(py_type, type):
        return _dataclass_schema(py_type, seen)

    return {}  # unknown/custom type — an open schema matches anything


def _dataclass_schema(cls: type, seen: tuple[Any, ...]) -> Schema:
    if cls in seen:
        return {}  # recursive type: cut the cycle with an open schema (no $ref)
    seen = (*seen, cls)

    try:
        hints = get_type_hints(cls)  # resolves string annotations (from __future__ import annotations)
    except Exception:
        # Unresolvable forward refs (e.g. a locally-scoped type) must not crash a descriptor build;
        # fall back to the raw (possibly string) annotations, which derive to the open schema {}.
        hints = {}
    properties: Schema = {}
    required: list[str] = []
    for f in dataclasses.fields(cls):
        name = to_camel(f.name)
        properties[name] = _schema(hints.get(f.name, f.type), seen)
        if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING:
            required.append(name)

    schema: Schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _as_nullable(schema: Schema) -> Schema:
    """Add ``"null"`` to a schema's ``type`` (mesh.md: nullable = original type + null)."""
    existing = schema.get("type")
    if existing is None:
        return schema  # open schema already matches null
    types = existing if isinstance(existing, list) else [existing]
    if "null" not in types:
        types = [*types, "null"]
    return {**schema, "type": types}
