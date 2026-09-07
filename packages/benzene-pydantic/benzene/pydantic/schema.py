"""Payload-schema derivation for pydantic models — the provider ``benzene.core`` asks first.

``benzene.core.json_schema`` projects a handler's declared type into the JSON Schema 2020-12 subset
that four published documents embed: the Contract Document (``/benzene/spec``), this port's native
``ServiceSpec`` (``?type=native``), the mesh ``ServiceDescriptor``, and the OpenAPI document. Its
table covers primitives, containers and dataclasses, and everything else — a pydantic ``BaseModel``
included — fell through to the open schema ``{}``. A service written the way this package
documents therefore published *no* contract at all: a client generator reading it emits an untyped
client, and the mesh's per-topic schema-change detection compares two empty schemas and sees no
drift, forever.

pydantic already knows the whole answer (``model_json_schema()`` emits 2020-12 with constraints,
formats, enums and discriminated unions), and ``benzene-core`` must not import pydantic — it is an
optional adoption choice, which is the entire reason this package exists. So core exposes a
provider seam (:func:`benzene.core.register_schema_provider`) and this module fills it;
:mod:`benzene.pydantic` registers :func:`pydantic_schema` on import, so installing and importing the
adapter is the opt-in and a core-only service is unaffected.

**Two things the raw pydantic document cannot be published as.**

* ``$defs`` / ``$ref``. pydantic hoists every nested model into ``$defs`` and points at it with
  ``#/$defs/<name>``. contract-document.md §4 allows exactly one ``$ref`` form anywhere in the
  document — ``#/components/schemas/<name>`` — and the mesh descriptor embeds each topic schema
  standalone and then *hashes* it, so a pointer into a sibling section it does not carry is not a
  contract, it is a dangling reference. Every ``$ref`` is therefore resolved in place and ``$defs``
  dropped, leaving the self-contained schemas the rest of the port emits. A cycle is cut with the
  open schema ``{}`` — the identical rule ``schema.py`` already applies to a recursive dataclass.
* ``title``. pydantic synthesises one for every model and every property from the identifier
  (``line_one`` becomes ``"Lineone"``). It is annotation, never constraint; the port's dataclass
  schemas carry none; and it would sit in the hashed contract as noise that reads like meaning.
  ``description`` — the member that carries prose an author actually wrote — is kept, as is every
  constraint keyword (``minLength``, ``pattern``, ``minimum``, ``enum``, ``format``, ``default``, …),
  because those *are* the contract.
"""

from __future__ import annotations

from typing import Any

from benzene.core import Schema

from pydantic import BaseModel

#: The prefix of the internal pointers ``model_json_schema`` emits for its own ``$defs`` entries.
_DEFS_PREFIX = "#/$defs/"


def pydantic_schema(py_type: Any) -> Schema | None:
    """Derive a Benzene payload schema from a pydantic ``BaseModel`` subclass, else ``None``.

    ``None`` means "not mine" and lets ``benzene.core`` carry on down its own table — so a
    dataclass, a primitive or a ``list[...]`` is derived exactly as before, and a ``list[Model]``
    reaches this provider again through core's element recursion.

    ``by_alias=True`` is passed explicitly rather than relied on as pydantic's default: the wire
    mapper serialises a model with ``model_dump(by_alias=True)``, so a schema in field names rather
    than alias names would describe a body the service never sends.
    """
    if not (isinstance(py_type, type) and issubclass(py_type, BaseModel)):
        return None
    try:
        document = py_type.model_json_schema(by_alias=True, ref_template=_DEFS_PREFIX + "{model}")
    except Exception:
        # A model pydantic cannot render (a field type with no JSON Schema, a broken forward ref) is
        # the open schema's honest case — and a descriptor build must never crash on one.
        return {}
    return inline_defs(document)


def inline_defs(document: dict[str, Any]) -> Schema:
    """Resolve every ``$ref`` against the document's own ``$defs`` and drop the ``$defs`` section.

    Exposed (and tested) in its own right because it is the half of the derivation that has to be
    exactly right: a leftover ``$ref`` reaches the wire as a pointer into a section that no longer
    exists, in a document that is then hashed as a service's contract.
    """
    defs = document.get("$defs")
    body = {key: value for key, value in document.items() if key != "$defs"}
    return _resolve(body, defs if isinstance(defs, dict) else {}, ())


def _resolve(node: Any, defs: dict[str, Any], seen: tuple[str, ...]) -> Any:
    """``node`` with its refs inlined. ``seen`` is the chain of definitions currently being expanded."""
    if isinstance(node, list):
        return [_resolve(item, defs, seen) for item in node]
    if not isinstance(node, dict):
        return node

    ref = node.get("$ref")
    if isinstance(ref, str):
        name = ref[len(_DEFS_PREFIX) :] if ref.startswith(_DEFS_PREFIX) else None
        if name is None or name not in defs or name in seen:
            # Unresolvable, or the cycle closing: the open schema, never a dangling pointer.
            target: Schema = {}
        else:
            target = _resolve(defs[name], defs, (*seen, name))
        # A sibling of `$ref` (JSON Schema 2020-12 allows them, and pydantic writes `description`
        # and `default` there) is the more specific statement, so it wins over the target's.
        siblings = {k: _resolve(v, defs, seen) for k, v in node.items() if k != "$ref"}
        return _without_title({**target, **siblings})

    return _without_title({key: _resolve(value, defs, seen) for key, value in node.items()})


def _without_title(schema: dict[str, Any]) -> Schema:
    """``schema`` minus pydantic's synthesised ``title`` (see this module's docstring)."""
    return {key: value for key, value in schema.items() if key != "title"}
