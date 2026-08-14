"""OpenAPI 3.0 Schema Object -> Python type mapping and ``@dataclass`` source emission.

Emits plain stdlib ``@dataclass`` types (no pydantic — none exists anywhere in this port, and the
generated types are meant to interoperate with the existing ``benzene.core.mapping`` serialization
machinery, not bring in a validation library of their own).

**Deliberate divergence from the .NET reference
(`OpenApiSchemaCSharpTypeBuilder <https://github.com/daniellepelley/benzene-dotnet>`_):** the .NET
builder maps ``format: date-time`` -> ``DateTime?``, ``format: uuid`` -> ``Guid?``, and
``format: int64`` -> ``long`` — heuristics layered on top of the declared ``type``.
``benzene.core.mapping`` (``to_jsonable``/``to_request``, the serialization idiom every generated
dataclass is meant to plug into) has no ``datetime``/UUID/decimal awareness today: it would silently
fail to JSON-encode a ``datetime.datetime`` field. Rather than add unrequested serializer support as
a side effect of code generation, this builder honours only the schema's declared JSON ``type``
(``string``/``integer``/``number``/``boolean``/``array``/``object``) and ignores ``format`` — the
schema still governs the wire shape (a ``string`` is a ``string``, however it's formatted); this is
"no format heuristics beyond what the schema says" read as *the declared type*, not an invented
extra type per format. If ``benzene.core.mapping`` grows ``datetime``/UUID support, this builder can
follow.

**oneOf-with-shared-base (the .NET builder's polymorphism case):** the .NET builder types a oneOf
union member site as the members' shared ``allOf`` base class when discoverable, else ``object``.
Python has no compile-time discriminated union, so the honest equivalent is a real
``typing.Union[...]`` of the member dataclasses — this builder emits that (falling back to the
first common ``allOf`` base only when *every* member shares one, matching the shared-base case's
intent). A oneOf entry in ``components.schemas`` with no properties of its own (a pure union, like
the conformance fixture's ``D``) is emitted as a ``Union[...]`` **type alias**, not a dataclass — it
has no fields to hold. A discriminator's own property is not special-cased (no analog of
``[JsonPolymorphic]`` exists in this port); it is emitted as an ordinary string field, which is
sufficient for ``benzene.core.mapping`` to round-trip it, at the cost of not auto-selecting the
concrete member type on decode — deserializing a tagged union member back to the *right* dataclass
is left to the caller, exactly as ``to_request`` already leaves any type selection to whatever
static type it's given.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .document import SCHEMA_REF_PREFIX
from .naming import class_name, field_name


def _ref_name(schema: Any) -> str | None:
    if not isinstance(schema, dict):
        return None
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith(SCHEMA_REF_PREFIX):
        return ref[len(SCHEMA_REF_PREFIX) :]
    return None


def is_union_schema(schema: dict[str, Any]) -> bool:
    """A catalogue entry that is a bare ``oneOf`` with no properties of its own — a pure union."""
    return (
        isinstance(schema.get("oneOf"), list)
        and bool(schema["oneOf"])
        and not schema.get("properties")
        and "allOf" not in schema
    )


def get_type_name(schema: Any, catalogue: dict[str, Any] | None = None) -> str:
    """The Python type annotation for an OpenAPI 3.0 Schema Object (inline or ``$ref``)."""
    if schema is None or not isinstance(schema, dict) or not schema:
        return "Any"

    ref_name = _ref_name(schema)
    if ref_name is not None:
        return class_name(ref_name)

    nullable = bool(schema.get("nullable"))
    schema_type = schema.get("type")

    if isinstance(schema.get("oneOf"), list) and schema["oneOf"]:
        result = _oneof_type_name(schema["oneOf"], catalogue)
    elif schema_type == "array":
        result = f"list[{get_type_name(schema.get('items'), catalogue)}]"
    elif schema_type == "object" and isinstance(schema.get("additionalProperties"), dict):
        result = f"dict[str, {get_type_name(schema['additionalProperties'], catalogue)}]"
    elif schema_type == "object" and schema.get("additionalProperties") is True:
        result = "dict[str, Any]"
    elif schema_type == "integer":
        result = "int"
    elif schema_type == "number":
        result = "float"
    elif schema_type == "boolean":
        result = "bool"
    elif schema_type == "string":
        result = "str"
    elif schema_type == "object" or "properties" in schema:
        result = "dict[str, Any]"  # an inline, unnamed object — no generated class to reference
    else:
        result = "Any"

    if nullable and result != "Any":
        result = f"{result} | None"
    return result


def _oneof_type_name(members: list[Any], catalogue: dict[str, Any] | None) -> str:
    """A oneOf union member site: ``Union[...]`` of the member types (see module docstring)."""
    member_types = [get_type_name(m, catalogue) for m in members]
    unique = list(dict.fromkeys(member_types))
    if len(unique) == 1:
        return unique[0]
    return f"Union[{', '.join(unique)}]"


def uses_union(schema: dict[str, Any]) -> bool:
    return isinstance(schema.get("oneOf"), list) and len(schema["oneOf"]) > 1


@dataclass(frozen=True)
class Field:
    wire_name: str
    python_name: str
    type_annotation: str
    default_expr: str


def _default_for(type_annotation: str) -> str:
    if type_annotation.endswith("| None") or type_annotation == "Any":
        return "None"
    if type_annotation.startswith("list["):
        return "field(default_factory=list)"
    if type_annotation.startswith("dict["):
        return "field(default_factory=dict)"
    if type_annotation == "str":
        return '""'
    if type_annotation == "int":
        return "0"
    if type_annotation == "float":
        return "0.0"
    if type_annotation == "bool":
        return "False"
    if type_annotation.startswith("Union["):
        return "None"
    # A reference to another generated dataclass: default to None (typed optional) rather than
    # constructing an instance, so field ordering across allOf inheritance never has to worry about
    # a mutable-default trap or a required-before-optional MRO conflict — see module docstring's
    # "every field gets a default" rule (naming.py has the parallel identifier-safety rule).
    return "None"


def _own_properties(schema: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    """Own (non-inherited) ``properties`` of a schema, plus its ``allOf`` base ``$ref`` if any.

    allOf composition (contract-document.md doesn't mandate a shape, but the .NET reference and
    Swashbuckle both use this convention): a single ``$ref`` branch is the base type; inline
    branches carry the schema's own properties directly.
    """
    if "allOf" not in schema:
        return dict(schema.get("properties") or {}), None

    base_ref: str | None = None
    own: dict[str, Any] = {}
    for member in schema["allOf"]:
        if not isinstance(member, dict):
            continue
        ref_name = _ref_name(member)
        if ref_name is not None and base_ref is None:
            base_ref = ref_name
        else:
            own.update(member.get("properties") or {})
    own.update(schema.get("properties") or {})
    return own, base_ref


def base_ref_name(schema: dict[str, Any]) -> str | None:
    """The catalogue name of ``schema``'s ``allOf`` base, if it composes one (see :func:`_own_properties`)."""
    return _own_properties(schema)[1]


def dependency_names(schema: dict[str, Any]) -> list[str]:
    """Catalogue names that must be *defined* (emitted) before ``schema``'s own source.

    A ``@dataclass``'s field type annotations are never evaluated eagerly (``from __future__ import
    annotations`` makes them lazy strings), so they impose no ordering constraint. Two things *are*
    evaluated immediately at module-exec time, though: an ``allOf`` base class (real Python
    inheritance) and a union type alias's ``Union[...]`` expression (:func:`is_union_schema`) — both
    need their referenced names to already exist.
    """
    deps: list[str] = []
    base = base_ref_name(schema)
    if base:
        deps.append(base)
    if is_union_schema(schema):
        for member in schema.get("oneOf", []):
            name = _ref_name(member)
            if name:
                deps.append(name)
    return deps


def build_fields(schema: dict[str, Any], catalogue: dict[str, Any]) -> list[Field]:
    own_properties, _ = _own_properties(schema)
    fields: list[Field] = []
    seen_python_names: set[str] = set()
    for wire_name, prop_schema in own_properties.items():
        python_name = field_name(wire_name)
        if python_name in seen_python_names:
            continue  # two wire names collide after snake_case-folding; keep the first (declaration order)
        seen_python_names.add(python_name)
        type_annotation = get_type_name(prop_schema, catalogue)
        fields.append(
            Field(
                wire_name=wire_name,
                python_name=python_name,
                type_annotation=type_annotation,
                default_expr=_default_for(type_annotation),
            )
        )
    return fields


def build_schema_source(name: str, schema: dict[str, Any], catalogue: dict[str, Any]) -> str:
    """The Python source for one ``components.schemas`` entry: a ``@dataclass`` or a union alias."""
    cls = class_name(name)

    if is_union_schema(schema):
        members = [get_type_name(m, catalogue) for m in schema["oneOf"]]
        unique = list(dict.fromkeys(members))
        alias = unique[0] if len(unique) == 1 else f"Union[{', '.join(unique)}]"
        return f"{cls} = {alias}\n"

    _, base_ref = _own_properties(schema)
    base_class = class_name(base_ref) if base_ref else None
    fields = build_fields(schema, catalogue)

    lines: list[str] = []
    header = f"class {cls}({base_class}):" if base_class else f"class {cls}:"
    lines.append("@dataclass")
    lines.append(header)
    if not fields:
        lines.append("    pass")
    else:
        for f in fields:
            lines.append(f"    {f.python_name}: {f.type_annotation} = {f.default_expr}")
    return "\n".join(lines) + "\n"
