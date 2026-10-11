"""Deriving a Contract Document's schemas from Python types (contract-document.md §4).

The Contract Document's schemas are OpenAPI 3.0 Schema Objects (``schemaDialect: "openapi-3.0"``)
describing each payload's **marshaled JSON form** — what :func:`benzene.core.to_jsonable` writes.
The mapping (§4.2), read through Python's type system:

=================================  ================================================================
Python type                        Schema
=================================  ================================================================
``str``                            ``{"type": "string"}``
``bytes``                          ``{"type": "string", "format": "byte"}``        (base64)
``bool``                           ``{"type": "boolean"}``
``int``                            ``{"type": "integer"}``
``float``                          ``{"type": "number", "format": "double"}``
``datetime``                       ``{"type": "string", "format": "date-time"}``   (RFC 3339)
a ``str``/``int`` ``Enum``         hoisted: ``{"type": "string", "enum": [values]}``
``list[T]``/``tuple``/``set``      ``{"type": "array", "items": T}``
``dict[str, T]``                   ``{"type": "object", "additionalProperties": T}``
a ``@dataclass``                   hoisted: ``{"type": "object", "properties": …, "required": …}``
``T | None`` (T inline)            T's schema plus ``"nullable": true``
``T | None`` (T hoisted)           ``{"type": <T's type>, "nullable": true, "allOf": [{"$ref": …}]}``
anything else                      ``{}``  (``Any``, a wider union, a plain ``Enum``, a pydantic model)
=================================  ================================================================

Properties are the dataclass fields, camelCased exactly as the marshaler writes them; inherited
fields are flattened as ``dataclasses.fields`` flattens them. Every field is ``required``: the
marshaler (:func:`~benzene.core.to_jsonable`) always emits every dataclass field, and §4.2 makes a
property the marshaler always emits ``required`` even when the type declares a default for it —
``required`` describes the payload as written, not what a sender may leave out. Records carry no
``additionalProperties``. ``format`` is emitted where it is known; it is
stripped before hashing (§6.2).

**Components and names (§4.1).** Every dataclass and serializable enum reachable from an entry is
hoisted into ``components.schemas`` and referenced by ``$ref``; a recursive type refers to itself.
A component's name is the type's declared schema name (:func:`schema_name`), else its simple
``__name__``; a generic instance puts its type arguments first (``Page[Order]`` → ``OrderPage``);
characters outside ``[A-Za-z0-9._-]`` are dropped. Two types that claim one name with different
schemas fail derivation (:class:`ComponentNameCollisionError`) — never a suffix, never first-wins —
so names do not depend on registration order. Two types with identical schemas share the entry.

**Known gap (Q11, deferred).** A pydantic ``BaseModel`` payload derives to ``{}``: this module reads
dataclasses, and a pydantic-typed handler therefore publishes an unconstrained schema for it.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import re
import types
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeVar, Union, get_args, get_origin, get_type_hints

from .contract_hash import SCHEMA_REF_PREFIX, normalize_schema
from .mapping import to_camel

Schema = dict[str, Any]

_T = TypeVar("_T")

#: The attribute :func:`schema_name` sets. Read from the class's own ``__dict__`` only, so a subclass
#: does not inherit its parent's declared name.
_SCHEMA_NAME_ATTR = "__benzene_schema_name__"

_UNION_ORIGINS = (Union, types.UnionType)

_PRIMITIVES: dict[type, Schema] = {
    bool: {"type": "boolean"},  # before int — bool is a subclass of int
    int: {"type": "integer"},
    float: {"type": "number", "format": "double"},
    str: {"type": "string"},
    bytes: {"type": "string", "format": "byte"},
    datetime.datetime: {"type": "string", "format": "date-time"},
}

_INVALID_NAME_CHARS = re.compile(r"[^A-Za-z0-9._-]")

#: Internal ``$ref`` targets are placeholder tokens until names are settled; never on the wire.
_TOKEN_MARK = "\x00"


def schema_name(name: str) -> Callable[[_T], _T]:
    """Declare a payload type's component name (contract-document §4.1 rule 5)::

        @schema_name("OrderItem")
        @dataclass
        class Item: ...

    The remedy for a name collision: two different types that would both be called ``Item`` fail
    derivation until one of them declares another name.
    """
    if not isinstance(name, str) or not _valid_name(name):
        raise ValueError(
            f"schema_name({name!r}): a component name needs at least one of A-Z a-z 0-9 . _ -"
        )

    def decorate(cls: _T) -> _T:
        setattr(cls, _SCHEMA_NAME_ATTR, name)
        return cls

    return decorate


def declared_schema_name(py_type: Any) -> str | None:
    """The name declared on ``py_type`` with :func:`schema_name`, if any (not inherited)."""
    if isinstance(py_type, type):
        declared = vars(py_type).get(_SCHEMA_NAME_ATTR)
        return declared if isinstance(declared, str) else None
    return None


class ComponentNameCollisionError(ValueError):
    """Two different types claim one component name (contract-document §4.1 rule 4).

    Raised while the document is derived — at the composition root, before any message is handled.
    """

    kind = "component-name-collision"

    def __init__(self, name: str, types: tuple[str, str], topics: tuple[str, ...]) -> None:
        self.name = name
        self.types = types
        self.topics = topics
        super().__init__(
            f"Component name {name!r} is claimed by {types[0]} and {types[1]}, whose schemas "
            f"differ (used by topics {', '.join(topics)}). Declare a distinct schema name on one of "
            f'them with @schema_name("...") (contract-document §4.1).'
        )


@dataclass
class _Component:
    key: Any
    name: str
    token: str
    qualified: str
    schema: Schema = field(default_factory=dict)
    refs: set[Any] = field(default_factory=set)
    topics: set[str] = field(default_factory=set)


class SchemaCatalogue:
    """Derives entry schemas, hoisting named types into one ``components.schemas``.

    Call :meth:`schema` once per entry schema (naming the entry's topic), then :meth:`finish`, which
    settles the component names — raising :class:`ComponentNameCollisionError` on a collision — and
    returns ``(components, rename)``: the catalogue, and the function that turns every schema
    :meth:`schema` returned into its final ``$ref`` form.
    """

    def __init__(self) -> None:
        self._components: dict[Any, _Component] = {}
        self._entry_refs: dict[str, set[Any]] = {}

    def schema(self, py_type: Any, *, topic: str) -> Schema:
        refs = self._entry_refs.setdefault(topic, set())
        return self._derive(py_type, refs, {})

    def finish(self) -> tuple[dict[str, Schema], Callable[[Any], Any]]:
        self._propagate_topics()
        by_token = {c.token: c.schema for c in self._components.values()}
        groups: dict[str, list[_Component]] = {}
        for component in self._components.values():
            groups.setdefault(component.name, []).append(component)

        final: dict[str, str] = {}
        schemas: dict[str, Schema] = {}
        for name in sorted(groups):
            members = sorted(groups[name], key=lambda c: c.qualified)
            first = members[0]
            reference = normalize_schema(first.schema, by_token)
            for other in members[1:]:
                if normalize_schema(other.schema, by_token) != reference:
                    raise ComponentNameCollisionError(
                        name,
                        (first.qualified, other.qualified),
                        tuple(sorted(first.topics | other.topics)),
                    )
            for member in members:
                final[member.token] = name
            schemas[name] = first.schema

        def rename(value: Any) -> Any:
            if isinstance(value, dict):
                ref = value.get("$ref")
                if isinstance(ref, str) and ref.startswith(SCHEMA_REF_PREFIX + _TOKEN_MARK):
                    token = ref[len(SCHEMA_REF_PREFIX) :]
                    return {**value, "$ref": SCHEMA_REF_PREFIX + final[token]}
                return {key: rename(item) for key, item in value.items()}
            if isinstance(value, list):
                return [rename(item) for item in value]
            return value

        return {name: rename(schema) for name, schema in schemas.items()}, rename

    # --- derivation --------------------------------------------------------------------------

    def _derive(self, py_type: Any, refs: set[Any], bindings: Mapping[Any, Any]) -> Schema:
        if isinstance(py_type, TypeVar):
            py_type = bindings.get(py_type, Any)
        if py_type is None or py_type is type(None) or py_type is Any:
            return {}

        origin = get_origin(py_type)
        if origin in _UNION_ORIGINS:
            args = [a for a in get_args(py_type) if a is not type(None)]
            if len(args) != 1:
                return {}  # a genuine multi-type union has no single schema in this subset
            inner = self._derive(args[0], refs, bindings)
            if len(args) == len(get_args(py_type)):
                return inner
            return _nullable(inner, self._ref_type(args[0], bindings))

        if origin in (list, tuple, set, frozenset):
            item_args = [a for a in get_args(py_type) if a is not Ellipsis]
            item = self._derive(item_args[0], refs, bindings) if item_args else {}
            return {"type": "array", "items": item}

        if origin is dict:
            value_args = get_args(py_type)
            value = self._derive(value_args[1], refs, bindings) if len(value_args) == 2 else {}
            return {"type": "object", "additionalProperties": value}

        for primitive, schema in _PRIMITIVES.items():
            if py_type is primitive:
                return dict(schema)

        if _is_named(py_type):
            component = self._component(py_type, bindings)
            refs.add(component.key)
            return {"$ref": SCHEMA_REF_PREFIX + component.token}

        return {}

    def _ref_type(self, py_type: Any, bindings: Mapping[Any, Any]) -> str | None:
        """The JSON type of a hoisted type, for its nullable-reference wrapper; ``None`` if inline."""
        if isinstance(py_type, TypeVar):
            py_type = bindings.get(py_type, Any)
        if not _is_named(py_type):
            return None
        definition = get_origin(py_type) or py_type
        if isinstance(definition, type) and issubclass(definition, enum.Enum):
            return "string" if issubclass(definition, str) else "integer"
        return "object"

    def _component(self, py_type: Any, bindings: Mapping[Any, Any]) -> _Component:
        key = _resolve_alias(py_type, bindings)
        component = self._components.get(key)
        if component is not None:
            return component
        component = _Component(
            key=key,
            name=_component_name(key),
            token=f"{_TOKEN_MARK}{len(self._components)}",
            qualified=_qualified(key),
        )
        self._components[key] = component  # before deriving: a recursive type finds itself
        definition = get_origin(key) or key
        if issubclass(definition, enum.Enum):
            component.schema = _enum_schema(definition)
        else:
            component.schema = self._record(definition, get_args(key), component.refs)
        return component

    def _record(self, cls: type, args: tuple[Any, ...], refs: set[Any]) -> Schema:
        # A bare generic (no arguments) leaves its parameters unbound: they derive as Any, {}.
        bindings = dict(zip(getattr(cls, "__parameters__", ()), args, strict=False))
        try:
            hints = get_type_hints(cls)
        except Exception:
            # An unresolvable forward reference must not crash derivation; those fields derive
            # to the open schema, as an unknown type does.
            hints = {}
        properties: Schema = {}
        required: list[str] = []
        for f in dataclasses.fields(cls):
            name = to_camel(f.name)
            properties[name] = self._derive(hints.get(f.name, Any), refs, bindings)
            # to_jsonable writes every field, defaulted or not, so every field is always emitted.
            required.append(name)
        schema: Schema = {"type": "object", "properties": properties}
        if required:
            schema["required"] = required
        return schema

    def _propagate_topics(self) -> None:
        for topic, refs in self._entry_refs.items():
            pending = list(refs)
            seen: set[Any] = set()
            while pending:
                key = pending.pop()
                if key in seen:
                    continue
                seen.add(key)
                component = self._components[key]
                component.topics.add(topic)
                pending.extend(component.refs)


def _is_named(py_type: Any) -> bool:
    """A dataclass (or a generic dataclass instance) or a serializable enum: hoisted, named."""
    definition = get_origin(py_type) or py_type
    if not isinstance(definition, type):
        return False
    if issubclass(definition, enum.Enum):
        return issubclass(definition, (str, int))  # a plain Enum does not marshal: {} instead
    return dataclasses.is_dataclass(definition)


def _enum_schema(cls: type[enum.Enum]) -> Schema:
    values = [member.value for member in cls]
    if issubclass(cls, str):
        return {"type": "string", "enum": values}
    return {"type": "integer", "enum": values}


def _resolve_alias(py_type: Any, bindings: Mapping[Any, Any]) -> Any:
    """``Page[T]`` inside a generic definition, with ``T`` bound, becomes ``Page[Order]``."""
    args = get_args(py_type)
    if not args or not bindings:
        return py_type
    resolved = tuple(bindings.get(a, a) if isinstance(a, TypeVar) else a for a in args)
    origin = get_origin(py_type)
    return origin[resolved] if len(resolved) > 1 else origin[resolved[0]]


def _component_name(py_type: Any) -> str:
    definition = get_origin(py_type) or py_type
    base = declared_schema_name(definition) or str(getattr(definition, "__name__", "Schema"))
    prefix = "".join(_argument_name(arg) for arg in get_args(py_type))
    return _valid_name(prefix + base) or "Schema"


def _argument_name(arg: Any) -> str:
    if _is_named(arg):
        return _component_name(arg)
    origin = get_origin(arg)
    if origin is not None:
        inner = "".join(_argument_name(a) for a in get_args(arg))
        return inner + _capitalise(getattr(origin, "__name__", ""))
    return _capitalise(getattr(arg, "__name__", ""))


def _capitalise(name: str) -> str:
    return name[:1].upper() + name[1:]


def _qualified(py_type: Any) -> str:
    if isinstance(py_type, type):
        return f"{py_type.__module__}.{py_type.__qualname__}"
    return repr(py_type)


def _valid_name(name: str) -> str:
    return _INVALID_NAME_CHARS.sub("", name)


def _nullable(schema: Schema, ref_type: str | None) -> Schema:
    """§4.2's two nullable forms: a hoisted type is wrapped; anything else gains ``nullable``."""
    if not schema:
        return schema  # the open schema already admits null
    if ref_type is not None and "$ref" in schema:
        return {"type": ref_type, "nullable": True, "allOf": [schema]}
    return {**schema, "nullable": True}

