"""Topic-scoped schema closure (contract-document.md §5.3).

The reachability walk over a topic's ``request``/``response`` schema objects: ``$ref``, ``items``,
``additionalProperties`` (when itself a schema), ``properties``, and ``allOf``/``anyOf``/``oneOf``,
cycle-safe via the reached-set (an already-reached ``$ref`` name is not walked again). Shared by the
topic-client generator (to narrow ``components.schemas`` to one topic's reachable set) and the
conformance test against ``schemaClosureCases``.
"""

from __future__ import annotations

from typing import Any

from .document import SCHEMA_REF_PREFIX


def _ref_name(schema: dict[str, Any]) -> str | None:
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith(SCHEMA_REF_PREFIX):
        return ref[len(SCHEMA_REF_PREFIX) :]
    return None


def reachable_names(catalogue: dict[str, Any], *roots: Any) -> set[str]:
    """The set of ``catalogue`` schema names reachable from ``roots``, per §5.3's walk."""
    reached: set[str] = set()

    def walk(schema: Any) -> None:
        if not isinstance(schema, dict):
            return

        name = _ref_name(schema)
        # reached.add's return value (True the first time) is what makes a reference cycle
        # terminate: an already-reached name is not walked again.
        if name is not None and name in catalogue and name not in reached:
            reached.add(name)
            walk(catalogue[name])

        walk(schema.get("items"))

        additional = schema.get("additionalProperties")
        if isinstance(additional, dict):  # a bare `true`/`false` has nothing to walk
            walk(additional)

        properties = schema.get("properties")
        if isinstance(properties, dict):
            for value in properties.values():
                walk(value)

        for key in ("allOf", "anyOf", "oneOf"):
            members = schema.get(key)
            if isinstance(members, list):
                for member in members:
                    walk(member)

    for root in roots:
        walk(root)

    return reached


def reachable_schemas(catalogue: dict[str, Any], *roots: Any) -> dict[str, Any]:
    """``catalogue`` narrowed to exactly the entries reachable from ``roots``, keyed the same."""
    reached = reachable_names(catalogue, *roots)
    return {name: schema for name, schema in catalogue.items() if name in reached}
