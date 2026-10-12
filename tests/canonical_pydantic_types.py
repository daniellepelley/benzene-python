"""The canonical contract-document payload types (conformance/README.md) as pydantic v2 models.

The same wire shapes as the dataclasses in :mod:`tests.canonical_handlers`, written the way a
pydantic-typed Benzene service writes them: snake_case fields with a camelCase ``alias_generator``,
which the marshaler's ``model_dump(by_alias=True)`` writes - so ``nullable_child`` is on the wire,
and in the derived schema, as ``nullableChild``. Every field is always dumped (no ``exclude``), so
every field is ``required``, as the README requires of the canonical types.

Importing this module needs pydantic; the conformance runner imports it only when pydantic is
installed, so the runner itself stays dependency-free.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class _Wire(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class GreetRequest(_Wire):
    name: str


class GreetResponse(_Wire):
    greeting: str


class Greeted(_Wire):
    greeting: str


class LogEntry(_Wire):
    message: str


class LookupRequest(_Wire):
    key: str


class LookupResponse(_Wire):
    value: str


class ShapesChild(_Wire):
    name: str


class ShapesRequest(_Wire):
    count: int
    ratio: float
    active: bool
    tags: list[str]
    labels: dict[str, str]
    note: str | None
    child: ShapesChild
    nullable_child: ShapesChild | None  # wire name nullableChild, via the alias generator


class TreeNode(_Wire):
    name: str
    parent: TreeNode | None
    children: list[TreeNode]


#: The pydantic canonical payload types by their fixed names, as CANONICAL_PAYLOAD_TYPES.
CANONICAL_PYDANTIC_PAYLOAD_TYPES: dict[str, type] = {
    cls.__name__: cls
    for cls in (
        GreetRequest,
        GreetResponse,
        Greeted,
        LogEntry,
        LookupRequest,
        LookupResponse,
        ShapesRequest,
        ShapesChild,
        TreeNode,
    )
}
