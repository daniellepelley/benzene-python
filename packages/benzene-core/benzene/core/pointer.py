"""JSON Pointers for ``errors[].field`` (wire-contracts §1.3, RFC 6901).

A structured error's ``field`` is a JSON Pointer to the offending value in the handler's request,
addressed in the request type's **JSON form** — property names as the serializer writes them —
``/name``, ``/items/0/sku``. A validation integration builds one from its library's own location
with :func:`json_pointer`; the gRPC binding converts one to ``FieldViolation``'s dotted path with
:func:`pointer_to_dotted` (wire-contracts §4.2) and back with :func:`dotted_to_pointer`.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

_DOTTED_TOKEN = re.compile(r"\[(\d+)\]|([^.\[\]]+)")


def json_pointer(segments: Iterable[str | int]) -> str:
    """The RFC 6901 pointer for a path of property names and array indexes.

    Each segment is escaped (``~`` → ``~0``, ``/`` → ``~1``). An empty path is ``""``, the whole
    document — a rule on the whole request omits ``field`` instead, so callers check for that.
    """
    return "".join("/" + str(segment).replace("~", "~0").replace("/", "~1") for segment in segments)


def pointer_segments(pointer: str) -> list[str]:
    """The unescaped segments of an RFC 6901 pointer (``"/items/0/sku"`` → ``["items", "0", "sku"]``)."""
    if not pointer:
        return []
    if not pointer.startswith("/"):
        raise ValueError(f"{pointer!r} is not a JSON Pointer: a non-empty pointer starts with '/'")
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]


def pointer_to_dotted(field: str) -> str:
    """A §1.3 pointer as gRPC ``FieldViolation.field``'s dotted path (wire-contracts §4.2).

    Segments unescaped and joined with ``.``; a segment made only of decimal digits is an index,
    written ``[n]``: ``/items/0/sku`` → ``items[0].sku``. A value that is not a pointer (an
    application-authored field from before pointers) passes through unchanged.
    """
    if not field.startswith("/"):
        return field
    dotted = ""
    for segment in pointer_segments(field):
        if segment.isdigit() and segment.isascii():
            dotted += f"[{segment}]"
        else:
            dotted += f".{segment}" if dotted else segment
    return dotted


def dotted_to_pointer(path: str) -> str:
    """The inverse of :func:`pointer_to_dotted`: ``items[0].sku`` → ``/items/0/sku``.

    A value that already is a pointer passes through; an empty path is ``""``.
    """
    if not path or path.startswith("/"):
        return path
    return json_pointer(name or index for index, name in _DOTTED_TOKEN.findall(path))
