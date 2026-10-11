"""JSON Pointers for ``errors[].field`` (wire-contracts §1.3) and gRPC's dotted form (§4.2)."""

from __future__ import annotations

import pytest
from benzene.core import dotted_to_pointer, json_pointer, pointer_segments, pointer_to_dotted


def test_json_pointer_escapes_per_rfc_6901() -> None:
    assert json_pointer(["items", 0, "sku"]) == "/items/0/sku"
    assert json_pointer(["a/b", "m~n"]) == "/a~1b/m~0n"
    assert json_pointer([]) == ""


def test_pointer_segments_unescape() -> None:
    assert pointer_segments("/a~1b/m~0n/0") == ["a/b", "m~n", "0"]
    assert pointer_segments("") == []
    with pytest.raises(ValueError, match="starts with '/'"):
        pointer_segments("name")


@pytest.mark.parametrize(
    "pointer,dotted",
    [
        ("/items/0/sku", "items[0].sku"),
        ("/name", "name"),
        ("/0/sku", "[0].sku"),
        ("/matrix/1/2", "matrix[1][2]"),
        ("/a~1b/c", "a/b.c"),
    ],
)
def test_pointer_to_dotted_and_back(pointer: str, dotted: str) -> None:
    assert pointer_to_dotted(pointer) == dotted
    assert dotted_to_pointer(dotted) == pointer


def test_non_pointers_pass_through() -> None:
    assert pointer_to_dotted("Name") == "Name"  # an application's own pre-pointer field
    assert dotted_to_pointer("/already/a/pointer") == "/already/a/pointer"
    assert dotted_to_pointer("") == ""
