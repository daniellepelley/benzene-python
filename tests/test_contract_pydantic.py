"""Contract Document derivation from pydantic v2 models (contract-document.md §4.1-§4.2).

A pydantic payload's schema describes what the marshaler writes for it -
``model_dump(by_alias=True)`` and then ``to_jsonable`` - through the same §4.2 table as a
dataclass, so a model and a dataclass with one wire shape derive one schema (the conformance runner
holds the canonical types to that: the pydantic versions derive the derivationCases' exact hashes).
"""

from __future__ import annotations

import datetime
import enum
from dataclasses import dataclass
from typing import Annotated, Any, Generic, TypeVar

import pytest

pydantic = pytest.importorskip("pydantic")

from benzene.core import (  # noqa: E402
    ComponentNameCollisionError,
    ContractDocument,
    Registry,
    schema_name,
    to_jsonable,
)
from benzene.results import Result  # noqa: E402
from pydantic import (  # noqa: E402
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    RootModel,
    computed_field,
    field_serializer,
    model_serializer,
)
from pydantic.alias_generators import to_camel  # noqa: E402

T = TypeVar("T")


async def handle(_request: Any) -> Result:
    return Result.ok()


def _derive(*types: type) -> dict[str, Any]:
    registry = Registry()
    for index, py_type in enumerate(types):
        registry.register(f"t:{index}", handle, request_type=py_type)
    return ContractDocument.derive(registry, service="s").to_payload()


class Colour(str, enum.Enum):
    RED = "red"
    BLUE = "blue"


class Line(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    unit_price: float


class Order(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    order_id: str
    placed_at: datetime.datetime
    colour: Colour
    maybe_colour: Colour | None = None
    lines: list[Line] = []
    first_line: Line | None = None
    note: str | None = None
    quantity: int = 1
    explicit: str = Field("x", alias="EXPLICIT")
    renamed_on_output: str = Field("y", serialization_alias="outputName")
    secret: str = Field("z", exclude=True)
    tag: Annotated[str, Field(min_length=1)] = "t"
    codes: list[Annotated[str, Field(min_length=2)]] = []

    @computed_field  # type: ignore[prop-decorator]
    @property
    def line_count(self) -> int:
        return len(self.lines)


def _sample_order() -> Order:
    return Order(
        order_id="o-1",
        placed_at=datetime.datetime(2026, 1, 1),
        colour=Colour.RED,
        lines=[Line(unit_price=1.5)],
    )


def test_a_model_is_a_hoisted_record_of_what_the_marshaler_writes() -> None:
    document = _derive(Order)
    assert document["requests"][0]["request"] == {"$ref": "#/components/schemas/Order"}
    schemas = document["components"]["schemas"]
    order = schemas["Order"]
    assert order["type"] == "object" and "additionalProperties" not in order

    # The property names are exactly the keys the marshaler writes: aliases (an alias generator, an
    # explicit alias, a serialization-only alias), computed fields in, an excluded field out.
    written = to_jsonable(_sample_order())
    assert set(order["properties"]) == set(written)
    assert set(order["properties"]) == {
        "orderId", "placedAt", "colour", "maybeColour", "lines", "firstLine", "note", "quantity",
        "EXPLICIT", "outputName", "tag", "codes", "lineCount",
    }
    # Every property the dump always writes is required - defaulted and nullable ones included.
    assert order["required"] == list(order["properties"])

    properties = order["properties"]
    assert properties["placedAt"] == {"type": "string", "format": "date-time"}
    assert properties["quantity"] == {"type": "integer"}
    assert properties["lineCount"] == {"type": "integer"}
    assert properties["note"] == {"type": "string", "nullable": True}
    assert properties["tag"] == {"type": "string"}  # constraints are a MAY, not emitted
    assert properties["codes"] == {"type": "array", "items": {"type": "string"}}
    # Enums are named and typed; nullable named types take the §4.2 wrapper.
    assert properties["colour"] == {"$ref": "#/components/schemas/Colour"}
    assert schemas["Colour"] == {"type": "string", "enum": ["red", "blue"]}
    assert properties["maybeColour"] == {
        "type": "string", "nullable": True, "allOf": [{"$ref": "#/components/schemas/Colour"}],
    }
    assert properties["lines"] == {"type": "array", "items": {"$ref": "#/components/schemas/Line"}}
    assert properties["firstLine"] == {
        "type": "object", "nullable": True, "allOf": [{"$ref": "#/components/schemas/Line"}],
    }
    assert schemas["Line"] == {
        "type": "object", "properties": {"unitPrice": {"type": "number", "format": "double"}},
        "required": ["unitPrice"],
    }


def test_a_field_that_exclude_if_may_drop_is_a_property_but_not_required() -> None:
    if "exclude_if" not in pydantic.fields.FieldInfo.__slots__:
        pytest.skip("Field(exclude_if=...) needs pydantic 2.11+")

    class Sparse(BaseModel):
        kept: str
        dropped_when_empty: str = Field("", exclude_if=lambda value: not value)

    schema = _derive(Sparse)["components"]["schemas"]["Sparse"]
    assert set(schema["properties"]) == {"kept", "dropped_when_empty"}
    assert schema["required"] == ["kept"]


def test_custom_serializers_derive_to_the_open_schema() -> None:
    class Money(BaseModel):
        amount: int
        currency: str

        @field_serializer("amount")
        def _cents(self, value: int) -> str:
            return f"{value / 100:.2f}"

    class Custom(BaseModel):
        value: str

        @model_serializer
        def _flat(self) -> str:
            return self.value

    class Holder(BaseModel):
        money: Money
        custom: Custom
        stamped: Annotated[int, PlainSerializer(lambda v: str(v))]

    schemas = _derive(Holder)["components"]["schemas"]
    assert schemas["Money"]["properties"] == {"amount": {}, "currency": {"type": "string"}}
    # A model-level serializer replaces the record: {} inline, and no component for it.
    assert schemas["Holder"]["properties"]["custom"] == {}
    assert "Custom" not in schemas
    assert schemas["Holder"]["properties"]["stamped"] == {}


@dataclass
class Nested:
    some_name: str


def test_a_stdlib_dataclass_inside_a_model_is_open_but_keeps_its_own_shape_elsewhere() -> None:
    # pydantic dumps a nested stdlib dataclass under the enclosing model's alias config, not the
    # marshaler's camelCase, so that position has no one shape; the dataclass's own component is
    # unaffected where the marshaler writes it.
    class Wrapper(BaseModel):
        nested: Nested

    schemas = _derive(Wrapper, Nested)["components"]["schemas"]
    assert schemas["Wrapper"]["properties"]["nested"] == {}
    assert schemas["Nested"]["properties"] == {"someName": {"type": "string"}}


class Page(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None = None


def test_a_generic_model_is_named_with_its_arguments_first() -> None:
    schemas = _derive(Page[Line])["components"]["schemas"]
    assert schemas["LinePage"]["properties"]["items"] == {
        "type": "array", "items": {"$ref": "#/components/schemas/Line"},
    }
    assert schemas["LinePage"]["required"] == ["items", "next_cursor"]


def test_a_root_model_is_its_root_value_inline() -> None:
    Codes = RootModel[list[int]]

    class Holder(BaseModel):
        codes: Codes  # type: ignore[valid-type]

    schemas = _derive(Holder)["components"]["schemas"]
    assert schemas["Holder"]["properties"]["codes"] == {"type": "array", "items": {"type": "integer"}}
    assert set(schemas) == {"Holder"}


def test_schema_name_and_the_collision_rule_apply_to_models() -> None:
    @schema_name("OrderLine")
    class Item(BaseModel):
        sku: str

    assert set(_derive(Item)["components"]["schemas"]) == {"OrderLine"}

    @dataclass
    class Thing:
        id: str

    class OtherThing(BaseModel):
        sku: str

    OtherThing.__name__ = "Thing"
    with pytest.raises(ComponentNameCollisionError, match="Thing"):
        _derive(Thing, OtherThing)

    # A model and a dataclass with one wire shape are one schema: they may share the name.
    class SameThing(BaseModel):
        id: str

    SameThing.__name__ = "Thing"
    assert set(_derive(Thing, SameThing)["components"]["schemas"]) == {"Thing"}
