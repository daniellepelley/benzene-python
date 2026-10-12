"""The OpenAPI projection describes pydantic v2 payloads, not ``{}``.

``benzene-openapi`` embeds :func:`benzene.core.json_schema`, which read only dataclasses: a pydantic
request or response type became the open schema. It now reads a model as the marshaler writes it
(``model_dump(by_alias=True)`` through ``to_jsonable``), the same reading the Contract Document
derivation makes. Each schema is checked against a real dumped instance, so it describes the wire.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import json
import uuid
from typing import Literal

import pytest

pydantic = pytest.importorskip("pydantic")

from benzene.core import Registry, json_schema, to_jsonable  # noqa: E402
from benzene.openapi import openapi_document  # noqa: E402
from benzene.results import Result  # noqa: E402
from pydantic import (  # noqa: E402
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    computed_field,
    field_serializer,
)
from pydantic.alias_generators import to_camel  # noqa: E402


class Colour(str, enum.Enum):
    RED = "red"
    BLUE = "blue"


@dataclasses.dataclass
class Dimensions:
    width_cm: int


class Line(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    sku: str
    unit_price: float = 0.0


class PlaceOrder(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    order_id: uuid.UUID
    placed_at: datetime.datetime
    kind: Literal["parcel", "letter"]
    colour: Colour
    lines: list[Line]
    note: str | None = None
    secret: str = Field(default="hidden", exclude=True)
    raw: int = 0
    dimensions: Dimensions | None = None
    parent: PlaceOrder | None = None

    @field_serializer("raw")
    def _raw(self, value: int) -> str:
        return str(value)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def line_count(self) -> int:
        return len(self.lines)


class OrderIds(RootModel[list[uuid.UUID]]):
    pass


def test_a_model_is_described_as_the_marshaler_writes_it() -> None:
    schema = json_schema(PlaceOrder)

    assert schema["type"] == "object"
    assert schema["properties"] == {
        "orderId": {"type": "string", "format": "uuid"},
        "placedAt": {"type": "string", "format": "date-time"},
        "kind": {"type": "string", "enum": ["parcel", "letter"]},
        "colour": {"type": "string", "enum": ["red", "blue"]},
        "lines": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"sku": {"type": "string"}, "unitPrice": {"type": "number"}},
                "required": ["sku"],
            },
        },
        "note": {"type": ["string", "null"]},
        # excluded fields are never dumped, so never described
        "raw": {},  # a custom serializer: no one shape
        "dimensions": {},  # a stdlib dataclass dumped by pydantic keeps its own field names
        "parent": {},  # recursion is cut with the open schema
        "lineCount": {"type": "integer"},  # a computed field is written, never supplied
    }
    # required = what the caller must supply (this module's rule, as for a dataclass)
    assert schema["required"] == ["orderId", "placedAt", "kind", "colour", "lines"]


def test_the_described_properties_are_the_written_keys() -> None:
    order = PlaceOrder(
        order_id=uuid.uuid4(),
        placed_at=datetime.datetime(2026, 10, 12, 9, 30),
        kind="parcel",
        colour=Colour.RED,
        lines=[Line(sku="a")],
    )
    written = json.loads(json.dumps(to_jsonable(order)))

    assert set(written) == set(json_schema(PlaceOrder)["properties"])
    assert set(written["lines"][0]) == set(json_schema(Line)["properties"])


def test_a_root_model_is_its_root_value() -> None:
    assert json_schema(OrderIds) == {"type": "array", "items": {"type": "string", "format": "uuid"}}


def test_the_openapi_document_embeds_the_model_schemas() -> None:
    async def place(request: PlaceOrder) -> Result:
        return Result.ok(OrderIds([request.order_id]))

    registry = Registry().register("orders:place", place, request_type=PlaceOrder, response_type=OrderIds)
    schemas = openapi_document(registry)["components"]["schemas"]

    assert schemas["OrdersPlaceRequest"] == json_schema(PlaceOrder)
    assert schemas["OrdersPlaceRequest"]["properties"]["orderId"] == {"type": "string", "format": "uuid"}
    assert schemas["OrdersPlaceResponse"] == json_schema(OrderIds)
