"""Value types the marshaler writes in their standard JSON forms, and the schemas that describe them.

contract-document §4.2: a schema describes a payload's *marshaled* JSON form. So each type here is
checked end to end - the marshaler (``to_jsonable`` + ``json.dumps``) writes the value, and the
schema derived for its declared type validates exactly what was written - for a stdlib dataclass
and for a pydantic model alike, which must also agree with each other on both the wire form and the
schema. Covered: ``UUID``, ``date``, ``time``, ``timedelta``, ``Decimal``, ``Path``, ``str``/``int``
enum members, and ``Literal``.
"""

from __future__ import annotations

import base64
import datetime
import decimal
import enum
import json
import pathlib
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

import pytest
from benzene.core import ContractDocument, Registry, encode_body, json_schema, to_jsonable
from benzene.results import Result

# --- a small OpenAPI 3.0 Schema Object validator (the subset §4.2 derives) ---------------------------

_DURATION = re.compile(r"^-?P(?=\d|T\d)(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+(\.\d+)?S)?)?$")


def _format_ok(fmt: str, value: str) -> bool:
    try:
        if fmt == "uuid":
            return str(uuid.UUID(value)) == value.lower()
        if fmt == "date":
            return re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is not None and bool(
                datetime.date.fromisoformat(value)
            )
        if fmt == "date-time":  # RFC 3339 requires the offset
            return datetime.datetime.fromisoformat(value).tzinfo is not None and "T" in value
        if fmt == "duration":
            return _DURATION.match(value) is not None
        if fmt == "byte":
            return base64.b64encode(base64.b64decode(value, validate=True)).decode() == value
    except ValueError:
        return False
    return True  # a format this validator does not know constrains nothing


def _errors(schema: dict[str, Any], value: Any, components: dict[str, Any], at: str) -> list[str]:
    if "$ref" in schema:
        name = schema["$ref"].rsplit("/", 1)[-1]
        return _errors(components[name], value, components, at)
    if value is None:
        return [] if not schema or schema.get("nullable") else [f"{at}: null not allowed"]
    errors: list[str] = []
    for part in schema.get("allOf", []):
        errors += _errors(part, value, components, at)
    kind = schema.get("type")
    checks = {
        "string": lambda v: isinstance(v, str),
        "boolean": lambda v: isinstance(v, bool),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "array": lambda v: isinstance(v, list),
        "object": lambda v: isinstance(v, dict),
    }
    if kind is not None and not checks[kind](value):
        return [*errors, f"{at}: {value!r} is not {kind}"]
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{at}: {value!r} not in {schema['enum']}")
    if kind == "string" and "format" in schema and not _format_ok(schema["format"], value):
        errors.append(f"{at}: {value!r} is not format {schema['format']}")
    if kind == "array":
        for i, item in enumerate(value):
            errors += _errors(schema.get("items", {}), item, components, f"{at}[{i}]")
    if kind == "object":
        for name in schema.get("required", []):
            if name not in value:
                errors.append(f"{at}: missing required {name!r}")
        properties = schema.get("properties", {})
        for name, item in value.items():
            sub = properties.get(name, schema.get("additionalProperties", {}))
            errors += _errors(sub, item, components, f"{at}.{name}")
    return errors


def _validate(document: dict[str, Any], schema: dict[str, Any], value: Any) -> list[str]:
    return _errors(schema, value, document["components"]["schemas"], "$")


async def _handle(_request: Any) -> Result:
    return Result.ok()


def _document(py_type: type) -> dict[str, Any]:
    registry = Registry().register("values:show", _handle, request_type=py_type)
    return ContractDocument.derive(registry, service="s").to_payload()


def _wire(payload: Any) -> Any:
    """What crosses the wire: the marshaler's JSON text, read back."""
    return json.loads(encode_body(payload))


# --- the payload, as a dataclass ---------------------------------------------------------------------


class Colour(str, enum.Enum):
    RED = "red"
    BLUE = "blue"


class Priority(enum.IntEnum):
    LOW = 1
    HIGH = 2


ID = uuid.UUID("6f1c2d9e-8a4b-4c3d-9e2f-1a2b3c4d5e6f")
UTC = datetime.timezone.utc


@dataclass
class Values:
    id: uuid.UUID
    day: datetime.date
    at: datetime.datetime
    clock: datetime.time
    span: datetime.timedelta
    amount: decimal.Decimal
    whole: decimal.Decimal
    where: pathlib.Path
    colour: Colour
    priority: Priority
    kind: Literal["parcel", "letter"]
    level: Literal[1, 2, 3]
    flag: Literal[True]
    pinned: Literal[Colour.RED, "green"]
    maybe_kind: Literal["parcel", "letter"] | None
    maybe_id: uuid.UUID | None
    ids: list[uuid.UUID] = field(default_factory=list)
    by_day: dict[str, datetime.date] = field(default_factory=dict)


def _values() -> Values:
    return Values(
        id=ID,
        day=datetime.date(2026, 10, 12),
        at=datetime.datetime(2026, 10, 12, 9, 30, tzinfo=UTC),
        clock=datetime.time(9, 30, 15),
        span=datetime.timedelta(hours=1, minutes=30),
        amount=decimal.Decimal("12.50"),
        whole=decimal.Decimal("1000"),
        where=pathlib.Path("orders/o-1.json"),
        colour=Colour.BLUE,
        priority=Priority.HIGH,
        kind="letter",
        level=2,
        flag=True,
        pinned=Colour.RED,
        maybe_kind=None,
        maybe_id=None,
        ids=[ID],
        by_day={"start": datetime.date(2026, 1, 1)},
    )


#: Each value's standard JSON form, as written.
EXPECTED_WIRE = {
    "id": "6f1c2d9e-8a4b-4c3d-9e2f-1a2b3c4d5e6f",
    "day": "2026-10-12",
    "at": "2026-10-12T09:30:00+00:00",
    "clock": "09:30:15",
    "span": "PT1H30M",
    "amount": 12.5,
    "whole": 1000,
    "where": "orders/o-1.json",
    "colour": "blue",
    "priority": 2,
    "kind": "letter",
    "level": 2,
    "flag": True,
    "pinned": "red",
    "maybeKind": None,
    "maybeId": None,
    "ids": ["6f1c2d9e-8a4b-4c3d-9e2f-1a2b3c4d5e6f"],
    "byDay": {"start": "2026-01-01"},
}

#: The §4.2 schema for each property (enums hoisted as components).
EXPECTED_PROPERTIES = {
    "id": {"type": "string", "format": "uuid"},
    "day": {"type": "string", "format": "date"},
    "at": {"type": "string", "format": "date-time"},
    "clock": {"type": "string"},
    "span": {"type": "string", "format": "duration"},
    "amount": {"type": "number"},
    "whole": {"type": "number"},
    "where": {"type": "string"},
    "colour": {"$ref": "#/components/schemas/Colour"},
    "priority": {"$ref": "#/components/schemas/Priority"},
    "kind": {"type": "string", "enum": ["parcel", "letter"]},
    "level": {"type": "integer", "enum": [1, 2, 3]},
    "flag": {"type": "boolean", "enum": [True]},
    "pinned": {"type": "string", "enum": ["red", "green"]},
    "maybeKind": {"type": "string", "enum": ["parcel", "letter"], "nullable": True},
    "maybeId": {"type": "string", "format": "uuid", "nullable": True},
    "ids": {"type": "array", "items": {"type": "string", "format": "uuid"}},
    "byDay": {"type": "object", "additionalProperties": {"type": "string", "format": "date"}},
}


def test_the_dataclass_marshals_each_type_in_its_standard_json_form() -> None:
    assert _wire(_values()) == EXPECTED_WIRE


def test_the_dataclass_schema_is_the_42_mapping_and_validates_what_was_written() -> None:
    document = _document(Values)
    schema = document["components"]["schemas"]["Values"]
    assert schema["properties"] == EXPECTED_PROPERTIES
    assert schema["required"] == list(EXPECTED_PROPERTIES)
    assert _validate(document, {"$ref": "#/components/schemas/Values"}, _wire(_values())) == []


def test_the_schema_validator_is_not_vacuous() -> None:
    document = _document(Values)
    ref = {"$ref": "#/components/schemas/Values"}
    for name, bad in [
        ("id", "not-a-uuid"),
        ("day", "12/10/2026"),
        ("span", "90 minutes"),
        ("amount", "12.50"),  # a Decimal is a number, not a string
        ("kind", "box"),
        ("level", 4),
        ("flag", False),
    ]:
        assert _validate(document, ref, {**EXPECTED_WIRE, name: bad}), name


@pytest.mark.parametrize(
    ("span", "text"),
    [
        (datetime.timedelta(0), "PT0S"),
        (datetime.timedelta(days=2), "P2D"),
        (datetime.timedelta(days=1, hours=2, microseconds=5), "P1DT2H0.000005S"),
        (datetime.timedelta(seconds=-1), "-PT1S"),
        (datetime.timedelta(milliseconds=500), "PT0.5S"),
        (datetime.timedelta(days=400, seconds=3), "P400DT3S"),
    ],
)
def test_a_timedelta_is_an_iso_8601_duration(span: datetime.timedelta, text: str) -> None:
    assert to_jsonable(span) == text
    assert _DURATION.match(text)


@pytest.mark.parametrize(
    ("amount", "number"),
    [
        (decimal.Decimal("12.50"), 12.5),
        (decimal.Decimal("0.1"), 0.1),
        (decimal.Decimal("-3"), -3),
        (decimal.Decimal("1E+3"), 1000),
        (decimal.Decimal("123456789012345678901234567890"), 123456789012345678901234567890),
    ],
)
def test_a_decimal_is_a_json_number(amount: decimal.Decimal, number: float) -> None:
    text = encode_body(amount)
    assert json.loads(text) == number
    assert decimal.Decimal(text) == amount  # exact: integral, or within 15 significant digits


def test_a_date_is_a_date_not_a_date_time() -> None:
    assert to_jsonable(datetime.date(2026, 1, 2)) == "2026-01-02"
    assert to_jsonable(datetime.datetime(2026, 1, 2, tzinfo=UTC)) == "2026-01-02T00:00:00+00:00"


def test_a_literal_without_one_json_type_is_the_open_schema() -> None:
    class Plain(enum.Enum):  # no str/int mixin: it does not marshal
        A = object()

    @dataclass
    class Odd:
        mixed: Literal[1, "one"]
        number_or_bool: Literal[1, True]
        plain: Literal[Plain.A]

    properties = _document(Odd)["components"]["schemas"]["Odd"]["properties"]
    assert properties == {"mixed": {}, "numberOrBool": {}, "plain": {}}


def test_the_json_schema_projection_maps_the_same_types() -> None:
    """``json_schema`` (the 2020-12 projection benzene-openapi embeds) follows the same mapping; a
    nullable literal lists ``null`` in its enum too, since a 2020-12 enum admits only what it lists."""
    assert json_schema(uuid.UUID) == {"type": "string", "format": "uuid"}
    assert json_schema(datetime.date) == {"type": "string", "format": "date"}
    assert json_schema(decimal.Decimal) == {"type": "number"}
    assert json_schema(Literal["parcel", "letter"] | None) == {
        "type": ["string", "null"], "enum": ["parcel", "letter", None],
    }
    assert json_schema(Literal[1, "one"]) == {}


# --- the same payload, as a pydantic model -----------------------------------------------------------


def _model_types() -> tuple[Any, Any]:
    pydantic: Any = pytest.importorskip("pydantic")
    from pydantic.alias_generators import to_camel

    class ValuesModel(pydantic.BaseModel):
        model_config = pydantic.ConfigDict(alias_generator=to_camel, populate_by_name=True)

        id: uuid.UUID
        day: datetime.date
        at: datetime.datetime
        clock: datetime.time
        span: datetime.timedelta
        amount: decimal.Decimal
        whole: decimal.Decimal
        where: pathlib.Path
        colour: Colour
        priority: Priority
        kind: Literal["parcel", "letter"]
        level: Literal[1, 2, 3]
        flag: Literal[True]
        pinned: Literal[Colour.RED, "green"]
        maybe_kind: Literal["parcel", "letter"] | None
        maybe_id: uuid.UUID | None
        ids: list[uuid.UUID] = []
        by_day: dict[str, datetime.date] = {}

    class Wrapper(pydantic.BaseModel):  # the same values nested one level down
        values: ValuesModel

    ValuesModel.__name__ = "Values"  # one component name for both shapes
    return ValuesModel, Wrapper


def _model(model_type: Any) -> Any:
    return model_type(**{name: getattr(_values(), name) for name in Values.__dataclass_fields__})


def test_a_model_marshals_each_type_exactly_as_the_dataclass_does() -> None:
    values_model, _ = _model_types()
    model = _model(values_model)
    assert _wire(model) == EXPECTED_WIRE
    # And pydantic reads the written form straight back to the same values.
    assert values_model.model_validate(_wire(model)) == model


def test_a_model_derives_the_dataclass_schema_and_it_validates_what_was_written() -> None:
    values_model, wrapper = _model_types()
    document = _document(values_model)
    schema = document["components"]["schemas"]["Values"]
    assert schema == _document(Values)["components"]["schemas"]["Values"]
    written = _wire(_model(values_model))
    assert _validate(document, {"$ref": "#/components/schemas/Values"}, written) == []

    nested = _document(wrapper)
    written = _wire(wrapper(values=_model(values_model)))
    assert written == {"values": EXPECTED_WIRE}
    assert _validate(nested, {"$ref": "#/components/schemas/Wrapper"}, written) == []


def test_a_naive_datetime_is_written_as_utc() -> None:
    # RFC 3339 (format: date-time) requires an offset, so a naive datetime is taken as UTC and
    # written exactly as the aware UTC datetime is; an aware one keeps its own offset.
    naive = datetime.datetime(2026, 10, 12, 9, 30)
    assert to_jsonable(naive) == to_jsonable(naive.replace(tzinfo=UTC)) == "2026-10-12T09:30:00+00:00"
    assert _format_ok("date-time", to_jsonable(naive))
    plus_two = datetime.timezone(datetime.timedelta(hours=2))
    assert to_jsonable(datetime.datetime(2026, 10, 12, 9, 30, tzinfo=plus_two)) == "2026-10-12T09:30:00+02:00"
