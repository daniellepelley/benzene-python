"""Request/response payload mapping + serialization policy (core-concepts §6, wire-contracts §6).

This is the single seam where Benzene's on-the-wire payload naming policy lives, so every transport
and outbound client inherits it identically:

- **Writing** (``to_jsonable`` / ``encode_body``): dataclass field names are emitted **camelCase**
  (wire-contracts §6). Plain ``dict`` keys the application supplied are written verbatim — the
  developer chose those, exactly as the spec writes a free-form ``data`` bag verbatim.
- **Reading** (``to_request``): property-name matching into a dataclass is **case-insensitive** and
  separator-insensitive, so an inbound ``orderId`` (from a .NET/Go/TS peer), ``order_id``, or
  ``orderid`` all populate a Python field named ``order_id``.

**Values** are written in their standard JSON forms (:func:`to_jsonable`): a ``UUID`` as its
hyphenated string, a ``date``/``time``/``datetime`` in ISO 8601 (RFC 3339 for an aware datetime), a
``timedelta`` as an ISO 8601 duration, ``bytes`` as base64 and a ``Decimal`` as a JSON **number**
(contract-document §4.2 types a decimal as a number, as the .NET port writes ``decimal``; pydantic's
own ``mode="json"`` would write it as a string). A number is exact for an integral Decimal and for
up to 15 significant digits otherwise; a value that needs more precision on the wire should be
declared ``str``.
"""

from __future__ import annotations

import base64
import datetime
import decimal
import json
import pathlib
import uuid
from dataclasses import asdict, fields, is_dataclass
from typing import Any


def to_camel(name: str) -> str:
    """``order_id`` → ``orderId``; a name with no underscores is unchanged."""
    head, *tail = name.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in tail)


def _normalize(name: str) -> str:
    """Fold a property name for case-/separator-insensitive matching: ``orderId`` → ``orderid``."""
    return name.replace("_", "").lower()


def to_request(request_type: type | None, data: Any) -> Any:
    """Map a decoded payload to the handler's declared request type (case-insensitive fields)."""
    if request_type is None:
        return data  # handler takes the raw decoded payload (e.g. a dict)
    if isinstance(data, request_type):
        return data  # already the right type — pass through untouched
    if is_dataclass(request_type) and isinstance(data, dict):
        by_norm = {_normalize(str(k)): v for k, v in data.items()}
        kwargs = {
            f.name: by_norm[_normalize(f.name)]
            for f in fields(request_type)
            if _normalize(f.name) in by_norm
        }
        return request_type(**kwargs)
    if isinstance(data, dict):
        try:
            return request_type(**data)
        except TypeError:
            return request_type(data)
    return data


def to_jsonable(payload: Any) -> Any:
    """Convert a response payload to a ``json.dumps``-able value, camelCasing dataclass fields."""
    if payload is None:
        return None
    if is_dataclass(payload) and not isinstance(payload, type):
        return {to_camel(f.name): to_jsonable(getattr(payload, f.name)) for f in fields(payload)}
    if isinstance(payload, (list, tuple)):
        return [to_jsonable(item) for item in payload]
    if isinstance(payload, dict):
        return {key: to_jsonable(value) for key, value in payload.items()}  # keys verbatim
    # The non-JSON-native wire types the schema layers declare (schema.py, contract_schema.py), each
    # in its standard JSON form - kept in step so a handler can return them without json.dumps
    # crashing on a type the derived schema says is valid:
    #   bytes → base64 string; datetime → isoformat (RFC 3339 when aware); date → ISO 8601 date;
    #   time → ISO 8601 time; timedelta → ISO 8601 duration; UUID → canonical hyphenated string;
    #   Decimal → JSON number; PurePath → its string; set/frozenset → array.
    # datetime is a date subclass, so it is matched first.
    if isinstance(payload, (bytes, bytearray)):
        return base64.b64encode(bytes(payload)).decode("ascii")
    if isinstance(payload, (datetime.datetime, datetime.date, datetime.time)):
        return payload.isoformat()
    if isinstance(payload, datetime.timedelta):
        return _iso_duration(payload)
    if isinstance(payload, uuid.UUID):
        return str(payload)
    if isinstance(payload, decimal.Decimal):
        return _decimal_number(payload)
    if isinstance(payload, pathlib.PurePath):
        return str(payload)
    if isinstance(payload, (set, frozenset)):
        return [to_jsonable(item) for item in payload]
    # Duck-typed support for objects that know how to serialize themselves (e.g. a pydantic
    # BaseModel): dump with aliases so a model's camelCase alias generator reaches the wire. The
    # dump is pydantic's default (python) mode, and its values go back through this function, so a
    # model's UUID, date, Decimal, bytes, ... take exactly the forms above - the forms a dataclass
    # field of the same type takes, and the forms contract_schema derives. (``mode="json"`` would
    # write Decimal as a string and bytes as UTF-8 text, so one Python type would have two wire
    # forms depending on whether it sat in a dataclass or a model.)
    model_dump = getattr(payload, "model_dump", None)
    if callable(model_dump):
        return to_jsonable(model_dump(by_alias=True))
    return payload


def _iso_duration(value: datetime.timedelta) -> str:
    """ISO 8601 duration in days, hours, minutes and seconds: ``P1DT2H30M``, ``-PT1.5S``, ``PT0S``.

    No years or months - their length varies, so a ``timedelta`` never implies them."""
    micros = (value.days * 86_400 + value.seconds) * 1_000_000 + value.microseconds
    sign = "-" if micros < 0 else ""
    micros = abs(micros)
    days, micros = divmod(micros, 86_400 * 1_000_000)
    hours, micros = divmod(micros, 3_600 * 1_000_000)
    minutes, micros = divmod(micros, 60 * 1_000_000)
    seconds, micros = divmod(micros, 1_000_000)
    clock = "".join(
        f"{amount}{unit}" for amount, unit in ((hours, "H"), (minutes, "M")) if amount
    )
    if micros:
        clock += f"{seconds}.{micros:06d}".rstrip("0") + "S"
    elif seconds or not (days or clock):
        clock += f"{seconds}S"
    return f"{sign}P{f'{days}D' if days else ''}{f'T{clock}' if clock else ''}"


def _decimal_number(value: decimal.Decimal) -> int | float:
    """A ``Decimal`` as a JSON number: exact when integral (a Python ``int`` has no size limit),
    otherwise a ``float`` - exact on the wire for up to 15 significant digits, since ``json.dumps``
    writes a float's shortest round-tripping text. ``json`` cannot write a Decimal's own digits as
    a number, and a string would make it a different wire type (see the module notes)."""
    if value.is_finite() and value == value.to_integral_value():
        return int(value)
    return float(value)


def encode_body(payload: Any) -> str:
    """Serialize an outbound payload to a JSON string using the wire naming policy.

    The single serialization entry point for outbound clients and transport bodies — a ``str`` is
    passed through (already encoded), anything else goes through :func:`to_jsonable`.
    """
    if isinstance(payload, str):
        return payload
    return json.dumps(to_jsonable(payload))


# `asdict` re-exported for callers that genuinely want the raw (snake_case) dict, not the wire form.
__all__ = ["asdict", "encode_body", "to_camel", "to_jsonable", "to_request"]
