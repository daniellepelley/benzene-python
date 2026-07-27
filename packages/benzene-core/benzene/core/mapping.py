"""Request/response payload mapping + serialization policy (core-concepts §6, wire-contracts §6).

This is the single seam where Benzene's on-the-wire payload naming policy lives, so every transport
and outbound client inherits it identically:

- **Writing** (``to_jsonable`` / ``encode_body``): dataclass field names are emitted **camelCase**
  (wire-contracts §6). Plain ``dict`` keys the application supplied are written verbatim — the
  developer chose those, exactly as the spec writes a free-form ``data`` bag verbatim.
- **Reading** (``to_request``): property-name matching into a dataclass is **case-insensitive** and
  separator-insensitive, so an inbound ``orderId`` (from a .NET/Go/TS peer), ``order_id``, or
  ``orderid`` all populate a Python field named ``order_id``.
"""

from __future__ import annotations

import json
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
        kwargs = {f.name: by_norm[_normalize(f.name)] for f in fields(request_type) if _normalize(f.name) in by_norm}
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
    return payload


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
