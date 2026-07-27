"""Request/response payload mapping (core-concepts.md section 6).

Converts between the transport's decoded payload (a plain ``dict`` from JSON) and the handler's
declared ``request_type`` — zero-copy when already the right type, else constructed from the dict.
"""

from __future__ import annotations

from dataclasses import asdict, fields, is_dataclass
from typing import Any


def to_request(request_type: type | None, data: Any) -> Any:
    """Map a decoded payload to the handler's declared request type."""
    if request_type is None:
        return data  # handler takes the raw decoded payload (e.g. a dict)
    if isinstance(data, request_type):
        return data  # already the right type — pass through untouched
    if is_dataclass(request_type) and isinstance(data, dict):
        names = {f.name for f in fields(request_type)}
        return request_type(**{k: v for k, v in data.items() if k in names})
    if isinstance(data, dict):
        try:
            return request_type(**data)
        except TypeError:
            return request_type(data)
    return data


def to_jsonable(payload: Any) -> Any:
    """Convert a response payload to something ``json.dumps`` can serialize."""
    if payload is None:
        return None
    if is_dataclass(payload) and not isinstance(payload, type):
        return asdict(payload)
    return payload
