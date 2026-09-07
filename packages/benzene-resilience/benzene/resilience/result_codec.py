"""Serialising a :class:`~benzene.results.Result` into a durable idempotency store, and back.

An in-process store holds the first delivery's ``Result`` object itself. A *shared* store holds
bytes, so the replayed result is only as good as this codec — and the middleware compares what comes
back (``settled == IN_PROGRESS``) and hands it straight to the wire edge. Two rules follow:

* **``errors`` must come back a tuple.** :class:`~benzene.results.Result` is a frozen dataclass and
  its equality is field-by-field, so a codec that restored ``errors`` as a ``list`` would make every
  reservation marker compare *unequal* to :data:`~benzene.resilience.IN_PROGRESS`. Every concurrent
  duplicate would then be handed the marker as if it were a settled answer. ``__post_init__``
  re-coerces, so this is belt and braces — but it is the failure mode to know about.
* **It is not the response envelope.** :func:`benzene.core.encode_response` /
  :func:`~benzene.core.decode_response` look like the right tool and are lossy here: they render a
  failure as a problem document whose ``detail`` collapses several errors into one string, and they
  bake in HTTP-shaped headers a store has no business keeping.

**What is persisted:** ``status``; ``payload`` in its **wire form** (:func:`benzene.core.to_jsonable`
— the same shape the first delivery put on the wire); every ``error`` whole, with its ``field`` and
``code``; an explicit ``successful`` classification when one was stated (``isSuccessful`` is
authoritative on the wire, so a replay must not re-derive it); and an application-authored
``problem_document`` verbatim, so a remembered failure replays the caller's own problem type rather
than one derived from the status.

**What is deliberately dropped:** the payload's *Python type*. A replayed payload is JSON data — a
``dict``, not the dataclass or pydantic model the handler returned — because that is what a durable
store can honestly hold and exactly what the wire edge would have produced anyway. Middleware that
sits between the router and the idempotency layer and reaches into ``result.payload`` attributes
must therefore tolerate the wire shape. Nothing else is dropped, and nothing is *added*: no
exception text, no traceback, no host identity. A shared store is read by every instance and by
whoever can read the table, so it holds the answer the caller already received and not one byte more.

A payload that is not JSON-encodable raises :class:`TypeError` from :func:`encode_result` rather than
being silently mangled — a handler whose success cannot be persisted cannot be deduplicated, and
that is a fact worth failing on.
"""

from __future__ import annotations

import json
from typing import Any

from benzene.core import to_jsonable
from benzene.results import BenzeneError, ProblemDetails, Result


def encode_result(result: Result[Any]) -> str:
    """Serialise ``result`` to the JSON string a durable idempotency store persists."""
    entry: dict[str, Any] = {"status": result.status}
    if result.payload is not None:
        entry["payload"] = to_jsonable(result.payload)
    if result.errors:
        entry["errors"] = [error.to_payload() for error in result.errors]
    if result.successful is not None:
        entry["successful"] = result.successful
    if result.problem_document is not None:
        entry["problem"] = result.problem_document.to_payload()
    return json.dumps(entry)


def decode_result(raw: str | bytes) -> Result[Any]:
    """Rebuild the :class:`~benzene.results.Result` :func:`encode_result` wrote.

    Raises :class:`ValueError` on an entry that is not a JSON object — a corrupt or foreign value
    under an idempotency key is an operational fault worth surfacing, not something to paper over
    with a fabricated result that a duplicate delivery would then be told is the real answer.
    """
    try:
        entry = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"malformed idempotency entry: {exc}") from exc
    if not isinstance(entry, dict):
        raise ValueError(f"malformed idempotency entry: expected a JSON object, got {type(entry)}")

    problem = entry.get("problem")
    return Result(
        str(entry.get("status") or ""),
        entry.get("payload"),
        _errors(entry.get("errors")),
        None if not isinstance(problem, dict) else _problem(problem),
        entry.get("successful"),
    )


def _errors(raw: Any) -> tuple[BenzeneError, ...]:
    """The stored errors as a **tuple** — see this module's docstring for why that matters."""
    if not isinstance(raw, list):
        return ()
    return tuple(
        BenzeneError.coerce(item if isinstance(item, (dict, str)) else str(item)) for item in raw
    )


def _problem(document: dict[str, Any]) -> ProblemDetails:
    """The inverse of :meth:`ProblemDetails.to_payload` — every member it emits, read back."""
    return ProblemDetails(
        benzene_status=str(document.get("benzeneStatus") or ""),
        type=_optional(document.get("type")),
        title=_optional(document.get("title")),
        detail=_optional(document.get("detail")),
        instance=_optional(document.get("instance")),
        errors=_errors(document.get("errors")),
    )


def _optional(value: Any) -> str | None:
    return None if value is None else str(value)
