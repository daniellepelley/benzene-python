"""The contractHash algorithm (contract-document.md §6).

    contractHash = "sha256:" + lowercase-hex(sha256(canonicalJSON(normalize(document))))

``canonicalJSON`` is RFC 8785 (JCS) via the `rfc8785 <https://pypi.org/project/rfc8785/>`_ PyPI
package — per §6.3, JCS is used precisely so no port hand-writes its own canonicalizer's ordering/
number-formatting/escaping rules, which is what makes the hash comparable *across* ports.

This module operates on the raw JSON ``dict`` shape of a Contract Document (or a projection of
one), not the parsed :class:`~benzene.codegen_client.document.ContractDocument` model — that is
exactly the shape ``contract-hash-cases.json`` hands a conformant implementation, and it is also
what :func:`benzene.codegen_client.document.to_raw_document` produces for this generator's own
embedded hashes.
"""

from __future__ import annotations

import copy
import hashlib
from typing import Any

import rfc8785

from .document import is_reserved_topic

_PREFIX = "sha256:"


def normalize(document: dict[str, Any], *, topic_scoped: bool = False) -> dict[str, Any]:
    """§6.2's ``normalize()``: strip ``example``/``messageEndpoint``/``transports``/``reserved``.

    ``topic_scoped=True`` selects §5.3's topic-scoped ``normalize()`` behaviour: a reserved
    ``requests[]`` entry survives (with only its ``reserved`` flag stripped) rather than being
    removed entirely — set only when hashing a document that is already a single-topic projection
    that explicitly asked for that (possibly reserved) topic.
    """
    root = copy.deepcopy(document)
    root.pop("messageEndpoint", None)
    root.pop("transports", None)

    requests = root.get("requests")
    if isinstance(requests, list):
        surviving: list[Any] = []
        for entry in requests:
            if not isinstance(entry, dict):
                surviving.append(entry)
                continue

            entry.pop("example", None)

            # §6.2: reserved-ness (flag OR the §5.1 `benzene:` prefix rule) MUST be evaluated
            # before the `reserved` flag itself is stripped.
            reserved_by_flag = bool(entry.get("reserved", False))
            is_reserved = reserved_by_flag or is_reserved_topic(entry.get("topic", ""))
            entry.pop("reserved", None)

            if is_reserved and not topic_scoped:
                continue  # whole-service projection: drop the entry entirely

            surviving.append(entry)
        root["requests"] = surviving

    events = root.get("events")
    if isinstance(events, list):
        for entry in events:
            if isinstance(entry, dict):
                entry.pop("example", None)

    return root


def canonical_json(document: dict[str, Any]) -> bytes:
    """RFC 8785 (JCS) canonicalization of ``document``, as UTF-8 bytes."""
    return rfc8785.dumps(document)


def compute(document: dict[str, Any], *, topic_scoped: bool = False) -> str:
    """The full §6.2 pipeline: ``normalize`` -> ``canonicalJSON`` (JCS) -> SHA-256."""
    normalized = normalize(document, topic_scoped=topic_scoped)
    canonical = canonical_json(normalized)
    digest = hashlib.sha256(canonical).hexdigest()
    return f"{_PREFIX}{digest}"
