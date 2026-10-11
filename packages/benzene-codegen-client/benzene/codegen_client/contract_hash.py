"""The Contract Document hashes, for generated clients (contract-document.md §6).

The algorithm lives in ``benzene-core`` (:mod:`benzene.core.contract_hash`) — the service that serves
a document and the client generated from it hash with the same code, so a drift check compares two
values of one function over one projection. This module applies it to what the generator emits:

- a default service-level client embeds the document's whole-service ``contractHash``;
- an include-list client (§5.2) hashes ``{listed requests, every event}``;
- a topic-scoped client (§5.3) hashes ``{one request, no events}``.

Each is recomputed over its own projection rather than copied from the provider's document — a hash
is only comparable with the identically scoped projection (§6.4).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from benzene.core.contract_hash import contract_hash, registration_hash

from .document import ContractDocument, to_raw_document

__all__ = ["compute", "compute_registration", "contract_hash", "registration_hash"]


def compute(document: ContractDocument | Mapping[str, Any]) -> str:
    """``contractHash`` of a raw document or a parsed (possibly projected) one."""
    raw = to_raw_document(document) if isinstance(document, ContractDocument) else document
    return contract_hash(raw)


def compute_registration(document: ContractDocument | Mapping[str, Any]) -> str:
    """``registrationHash`` of a raw document or a parsed (possibly projected) one."""
    raw = to_raw_document(document) if isinstance(document, ContractDocument) else document
    return registration_hash(raw)
