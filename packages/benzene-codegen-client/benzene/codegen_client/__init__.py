"""Generates a typed, topic-scoped Python client from a Benzene Contract Document (``.spec.json``).

See ``docs/codegen-client.md`` (repo root) for the guide, and the language-neutral
``contract-document.md`` spec for the normative format/algorithm this package implements.

Public surface:

- :func:`benzene.codegen_client.document.parse_document` — parse a raw Contract Document.
- :func:`benzene.codegen_client.topic_scope.apply_topic_scope` — §5.1/§5.2 topic-scope projection.
- :func:`benzene.codegen_client.schema_closure.reachable_schemas` — §5.3 schema-closure walk.
- :func:`benzene.codegen_client.contract_hash.compute` — §6 ``contractHash``.
- :func:`benzene.codegen_client.generator.generate_service_client` /
  :func:`benzene.codegen_client.generator.generate_topic_client` — the two output shapes.
- ``benzene-codegen`` — the console-script CLI (:mod:`benzene.codegen_client.cli`).
"""

from __future__ import annotations

from .contract_hash import compute as compute_contract_hash
from .document import ContractDocument, ContractDocumentError, parse_document
from .generator import generate_service_client, generate_topic_client
from .schema_closure import reachable_names, reachable_schemas
from .topic_scope import TopicScopeOptions, UnknownTopicsError, apply_topic_scope

__all__ = [
    "ContractDocument",
    "ContractDocumentError",
    "TopicScopeOptions",
    "UnknownTopicsError",
    "apply_topic_scope",
    "compute_contract_hash",
    "generate_service_client",
    "generate_topic_client",
    "parse_document",
    "reachable_names",
    "reachable_schemas",
]
