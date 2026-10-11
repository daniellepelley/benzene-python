"""Generates a typed, topic-scoped Python client from a Benzene Contract Document (``.spec.json``).

See ``docs/codegen-client.md`` (repo root) for the guide, and the language-neutral
``contract-document.md`` spec for the normative format/algorithm this package implements.

Public surface:

- :func:`benzene.codegen_client.document.parse_document` — parse a raw Contract Document.
- :func:`benzene.codegen_client.topic_scope.apply_topic_scope` — §5.2 include-list projection;
  :func:`~benzene.codegen_client.topic_scope.topic_projection` — §5.3 topic-scoped projection.
- :func:`benzene.codegen_client.schema_closure.reachable_schemas` — §5.3 schema-closure walk.
- :func:`benzene.codegen_client.contract_hash.compute` — §6 ``contractHash`` (``benzene-core``'s).
- :func:`benzene.codegen_client.generator.generate_service_client` /
  :func:`benzene.codegen_client.generator.generate_topic_client` — the two output shapes.
- ``benzene-codegen`` — the console-script CLI (:mod:`benzene.codegen_client.cli`).
"""

from __future__ import annotations

from .contract_hash import compute as compute_contract_hash
from .contract_hash import compute_registration as compute_registration_hash
from .document import (
    ContractDocument,
    ContractDocumentError,
    UnknownSchemaDialectError,
    parse_document,
)
from .generator import generate_service_client, generate_topic_client
from .schema_closure import reachable_names, reachable_schemas
from .topic_scope import (
    TopicNotFoundError,
    TopicScopeOptions,
    UnknownTopicsError,
    apply_topic_scope,
    topic_projection,
)

__all__ = [
    "ContractDocument",
    "ContractDocumentError",
    "TopicNotFoundError",
    "TopicScopeOptions",
    "UnknownSchemaDialectError",
    "UnknownTopicsError",
    "apply_topic_scope",
    "compute_contract_hash",
    "compute_registration_hash",
    "generate_service_client",
    "generate_topic_client",
    "parse_document",
    "reachable_names",
    "reachable_schemas",
    "topic_projection",
]
