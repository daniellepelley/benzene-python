"""Payload-schema derivation for the mesh descriptor (mesh.md section 2).

The derivation itself lives in :mod:`benzene.core.schema` — the JSON Schema subset the descriptor
carries until it is retired (the Contract Document derives its OpenAPI 3.0 schemas separately, in
:mod:`benzene.core.contract_schema`). This module re-exports it so ``benzene.mesh``'s public surface
(``benzene.mesh.schema.json_schema`` / ``Schema``) is unchanged.
"""

from __future__ import annotations

from benzene.core.schema import Schema, json_schema

__all__ = ["Schema", "json_schema"]
