"""Payload-schema derivation for the mesh descriptor (mesh.md section 2).

The derivation itself lives in :mod:`benzene.core.schema` — it is a projection of the registry's
declared types, shared by the mesh :class:`ServiceDescriptor` and the profile's derived spec document
(:class:`~benzene.core.ServiceSpec`). This module re-exports it so ``benzene.mesh``'s public surface
(``benzene.mesh.schema.json_schema`` / ``Schema``) is unchanged.
"""

from __future__ import annotations

from benzene.core.schema import Schema, json_schema

__all__ = ["Schema", "json_schema"]
