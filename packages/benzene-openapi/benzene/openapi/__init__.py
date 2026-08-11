"""``benzene.openapi`` — an OpenAPI document derived from the handler registry.

The Benzene Python port already derives a per-topic JSON Schema (:func:`benzene.core.json_schema`) and
projects the registry into the Cloud Service Profile's :class:`~benzene.core.ServiceSpec` and the mesh
:class:`~benzene.mesh.ServiceDescriptor`. This distribution (``benzene-openapi``) adds a further
projection of that *same* registry — a standard **OpenAPI 3.1** document — so the contract a service
serves can be handed to the OpenAPI tooling ecosystem (Swagger UI, client generators, contract linters)
instead of being maintained by hand.

Like ``ServiceSpec`` it is a pure, deterministic registry projection: one ``POST`` operation per
registered ``(topic, version)`` under the profile's ``/benzene/invoke`` base, with every request and
response schema reused verbatim from :func:`benzene.core.json_schema` and every failure status mapped
to its HTTP code through the port's own :func:`benzene.http.to_http` table. Depends on ``benzene-core``
(the schemas and registry) and ``benzene-http`` (the invoke paths and status mapping); no third-party
runtime dependencies.

Mirrors .NET's ``Benzene.Schema.OpenApi``. Contributes the ``benzene.openapi`` subpackage to the shared
``benzene`` namespace.
"""

from __future__ import annotations

from .generator import OPENAPI_VERSION, openapi_document, operation_id

__all__ = [
    "OPENAPI_VERSION",
    "openapi_document",
    "operation_id",
]
