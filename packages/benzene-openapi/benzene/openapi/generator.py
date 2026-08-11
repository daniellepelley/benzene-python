"""OpenAPI 3.1 generation from the handler registry — a sibling projection to the derived spec.

The port already derives two documents from the same registry: the Cloud Service Profile's
:class:`~benzene.core.ServiceSpec` (``/benzene/spec``) and the mesh
:class:`~benzene.mesh.ServiceDescriptor`, both of which embed the per-topic JSON Schema that
:func:`benzene.core.json_schema` derives from a handler's declared request/response type. This module
adds a third projection of that *same* registry: a standard **OpenAPI 3.1** document, so the contract
a service serves can be browsed, diffed, and fed to the OpenAPI tooling ecosystem (Swagger UI, code
generators, contract linters) without a hand-maintained spec. Mirrors .NET's ``Benzene.Schema.OpenApi``.

**The mapping.** Benzene is message-topic-based, not REST, so there is no natural resource hierarchy
to project. The faithful anchor is the profile's well-known invoke endpoint (``POST /benzene/invoke``,
:class:`~benzene.http.StandardPaths`), through which every transport invokes a topic uniformly. Because
an OpenAPI path holds at most one ``POST`` operation, each registered ``(topic, version)`` becomes its
own sub-path of that base — ``POST {prefix}/invoke/{topic}`` (with a ``/{version}`` segment appended for
a versioned handler, mirroring the ``{version}`` route segment the HTTP binding already understands).
The document therefore reads as one browsable operation per topic, while the operations all live under
the single invoke base the wire binding actually multiplexes them through.

**Schemas are reused, never re-derived.** Every operation's ``requestBody`` and success response
``$ref`` a component in ``components.schemas`` whose value is exactly :func:`benzene.core.json_schema`
of the topic's request/response type — the same 2020-12 subset the spec and descriptor embed (OpenAPI
3.1 adopts the 2020-12 dialect, so the schemas drop in unchanged). Failure responses map the Benzene
failure statuses to HTTP codes through the port's own :func:`benzene.http.to_http` table and share one
``BenzeneError`` problem-details component (``{status, detail}``, wire-contracts §1.3).

**Deterministic output.** As with :class:`~benzene.core.ServiceSpec`, the document is built in a stable
order — topics sorted by ``(id, version)``, paths and component-schema keys sorted lexicographically,
responses ordered by HTTP code — so the same registry always yields an identical, diff-friendly dict.
"""

from __future__ import annotations

import re
from typing import Any

from benzene.core import HandlerDefinition, Registry, Schema, json_schema
from benzene.http import StandardPaths, to_http
from benzene.results import FAILURE_STATUSES

#: The OpenAPI specification version emitted. 3.1 is chosen (over 3.0.3) because it adopts the JSON
#: Schema 2020-12 dialect that :func:`benzene.core.json_schema` already derives, so the embedded
#: schemas are used verbatim rather than down-converted.
OPENAPI_VERSION = "3.1.0"

#: Component name of the shared problem-details failure body every failure response references.
_ERROR_SCHEMA_NAME = "BenzeneError"

#: The Benzene failure envelope (``benzene.core.error_payload`` — wire-contracts §1.3).
_ERROR_SCHEMA: Schema = {
    "type": "object",
    "properties": {
        "status": {"type": "string"},
        "detail": {"type": "string"},
    },
    "required": ["status", "detail"],
}


def openapi_document(
    registry: Registry,
    *,
    title: str = "Benzene service",
    version: str = "1.0.0",
    server_paths: StandardPaths | None = None,
) -> dict[str, Any]:
    """Project ``registry`` into an OpenAPI 3.1 document (the .NET ``Benzene.Schema.OpenApi`` analog).

    One ``POST`` operation is emitted per registered ``(topic, version)`` under the invoke base of
    ``server_paths`` (default :class:`~benzene.http.StandardPaths`, i.e. ``/benzene/invoke``); the
    request/response schemas are :func:`benzene.core.json_schema` of the handler's declared types,
    placed in ``components.schemas`` and ``$ref``-d from the operations. Output is deterministic: the
    same registry yields an identical dict every call.
    """
    paths_config = server_paths or StandardPaths()
    invoke_base = paths_config.invoke_path

    # Topics sorted by (id, version) — the same ordering ServiceSpec.derive uses (spec.py).
    definitions = sorted(registry.definitions(), key=lambda d: (d.topic, d.version))

    paths: dict[str, Any] = {}
    schemas: dict[str, Schema] = {_ERROR_SCHEMA_NAME: dict(_ERROR_SCHEMA)}

    for definition in definitions:
        request_name = _schema_name(definition.topic, definition.version, "Request")
        response_name = _schema_name(definition.topic, definition.version, "Response")
        schemas[request_name] = json_schema(definition.request_type)
        schemas[response_name] = json_schema(definition.response_type)

        path = f"{invoke_base}/{definition.topic}"
        if definition.version:
            path = f"{path}/{definition.version}"
        paths[path] = {"post": _operation(definition, request_name, response_name)}

    return {
        "openapi": OPENAPI_VERSION,
        "info": {"title": title, "version": version},
        # Sort the built maps so the document is byte-stable and diff-friendly.
        "paths": {path: paths[path] for path in sorted(paths)},
        "components": {"schemas": {name: schemas[name] for name in sorted(schemas)}},
    }


def operation_id(topic: str, version: str = "") -> str:
    """The stable ``operationId`` for a ``(topic, version)`` — camelCased topic, version suffixed.

    e.g. ``("orders:place", "v2") -> "ordersPlace_v2"``. Deterministic and unique per registered pair.
    """
    base = _camel(_tokens(topic)) or "operation"
    return f"{base}_{_slug(version)}" if version else base


def _operation(
    definition: HandlerDefinition, request_name: str, response_name: str
) -> dict[str, Any]:
    """The OpenAPI Operation object for one registered handler."""
    summary = f"Invoke topic {definition.topic}"
    if definition.version:
        summary = f"{summary} (version {definition.version})"
    return {
        "operationId": operation_id(definition.topic, definition.version),
        "summary": summary,
        "requestBody": {
            "required": True,
            "content": _json_content(request_name),
        },
        "responses": _responses(response_name),
    }


def _responses(response_name: str) -> dict[str, Any]:
    """The Responses object: 200 (success payload) plus each failure status mapped to its HTTP code."""
    responses: dict[str, Any] = {
        "200": {"description": "ok", "content": _json_content(response_name)}
    }
    # Every Benzene failure status → its HTTP code (benzene.http.to_http), ordered by code so the
    # block is deterministic; each shares the one BenzeneError problem-details body.
    for code, status in sorted({(to_http(status), status) for status in FAILURE_STATUSES}):
        responses[str(code)] = {
            "description": status,
            "content": _json_content(_ERROR_SCHEMA_NAME),
        }
    return responses


def _json_content(schema_name: str) -> dict[str, Any]:
    """An ``application/json`` Media Type object referencing a component schema by name."""
    return {"application/json": {"schema": {"$ref": f"#/components/schemas/{schema_name}"}}}


def _schema_name(topic: str, version: str, role: str) -> str:
    """The ``components.schemas`` key for a topic's request/response — PascalCase topic + role."""
    base = _pascal(_tokens(topic)) or "Topic"
    suffix = f"_{_slug(version)}" if version else ""
    return f"{base}{suffix}{role}"


def _tokens(text: str) -> list[str]:
    """Split an identifier-ish string on any run of non-alphanumeric characters (``:``, ``-``, ``.``)."""
    return [token for token in re.split(r"[^0-9A-Za-z]+", text) if token]


def _camel(tokens: list[str]) -> str:
    if not tokens:
        return ""
    head, *tail = tokens
    return head.lower() + "".join(_capitalize(token) for token in tail)


def _pascal(tokens: list[str]) -> str:
    return "".join(_capitalize(token) for token in tokens)


def _capitalize(token: str) -> str:
    return token[:1].upper() + token[1:]


def _slug(text: str) -> str:
    """A component/operationId-safe slug: non-alphanumeric runs collapsed to ``_``."""
    return re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")
