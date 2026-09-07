"""Reading a service's ``/benzene/spec`` whatever shape it is in.

Two documents answer at ``/benzene/spec``, and mesh tooling that only understands one of them is
tooling that only grades its own port:

* the **Contract Document** (contract-document.md) — ``{openapi, info, requests[], events[],
  components}`` — what the Cloud Service Profile's R5 requires and what every language's client
  generator parses. This is what a .NET, Go, or TypeScript service serves, and what this port now
  serves by default.
* this port's **native** ``{service, topics[, produces]}`` payload
  (:class:`~benzene.core.ServiceSpec`), still reachable at ``?type=native``.

The probe and the poller both need three things out of either shape — the service name, the topics
it consumes, and the topics it produces — so the reading lives here once rather than being written
twice and drifting. Topics come back in the *native* entry shape (``{id, version?, requestSchema,
responseSchema}``), because that is what :class:`~benzene.mesh.MeshCollector` ingests.

Reserved Benzene utility topics (``benzene:spec``, ``benzene:healthcheck``, …) are dropped from both
shapes, by contract-document.md §5.1's prefix rule and its ``reserved`` flag. A Contract Document
lists them where the native payload never did, and folding framework plumbing into a fleet's
producer/consumer graph would make the same service look different depending on which shape it
happened to serve.
"""

from __future__ import annotations

from typing import Any

#: The reserved-topic prefix (contract-document.md §5.1).
RESERVED_TOPIC_PREFIX = "benzene:"

#: The only ``$ref`` form a Contract Document uses (contract-document.md §4).
_SCHEMA_REF_PREFIX = "#/components/schemas/"


def is_reserved_topic(topic: str) -> bool:
    return topic.startswith(RESERVED_TOPIC_PREFIX)


def is_contract_document(document: dict[str, Any]) -> bool:
    """Whether ``document`` is a Contract Document rather than the native payload.

    ``requests`` and ``events`` are both REQUIRED and always present (contract-document.md §1), so
    the pair identifies the shape without leaning on ``openapi`` — which §1 explicitly forbids
    rejecting a document over, and which a lenient producer could therefore omit.
    """
    return isinstance(document.get("requests"), list) and isinstance(document.get("events"), list)


def is_native_document(document: dict[str, Any]) -> bool:
    """Whether ``document`` is this port's native ``{service, topics}`` spec payload."""
    return "service" in document and isinstance(document.get("topics"), list)


def is_spec_document(document: dict[str, Any]) -> bool:
    """Whether ``document`` is a derived spec document at all, in either shape."""
    return is_contract_document(document) or is_native_document(document)


def spec_service(document: dict[str, Any], default: str = "") -> str:
    """The service name — native ``service``, or the Contract Document's ``info.title`` (§1)."""
    native = document.get("service")
    if native:
        return str(native)
    info = document.get("info")
    if isinstance(info, dict) and info.get("title"):
        return str(info["title"])
    return default


def spec_topics(document: dict[str, Any]) -> list[dict[str, Any]]:
    """The topics the service **consumes**, as native topic entries."""
    if is_contract_document(document):
        schemas = _catalogue(document)
        return [
            entry
            for entry in (
                _from_request(request, schemas)
                for request in document.get("requests") or []
                if isinstance(request, dict)
            )
            if entry is not None
        ]
    return _native_topics(document.get("topics"))


def spec_produces(document: dict[str, Any]) -> list[dict[str, Any]]:
    """The topics the service **produces**, as native topic entries."""
    if is_contract_document(document):
        schemas = _catalogue(document)
        return [
            entry
            for entry in (
                _from_event(event, schemas)
                for event in document.get("events") or []
                if isinstance(event, dict)
            )
            if entry is not None
        ]
    return _native_topics(document.get("produces"))


def _native_topics(topics: Any) -> list[dict[str, Any]]:
    if not isinstance(topics, list):
        return []
    return [
        topic
        for topic in topics
        if isinstance(topic, dict) and topic.get("id") and not is_reserved_topic(str(topic["id"]))
    ]


def _catalogue(document: dict[str, Any]) -> dict[str, Any]:
    components = document.get("components")
    schemas = components.get("schemas") if isinstance(components, dict) else None
    return schemas if isinstance(schemas, dict) else {}


def _from_request(request: dict[str, Any], schemas: dict[str, Any]) -> dict[str, Any] | None:
    topic = request.get("topic")
    # Both halves of §5.1's detection rule: the flag, and the prefix a producer may not have flagged.
    if not topic or request.get("reserved") is True or is_reserved_topic(str(topic)):
        return None
    return _entry(
        str(topic),
        request.get("version"),
        requestSchema=_resolve(request.get("request"), schemas),
        responseSchema=_resolve(request.get("response"), schemas),
    )


def _from_event(event: dict[str, Any], schemas: dict[str, Any]) -> dict[str, Any] | None:
    topic = event.get("topic")
    if not topic or is_reserved_topic(str(topic)):
        return None
    # A produced event's payload is its request half; there is no response to a fire-and-forget
    # event (§3), so the native entry carries no responseSchema rather than an empty one.
    return _entry(
        str(topic), event.get("version"), requestSchema=_resolve(event.get("message"), schemas)
    )


def _entry(topic: str, version: Any, **schemas: dict[str, Any]) -> dict[str, Any]:
    entry: dict[str, Any] = {"id": topic}
    # Absent and empty are not the same thing (§2), and the native entry omits an empty version too.
    if version:
        entry["version"] = str(version)
    for name, schema in schemas.items():
        if schema:
            entry[name] = schema
    return entry


def _resolve(schema: Any, schemas: dict[str, Any], seen: tuple[str, ...] = ()) -> dict[str, Any]:
    """A schema object with its ``$ref`` s replaced by the catalogue entries they name.

    The collector stores and compares payload schemas across a fleet, so a reference into a
    catalogue the collector never sees would compare as "the shape changed" the moment a producer
    renamed a class. ``seen`` cuts a reference cycle by leaving the innermost ``$ref`` in place —
    the same guard contract-document.md §5.3's closure walk uses, for the same reason.
    """
    if not isinstance(schema, dict):
        return {}
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref.startswith(_SCHEMA_REF_PREFIX):
        name = ref[len(_SCHEMA_REF_PREFIX) :]
        if name in seen or name not in schemas:
            return dict(schema)
        return _resolve(schemas[name], schemas, (*seen, name))
    return {key: _resolve_value(value, schemas, seen) for key, value in schema.items()}


def _resolve_value(value: Any, schemas: dict[str, Any], seen: tuple[str, ...]) -> Any:
    if isinstance(value, dict):
        return _resolve(value, schemas, seen)
    if isinstance(value, list):
        return [_resolve_value(item, schemas, seen) for item in value]
    return value
