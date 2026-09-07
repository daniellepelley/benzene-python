"""The Contract Document a service derives and serves at ``/benzene/spec`` (profile R5).

``contract-document.md`` is the shape every language's client generator parses; this port used to
serve its own ``{service, topics}`` payload there instead, which meant a .NET/Go/TypeScript
generator pointed at a Python service could not read the response at all — and this port's own mesh
tooling could not read a .NET service's.

The cheapest check that the writer got the shape right is the reader this repo already ships:
``benzene.codegen_client`` *parses* this format, so a document derived here is fed straight through
it. Note what that does and does not prove — see ``test_the_fixtures_pin_the_reader_not_the_writer``.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

from benzene.codegen_client import (
    compute_contract_hash,
    generate_service_client,
    parse_document,
)
from benzene.core import (
    BenzeneMessageApplication,
    ContractDocument,
    HttpMapping,
    Registry,
    ServiceSpec,
    message,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths, http_endpoint
from benzene.results import Result

from .conformance_runner import CONFORMANCE_DIR


@dataclass
class PlaceOrder:
    sku: str
    quantity: int = 1


@dataclass
class OrderDto:
    id: str
    sku: str


@message("orders:place", response_type=OrderDto)
@http_endpoint("POST", "/orders")
async def place(request: PlaceOrder) -> Result:
    return Result.created(OrderDto(id="1", sku=request.sku))


@message("orders:list")
async def list_orders(request: dict) -> Result:
    return Result.ok([])


def _router() -> HttpRouter:
    return HttpRouter().add(place).register("GET", "/orders", "orders:list", list_orders)


def _app(**standard: object) -> BenzeneHttpApp:
    router = _router()
    registry = Registry.from_definitions(router)
    return BenzeneHttpApp(
        router,
        application=BenzeneMessageApplication(registry),
        standard_paths=StandardPaths(
            spec=ServiceSpec.derive(registry, service="orders", produces=("order:placed",)),
            **standard,  # type: ignore[arg-type]
        ),
    )


def _get(app: BenzeneHttpApp, query: str = "") -> dict:
    response = asyncio.run(app.handle("GET", "/benzene/spec", query))
    assert response.status_code == 200
    return json.loads(response.body)


# --- the projection (contract-document.md §§1-3) --------------------------------------------------


def test_the_registry_projects_into_a_contract_document() -> None:
    document = ContractDocument.derive(
        Registry.from_definitions(_router()),
        service="orders",
        version="1.0.0",
        message_endpoint="/benzene/invoke",
        produces=("order:placed",),
        http_mappings={("orders:place", ""): [HttpMapping("POST", "/orders")]},
    ).to_payload()

    assert document["openapi"] == "3.0.1"
    assert document["info"] == {"title": "orders", "description": "", "version": "1.0.0"}
    assert document["messageEndpoint"] == "/benzene/invoke"
    assert [r["topic"] for r in document["requests"]] == ["orders:list", "orders:place"]
    assert [e["topic"] for e in document["events"]] == ["order:placed"]

    placed = next(r for r in document["requests"] if r["topic"] == "orders:place")
    assert placed["httpMappings"] == [{"method": "POST", "path": "/orders"}]
    # A declared dataclass payload is named once in the catalogue and referenced by $ref, so a
    # generator can emit one named type per payload rather than one anonymous type per topic.
    assert placed["request"] == {"$ref": "#/components/schemas/PlaceOrder"}
    assert placed["response"] == {"$ref": "#/components/schemas/OrderDto"}
    schemas = document["components"]["schemas"]
    assert schemas["PlaceOrder"]["properties"]["sku"] == {"type": "string"}
    assert schemas["PlaceOrder"]["required"] == ["sku"]  # quantity has a default


def test_an_untyped_handler_writes_its_schema_inline_rather_than_naming_nothing() -> None:
    document = ContractDocument.derive(
        Registry.from_definitions(_router()), service="orders"
    ).to_payload()
    listing = next(r for r in document["requests"] if r["topic"] == "orders:list")
    # §2 takes a Schema Object inline or by $ref; a `dict`-typed handler has no name worth
    # publishing, and cataloguing it would fill components.schemas with entries meaning nothing.
    assert listing["request"] == {} and listing["response"] == {}
    assert "dict" not in document["components"]["schemas"]


def test_presence_rules_are_the_specs_and_not_the_convenient_ones() -> None:
    document = ContractDocument.derive(Registry(), service="").to_payload()
    # transports: OPTIONAL, omitted when empty — never an empty array (§1).
    assert "transports" not in document
    # messageEndpoint: absent when the service exposes none — consumers feature-detect on it (§1).
    assert "messageEndpoint" not in document
    # info: REQUIRED; a producer with neither title nor version writes empty strings (§1).
    assert document["info"] == {"title": "", "description": "", "version": ""}
    # requests/events/components: REQUIRED, always present, possibly empty.
    assert document["requests"] == [] and document["events"] == []
    assert document["components"] == {"schemas": {}}


def test_an_unversioned_entry_omits_version_entirely() -> None:
    registry = Registry().register("orders:place", place, version="v2")
    document = ContractDocument.derive(registry, service="orders").to_payload()
    assert document["requests"][0]["version"] == "v2"

    unversioned = ContractDocument.derive(
        Registry.from_definitions(_router()), service="orders"
    ).to_payload()
    # Absent and empty are not the same thing (§2): a producer MUST NOT write "version": "".
    assert all("version" not in entry for entry in unversioned["requests"])


def test_a_reserved_topic_carries_the_flag_and_a_domain_topic_carries_nothing() -> None:
    registry = Registry.from_definitions(_router()).register("benzene:ping", list_orders)
    document = ContractDocument.derive(registry, service="orders").to_payload()
    entries = {r["topic"]: r for r in document["requests"]}
    assert entries["benzene:ping"]["reserved"] is True
    # Never written as false — its absence also means not-reserved (§2).
    assert "reserved" not in entries["orders:place"]


# --- the reader this repo already ships is the writer's cheapest check ----------------------------


def test_the_derived_document_parses_with_this_repos_own_contract_document_reader() -> None:
    raw = ContractDocument.derive(
        Registry.from_definitions(_router()),
        service="orders",
        message_endpoint="/benzene/invoke",
        produces=("order:placed",),
        transports=("http", "sqs"),
        http_mappings={("orders:place", ""): [HttpMapping("POST", "/orders")]},
    ).to_payload()

    parsed = parse_document(raw)
    assert parsed.topics() == ("orders:list", "orders:place")
    assert parsed.message_endpoint == "/benzene/invoke"
    assert parsed.transports == ("http", "sqs")
    assert set(parsed.schemas) == {"PlaceOrder", "OrderDto"}
    placed = parsed.find_request("orders:place")
    assert placed is not None
    assert placed.http_mappings[0].method == "POST"
    assert not placed.is_reserved()

    # And the whole point of the format: a client generator can consume it end to end.
    generated = generate_service_client(parsed, service_name="Orders")
    assert generated.required_topics == ("orders:list", "orders:place")
    assert "PlaceOrder" in generated.source  # the named catalogue becomes a named generated type
    assert generated.contract_hash.startswith("sha256:")
    assert generated.contract_hash == compute_contract_hash(raw, topic_scoped=False)


def test_the_fixtures_pin_the_reader_not_the_writer() -> None:
    """What ``conformance/contract-*-cases.json`` actually pins, stated so nobody overclaims it.

    Every case in both files starts from an **input document** and asserts what a consumer must make
    of it — parse/validate cases, topic-scope projections, schema closures, exact hash values. None
    starts from a producer's input: there is no fixture saying "a registry shaped like this must emit
    that document", so this port's projection is checked only against the format's own presence
    rules (§1-§3, the tests above) and by surviving a conformant reader (the test above this one).

    This asserts that reading of the fixtures rather than describing it, so a re-vendor that *does*
    add producer-side cases fails here instead of quietly leaving them unimplemented.
    """
    document_cases = json.loads((CONFORMANCE_DIR / "contract-document-cases.json").read_text())
    hash_cases = json.loads((CONFORMANCE_DIR / "contract-hash-cases.json").read_text())

    groups = ["parseCases", "topicScopeCases", "schemaClosureCases"]
    assert [group for group in document_cases if isinstance(document_cases[group], list)] == groups
    for group in groups:
        assert all("documentRef" in case for case in document_cases[group])
    assert all("document" in case for case in hash_cases["cases"])


# --- the served surface (R5, the ?type= switch) ---------------------------------------------------


def test_benzene_spec_serves_the_contract_document_by_default() -> None:
    document = _get(_app())
    assert document["openapi"] == "3.0.1"
    assert document["info"]["title"] == "orders"
    assert {r["topic"] for r in document["requests"]} == {"orders:place", "orders:list"}
    assert [e["topic"] for e in document["events"]] == ["order:placed"]
    # The host folds in what only it knows: its message endpoint and each topic's routes.
    assert document["messageEndpoint"] == "/benzene/invoke"
    placed = next(r for r in document["requests"] if r["topic"] == "orders:place")
    assert placed["httpMappings"] == [{"method": "POST", "path": "/orders"}]
    assert placed["request"]["properties"]["sku"] == {"type": "string"}  # inline, from the spec


def test_the_native_shape_stays_reachable_under_type_native() -> None:
    document = _get(_app(), "type=native")
    assert document["service"] == "orders"
    assert {t["id"] for t in document["topics"]} == {"orders:place", "orders:list"}
    assert [t["id"] for t in document["produces"]] == ["order:placed"]


def test_an_unrecognised_type_answers_the_contract_document() -> None:
    # The R5 spelling is ?type=benzene&format=json; anything else falls through to the same
    # document, matching the .NET reference's SpecBuilder rather than 400-ing a caller.
    assert _get(_app(), "type=benzene&format=json")["openapi"] == "3.0.1"
    assert _get(_app(), "type=asyncapi")["openapi"] == "3.0.1"


def test_declared_transports_reach_the_served_document() -> None:
    assert _get(_app(transports=("http", "sqs")))["transports"] == ["http", "sqs"]
    assert "transports" not in _get(_app())


def test_an_authored_contract_document_is_served_verbatim() -> None:
    # ContractDocument.derive sees the handlers' declared types, so an authored document is how a
    # service gets named component schemas rather than inline ones.
    router = _router()
    registry = Registry.from_definitions(router)
    app = BenzeneHttpApp(
        router,
        application=BenzeneMessageApplication(registry),
        standard_paths=StandardPaths(
            spec=ServiceSpec.derive(registry, service="orders"),
            contract=ContractDocument.derive(
                registry, service="orders", message_endpoint="/benzene/invoke"
            ),
        ),
    )
    document = _get(app)
    assert set(document["components"]["schemas"]) == {"PlaceOrder", "OrderDto"}
    # …and ?type=native still answers the native payload alongside it.
    assert _get(app, "type=native")["service"] == "orders"


def test_the_spec_surface_stays_off_when_no_source_is_wired() -> None:
    router = _router()
    app = BenzeneHttpApp(
        router,
        application=BenzeneMessageApplication(Registry.from_definitions(router)),
        standard_paths=StandardPaths(),
    )
    assert asyncio.run(app.handle("GET", "/benzene/spec")).status_code == 404


def test_an_authored_contract_alone_enables_the_surface() -> None:
    # `contract` without `spec` is a service that only serves R5's document; the surface must come up
    # for it rather than 404-ing because the older `spec` source happened to be the enabling switch.
    router = _router()
    registry = Registry.from_definitions(router)
    app = BenzeneHttpApp(
        router,
        application=BenzeneMessageApplication(registry),
        standard_paths=StandardPaths(contract=ContractDocument.derive(registry, service="orders")),
    )
    assert _get(app)["info"]["title"] == "orders"
    # …and ?type=native is honestly answered "not served here" rather than quietly given the other
    # document under a name that does not describe it.
    assert asyncio.run(app.handle("GET", "/benzene/spec", "type=native")).status_code == 404
