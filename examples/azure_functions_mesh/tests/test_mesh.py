"""In-memory tests for the mesh's discover -> interrogate -> publish pass (``mesh/discovery_service.py``).

Fakes the two external edges only: a fake :class:`~benzene.mesh_fleet.Discovery` stands in for the real
Azure Resource Manager ``resources.list`` call, and an injected HTTP ``fetch`` routes straight to each
service's real, in-memory :class:`~benzene.azure.AzureFunctionsApp` (built from the *actual*
``ServiceStartUp`` composition root, exactly as ``service/host.py`` builds it for deployment) — so the
interrogation exercises the real ``/benzene/spec``/``/benzene/health`` handling over HTTP, not a stub.
A fake Blob ``ContainerClient`` captures what gets published, matching
``tests/test_mesh_blob_artifacts.py``'s pattern one level up (that file tests ``BlobArtifactStore``
itself; this proves the example wires it correctly end to end) — proving discover -> poll -> collector
-> Blob catalog without a single real Azure call.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

from benzene.core import Container, MessageSender, build_application
from benzene.mesh import BlobArtifactStore
from benzene.mesh_fleet import ServiceEndpoint

from azure_functions_mesh.mesh.discovery_service import azure_service_source, run_mesh_aggregation
from azure_functions_mesh.service.domain import (
    ORDER_CREATE_TOPIC,
    ORDER_PLACED_TOPIC,
    PAYMENT_CAPTURED_TOPIC,
    PAYMENT_TAKE_TOPIC,
    SERVICE_PRODUCES,
    SERVICE_TOPICS,
    SHIPMENT_BOOK_TOPIC,
    SHIPMENT_DISPATCHED_TOPIC,
)
from azure_functions_mesh.service.startup import KNOWN_SERVICES, ServiceStartUp

if TYPE_CHECKING:
    from benzene.azure import AzureFunctionsApp

_TAG_METADATA = {"benzene:service": "true"}


class _FakeDiscovery:
    """A :class:`~benzene.mesh_fleet.Discovery` returning a fixed endpoint list."""

    def __init__(self, endpoints: list[ServiceEndpoint]) -> None:
        self._endpoints = endpoints

    async def discover(self) -> list[ServiceEndpoint]:
        return list(self._endpoints)


class _FakeContainerClient:
    def __init__(self) -> None:
        self.blobs: dict[str, dict] = {}

    def upload_blob(self, name, data, *, overwrite, content_settings=None):
        self.blobs[name] = json.loads(data.decode("utf-8"))


class _NullSender:
    async def send_message(self, topic, message, headers=None):
        from benzene.results import Result

        return Result.accepted()


def _build_app(name: str) -> AzureFunctionsApp:
    from benzene.azure import AzureFunctionsApp

    def overrides(services: Container) -> None:
        services.add_instance(MessageSender, _NullSender())

    definition, _ = build_application(ServiceStartUp(name), overrides=[overrides])
    return AzureFunctionsApp.from_definition(definition)


def _endpoint(name: str) -> ServiceEndpoint:
    # Mirrors real AzureDiscovery: address is a bare hostname (no scheme) — azure_service_source is the
    # one place that prepends https:// before polling (see discovery_service.py).
    return ServiceEndpoint(name=name, address=f"{name}.azurewebsites.net", metadata=_TAG_METADATA)


def _fetch_via_apps(apps: dict[str, AzureFunctionsApp]):
    """Build an injectable HTTP GET that routes ``https://{name}.azurewebsites.net/benzene/{spec|health}``
    straight to the matching in-memory :class:`~benzene.azure.AzureFunctionsApp`, so the poll exercises
    the real HTTP handling with no network and no real Azure Functions host."""

    async def fetch(url: str) -> tuple[int, str]:
        # url looks like "https://<name>.azurewebsites.net/benzene/spec". AzureFunctionsApp.handle_http
        # is sync and calls asyncio.run() internally, so it must run off-thread here — the poller
        # already awaits this fetch from inside its own running event loop.
        host = url.split("//", 1)[1].split("/", 1)[0]
        name = host.removesuffix(".azurewebsites.net")
        path = "/" + url.split("/", 3)[3]
        response = await asyncio.to_thread(apps[name].handle_http, method="GET", path=path)
        return response.status_code, response.body

    return fetch


def _fleet() -> tuple[list[ServiceEndpoint], dict]:
    endpoints = [_endpoint(name) for name in KNOWN_SERVICES]
    apps = {name: _build_app(name) for name in KNOWN_SERVICES}
    return endpoints, apps


def test_run_mesh_aggregation_discovers_and_interrogates_all_six_services() -> None:
    endpoints, apps = _fleet()
    fetch = _fetch_via_apps(apps)
    container = _FakeContainerClient()
    store = BlobArtifactStore(prefix="mesh", client=container)

    def source_factory(endpoint: ServiceEndpoint):
        return azure_service_source(endpoint, fetch=fetch)

    summary = asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery(endpoints),
            store=store,
            source_factory=source_factory,
            generated_at="2026-08-15T00:00:00+00:00",
        )
    )

    assert summary.discovered == 6


def test_run_mesh_aggregation_publishes_registry_and_full_catalog_to_blob() -> None:
    endpoints, apps = _fleet()
    fetch = _fetch_via_apps(apps)
    container = _FakeContainerClient()
    store = BlobArtifactStore(prefix="mesh", client=container)

    def source_factory(endpoint: ServiceEndpoint):
        return azure_service_source(endpoint, fetch=fetch)

    asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery(endpoints),
            store=store,
            source_factory=source_factory,
            generated_at="2026-08-15T00:00:00+00:00",
        )
    )

    assert {
        "mesh/registry.json",
        "mesh/manifest.json",
        "mesh/topology.json",
        "mesh/topics.json",
        "mesh/usage.json",
        "mesh/asyncapi.json",
        "mesh/annotations.json",
        *(f"mesh/services/{name}.json" for name in KNOWN_SERVICES),
    } == set(container.blobs)

    # registry.json reflects DISCOVERY-time identity: the bare hostnames AzureDiscovery hands back.
    registry = container.blobs["mesh/registry.json"]
    assert {s["name"] for s in registry["services"]} == set(KNOWN_SERVICES)
    assert all(s["address"].endswith(".azurewebsites.net") for s in registry["services"])

    # manifest.json (and the rest of the catalog) reflect INTERROGATION-time identity: each service's
    # own descriptor "service" field, read off the /benzene/spec response.
    manifest = container.blobs["mesh/manifest.json"]
    manifest_services = {s["name"]: s for s in manifest["services"]}
    assert set(manifest_services) == set(KNOWN_SERVICES)
    assert all(s["status"] == "healthy" for s in manifest_services.values())

    topics = {t["topic"] for t in container.blobs["mesh/topics.json"]["topics"]}
    assert topics == {topic for topics_ in SERVICE_TOPICS.values() for topic in topics_}


def test_the_published_catalog_carries_the_whole_declared_producer_consumer_graph() -> None:
    # The point of the estate: after one discover -> interrogate -> publish pass, every topic in the
    # catalog must name BOTH sides — the providers each producing service declares on its
    # /benzene/spec (SERVICE_PRODUCES, mesh.md §2.3) and the consumers derived from the handler
    # registrations. A topic with consumers and an empty producers list means the declared provider
    # edge never made it across the pull path, which is exactly the failure this asserts against.
    endpoints, apps = _fleet()
    container = _FakeContainerClient()
    store = BlobArtifactStore(prefix="mesh", client=container)

    def source_factory(endpoint: ServiceEndpoint):
        return azure_service_source(endpoint, fetch=_fetch_via_apps(apps))

    asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery(endpoints),
            store=store,
            source_factory=source_factory,
            generated_at="2026-08-15T00:00:00+00:00",
        )
    )

    topics = {t["topic"]: t for t in container.blobs["mesh/topics.json"]["topics"]}
    graph = {
        topic: (
            [p["service"] for p in entry["producers"]],
            [c["service"] for c in entry["consumers"]],
        )
        for topic, entry in topics.items()
    }
    assert graph == {
        # orders' HTTP ingress: nothing in this estate declares it (a browser/curl produces it).
        ORDER_CREATE_TOPIC: ([], ["orders"]),
        PAYMENT_TAKE_TOPIC: (["orders"], ["payments"]),  # Service Bus, orders -> payments
        ORDER_PLACED_TOPIC: (["orders"], ["inventory", "notifications"]),  # Event Hub fan-out
        SHIPMENT_BOOK_TOPIC: (["payments"], ["shipping"]),  # Service Bus, payments -> shipping
        PAYMENT_CAPTURED_TOPIC: (["payments"], ["analytics", "notifications"]),  # Event Grid
        SHIPMENT_DISPATCHED_TOPIC: (  # Event Grid, shipping -> the three terminal consumers
            ["shipping"],
            ["analytics", "inventory", "notifications"],
        ),
    }

    # ...and the same graph is what the topology document draws its service-to-service edges from.
    edges = {
        (edge["client"], edge["server"]) for edge in container.blobs["mesh/topology.json"]["edges"]
    }
    assert ("orders", "payments") in edges
    assert ("payments", "shipping") in edges


def test_every_interrogated_spec_document_declares_what_that_service_produces() -> None:
    # The pull path's contract, service by service: /benzene/spec must carry `produces` for the three
    # producers and omit it for the three terminal consumers (which genuinely send nothing).
    apps = {name: _build_app(name) for name in KNOWN_SERVICES}
    declared = {}
    for name, app in apps.items():
        document = json.loads(app.handle_http(method="GET", path="/benzene/spec").body)
        # events[] is where a Contract Document declares what a service produces (§3).
        declared[name] = [event["topic"] for event in document.get("events", [])]

    assert declared == {name: sorted(SERVICE_PRODUCES[name]) for name in KNOWN_SERVICES}


def test_run_mesh_aggregation_empty_mesh_is_zero_not_an_error() -> None:
    container = _FakeContainerClient()
    store = BlobArtifactStore(client=container)

    summary = asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery([]),
            store=store,
            generated_at="2026-08-15T00:00:00+00:00",
        )
    )

    assert summary.discovered == 0
    assert container.blobs["registry.json"]["services"] == []
    assert container.blobs["manifest.json"]["services"] == []


def test_azure_service_source_prepends_https_to_the_bare_hostname() -> None:
    endpoint = ServiceEndpoint(name="orders", address="orders.azurewebsites.net")
    source = azure_service_source(endpoint)
    assert source.spec_url == "https://orders.azurewebsites.net/benzene/spec"
    assert source.health_url == "https://orders.azurewebsites.net/benzene/health"
