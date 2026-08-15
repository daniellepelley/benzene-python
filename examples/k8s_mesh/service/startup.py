"""The service's composition root: one ``BenzeneStartUp``, the domain it wires is chosen at
construction time by ``service_name`` (``orders`` / ``payments`` / ``shipping`` — the ``MESH_SERVICE``
env var, read by ``host.py``). Mirrors ``examples/orders_domain/startup.py``'s shape, extended with:

- a ``QueueTraceExporter`` (registered so ``host.py`` can pull the same instance both into the
  pipeline's ``trace_middleware`` and into the mesh reporter that drains it), and
- the Cloud Service Profile's well-known surfaces (``StandardPaths`` — health, derived spec, and the
  generic ``/benzene/invoke`` envelope endpoint every downstream hop and the mesh's poller both use).

The outbound ``MessageSender`` (a real ``EnvelopeHttpMessageSender`` in deployment, a
``FakeMessageSender`` in a test) is supplied by the host/test overriding one registration — nothing
else changes between "deployed to Kubernetes" and "driven in memory in a test".
"""

from __future__ import annotations

from collections.abc import Mapping

from benzene.core import (
    AppDefinition,
    BenzeneStartUp,
    Container,
    HealthChecks,
    MessageSender,
    Registry,
    Scope,
    ServiceSpec,
)
from benzene.http import HttpRouter, StandardPaths
from benzene.mesh import QueueTraceExporter, trace_middleware

from .domain import (
    ORDER_CREATE_TOPIC,
    PAYMENT_TAKE_TOPIC,
    SHIPMENT_BOOK_TOPIC,
    make_book_shipment,
    make_create_order,
    make_take_payment,
)


class ServiceStartUp(BenzeneStartUp):
    """Registers the domain the ``service_name`` selects and wires its HTTP + envelope routes."""

    def __init__(self, service_name: str) -> None:
        self._service_name = service_name

    def configure_services(self, services: Container, config: Mapping[str, str]) -> None:
        services.try_add_singleton(QueueTraceExporter)

    def configure(self, services: Scope, config: Mapping[str, str]) -> AppDefinition:
        name = self._service_name
        downstream = services.get_service(MessageSender)  # a host/test must register this
        exporter = services.get_service(QueueTraceExporter)

        router = HttpRouter()
        if name == "payments":
            router.register("POST", "/payments", PAYMENT_TAKE_TOPIC, make_take_payment(downstream))
        elif name == "shipping":
            router.register("POST", "/shipments", SHIPMENT_BOOK_TOPIC, make_book_shipment())
        else:
            router.register("POST", "/orders", ORDER_CREATE_TOPIC, make_create_order(downstream))

        registry = Registry.from_definitions(router)
        standard = StandardPaths(
            # /benzene/invoke (on by default) is the generic envelope endpoint every downstream hop
            # (DOWNSTREAM_MSG_URL) and the mesh's poller / interrogation both reach this service
            # through — "one endpoint serves every topic, routed by the envelope's topic", exactly
            # like .NET's POST /benzene-message.
            health=HealthChecks().add("self", lambda: True),
            spec=ServiceSpec.derive(registry, service=name),
        )
        # trace_middleware installed OUTERMOST so it times the whole invocation (including the
        # /benzene/invoke path, which runs through this same pipeline) and joins/starts the W3C
        # trace the mesh's Fleet plane later derives consumer edges from.
        middleware = [trace_middleware(exporter, service=name, instance_id=name)]
        return AppDefinition(
            registry=registry, router=router, middleware=middleware, standard_paths=standard
        )
