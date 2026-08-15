"""The service's composition root: one ``BenzeneStartUp``, the domain it wires is chosen at
construction time by ``service_name`` (``orders`` / ``payments`` / ``shipping`` / ``inventory`` /
``notifications`` / ``analytics`` — the ``SERVICE_NAME`` env var, read by ``host.py``). Mirrors
``examples/k8s_mesh/service/startup.py``'s shape, extended with the two reserved-topic interceptors a
*direct Lambda invoke* interrogation needs (a Lambda has no HTTP GET the mesh can poll, unlike K8s):

- :func:`~benzene.mesh.mesh_interception` answers ``benzene:mesh`` with the derived
  :class:`~benzene.mesh.ServiceDescriptor` — what the mesh's ``spec`` interrogation invokes.
- :func:`~benzene.core.health_interception` answers ``benzene:healthcheck`` with the health aggregate
  — what the mesh's ``health`` interrogation invokes.

Both are *also* reachable over HTTP (``StandardPaths``, R3/R5) for the API Gateway front door every
service Lambda gets, but the mesh in this example always interrogates by direct invoke.

The outbound ``MessageSender`` (a real per-topic router in deployment — see ``host.py`` — a
``FakeMessageSender`` in a test) is supplied by the host/test overriding one registration; the three
terminal consumers (inventory/notifications/analytics) send nothing downstream and never resolve one.
"""

from __future__ import annotations

from collections.abc import Mapping

from benzene.core import (
    AppDefinition,
    BenzeneStartUp,
    Container,
    MessageSender,
    Registry,
    Scope,
    ServiceSpec,
    health_interception,
)
from benzene.http import HttpRouter, StandardPaths
from benzene.mesh import ServiceDescriptor, ServiceInfo, mesh_interception

from .domain import (
    ORDER_CREATE_TOPIC,
    ORDER_PLACED_TOPIC,
    PAYMENT_CAPTURED_TOPIC,
    PAYMENTS_CAPTURE_TOPIC,
    SHIPMENT_DISPATCHED_TOPIC,
    SHIPPING_BOOK_TOPIC,
    health_checks,
    make_book_shipment,
    make_capture_payment,
    make_create_order,
    make_event_consumer,
)

#: The three services that send anything downstream — the only ones that need a real MessageSender.
_SENDING_SERVICES = frozenset({"orders", "payments", "shipping"})

KNOWN_SERVICES = (
    "orders",
    "payments",
    "shipping",
    "inventory",
    "notifications",
    "analytics",
)


class ServiceStartUp(BenzeneStartUp):
    """Registers the domain ``service_name`` selects and wires its HTTP + topic routes."""

    def __init__(self, service_name: str) -> None:
        if service_name not in KNOWN_SERVICES:
            raise ValueError(
                f"Unknown mesh service {service_name!r}; expected one of {KNOWN_SERVICES}"
            )
        self._service_name = service_name

    def configure_services(self, services: Container, config: Mapping[str, str]) -> None:
        pass  # no shared singletons needed — every handler closes over its own sender

    def configure(self, services: Scope, config: Mapping[str, str]) -> AppDefinition:
        name = self._service_name
        sender = services.get_service(MessageSender) if name in _SENDING_SERVICES else None

        router = HttpRouter()
        registry = Registry()
        if name == "orders":
            assert sender is not None
            router.register("POST", "/orders", ORDER_CREATE_TOPIC, make_create_order(sender))
            registry = Registry.from_definitions(router)
        elif name == "payments":
            assert sender is not None
            registry.register(PAYMENTS_CAPTURE_TOPIC, make_capture_payment(sender))
        elif name == "shipping":
            assert sender is not None
            registry.register(SHIPPING_BOOK_TOPIC, make_book_shipment(sender))
        elif name == "inventory":
            registry.register(ORDER_PLACED_TOPIC, make_event_consumer("inventory", ORDER_PLACED_TOPIC))
            registry.register(
                SHIPMENT_DISPATCHED_TOPIC, make_event_consumer("inventory", SHIPMENT_DISPATCHED_TOPIC)
            )
        elif name == "notifications":
            registry.register(
                ORDER_PLACED_TOPIC, make_event_consumer("notifications", ORDER_PLACED_TOPIC)
            )
            registry.register(
                PAYMENT_CAPTURED_TOPIC, make_event_consumer("notifications", PAYMENT_CAPTURED_TOPIC)
            )
            registry.register(
                SHIPMENT_DISPATCHED_TOPIC,
                make_event_consumer("notifications", SHIPMENT_DISPATCHED_TOPIC),
            )
        else:  # analytics
            registry.register(
                PAYMENT_CAPTURED_TOPIC, make_event_consumer("analytics", PAYMENT_CAPTURED_TOPIC)
            )
            registry.register(
                SHIPMENT_DISPATCHED_TOPIC, make_event_consumer("analytics", SHIPMENT_DISPATCHED_TOPIC)
            )

        checks = health_checks(name)
        descriptor = ServiceDescriptor.derive(registry, ServiceInfo(service=name))
        standard = StandardPaths(
            # /benzene/invoke, /benzene/health, /benzene/spec over HTTP (API Gateway) — plus, via the
            # two interceptors below, the *same* benzene:mesh / benzene:healthcheck topics answer a
            # direct Lambda invoke too, which is how this example's mesh actually interrogates.
            health=checks,
            spec=ServiceSpec.derive(registry, service=name),
        )
        middleware = [mesh_interception(descriptor), health_interception(checks)]
        return AppDefinition(
            registry=registry, router=router, middleware=middleware, standard_paths=standard
        )
