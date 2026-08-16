"""The service's composition root: one ``BenzeneStartUp``, the domain it wires is chosen at
construction time by ``service_name`` (``orders`` / ``payments`` / ``shipping`` / ``inventory`` /
``notifications`` / ``analytics`` — the ``SERVICE_NAME`` env var, read by ``host.py``). Mirrors
``examples/aws_lambda_mesh/service/startup.py``'s shape, minus the reserved-topic interceptors that
example needs for a direct Lambda invoke: this mesh interrogates over **real HTTP**
(``/benzene/spec``/``/benzene/health`` — :class:`~benzene.http.StandardPaths`), exactly like
``examples/k8s_mesh``, so no ``benzene.mesh`` middleware is needed here at all — the HTTP surface every
Cloud Service already exposes *is* the interrogation surface.

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
    HealthChecks,
    MessageSender,
    Registry,
    Scope,
    ServiceSpec,
)
from benzene.http import HttpRouter, StandardPaths

from .domain import (
    ORDER_CREATE_TOPIC,
    ORDER_PLACED_TOPIC,
    PAYMENT_CAPTURED_TOPIC,
    PAYMENT_TAKE_TOPIC,
    SERVICE_PRODUCES,
    SHIPMENT_BOOK_TOPIC,
    SHIPMENT_DISPATCHED_TOPIC,
    health_checks,
    make_book_shipment,
    make_create_order,
    make_event_consumer,
    make_take_payment,
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
            registry.register(PAYMENT_TAKE_TOPIC, make_take_payment(sender))
        elif name == "shipping":
            assert sender is not None
            registry.register(SHIPMENT_BOOK_TOPIC, make_book_shipment(sender))
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

        checks: HealthChecks = health_checks(name)
        standard = StandardPaths(
            # /benzene/invoke, /benzene/health, /benzene/spec over HTTP — the mesh Function's real,
            # pull-based interrogation surface (HttpServiceSource GETs /benzene/spec + /benzene/health,
            # exactly as examples/k8s_mesh's mesh does).
            health=checks,
            # The registry gives the topics this service *consumes* (a handler registration is a
            # consumer, mesh.md §2.3); SERVICE_PRODUCES declares what it *sends*, so the pulled spec
            # document carries this estate's provider edges too. Declared, never inferred from the
            # send call sites (mesh.md §2.3) — domain.py's map is the one place they're written down.
            spec=ServiceSpec.derive(registry, service=name, produces=SERVICE_PRODUCES[name]),
        )
        return AppDefinition(registry=registry, router=router, standard_paths=standard)
