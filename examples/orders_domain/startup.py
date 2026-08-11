"""The order domain's composition root — one ``BenzeneStartUp`` every host and test boots from.

This is the "boot the real app from its startup" seam (test-champion invariant 1): it registers the
domain's services and wires its routes/topics, independent of any cloud. A host or a test supplies the
outbound ``MessageSender`` (the real cloud client for deployment, a ``FakeMessageSender`` in a test) by
overriding that one registration — nothing else changes between clouds. If a host forgets to register a
sender, ``configure`` fails fast with ``ServiceNotRegisteredError`` naming the missing dependency.
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
)
from benzene.http import HttpRouter

from .handlers import OrderService, make_get_order, make_on_order_created, make_place_order
from .model import ORDER_CREATED_TOPIC, OrderEventLog

PLACE_ORDER_TOPIC = "orders:place"
GET_ORDER_TOPIC = "orders:get"


class OrdersStartUp(BenzeneStartUp):
    """Registers the order domain and wires its HTTP routes + topics."""

    def configure_services(self, services: Container, config: Mapping[str, str]) -> None:
        services.try_add_singleton(OrderService)  # construct the type (no factory needed)
        services.try_add_singleton(OrderEventLog)

    def configure(self, services: Scope, config: Mapping[str, str]) -> AppDefinition:
        service = services.get_service(OrderService)
        sender = services.get_service(MessageSender)  # a host/test must register this
        events = services.get_service(OrderEventLog)

        # request_type is inferred from each handler's first-parameter annotation at registration.
        router = (
            HttpRouter()
            .register("POST", "/orders", PLACE_ORDER_TOPIC, make_place_order(service, sender))
            .register("GET", "/orders/{id}", GET_ORDER_TOPIC, make_get_order(service))
        )
        registry = Registry.from_definitions(router).register(  # HTTP topics + the subscriber
            ORDER_CREATED_TOPIC, make_on_order_created(events)
        )
        return AppDefinition(registry=registry, router=router)
