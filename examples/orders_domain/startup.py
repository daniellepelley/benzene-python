"""The order domain's composition root — one ``BenzeneStartUp`` every host and test boots from.

This is the "boot the real app from its startup" seam (test-champion invariant 1): it registers the
domain's services and wires its routes/topics, independent of any cloud. A host or a test supplies
the outbound ``MessageSender`` (the real cloud client for deployment, a ``FakeMessageSender`` in a
test) by overriding that one registration — nothing else changes between clouds.
"""

from __future__ import annotations

from typing import Any, Mapping

from benzene.core import AppDefinition, BenzeneStartUp, Container, MessageSender, Scope
from benzene.results import Result, Status

from .handlers import OrderService
from .wiring import build_orders

#: Container key for the in-memory log the ``orders:created`` subscriber appends to.
ORDER_EVENTS_KEY = "orders:events"


class _UnconfiguredMessageSender:
    """Placeholder outbound client — fails loudly if a host/test forgot to register a real one."""

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        return Result.failure(
            Status.SERVICE_UNAVAILABLE,
            "No MessageSender configured. Register a real outbound client for your cloud "
            "(PubSubMessageSender / SnsMessageSender / ServiceBusMessageSender), or override "
            "MessageSender with a FakeMessageSender in tests.",
        )


class OrdersStartUp(BenzeneStartUp):
    """Registers the order domain and wires its HTTP routes + topics."""

    def configure_services(self, services: Container, config: Mapping[str, str]) -> None:
        services.try_add_singleton(OrderService, lambda _scope: OrderService())
        services.try_add_singleton(MessageSender, lambda _scope: _UnconfiguredMessageSender())
        services.try_add_singleton(ORDER_EVENTS_KEY, lambda _scope: [])

    def configure(self, services: Scope, config: Mapping[str, str]) -> AppDefinition:
        wiring = build_orders(
            services.get_service(OrderService),
            services.get_service(MessageSender),
            services.get_service(ORDER_EVENTS_KEY),
        )
        return AppDefinition(registry=wiring.registry, router=wiring.router)
