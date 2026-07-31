"""Wires the order handlers onto Benzene's registry + HTTP router — reused by every cloud host.

A host example is then just: build this wiring with its store + outbound client, and mount the
router/registry onto its transport. The handlers and topics never change between clouds.
"""

from __future__ import annotations

from dataclasses import dataclass

from benzene.core import MessageSender, Registry
from benzene.http import HttpRouter

from .handlers import OrderService, make_get_order, make_on_order_created, make_place_order
from .model import ORDER_CREATED_TOPIC, OrderCreated, PlaceOrder

PLACE_ORDER_TOPIC = "orders:place"
GET_ORDER_TOPIC = "orders:get"


@dataclass
class OrdersWiring:
    """The HTTP routes and the full topic registry for the order domain."""

    router: HttpRouter
    registry: Registry


def build_orders(
    service: OrderService, sender: MessageSender, seen: list[str] | None = None
) -> OrdersWiring:
    """Build the order domain's routes + registry against a store, outbound client, and event log.

    - ``POST /orders``      → ``orders:place``   (creates an order, publishes ``orders:created``)
    - ``GET  /orders/{id}`` → ``orders:get``
    - Pub/Sub ``orders:created`` → records the id in ``seen`` (the subscriber side)
    """
    place = make_place_order(service, sender)
    get = make_get_order(service)
    on_created = make_on_order_created(seen if seen is not None else [])

    router = HttpRouter()
    router.register("POST", "/orders", PLACE_ORDER_TOPIC, place, request_type=PlaceOrder)
    router.register("GET", "/orders/{id}", GET_ORDER_TOPIC, get)

    registry = Registry()
    for definition in router.definitions():
        registry.add_definition(definition)
    registry.register(ORDER_CREATED_TOPIC, on_created, request_type=OrderCreated)

    return OrdersWiring(router=router, registry=registry)
