"""Order handlers — plain async functions, built with their dependencies injected via a factory.

Handlers that need a collaborator (the store, the outbound client) are produced by a ``make_*``
factory that closes over it. This is the Pythonic take on the .NET example's constructor injection,
and it is exactly what makes the handlers testable: a test injects an in-memory store and a
``FakeMessageSender`` and asserts ingress → handler → egress.
"""

from __future__ import annotations

import uuid

from benzene.core import Handler, MessageSender
from benzene.results import Result

from .model import ORDER_CREATED_TOPIC, Order, OrderCreated, PlaceOrder


class OrderService:
    """A trivial in-memory order store (stands in for a database)."""

    def __init__(self) -> None:
        self.orders: dict[str, Order] = {}

    def place(self, sku: str, quantity: int) -> Order:
        order = Order(id=uuid.uuid4().hex, sku=sku, quantity=quantity)
        self.orders[order.id] = order
        return order

    def get(self, order_id: str) -> Order | None:
        return self.orders.get(order_id)


def make_place_order(service: OrderService, sender: MessageSender) -> Handler:
    """POST /orders → create an order and publish OrderCreated (ingress → handler → egress)."""

    async def place_order(request: PlaceOrder) -> Result:
        if not request.sku:
            return Result.bad_request("sku is required")
        order = service.place(request.sku, request.quantity)
        await sender.send_message(
            ORDER_CREATED_TOPIC, OrderCreated(id=order.id, sku=order.sku)
        )
        return Result.created(order)

    return place_order


def make_get_order(service: OrderService) -> Handler:
    """GET /orders/{id} → fetch an order."""

    async def get_order(request: dict) -> Result:
        order = service.get(request.get("id", ""))
        if order is None:
            return Result.not_found(f"No order {request.get('id')!r}")
        return Result.ok(order)

    return get_order


def make_on_order_created(seen: list[str]) -> Handler:
    """Pub/Sub subscriber for the OrderCreated topic → records the id it saw."""

    async def on_order_created(request: OrderCreated) -> Result:
        seen.append(request.id)
        return Result.ok()

    return on_order_created
