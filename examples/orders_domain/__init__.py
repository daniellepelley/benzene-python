"""Shared order domain for the Benzene examples — transport-agnostic handlers and models.

This is the "write your handlers once" half of the promise: the same handlers are hosted by the
GCP, AWS, and Azure examples by swapping only the transport wiring, never the domain code. Mirrors
.NET's ``examples/App/Benzene.Examples.App`` shared domain.
"""

from __future__ import annotations

from .handlers import OrderService, make_get_order, make_on_order_created, make_place_order
from .model import ORDER_CREATED_TOPIC, Order, OrderCreated, PlaceOrder
from .wiring import (
    GET_ORDER_TOPIC,
    PLACE_ORDER_TOPIC,
    OrdersWiring,
    build_orders,
    build_orders_registry,
    build_orders_router,
)

__all__ = [
    "GET_ORDER_TOPIC",
    "ORDER_CREATED_TOPIC",
    "PLACE_ORDER_TOPIC",
    "Order",
    "OrderCreated",
    "OrderService",
    "OrdersWiring",
    "PlaceOrder",
    "build_orders",
    "build_orders_registry",
    "build_orders_router",
    "make_get_order",
    "make_on_order_created",
    "make_place_order",
]
