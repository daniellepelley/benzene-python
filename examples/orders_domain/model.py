"""Order domain models (plain dataclasses — the request/response payload shapes)."""

from __future__ import annotations

from dataclasses import dataclass

#: Topic the order-created event is published on (and subscribed to over Pub/Sub, SNS, etc.).
ORDER_CREATED_TOPIC = "orders.created"


@dataclass
class PlaceOrder:
    sku: str = ""
    quantity: int = 1


@dataclass
class Order:
    id: str
    sku: str
    quantity: int


@dataclass
class OrderCreated:
    id: str
    sku: str
