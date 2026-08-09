"""Order domain models (plain dataclasses — the request/response payload shapes)."""

from __future__ import annotations

from dataclasses import dataclass

#: Topic the order-created event is published on (and subscribed to over Pub/Sub, SNS, etc.).
#: Uses the ``:`` separator convention from core-concepts §2 (matching the .NET reference examples).
ORDER_CREATED_TOPIC = "orders:created"


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


class OrderEventLog(list[str]):
    """The in-memory log of order ids the ``orders:created`` subscriber has seen.

    A tiny ``list`` subclass so it can be a **typed** container key (resolved by type, like
    ``OrderService`` / ``MessageSender``) instead of a stringly-typed one — one less special case for a
    reader. A host or test swaps it for its own instance the same way it swaps any dependency.
    """
