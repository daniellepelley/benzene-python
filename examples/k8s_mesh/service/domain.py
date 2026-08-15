"""The three domain models — one per deployed service (orders/payments/shipping), selected at startup
by the ``MESH_SERVICE`` env var. Only the selected domain's handler is registered, so each service
exposes its own domain topic (and the mesh's aggregated topic catalog shows real cross-service
ownership) — mirrors .NET's ``examples/K8sMesh/Service/Domain.cs``, kept demo-proportionate (no store,
no persistence, just enough state to prove the chain).

The services also **chain to each other over HTTP**: orders -> payments -> shipping, each hop carried
as a Benzene message envelope POSTed to the next service's ``/benzene/invoke`` endpoint (see
``envelope_client.py`` and ``startup.py``). This turns the mesh from a discovery-only demo into one
with real service-to-service traffic to observe.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from benzene.core import Handler, MessageSender
from benzene.results import Result

ORDER_CREATE_TOPIC = "order:create"
PAYMENT_TAKE_TOPIC = "payment:take"
SHIPMENT_BOOK_TOPIC = "shipment:book"


@dataclass
class CreateOrderRequest:
    customer_id: str = ""
    sku: str = ""
    quantity: int = 1


@dataclass
class OrderCreated:
    order_id: str
    status: str


@dataclass
class TakePaymentRequest:
    order_id: str = ""
    amount: float = 0.0


@dataclass
class PaymentTaken:
    payment_id: str
    status: str


@dataclass
class BookShipmentRequest:
    order_id: str = ""
    address: str = ""
    carrier: str = ""


@dataclass
class ShipmentBooked:
    shipment_id: str
    status: str


def make_create_order(downstream: MessageSender) -> Handler:
    """POST /orders -> order:create. Chains the next hop: asks payments to take payment."""

    async def create_order(request: CreateOrderRequest) -> Result:
        order_id = f"order-{uuid.uuid4().hex[:8]}"
        payment = TakePaymentRequest(order_id=order_id, amount=request.quantity * 10.0)
        # Best-effort: a downstream hiccup never fails the order (mirrors HttpBenzeneMessageClient's
        # never-throws contract, and MessageSender's fire-and-report shape).
        await downstream.send_message(PAYMENT_TAKE_TOPIC, payment)
        return Result.created(OrderCreated(order_id=order_id, status="created"))

    return create_order


def make_take_payment(downstream: MessageSender) -> Handler:
    """POST /payments -> payment:take. Chains the next hop: asks shipping to book the shipment."""

    async def take_payment(request: TakePaymentRequest) -> Result:
        payment_id = f"pay-{uuid.uuid4().hex[:8]}"
        shipment = BookShipmentRequest(
            order_id=request.order_id, address="123 Example St", carrier="royal-mail"
        )
        await downstream.send_message(SHIPMENT_BOOK_TOPIC, shipment)
        return Result.created(PaymentTaken(payment_id=payment_id, status="captured"))

    return take_payment


def make_book_shipment() -> Handler:
    """POST /shipments -> shipment:book. Terminal: books the shipment and returns, no downstream."""

    async def book_shipment(request: BookShipmentRequest) -> Result:
        return Result.created(ShipmentBooked(shipment_id=f"ship-{uuid.uuid4().hex[:8]}", status="booked"))

    return book_shipment


class NullMessageSender:
    """Stand-in :class:`MessageSender` for a service with no downstream wired (``DOWNSTREAM_MSG_URL``
    unset — e.g. the terminal ``shipping`` service, or a standalone run). Accepts every send as a
    no-op so the chaining handlers still resolve and run unchanged without a real next hop. Mirrors
    .NET's ``NullBenzeneMessageClient``.
    """

    async def send_message(self, topic: str, message: object, headers: dict[str, str] | None = None) -> Result:
        return Result.accepted()
