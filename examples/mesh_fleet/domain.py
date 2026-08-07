"""The three services' domain — payload models and handler factories (orders / payments / shipping).

Plain dataclasses give each topic a real request/response schema (derived by
:func:`benzene.core.json_schema`), and each handler is a closure over its collaborators — the same
factory-injection shape as :mod:`orders_domain`, so a test (or the fleet driver) wires an in-memory
store and an outbound sender and drives ingress → handler → egress. ``orders`` forwards to
``payments`` and ``shipping`` through an outbound :class:`~benzene.core.MessageSender`; ``payments``
forwards to ``shipping``. Those outbound calls, made under the trace middleware, are what let the
collector derive the mesh's consumer edges.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from benzene.core import Handler, MessageSender
from benzene.results import Result

PAYMENT_CAPTURE_TOPIC = "payment:capture"
SHIPPING_BOOK_TOPIC = "shipping:book"


# --- payload models (one dataclass per topic → a titled JSON-schema object) --------------------
@dataclass
class CreateOrder:
    customer_email: str
    sku: str
    quantity: int = 1


@dataclass
class Order:
    id: str
    customer_email: str
    total: float
    status: str


@dataclass
class CapturePayment:
    order_id: str
    amount: float
    currency: str


@dataclass
class BookShipment:
    order_id: str
    line1: str
    postcode: str


# --- orders -----------------------------------------------------------------------------------
class OrderStore:
    """A trivial in-memory order book (stands in for a database)."""

    def __init__(self) -> None:
        self.orders: dict[str, Order] = {}

    def place(self, request: CreateOrder) -> Order:
        order = Order(
            id=uuid.uuid4().hex,
            customer_email=request.customer_email,
            total=round(19.99 * request.quantity, 2),
            status="paid",
        )
        self.orders[order.id] = order
        return order


def make_create_order(store: OrderStore, sender: MessageSender) -> Handler:
    """POST /orders → place an order, capture payment, then book shipping (two forwarded mesh spans)."""

    async def create_order(request: CreateOrder) -> Result:
        if not request.customer_email:
            return Result.bad_request("customerEmail is required")
        order = store.place(request)
        # Forward to payments (and, on this path, straight to shipping) — the sender injects the
        # current trace span, so the collector attributes both edges to this order.
        await sender.send_message(
            PAYMENT_CAPTURE_TOPIC,
            CapturePayment(order_id=order.id, amount=order.total, currency="GBP"),
        )
        await sender.send_message(
            SHIPPING_BOOK_TOPIC,
            BookShipment(order_id=order.id, line1="1 High St", postcode="AB1 2CD"),
        )
        return Result.created(order)

    return create_order


def make_get_all_orders(store: OrderStore) -> Handler:
    """GET /orders → list every order (no downstream calls — a leaf topic)."""

    async def get_all_orders(_request: dict) -> Result:
        return Result.ok(list(store.orders.values()))

    return get_all_orders


# --- payments ---------------------------------------------------------------------------------
def make_capture_payment(sender: MessageSender, fail_every: int = 6) -> Handler:
    """payment:capture → capture funds and book shipping; fails 1-in-``fail_every`` (a real error rate)."""

    state = {"n": 0}

    async def capture_payment(request: CapturePayment) -> Result:
        state["n"] += 1
        if state["n"] % fail_every == 0:
            # The payments gateway is flaky — surfaced as service-unavailable in usage + the edge error rate.
            return Result.service_unavailable("payment gateway timeout")
        await sender.send_message(
            SHIPPING_BOOK_TOPIC,
            BookShipment(order_id=request.order_id, line1="warehouse", postcode="ZZ9 9ZZ"),
        )
        return Result.ok({"captured": request.amount, "currency": request.currency})

    return capture_payment


# --- shipping ---------------------------------------------------------------------------------
def make_book_shipment(fail_every: int = 8) -> Handler:
    """shipping:book → book a shipment; rejects 1-in-``fail_every`` as a validation-error (bad address)."""

    state = {"n": 0}

    async def book_shipment(request: BookShipment) -> Result:
        state["n"] += 1
        if state["n"] % fail_every == 0:
            return Result.validation_error("postcode outside the delivery area")
        return Result.ok({"booked": request.order_id})

    return book_shipment
