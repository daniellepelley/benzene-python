"""The six AzureFunctionsMesh Cloud Service domains, one Function App each, mirroring .NET's
``examples/AzureFunctionsMesh`` topology:

    orders --payment:take (Service Bus)--> payments --shipment:book (Service Bus)--> shipping
      |                                        |                                        |
      +--order:placed (Event Hub, fan-out)---->+  payment:captured (Event Grid)         +  shipment:dispatched (Event Grid)
      v            v                           v            v                          v          v          v
   inventory  notifications                notifications  analytics                inventory  notifications  analytics

Kept demo-proportionate per the plan (matching ``examples/aws_lambda_mesh``'s stance): no store, no
persistence — just enough state to prove the chain and give the mesh something real to interrogate (a
handful of topics, one health check each). Every handler is registered against the transport-neutral
:class:`~benzene.core.Registry`; which native Azure trigger actually reaches it (Service Bus/Event
Hub/Event Grid/HTTP) is a hosting decision made in ``host.py``/``function_app.py`` and Terraform, not
here.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from benzene.core import Handler, HealthChecks, MessageSender
from benzene.results import Result

# --- topics --------------------------------------------------------------------------------------

ORDER_CREATE_TOPIC = "order:create"  # orders: HTTP ingress (POST /orders)
PAYMENT_TAKE_TOPIC = "payment:take"  # orders -> payments (Service Bus)
ORDER_PLACED_TOPIC = "order:placed"  # orders -> inventory, notifications (Event Hub fan-out)
SHIPMENT_BOOK_TOPIC = "shipment:book"  # payments -> shipping (Service Bus)
PAYMENT_CAPTURED_TOPIC = "payment:captured"  # payments -> notifications, analytics (Event Grid)
SHIPMENT_DISPATCHED_TOPIC = "shipment:dispatched"  # shipping -> inventory, notifications, analytics

#: Every service's domain topic, keyed by service name — the set of Registry entries each Function
#: App exposes. Feeds the README's topology table and the tests; not consumed at runtime.
SERVICE_TOPICS: dict[str, tuple[str, ...]] = {
    "orders": (ORDER_CREATE_TOPIC,),
    "payments": (PAYMENT_TAKE_TOPIC,),
    "shipping": (SHIPMENT_BOOK_TOPIC,),
    "inventory": (ORDER_PLACED_TOPIC, SHIPMENT_DISPATCHED_TOPIC),
    "notifications": (ORDER_PLACED_TOPIC, PAYMENT_CAPTURED_TOPIC, SHIPMENT_DISPATCHED_TOPIC),
    "analytics": (PAYMENT_CAPTURED_TOPIC, SHIPMENT_DISPATCHED_TOPIC),
}


# --- payloads (each just an order id — enough to route, chain, and observe) -----------------------


@dataclass
class CreateOrderRequest:
    order_id: str = ""


@dataclass
class OrderConfirmation:
    order_id: str
    status: str = "created"


@dataclass
class Message:
    """The one payload shape every downstream hop and terminal consumer accepts — deliberately
    permissive (an empty default), mirroring ``examples/aws_lambda_mesh``'s trivial ``Message``: the
    point of this example is the mesh wiring, not payload correctness."""

    order_id: str = ""


#: A delivery log so a test (or a curious operator) can see a send genuinely reached its consumer —
#: mirrors ``examples/aws_lambda_mesh``'s ``RECEIPTS``. Process-local; reset between tests.
RECEIPTS: list[str] = []


def record(service: str, topic: str, order_id: str) -> None:
    RECEIPTS.append(f"{service}<-{topic}:{order_id or '?'}")


# --- orders: POST /orders -> send payment:take (Service Bus) + order:placed (Event Hub) -----------


def make_create_order(sender: MessageSender) -> Handler:
    async def create_order(request: CreateOrderRequest) -> Result:
        order_id = request.order_id or f"order-{uuid.uuid4().hex[:8]}"
        record("orders", ORDER_CREATE_TOPIC, order_id)
        # Best-effort fan-out: a downstream hiccup never fails the order itself (mirrors
        # NullMessageSender's / HttpBenzeneMessageClient's never-throws contract elsewhere in this port).
        await sender.send_message(PAYMENT_TAKE_TOPIC, Message(order_id=order_id))
        await sender.send_message(ORDER_PLACED_TOPIC, Message(order_id=order_id))
        return Result.created(OrderConfirmation(order_id=order_id))

    return create_order


# --- payments: consume payment:take -> send shipment:book (Service Bus) + payment:captured (Event Grid)


def make_take_payment(sender: MessageSender) -> Handler:
    async def take_payment(request: Message) -> Result:
        record("payments", PAYMENT_TAKE_TOPIC, request.order_id)
        await sender.send_message(SHIPMENT_BOOK_TOPIC, Message(order_id=request.order_id))
        await sender.send_message(PAYMENT_CAPTURED_TOPIC, Message(order_id=request.order_id))
        return Result.ok()

    return take_payment


# --- shipping: consume shipment:book -> send shipment:dispatched (Event Grid) ----------------------


def make_book_shipment(sender: MessageSender) -> Handler:
    async def book_shipment(request: Message) -> Result:
        record("shipping", SHIPMENT_BOOK_TOPIC, request.order_id)
        await sender.send_message(SHIPMENT_DISPATCHED_TOPIC, Message(order_id=request.order_id))
        return Result.ok()

    return book_shipment


# --- terminal consumers (inventory / notifications / analytics): record only ------------------------


def make_event_consumer(service: str, topic: str) -> Handler:
    async def consume(request: Message) -> Result:
        record(service, topic, request.order_id)
        return Result.ok()

    return consume


def health_checks(service: str) -> HealthChecks:
    """One trivial, always-true check per service — enough for the mesh's health interrogation to
    have something real to aggregate, without a real dependency to fail against (demo-proportionate)."""
    return HealthChecks().add(f"{service}-self", lambda: True)
