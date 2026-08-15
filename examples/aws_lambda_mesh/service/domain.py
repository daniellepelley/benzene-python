"""The six AwsMesh Cloud Service domains, one Lambda each, mirroring .NET's ``examples/AwsMesh`` and
TypeScript's ``examples/aws-lambda-mesh`` (``functions/services.ts``) topology:

    orders --payments:capture (SQS)--> payments --shipping:book (SQS)--> shipping
      |                                    |                                |
      +--order:placed (SNS, fan-out)------>+  payment:captured (EventBridge)+  shipment:dispatched (EventBridge)
      v            v                       v            v                  v          v          v
   inventory  notifications           notifications  analytics        inventory  notifications  analytics

Kept demo-proportionate per the plan: no store, no persistence — just enough state to prove the chain
and give the mesh something real to interrogate (a handful of topics, one health check each). Every
handler is registered against the transport-neutral :class:`~benzene.core.Registry`; which native AWS
event actually reaches it (SQS/SNS/EventBridge/API Gateway) is a hosting decision made in ``host.py``
and Terraform, not here.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from benzene.core import Handler, HealthChecks, MessageSender
from benzene.results import Result

# --- topics --------------------------------------------------------------------------------------

ORDER_CREATE_TOPIC = "order:create"  # orders: HTTP ingress (POST /orders)
PAYMENTS_CAPTURE_TOPIC = "payments:capture"  # orders -> payments (SQS)
ORDER_PLACED_TOPIC = "order:placed"  # orders -> inventory, notifications (SNS fan-out)
SHIPPING_BOOK_TOPIC = "shipping:book"  # payments -> shipping (SQS)
PAYMENT_CAPTURED_TOPIC = "payment:captured"  # payments -> notifications, analytics (EventBridge)
SHIPMENT_DISPATCHED_TOPIC = "shipment:dispatched"  # shipping -> inventory, notifications, analytics

#: Every service's domain topic, keyed by service name — the set of Registry entries each Lambda
#: exposes. Feeds `mesh/README expectations` and the tests; not consumed at runtime.
SERVICE_TOPICS: dict[str, tuple[str, ...]] = {
    "orders": (ORDER_CREATE_TOPIC,),
    "payments": (PAYMENTS_CAPTURE_TOPIC,),
    "shipping": (SHIPPING_BOOK_TOPIC,),
    "inventory": (ORDER_PLACED_TOPIC, SHIPMENT_DISPATCHED_TOPIC),
    "notifications": (ORDER_PLACED_TOPIC, PAYMENT_CAPTURED_TOPIC, SHIPMENT_DISPATCHED_TOPIC),
    "analytics": (PAYMENT_CAPTURED_TOPIC, SHIPMENT_DISPATCHED_TOPIC),
}

#: Every topic a service *sends* (mirrors the ``sender.send_message`` calls below), keyed by service
#: name — what ``startup.py`` projects into each service's :class:`~benzene.mesh.OutboundRegistry`, so
#: the mesh's provider edges are declared (mesh.md §2.3) rather than waiting on a trace to prove them.
SERVICE_PRODUCES: dict[str, tuple[str, ...]] = {
    "orders": (PAYMENTS_CAPTURE_TOPIC, ORDER_PLACED_TOPIC),
    "payments": (SHIPPING_BOOK_TOPIC, PAYMENT_CAPTURED_TOPIC),
    "shipping": (SHIPMENT_DISPATCHED_TOPIC,),
    "inventory": (),
    "notifications": (),
    "analytics": (),
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
    permissive (an empty default), mirroring TS's trivial ``Message { orderId?: string }``: the point
    of this example is the mesh wiring, not payload correctness."""

    order_id: str = ""


#: A delivery log so a test (or a curious operator) can see a send genuinely reached its consumer —
#: mirrors TS's ``receipts`` / .NET's own request logs. Process-local; reset between tests.
RECEIPTS: list[str] = []


def record(service: str, topic: str, order_id: str) -> None:
    RECEIPTS.append(f"{service}<-{topic}:{order_id or '?'}")


# --- orders: POST /orders -> send payments:capture (SQS) + order:placed (SNS) ---------------------


def make_create_order(sender: MessageSender) -> Handler:
    async def create_order(request: CreateOrderRequest) -> Result:
        order_id = request.order_id or f"order-{uuid.uuid4().hex[:8]}"
        record("orders", ORDER_CREATE_TOPIC, order_id)
        # Best-effort fan-out: a downstream hiccup never fails the order itself (mirrors
        # NullMessageSender's / HttpBenzeneMessageClient's never-throws contract elsewhere in this port).
        await sender.send_message(PAYMENTS_CAPTURE_TOPIC, Message(order_id=order_id))
        await sender.send_message(ORDER_PLACED_TOPIC, Message(order_id=order_id))
        return Result.created(OrderConfirmation(order_id=order_id))

    return create_order


# --- payments: consume payments:capture -> send shipping:book (SQS) + payment:captured (EventBridge)


def make_capture_payment(sender: MessageSender) -> Handler:
    async def capture_payment(request: Message) -> Result:
        record("payments", PAYMENTS_CAPTURE_TOPIC, request.order_id)
        await sender.send_message(SHIPPING_BOOK_TOPIC, Message(order_id=request.order_id))
        await sender.send_message(PAYMENT_CAPTURED_TOPIC, Message(order_id=request.order_id))
        return Result.ok()

    return capture_payment


# --- shipping: consume shipping:book -> send shipment:dispatched (EventBridge) --------------------


def make_book_shipment(sender: MessageSender) -> Handler:
    async def book_shipment(request: Message) -> Result:
        record("shipping", SHIPPING_BOOK_TOPIC, request.order_id)
        await sender.send_message(SHIPMENT_DISPATCHED_TOPIC, Message(order_id=request.order_id))
        return Result.ok()

    return book_shipment


# --- terminal consumers (inventory / notifications / analytics): record only ----------------------


def make_event_consumer(service: str, topic: str) -> Handler:
    async def consume(request: Message) -> Result:
        record(service, topic, request.order_id)
        return Result.ok()

    return consume


def health_checks(service: str) -> HealthChecks:
    """One trivial, always-true check per service — enough for the mesh's health interrogation to
    have something real to aggregate, without a real dependency to fail against (demo-proportionate)."""
    return HealthChecks().add(f"{service}-self", lambda: True)
