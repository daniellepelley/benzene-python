"""Payloads for the handler-version-dispatch demo (versioning.md §3, "Mechanism A").

The topic ``order:create`` has TWO live payload versions, each served by its OWN handler
(:func:`~versioning.handlers.make_create_order_v1` / ``make_create_order_v2``). The incoming
``benzene-version`` header selects which handler runs — no casting is involved, the two shapes are
genuinely different. Each version keeps its request/response types side by side.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Topic id shared by both versions of the create-order handler.
ORDER_CREATE_TOPIC = "order:create"

#: The payload schema version names. Ordinary strings on the wire (the ``benzene-version`` header);
#: the framework compares them naturally when picking a handler, so ``v1`` < ``v2``.
V1 = "v1"
V2 = "v2"


@dataclass
class CreateOrderV1:
    """V1 of the create-order request: a flat customer name."""

    customer_name: str = ""
    quantity: int = 1


@dataclass
class OrderAcceptedV1:
    order_id: str
    handled_by: str


@dataclass
class CreateOrderV2:
    """V2 split the single customer name into first/last and added a currency — a genuinely different
    request shape, which is exactly when a second handler (rather than a caster) is the right tool."""

    first_name: str = ""
    last_name: str = ""
    quantity: int = 1
    currency: str = ""


@dataclass
class OrderAcceptedV2:
    order_id: str
    handled_by: str
    currency: str
