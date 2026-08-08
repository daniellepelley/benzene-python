"""Handler-version dispatch (Mechanism A): two handlers on one topic, selected by the version header.

``order:create`` has a V1 and a V2 handler registered against the *same* topic under different
versions. The router runs whichever matches the incoming ``benzene-version`` metadata — no casting,
the two request shapes are genuinely different implementations. Each handler is built by a ``make_*``
factory closing over its dependency (the processed log), the same injection style as
``orders_domain``.
"""

from __future__ import annotations

from benzene.core import Handler
from benzene.results import Result

from .contracts import (
    CreateOrderV1,
    CreateOrderV2,
    OrderAcceptedV1,
    OrderAcceptedV2,
)


class ProcessedLog:
    """A tiny in-memory record of what each handler received.

    A fire-and-forget transport returns no response body, so a test still needs to see *which*
    version handler ran; the real deployable would write to a data store instead. Mirrors the .NET
    example's ``IProcessedLog``.
    """

    def __init__(self) -> None:
        self.entries: list[str] = []

    def record(self, entry: str) -> None:
        self.entries.append(entry)


def make_create_order_v1(log: ProcessedLog) -> Handler:
    """The V1 implementation of ``order:create`` — runs only when ``benzene-version`` is ``v1``."""

    async def create_order_v1(request: CreateOrderV1) -> Result:
        log.record(f"order:create v1 | customer={request.customer_name} qty={request.quantity}")
        return Result.ok(OrderAcceptedV1(order_id="order-1", handled_by="create_order_v1"))

    return create_order_v1


def make_create_order_v2(log: ProcessedLog) -> Handler:
    """The V2 implementation of ``order:create``.

    Because ``v2`` is the highest registered version, it is also the one the ``highest_version``
    selector falls back to when a producer sends no version at all — so V2 is the topic's default.
    """

    async def create_order_v2(request: CreateOrderV2) -> Result:
        log.record(
            f"order:create v2 | customer={request.first_name} {request.last_name} "
            f"qty={request.quantity} currency={request.currency}"
        )
        return Result.ok(
            OrderAcceptedV2(
                order_id="order-1", handled_by="create_order_v2", currency=request.currency
            )
        )

    return create_order_v2
