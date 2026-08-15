"""Wire three Benzene services into one in-process mesh and report them to a collector.

Each service runs its own pipeline with ``trace_middleware`` (so every invocation emits a TraceEvent)
and calls the next service through a ``with_trace_propagation``-wrapped in-process sender (so the
``traceparent`` — hence the parent/child span link — crosses the hop). At startup each service
registers its descriptor — including an :class:`~benzene.mesh.OutboundRegistry` declaring what it
calls — with the collector, so the producer/consumer graph exists before a single order is placed;
after a call chain, every service's traces are pushed to the collector too, feeding invocation/error
stats and status counts, never graph membership (mesh.md §4).

The wiring here (in-process senders, shared collector) is the *demo substitute* for the deployed shape:
in AWS each service is a Lambda, the senders are SNS/HTTP, and the collector is a Fargate service fed by
the poller (pull spec/health) plus the pushed traces (the call graph). The handler code and the mesh
derivation are identical.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from benzene.core import (
    BenzeneMessageApplication,
    MessageSender,
    MiddlewarePipeline,
    Registry,
    encode_body,
)
from benzene.mesh import (
    MeshCollector,
    OutboundRegistry,
    QueueTraceExporter,
    ServiceDescriptor,
    ServiceInfo,
    trace_middleware,
    with_trace_propagation,
)
from benzene.results import Result

# Topics the fleet serves.
PLACE_ORDER = "orders:place"
RESERVE_STOCK = "inventory:reserve"
SEND_NOTIFICATION = "notify:send"


class _InProcessSender:
    """A :class:`~benzene.core.MessageSender` that delivers to another service's app in the same process.

    Stands in for a real outbound transport (SNS / HTTP). It forwards the Benzene headers — including
    the ``traceparent`` that :func:`~benzene.mesh.with_trace_propagation` injects — so the callee joins
    the caller's trace.
    """

    def __init__(self, target: BenzeneMessageApplication) -> None:
        self._target = target

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        response = await self._target.handle(
            {"topic": topic, "headers": headers or {}, "body": encode_body(message)}
        )
        return Result(response["statusCode"])


def _service(name: str, registry: Registry) -> tuple[BenzeneMessageApplication, QueueTraceExporter]:
    exporter = QueueTraceExporter()
    pipeline = MiddlewarePipeline().use(trace_middleware(exporter, service=name, instance_id=f"{name}-1"))
    return BenzeneMessageApplication(registry, pipeline), exporter


@dataclass
class Fleet:
    """The wired fleet: the shared collector, the entry service, and each service's trace exporter."""

    collector: MeshCollector
    orders: BenzeneMessageApplication
    exporters: dict[str, QueueTraceExporter] = field(default_factory=dict)

    async def place_order(self, sku: str, quantity: int = 1) -> Result:
        """Drive one order through orders → inventory → notifications, then report the traces."""
        response = await self.orders.handle(
            {"topic": PLACE_ORDER, "headers": {}, "body": encode_body({"sku": sku, "quantity": quantity})}
        )
        self._report_traces()
        return Result(response["statusCode"])

    def _report_traces(self) -> None:
        for exporter in self.exporters.values():
            events = [event.to_payload() for event in exporter.drain()]
            if events:
                self.collector.ingest_traces({"events": events})


def build_fleet(collector: MeshCollector | None = None) -> Fleet:
    """Wire the three services, register them with the collector, and return the :class:`Fleet`."""
    collector = collector or MeshCollector()

    # notifications — the leaf (no outbound).
    async def notify(request: dict) -> Result:
        return Result.ok({"sent": request.get("sku")})

    notif_registry = Registry().register(SEND_NOTIFICATION, notify)
    notifications, notif_traces = _service("notifications", notif_registry)

    # inventory — reserves stock, then notifies (propagating the trace to notifications).
    notify_sender: MessageSender = with_trace_propagation(_InProcessSender(notifications))

    async def reserve(request: dict) -> Result:
        await notify_sender.send_message(SEND_NOTIFICATION, {"sku": request.get("sku")})
        return Result.ok({"reserved": request.get("quantity", 1)})

    inv_registry = Registry().register(RESERVE_STOCK, reserve)
    inventory, inv_traces = _service("inventory", inv_registry)

    # orders — the entry point; reserves stock (propagating the trace to inventory).
    reserve_sender: MessageSender = with_trace_propagation(_InProcessSender(inventory))

    async def place(request: dict) -> Result:
        await reserve_sender.send_message(
            RESERVE_STOCK, {"sku": request.get("sku"), "quantity": request.get("quantity", 1)}
        )
        return Result.created({"sku": request.get("sku")})

    order_registry = Registry().register(PLACE_ORDER, place)
    orders, order_traces = _service("orders", order_registry)

    # Register each service's descriptor — including what it consumes — with the collector (what a
    # service does on startup; this alone is what puts consumer edges in the graph, mesh.md §2.3/§4).
    outbound: dict[str, OutboundRegistry] = {
        "orders": OutboundRegistry().register(RESERVE_STOCK),
        "inventory": OutboundRegistry().register(SEND_NOTIFICATION),
        "notifications": OutboundRegistry(),
    }
    for name, registry in (
        ("orders", order_registry),
        ("inventory", inv_registry),
        ("notifications", notif_registry),
    ):
        descriptor = ServiceDescriptor.derive(
            registry, ServiceInfo(service=name, placement={"cloud": "aws"}), outbound[name]
        )
        collector.ingest_register(descriptor.to_payload())

    return Fleet(
        collector=collector,
        orders=orders,
        exporters={"orders": order_traces, "inventory": inv_traces, "notifications": notif_traces},
    )
