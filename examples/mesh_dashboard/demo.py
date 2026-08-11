"""Publish the Benzene **Mesh UI** dashboard artifacts for a fleet.

Where [`examples/mesh_fleet`](../mesh_fleet/fleet.py) shows the *sending* side — services reporting
``register`` + ``traces`` into a collector — this example shows the *observer* side: it assembles a
realistic mesh in memory and projects the collector's catalog into the full mesh-ui artifact set.

To make the dashboard show something worth looking at, the demo drives every input the projection
reads from:

- **Typed descriptors** — each service is registered from a *typed* :class:`~benzene.core.Registry`
  (``request_type`` / ``response_type`` per topic), so :meth:`ServiceDescriptor.derive` carries real
  request/response schemas and versions into ``topics.json`` and ``services/{name}.json``.
- **Heartbeats** — each service beats with named health checks, so ``services/{name}.json`` renders
  per-check health, not just an aggregate.
- **Real traced invocations** — placing an order runs the handlers through ``trace_middleware`` with
  ``traceparent`` propagated across each hop, so the collector derives the call graph
  (``topology.json``) and the exercise counts (``usage.json``) from genuine span parentage.

The wiring here (in-process senders, one shared collector) is the demo substitute for the deployed
shape — Lambda services, SNS/HTTP senders, a Fargate collector fed by the poller (pull spec/health)
plus the pushed traces (the call graph). The descriptors, the feeds, and the artifact projection are
identical; see [`deploy/mesh`](../../deploy/mesh/README.md) for the host that serves the output.
"""

from __future__ import annotations

import os
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
    Heartbeat,
    MeshCollector,
    QueueTraceExporter,
    ServiceDescriptor,
    ServiceInfo,
    build_artifacts,
    trace_middleware,
    with_trace_propagation,
    write_artifacts,
)
from benzene.results import Result

# The domain topics this fleet serves (``service:action`` convention, core-concepts §2).
PLACE_ORDER = "orders:place"
RESERVE_STOCK = "inventory:reserve"
SEND_NOTIFICATION = "notify:send"


# --- Payload shapes -------------------------------------------------------------------------------
# Plain dataclasses are all the descriptor needs: ``json_schema`` derives each topic's request/response
# schema from these, so they show up as real contracts on the functional map.


@dataclass
class PlaceOrder:
    sku: str = ""
    quantity: int = 1


@dataclass
class OrderPlaced:
    sku: str = ""
    reserved: int = 0


@dataclass
class ReserveStock:
    sku: str = ""
    quantity: int = 1


@dataclass
class StockReserved:
    reserved: int = 0


@dataclass
class SendNotification:
    sku: str = ""


@dataclass
class NotificationSent:
    sent: str = ""


class _InProcessSender:
    """A :class:`~benzene.core.MessageSender` that delivers to another service's app in-process.

    Stands in for a real outbound transport (SNS / HTTP): it forwards the Benzene headers — including
    the ``traceparent`` :func:`~benzene.mesh.with_trace_propagation` injects — so the callee joins the
    caller's trace and the collector can stitch the call graph.
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
    pipeline = MiddlewarePipeline().use(
        trace_middleware(exporter, service=name, instance_id=f"{name}-1")
    )
    return BenzeneMessageApplication(registry, pipeline), exporter


@dataclass
class DashboardMesh:
    """A wired demo mesh: the shared collector, the entry service, its trace exporters, and the
    registries (kept so a caller can re-derive descriptors, e.g. to demo a redeploy)."""

    collector: MeshCollector
    orders: BenzeneMessageApplication
    exporters: dict[str, QueueTraceExporter] = field(default_factory=dict)
    registries: dict[str, Registry] = field(default_factory=dict)

    async def place_order(self, sku: str, quantity: int = 1) -> Result:
        """Drive one order through orders → inventory → notifications, then push the traces.

        ``orders:place`` is served at version ``2`` (the demo's contract-versioning signal), so the
        request carries the ``benzene-version`` header; the downstream hops are unversioned and only
        the ``traceparent`` propagates, so they resolve normally.
        """
        response = await self.orders.handle(
            {
                "topic": PLACE_ORDER,
                "headers": {"benzene-version": "2"},
                "body": encode_body({"sku": sku, "quantity": quantity}),
            }
        )
        self._report_traces()
        return Result(response["statusCode"])

    def _report_traces(self) -> None:
        for exporter in self.exporters.values():
            events = [event.to_payload() for event in exporter.drain()]
            if events:
                self.collector.ingest_traces({"events": events})


def build_demo_mesh(collector: MeshCollector | None = None) -> DashboardMesh:
    """Wire three typed services, register + heartbeat them into the collector, and return the mesh.

    The returned :class:`DashboardMesh` has a populated identity/topic/health catalog; call
    :meth:`~DashboardMesh.place_order` to also populate the topology + usage views from real traces.
    """
    collector = collector or MeshCollector()

    # Each handler is registered with a typed request/response, so the router deserializes the body
    # into the dataclass and the handler works against typed attributes (not a raw dict).

    # notifications — the leaf (no outbound call).
    async def notify(request: SendNotification) -> Result:
        return Result.ok({"sent": request.sku})

    notif_registry = Registry().register(SEND_NOTIFICATION, notify, response_type=NotificationSent)
    notifications, notif_traces = _service("notifications", notif_registry)

    # inventory — reserves stock, then notifies (propagating the trace to notifications).
    notify_sender: MessageSender = with_trace_propagation(_InProcessSender(notifications))

    async def reserve(request: ReserveStock) -> Result:
        await notify_sender.send_message(SEND_NOTIFICATION, {"sku": request.sku})
        return Result.ok({"reserved": request.quantity})

    inv_registry = Registry().register(RESERVE_STOCK, reserve, response_type=StockReserved)
    inventory, inv_traces = _service("inventory", inv_registry)

    # orders — the entry point; reserves stock (propagating the trace to inventory).
    reserve_sender: MessageSender = with_trace_propagation(_InProcessSender(inventory))

    async def place(request: PlaceOrder) -> Result:
        await reserve_sender.send_message(
            RESERVE_STOCK, {"sku": request.sku, "quantity": request.quantity}
        )
        return Result.created({"sku": request.sku})

    order_registry = Registry().register(PLACE_ORDER, place, version="2", response_type=OrderPlaced)
    orders, order_traces = _service("orders", order_registry)

    registries = {
        "orders": order_registry,
        "inventory": inv_registry,
        "notifications": notif_registry,
    }

    # Startup: each service registers its descriptor, then beats with named health checks. This is
    # what fills manifest.json (status), topics.json (schemas + version), and services/{name}.json
    # (spec + per-check health) before a single request is served.
    health_checks = {
        "orders": {"database": {"status": "healthy"}, "inventory-api": {"status": "healthy"}},
        "inventory": {"database": {"status": "healthy"}, "warehouse-feed": {"status": "healthy"}},
        "notifications": {"smtp": {"status": "healthy"}},
    }
    for name, registry in registries.items():
        descriptor = ServiceDescriptor.derive(
            registry,
            ServiceInfo(service=name, service_version="1.0.0", placement={"cloud": "aws"}),
        )
        collector.ingest_register(descriptor.to_payload())
        collector.ingest_heartbeat(
            Heartbeat(
                service=name,
                sent_at="2026-01-01T00:00:00Z",
                instance_id=f"{name}-1",
                descriptor_hash=descriptor.descriptor_hash(),
                is_healthy=True,
                health_checks=health_checks[name],
            ).to_payload()
        )

    return DashboardMesh(
        collector=collector,
        orders=orders,
        exporters={"orders": order_traces, "inventory": inv_traces, "notifications": notif_traces},
        registries=registries,
    )


async def write_demo_artifacts(
    directory: str | os.PathLike[str], *, orders: int = 3
) -> dict[str, Any]:
    """Build the demo mesh, exercise it, and lay the full mesh-ui artifact set down under ``directory``.

    Places ``orders`` orders so the topology + usage views have real traffic, then writes
    ``manifest.json`` / ``topics.json`` / ``topology.json`` / ``usage.json`` / ``asyncapi.json`` /
    ``annotations.json`` and one ``services/{name}.json`` per service. Point the canonical
    ``mesh-ui.html`` at ``directory`` (or set ``MESH_ARTIFACTS_DIR`` on the ``deploy/mesh`` host) to
    render the dashboard. Returns the built artifacts.
    """
    mesh = build_demo_mesh()
    for i in range(orders):
        await mesh.place_order(sku=f"SKU-{i:03d}", quantity=i + 1)
    # A fixed timestamp keeps the output deterministic (mesh-ui stamps it as generatedAtUtc).
    return write_artifacts(directory, mesh.collector, generated_at="2026-01-01T00:00:00Z")


def build_demo_artifacts() -> dict[str, Any]:
    """The in-memory artifact set for a freshly registered fleet (no traffic yet) — the identity,
    topic, and health views populated, with topology/usage empty until orders are placed."""
    mesh = build_demo_mesh()
    return build_artifacts(mesh.collector, generated_at="2026-01-01T00:00:00Z")


async def _main() -> None:
    import json
    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        artifacts = await write_demo_artifacts(directory)
        print(f"Wrote {len(list(os.listdir(directory)))} top-level artifacts to {directory}")
        print(json.dumps(artifacts["manifest"], indent=2))


if __name__ == "__main__":
    import asyncio

    asyncio.run(_main())
