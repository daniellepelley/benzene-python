"""The three domain services as **separate ASGI apps** that report into the Mesh Host over HTTP.

Each of ``orders`` / ``payments`` / ``shipping`` is a real :class:`~benzene.http.BenzeneHttpApp` — its own
routes, its own trace middleware, its own ``/benzene/*`` surfaces — with two outbound HTTP legs:

- **domain calls** to its peers, via an :class:`~benzene.http.InvokeMessageSender` that POSTs the wire
  envelope to the peer's ``/benzene/invoke`` (``orders`` → ``payments`` + ``shipping``, ``payments`` →
  ``shipping``), wrapped in :class:`~benzene.mesh.TracePropagatingMessageSender` so the caller's mesh span
  rides along and the collector can derive the consumer edges;
- **mesh feeds** to the host, via a :class:`~benzene.mesh.MeshFeedSender` over another
  :class:`~benzene.http.InvokeMessageSender` pointed at the host's ``/benzene/invoke`` — this is the
  distinguishing win over the in-process demo: the fleet reaches the collector by **real HTTP pushes
  between processes**, not a shared object.

The domain itself is reused verbatim from :mod:`mesh_fleet.domain` (the same orders/payments/shipping
handlers the in-process example drives). The three demonstrate the three manifest states over genuine
HTTP: ``orders`` healthy, ``payments`` unhealthy (its ``/benzene/health`` reports a failed gateway → 503)
**and** contract-drifted, ``shipping`` **unreachable to the aggregator** — it deliberately does *not*
expose ``/benzene/spec`` or ``/benzene/health`` (those 404), so the aggregator can't describe it, yet it
is fully alive on the mesh (it registers, heartbeats, receives calls, and traces into the collector).

All URLs are resolved lazily (a ``topic -> url`` callable reading a shared dict) so the apps can be built
before their peers' ephemeral ports are known; :mod:`deploy.mesh.stack` fills the dict once every server
is bound.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from benzene.core import (
    BenzeneMessageApplication,
    HealthCheckResult,
    HealthChecks,
    MessageSender,
    MiddlewarePipeline,
    Registry,
    ServiceSpec,
)
from benzene.http import BenzeneHttpApp, HttpRouter, InvokeMessageSender, StandardPaths
from benzene.mesh import (
    Heartbeat,
    MeshFeedSender,
    QueueTraceExporter,
    ServiceDescriptor,
    ServiceInfo,
    TracePropagatingMessageSender,
    trace_middleware,
)
from mesh_fleet.domain import (
    PAYMENT_CAPTURE_TOPIC,
    SHIPPING_BOOK_TOPIC,
    BookShipment,
    CapturePayment,
    CreateOrder,
    Order,
    OrderStore,
    make_book_shipment,
    make_capture_payment,
    make_create_order,
    make_get_all_orders,
)

ORDERS_CREATE_TOPIC = "orders:create"
ORDERS_GET_ALL_TOPIC = "orders:get-all"


@dataclass
class DomainService:
    """One built domain service: its ASGI app, its identity, and the feed machinery the stack drives."""

    name: str
    instance_id: str
    app: BenzeneHttpApp
    registry: Registry
    descriptor: ServiceDescriptor
    exporter: QueueTraceExporter
    feeds: MeshFeedSender
    is_healthy: bool
    health_checks: dict = field(default_factory=dict)

    async def announce(self, sent_at: str) -> None:
        """Register + heartbeat this service into the host (the two startup feeds), over HTTP."""
        await self.feeds.register(self.descriptor)
        await self.feeds.publish_heartbeat(
            Heartbeat(
                service=self.name,
                sent_at=sent_at,
                instance_id=self.instance_id,
                descriptor_hash=self.descriptor.descriptor_hash(),
                is_healthy=self.is_healthy,
                health_checks=self.health_checks,
            )
        )

    async def flush_traces(self) -> None:
        """Drain the trace buffer and push it to the host over HTTP (the ongoing telemetry feed)."""
        events = self.exporter.drain()
        if events:
            await self.feeds.publish_traces(events)


def _invoke_sender(url_for: Callable[[str], str]) -> MessageSender:
    return TracePropagatingMessageSender(InvokeMessageSender(url_for))


def _feed_sender(host_invoke_url: Callable[[], str]) -> MeshFeedSender:
    return MeshFeedSender(InvokeMessageSender(lambda _topic: host_invoke_url()))


def _health(*checks: tuple[str, HealthCheckResult]) -> HealthChecks:
    registry = HealthChecks()
    for name, result in checks:
        registry.add(name, (lambda r: (lambda: r))(result))
    return registry


def build_orders(peer_url_for: Callable[[str], str], host_invoke_url: Callable[[], str]) -> DomainService:
    exporter = QueueTraceExporter()
    store = OrderStore()
    sender = _invoke_sender(peer_url_for)
    router = HttpRouter()
    router.register(
        "POST", "/orders", ORDERS_CREATE_TOPIC, make_create_order(store, sender),
        request_type=CreateOrder, response_type=Order,
    )
    router.register(
        "GET", "/orders", ORDERS_GET_ALL_TOPIC, make_get_all_orders(store), response_type=list[Order]
    )
    registry = Registry.from_definitions(router)
    health = _health(
        ("PostgresDatabase", HealthCheckResult.healthy("latency 4ms")),
        ("RedisCache", HealthCheckResult.healthy("hit rate 0.97")),
    )
    app = _http_app(router, registry, "orders", "orders-1", exporter, health)
    return DomainService(
        name="orders", instance_id="orders-1", app=app, registry=registry,
        descriptor=ServiceDescriptor.derive(registry, ServiceInfo("orders", instance_id="orders-1")),
        exporter=exporter, feeds=_feed_sender(host_invoke_url), is_healthy=True,
        health_checks={"PostgresDatabase": {"isHealthy": True}, "RedisCache": {"isHealthy": True}},
    )


def build_payments(peer_url_for: Callable[[str], str], host_invoke_url: Callable[[], str]) -> DomainService:
    exporter = QueueTraceExporter()
    sender = _invoke_sender(peer_url_for)
    router = HttpRouter()
    router.register(
        "POST", "/payments/capture", PAYMENT_CAPTURE_TOPIC, make_capture_payment(sender),
        request_type=CapturePayment,
    )
    registry = Registry.from_definitions(router)
    # payments is unhealthy: its gateway check fails, so /benzene/health reports isHealthy=false (→ 503).
    health = _health(
        ("PaymentsGateway", HealthCheckResult.unhealthy("gateway timeout")),
        ("PostgresDatabase", HealthCheckResult.healthy("latency 6ms")),
    )
    app = _http_app(router, registry, "payments", "payments-1", exporter, health)
    return DomainService(
        name="payments", instance_id="payments-1", app=app, registry=registry,
        descriptor=ServiceDescriptor.derive(registry, ServiceInfo("payments", instance_id="payments-1")),
        exporter=exporter, feeds=_feed_sender(host_invoke_url), is_healthy=False,
        health_checks={"PaymentsGateway": {"isHealthy": False}, "PostgresDatabase": {"isHealthy": True}},
    )


def build_shipping(host_invoke_url: Callable[[], str]) -> DomainService:
    exporter = QueueTraceExporter()
    router = HttpRouter()
    router.register(
        "POST", "/shipping/book", SHIPPING_BOOK_TOPIC, make_book_shipment(), request_type=BookShipment
    )
    registry = Registry.from_definitions(router)
    # shipping is UNREACHABLE to the aggregator: it exposes /benzene/invoke (so it can receive calls and
    # feeds still flow) but deliberately not /benzene/spec or /benzene/health — those 404, so the
    # aggregator can't describe it. It stays alive on the mesh purely through its own HTTP feeds.
    pipeline = MiddlewarePipeline([trace_middleware(exporter=exporter, service="shipping", instance_id="shipping-1")])
    application = BenzeneMessageApplication(registry, pipeline)
    app = BenzeneHttpApp(router, application=application, standard_paths=StandardPaths(invoke=True))
    return DomainService(
        name="shipping", instance_id="shipping-1", app=app, registry=registry,
        descriptor=ServiceDescriptor.derive(registry, ServiceInfo("shipping", instance_id="shipping-1")),
        exporter=exporter, feeds=_feed_sender(host_invoke_url), is_healthy=True,
        health_checks={"ParcelApi": {"isHealthy": True}},
    )


def _http_app(
    router: HttpRouter,
    registry: Registry,
    service: str,
    instance_id: str,
    exporter: QueueTraceExporter,
    health: HealthChecks,
) -> BenzeneHttpApp:
    """A BenzeneHttpApp that traces every invocation and exposes /benzene/invoke|health|spec."""
    pipeline = MiddlewarePipeline([trace_middleware(exporter=exporter, service=service, instance_id=instance_id)])
    application = BenzeneMessageApplication(registry, pipeline)
    spec = ServiceSpec.derive(registry, service=service)
    standard = StandardPaths(invoke=True, health=health, spec=spec)
    return BenzeneHttpApp(router, application=application, standard_paths=standard)
