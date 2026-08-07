"""Build the three-service mesh, drive traffic through it, and return the mesh-UI inputs.

:func:`build_fleet` assembles ``orders`` / ``payments`` / ``shipping`` as in-process
:class:`~benzene.core.BenzeneMessageApplication`s (each wrapped in :func:`~benzene.mesh.trace_middleware`),
wires an outbound sender that dispatches to a sibling app while **forwarding the current mesh span**
(:class:`~benzene.mesh.TracePropagatingMessageSender`), registers + heartbeats each service into a shared
:class:`~benzene.mesh.MeshCollector`, generates a deterministic burst of traffic, and drains the traces
into the collector. It hands back a :class:`FleetResult`: the catalog spine (one
:class:`~benzene.mesh.ServiceCatalog` per service) and the live collector — exactly the two inputs
:class:`~benzene.mesh.MeshArtifactEmitter` needs.

The three services deliberately demonstrate the three manifest states (mirroring the .NET example):
``orders`` healthy, ``payments`` unhealthy **and** contract-drifted, ``shipping`` unreachable on its
spec/health endpoints — yet ``shipping`` is still alive in the collector (it registered, heartbeats, and
handles traffic), which is why its topic still appears with consumers/producers and observed usage. That
split — reachable-catalog vs live-collector — is the whole point of the two data planes.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone

from benzene.core import (
    BenzeneMessageApplication,
    HealthCheckResult,
    HealthChecks,
    MessageSender,
    MiddlewarePipeline,
    Registry,
    ServiceSpec,
    to_jsonable,
)
from benzene.http import HttpRouter
from benzene.mesh import (
    Heartbeat,
    HttpMapping,
    InMemoryTraceExporter,
    MeshCollector,
    ServiceCatalog,
    ServiceDescriptor,
    ServiceInfo,
    TracePropagatingMessageSender,
    spec_hash,
    trace_middleware,
)
from benzene.results import Result, is_successful

from .domain import (
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


@dataclass(frozen=True)
class FleetResult:
    """Everything the emitter needs: the catalog spine, the live collector, and the timing stamps."""

    services: list[ServiceCatalog]
    collector: MeshCollector
    generated_at: datetime
    window_start: datetime
    window_end: datetime


class _InProcessSender:
    """An outbound :class:`~benzene.core.MessageSender` that dispatches a topic to a sibling in-process app.

    Serializes the message to a wire envelope, invokes the target application, and maps its response
    envelope back to a :class:`~benzene.results.Result`. Wrapped in
    :class:`~benzene.mesh.TracePropagatingMessageSender` so the caller's ``traceparent`` rides along and
    the downstream service joins the same trace.
    """

    def __init__(self, routes: dict[str, BenzeneMessageApplication]) -> None:
        self._routes = routes

    async def send_message(
        self, topic: str, message: object, headers: dict[str, str] | None = None
    ) -> Result:
        target = self._routes.get(topic)
        if target is None:  # a topic nobody hosts — fire-and-report, never raise
            return Result.not_found(f"No in-process host for {topic!r}")
        envelope = {
            "topic": topic,
            "headers": headers or {},
            "body": json.dumps(to_jsonable(message)),
        }
        response = await target.handle(envelope)
        status = response.get("statusCode", "unexpected-error")
        body = response.get("body") or "null"
        payload = json.loads(body)
        return Result.ok(payload) if is_successful(status) else Result.failure(status, "")


def _app(
    registry: Registry, service: str, instance_id: str, exporter: InMemoryTraceExporter
) -> BenzeneMessageApplication:
    """One service as a message app whose outermost middleware traces every invocation."""
    pipeline = MiddlewarePipeline([trace_middleware(exporter=exporter, service=service, instance_id=instance_id)])
    return BenzeneMessageApplication(registry, pipeline)


def _orders_wiring(sender: MessageSender) -> tuple[Registry, HttpRouter, OrderStore]:
    store = OrderStore()
    router = HttpRouter()
    router.register(
        "POST", "/orders", ORDERS_CREATE_TOPIC, make_create_order(store, sender),
        request_type=CreateOrder, response_type=Order,
    )
    router.register(
        "GET", "/orders", ORDERS_GET_ALL_TOPIC, make_get_all_orders(store),
        response_type=list[Order],
    )
    return Registry.from_definitions(router), router, store


def _payments_wiring(sender: MessageSender) -> tuple[Registry, HttpRouter]:
    router = HttpRouter()
    router.register(
        "POST", "/payments/capture", PAYMENT_CAPTURE_TOPIC, make_capture_payment(sender),
        request_type=CapturePayment,
    )
    return Registry.from_definitions(router), router


def _shipping_wiring() -> tuple[Registry, HttpRouter]:
    router = HttpRouter()
    router.register(
        "POST", "/shipping/book", SHIPPING_BOOK_TOPIC, make_book_shipment(),
        request_type=BookShipment,
    )
    return Registry.from_definitions(router), router


def _http_mappings(router: HttpRouter) -> dict[str, list[HttpMapping]]:
    """Project a router's endpoints into the emitter's ``topic id → [HttpMapping]`` map."""
    mappings: dict[str, list[HttpMapping]] = {}
    for endpoint in router.endpoints():
        mappings.setdefault(endpoint.topic, []).append(HttpMapping(endpoint.method, endpoint.path))
    return mappings


def _health(*checks: tuple[str, HealthCheckResult]) -> HealthChecks:
    registry = HealthChecks()
    for name, result in checks:
        registry.add(name, (lambda r: (lambda: r))(result))
    return registry


async def _register_and_beat(
    collector: MeshCollector, descriptor: ServiceDescriptor, health_payload: dict, sent_at: str
) -> None:
    collector.ingest_register(descriptor.to_payload())
    collector.ingest_heartbeat(
        Heartbeat(
            service=descriptor.info.service,
            sent_at=sent_at,
            instance_id=descriptor.info.instance_id,
            descriptor_hash=descriptor.descriptor_hash(),
            is_healthy=bool(health_payload.get("isHealthy", True)),
            health_checks=health_payload.get("healthChecks", {}),
        ).to_payload()
    )


async def _drive_traffic(
    orders: BenzeneMessageApplication, creates: int, lists: int
) -> None:
    """A deterministic burst: ``creates`` orders (each fans out to payments + shipping) and ``lists`` reads."""
    for i in range(creates):
        body = json.dumps({"customerEmail": f"c{i}@example.com", "sku": "ABC-0001", "quantity": (i % 3) + 1})
        await orders.handle({"topic": ORDERS_CREATE_TOPIC, "headers": {}, "body": body})
    for _ in range(lists):
        await orders.handle({"topic": ORDERS_GET_ALL_TOPIC, "headers": {}, "body": ""})


async def build_fleet_async(*, generated_at: datetime | None = None) -> FleetResult:
    """Assemble the fleet, run traffic, and return the emitter inputs (async entry point)."""
    exporter = InMemoryTraceExporter()  # all three services export here; drained into the collector once
    now = generated_at or datetime.now(timezone.utc)
    sent_at = now.isoformat().replace("+00:00", "Z")

    # Build the three apps. `orders` and `payments` send outbound; the sender forwards their trace span.
    shipping_registry, shipping_router = _shipping_wiring()
    shipping_app = _app(shipping_registry, "shipping", "shipping-1", exporter)

    payments_sender = TracePropagatingMessageSender(_InProcessSender({SHIPPING_BOOK_TOPIC: shipping_app}))
    payments_registry, payments_router = _payments_wiring(payments_sender)
    payments_app = _app(payments_registry, "payments", "payments-1", exporter)

    orders_sender = TracePropagatingMessageSender(
        _InProcessSender({PAYMENT_CAPTURE_TOPIC: payments_app, SHIPPING_BOOK_TOPIC: shipping_app})
    )
    orders_registry, orders_router, _store = _orders_wiring(orders_sender)
    orders_app = _app(orders_registry, "orders", "orders-1", exporter)

    # Derived specs + health (the catalog spine the aggregator would pull from /benzene/spec + /health).
    orders_spec = ServiceSpec.derive(orders_registry, service="orders").to_payload()
    payments_spec = ServiceSpec.derive(payments_registry, service="payments").to_payload()
    # shipping's spec is deliberately not derived here: it is unreachable to the aggregator this run.

    orders_health = (await _health(
        ("PostgresDatabase", HealthCheckResult.healthy("latency 4ms")),
        ("RedisCache", HealthCheckResult.healthy("hit rate 0.97")),
    ).run()).to_payload()
    payments_health = (await _health(
        ("PaymentsGateway", HealthCheckResult.unhealthy("gateway timeout")),
        ("PostgresDatabase", HealthCheckResult.healthy("latency 6ms")),
    ).run()).to_payload()
    shipping_health = (await _health(("ParcelApi", HealthCheckResult.healthy())).run()).to_payload()

    # Register + heartbeat every service into the collector (shipping too — it is alive on the mesh
    # even though its spec/health endpoints are, for this run, unreachable to the aggregator).
    collector = MeshCollector()
    await _register_and_beat(
        collector, ServiceDescriptor.derive(orders_registry, ServiceInfo("orders", instance_id="orders-1")),
        orders_health, sent_at)
    await _register_and_beat(
        collector, ServiceDescriptor.derive(payments_registry, ServiceInfo("payments", instance_id="payments-1")),
        payments_health, sent_at)
    await _register_and_beat(
        collector, ServiceDescriptor.derive(shipping_registry, ServiceInfo("shipping", instance_id="shipping-1")),
        shipping_health, sent_at)

    # Traffic → traces → collector.
    await _drive_traffic(orders_app, creates=12, lists=5)
    collector.ingest_traces({"events": [event.to_payload() for event in exporter]})

    # The catalog spine. payments is drifted (its spec hash differs from the previous run's); shipping
    # is unreachable on its spec/health endpoints (spec/health None, error set).
    payments_previous_hash = spec_hash(
        json.dumps({"service": "payments", "topics": []}, separators=(",", ":"), sort_keys=True)
    )
    services = [
        ServiceCatalog(
            name="orders", spec=orders_spec, health=orders_health,
            spec_url="http://localhost:8081/benzene/spec", health_url="http://localhost:8081/benzene/health",
            http_mappings=_http_mappings(orders_router),
        ),
        ServiceCatalog(
            name="payments", spec=payments_spec, health=payments_health,
            spec_url="http://localhost:8082/benzene/spec", health_url="http://localhost:8082/benzene/health",
            previous_spec_hash=payments_previous_hash, http_mappings=_http_mappings(payments_router),
        ),
        ServiceCatalog(
            name="shipping", spec=None, health=None,
            spec_url="http://localhost:8083/benzene/spec", health_url="http://localhost:8083/benzene/health",
            error="ConnectionError: connection refused (shipping:8083)",
            http_mappings=_http_mappings(shipping_router),
        ),
    ]

    window_start = now.replace(microsecond=0)
    return FleetResult(
        services=services, collector=collector, generated_at=now,
        window_start=window_start, window_end=now,
    )


def build_fleet(*, generated_at: datetime | None = None) -> FleetResult:
    """Synchronous wrapper around :func:`build_fleet_async` for scripts and tests."""
    return asyncio.run(build_fleet_async(generated_at=generated_at))
